import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

const PORT = Number(process.env.PORT || 8080);

const {
  INGRESS_SHARED_SECRET,
  OPENAI_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
} = process.env;

const MODEL = "gpt-4o-realtime-preview-2024-12-17";
const AUDIO_FORMAT = "g711_ulaw";

// --------- Basic config sanity logs (no secrets printed) ----------
console.log("Booting...", {
  port: PORT,
  hasIngressSecret: !!INGRESS_SHARED_SECRET,
  hasOpenAIKey: !!OPENAI_API_KEY,
  hasSupabaseUrl: !!SUPABASE_URL,
  hasSupabaseServiceKey: !!SUPABASE_SERVICE_ROLE_KEY
});

const server = http.createServer((req, res) => {
  // Health check
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  // Default
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("OK");
});

const wss = new WebSocketServer({ noServer: true });

// Upgrade HTTP -> WebSocket only on /ws
server.on("upgrade", (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }

    // Auth via query param (?secret=...)
    const secret = url.searchParams.get("secret");
    if (!secret || secret !== INGRESS_SHARED_SECRET) {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, url);
    });
  } catch (e) {
    socket.destroy();
  }
});

wss.on("connection", async (clientWs, url) => {
  const callId = url.searchParams.get("call_id") || crypto.randomUUID();
  console.log("✅ Client connected:", { callId });

  // -----------------------------
  // 1) Create call row in Supabase
  // -----------------------------
  try {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/calls`, {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal"
      },
      body: JSON.stringify([{ id: callId, source: "yeastar" }])
    });

    if (!res.ok) {
      const txt = await res.text();
      console.log("⚠️ Supabase calls insert failed:", res.status, txt);
    }
  } catch (e) {
    console.log("⚠️ Supabase calls insert error:", String(e?.message || e));
  }

  // -----------------------------
  // 2) Connect to OpenAI Realtime
  // -----------------------------
  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    }
  );

  let openaiReady = false;
  let sessionConfigured = false;

  // Queue audio until OpenAI is ready (prevents lost audio)
  const audioQueue = [];
  let commitTimer = null;

  const flushAudio = () => {
    if (!openaiReady || !sessionConfigured) return;
    if (audioQueue.length === 0) return;

    // Send all queued chunks
    while (audioQueue.length) {
      const base64 = audioQueue.shift();
      openaiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: base64
        })
      );
    }

    // Commit and request transcription update (batched)
    openaiWs.send(JSON.stringify({ type: "input_audio_buffer.commit" }));
    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: { modalities: ["text"] }
      })
    );
  };

  openaiWs.on("open", () => {
    openaiReady = true;
    console.log("✅ OpenAI connected:", { callId });

    // Configure transcription session
    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["text"],
          input_audio_format: AUDIO_FORMAT,
          input_audio_transcription: { model: "gpt-4o-mini-transcribe" }
        }
      })
    );

    sessionConfigured = true;

    // If any audio arrived early, flush it
    flushAudio();
  });

  openaiWs.on("message", async (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      return;
    }

    // Try common text fields
    const text =
      data.delta ||
      data.text ||
      data.transcript ||
      data.response?.output_text ||
      data.response?.output?.[0]?.content?.[0]?.text ||
      data.output_text;

    if (!text) return;

    // Save transcript segment
    try {
      const res = await fetch(`${SUPABASE_URL}/rest/v1/transcript_segments`, {
        method: "POST",
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Content-Type": "application/json",
          Prefer: "return=minimal"
        },
        body: JSON.stringify([
          {
            call_id: callId,
            text: String(text),
            is_final: false,
            raw: data
          }
        ])
      });

      if (!res.ok) {
        const txt = await res.text();
        console.log("⚠️ Supabase transcript insert failed:", res.status, txt);
      }
    } catch (e) {
      console.log("⚠️ Supabase transcript insert error:", String(e?.message || e));
    }
  });

  openaiWs.on("close", () => {
    console.log("ℹ️ OpenAI closed:", { callId });
    try {
      clientWs.close();
    } catch {}
  });

  openaiWs.on("error", (e) => {
    console.log("❌ OpenAI WS error:", e?.message || e);
  });

  // -----------------------------
  // 3) Yeastar -> Relay -> OpenAI
  // -----------------------------
  clientWs.on("message", (data) => {
    try {
      // Yeastar may send:
      // - binary audio (Buffer)
      // - text base64
      // - text JSON like {"audio":"..."}
      let base64Audio = null;

      if (typeof data === "string") {
        // maybe JSON
        try {
          const obj = JSON.parse(data);
          base64Audio = obj.audio || null;
        } catch {
          // else assume already base64
          base64Audio = data;
        }
      } else if (Buffer.isBuffer(data)) {
        base64Audio = data.toString("base64");
      } else {
        // fallback
        base64Audio = Buffer.from(data).toString("base64");
      }

      if (!base64Audio) return;

      // Queue audio
      audioQueue.push(base64Audio);

      // Batch commits every 250ms (instead of per chunk)
      if (!commitTimer) {
        commitTimer = setTimeout(() => {
          commitTimer = null;
          flushAudio();
        }, 250);
      }
    } catch (e) {
      console.log("❌ Client WS message handling error:", String(e?.message || e));
    }
  });

  clientWs.on("close", () => {
    console.log("ℹ️ Client WS closed:", { callId });
    try {
      openaiWs.close();
    } catch {}
  });

  clientWs.on("error", (e) => {
    console.log("❌ Client WS error:", e?.message || e);
    try {
      openaiWs.close();
    } catch {}
  });
});

// ✅ CRITICAL: Railway requires listening on process.env.PORT
server.listen(PORT, () => {
  console.log(`✅ Listening on ${PORT}`);
});
