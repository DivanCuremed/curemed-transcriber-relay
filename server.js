import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

// ============================================================
// 1) RAILWAY PORT (NO FALLBACK - MUST USE process.env.PORT)
// ============================================================
const PORT_RAW = process.env.PORT;
const PORT = Number(PORT_RAW);

if (!PORT || Number.isNaN(PORT)) {
  console.error("❌ PORT not set by Railway (process.env.PORT). Value was:", PORT_RAW);
  console.error("❌ Exiting so Railway shows the real problem.");
  process.exit(1);
}

// ============================================================
// 2) ENV VARS
// ============================================================
const {
  INGRESS_SHARED_SECRET,
  OPENAI_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
} = process.env;

// Print presence only (never print secrets)
console.log("Booting...", {
  port: PORT,
  hasIngressSecret: !!INGRESS_SHARED_SECRET,
  hasOpenAIKey: !!OPENAI_API_KEY,
  hasSupabaseUrl: !!SUPABASE_URL,
  hasSupabaseServiceKey: !!SUPABASE_SERVICE_ROLE_KEY
});

// ============================================================
// 3) OPENAI SETTINGS
// ============================================================
const MODEL = "gpt-4o-realtime-preview-2024-12-17";
const AUDIO_FORMAT = "g711_ulaw";

// ============================================================
// 4) HTTP SERVER (health + default)
// ============================================================
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  res.writeHead(200, { "content-type": "text/plain" });
  res.end("OK");
});

// ============================================================
// 5) WSS SERVER (upgrade only)
// ============================================================
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // Only allow WebSocket on /ws
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }

    // Auth via query param ?secret=...
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

// ============================================================
// 6) SUPABASE HELPERS (never crash the relay)
// ============================================================
async function supabaseInsertCall(callId) {
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
    } else {
      console.log("✅ Supabase call inserted:", callId);
    }
  } catch (e) {
    console.log("⚠️ Supabase calls insert error:", String(e?.message || e));
  }
}

async function supabaseInsertSegment(callId, text, raw) {
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
          raw
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
}

// ============================================================
// 7) MAIN WS FLOW
// ============================================================
wss.on("connection", async (clientWs, url) => {
  const callId = url.searchParams.get("call_id") || crypto.randomUUID();
  console.log("✅ Client connected:", { callId });

  // Create call row (non-fatal if it fails)
  await supabaseInsertCall(callId);

  // OpenAI realtime websocket
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

  // Queue audio until OpenAI is ready
  const audioQueue = [];
  let flushTimer = null;

  function flushAudio() {
    if (!openaiReady || !sessionConfigured) return;
    if (audioQueue.length === 0) return;

    while (audioQueue.length) {
      const base64 = audioQueue.shift();
      openaiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: base64
        })
      );
    }

    openaiWs.send(JSON.stringify({ type: "input_audio_buffer.commit" }));
    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: { modalities: ["text"] }
      })
    );
  }

  openaiWs.on("open", () => {
    openaiReady = true;
    console.log("✅ OpenAI connected", { callId });

    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["text"],
          input_audio_format: AUDIO_FORMAT,
          input_audio_transcription: {
            model: "gpt-4o-mini-transcribe"
          }
        }
      })
    );

    sessionConfigured = true;

    // Flush any audio that arrived early
    flushAudio();
  });

  openaiWs.on("message", async (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      return;
    }

    const text =
      data.delta ||
      data.text ||
      data.transcript ||
      data.response?.output_text ||
      data.response?.output?.[0]?.content?.[0]?.text ||
      data.output_text;

    if (text) {
      await supabaseInsertSegment(callId, text, data);
    }
  });

  // Yeastar -> relay -> OpenAI
  clientWs.on("message", (data) => {
    try {
      let base64Audio = null;

      if (typeof data === "string") {
        // JSON or base64
        try {
          const obj = JSON.parse(data);
          base64Audio = obj.audio || null;
        } catch {
          base64Audio = data;
        }
      } else if (Buffer.isBuffer(data)) {
        base64Audio = data.toString("base64");
      } else {
        base64Audio = Buffer.from(data).toString("base64");
      }

      if (!base64Audio) return;

      audioQueue.push(base64Audio);

      // Batch flush every 250ms (instead of per chunk)
      if (!flushTimer) {
        flushTimer = setTimeout(() => {
          flushTimer = null;
          flushAudio();
        }, 250);
      }
    } catch (e) {
      console.log("❌ Client message handling error:", String(e?.message || e));
    }
  });

  clientWs.on("close", () => {
    console.log("ℹ️ Client WS closed", { callId });
    try { openaiWs.close(); } catch {}
  });

  clientWs.on("error", (e) => {
    console.log("❌ Client WS error:", e?.message || e);
    try { openaiWs.close(); } catch {}
  });

  openaiWs.on("close", () => {
    console.log("ℹ️ OpenAI WS closed", { callId });
    try { clientWs.close(); } catch {}
  });

  openaiWs.on("error", (e) => {
    console.log("❌ OpenAI WS error:", e?.message || e);
  });
});

// ============================================================
// 8) START SERVER (bind all interfaces)
// ============================================================
server.listen(PORT, "0.0.0.0", () => {
  console.log("✅ Listening on", PORT);
});
