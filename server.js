import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

// -----------------------------
// Railway port (CRITICAL)
// -----------------------------
const PORT_RAW = process.env.PORT;
const PORT = Number(PORT_RAW);

if (!PORT || Number.isNaN(PORT)) {
  console.error("❌ Railway did not provide process.env.PORT. Current value:", PORT_RAW);
  process.exit(1);
}

const {
  INGRESS_SHARED_SECRET,
  OPENAI_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
} = process.env;

if (!INGRESS_SHARED_SECRET) console.warn("⚠️ Missing INGRESS_SHARED_SECRET");
if (!OPENAI_API_KEY) console.warn("⚠️ Missing OPENAI_API_KEY");
if (!SUPABASE_URL) console.warn("⚠️ Missing SUPABASE_URL");
if (!SUPABASE_SERVICE_ROLE_KEY) console.warn("⚠️ Missing SUPABASE_SERVICE_ROLE_KEY");

const MODEL = "gpt-4o-realtime-preview-2024-12-17";
const AUDIO_FORMAT = "g711_ulaw";

// -----------------------------
// HTTP server (health + default)
// -----------------------------
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("OK");
});

// -----------------------------
// WS server (upgrade only)
// -----------------------------
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // Only accept WS at /ws
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

// -----------------------------
// Helpers: Supabase inserts
// -----------------------------
async function supabaseInsertCall(callId) {
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
    throw new Error(`Supabase calls insert failed: ${res.status} ${txt}`);
  }
}

async function supabaseInsertSegment(callId, text, raw) {
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
    throw new Error(`Supabase transcript insert failed: ${res.status} ${txt}`);
  }
}

// -----------------------------
// WS connection handler
// -----------------------------
wss.on("connection", async (clientWs, url) => {
  const callId = url.searchParams.get("call_id") || crypto.randomUUID();
  console.log("✅ Client connected:", { callId });

  // 1) Create call record (do not crash the relay if this fails)
  try {
    await supabaseInsertCall(callId);
    console.log("✅ Supabase call row inserted:", { callId });
  } catch (e) {
    console.log("⚠️ Supabase call insert error:", String(e?.message || e));
  }

  // 2) Connect to OpenAI realtime
  const openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${MODEL}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1"
    }
  });

  let openaiReady = false;
  let sessionConfigured = false;

  // Queue audio until OpenAI is ready
  const audioQueue = [];
  let flushTimer = null;

  function flushAudio() {
    if (!openaiReady || !sessionConfigured) return;
    if (audioQueue.length === 0) return;

    // Send queued chunks
    while (audioQueue.length) {
      const base64 = audioQueue.shift();
      openaiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: base64
        })
      );
    }

    // Commit + ask for text (batched)
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
    console.log("✅ OpenAI connected:", { callId });

    // Configure session
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

    // Flush any early audio
    flushAudio();
  });

  openaiWs.on("message", async (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      return;
    }

    // Try multiple likely text locations
    const text =
      data.delta ||
      data.text ||
      data.transcript ||
      data.response?.output_text ||
      data.response?.output?.[0]?.content?.[0]?.text ||
      data.output_text;

    if (!text) return;

    try {
      await supabaseInsertSegment(callId, text, data);
      // console.log("📝 segment saved", { callId, len: String(text).length });
    } catch (e) {
      console.log("⚠️ Supabase transcript insert error:", String(e?.message || e));
    }
  });

  openaiWs.on("close", () => {
    console.log("ℹ️ OpenAI closed:", { callId });
    try { clientWs.close(); } catch {}
  });

  openaiWs.on("error", (e) => {
    console.log("❌ OpenAI WS error:", e?.message || e);
  });

  // 3) Yeastar -> relay -> OpenAI
  clientWs.on("message", (data) => {
    try {
      let base64Audio = null;

      // Text frame (base64 or JSON)
      if (typeof data === "string") {
        try {
          const obj = JSON.parse(data);
          base64Audio = obj.audio || null;
        } catch {
          base64Audio = data; // assume base64 text
        }
      } else if (Buffer.isBuffer(data)) {
        // Binary frame
        base64Audio = data.toString("base64");
      } else {
        // Fallback conversion
        base64Audio = Buffer.from(data).toString("base64");
      }

      if (!base64Audio) return;

      audioQueue.push(base64Audio);

      // Batch flush every 250ms
      if (!flushTimer) {
        flushTimer = setTimeout(() => {
          flushTimer = null;
          flushAudio();
        }, 250);
      }
    } catch (e) {
      console.log("❌ Client WS message handling error:", String(e?.message || e));
    }
  });

  clientWs.on("close", () => {
    console.log("ℹ️ Client WS closed:", { callId });
    try { openaiWs.close(); } catch {}
  });

  clientWs.on("error", (e) => {
    console.log("❌ Client WS error:", e?.message || e);
    try { openaiWs.close(); } catch {}
  });
});

// -----------------------------
// Start server (bind all interfaces)
// -----------------------------
server.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Listening on ${PORT}`);
});
