import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

const PORT = process.env.PORT || 3000;

const {
  INGRESS_SHARED_SECRET,
  OPENAI_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
} = process.env;

const MODEL = "gpt-4o-realtime-preview-2024-12-17";
const AUDIO_FORMAT = "g711_ulaw";

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }
  res.writeHead(200);
  res.end("OK");
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  const secret = url.searchParams.get("secret");
  if (secret !== INGRESS_SHARED_SECRET) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit("connection", ws, url);
  });
});

wss.on("connection", async (clientWs, url) => {
  const callId = url.searchParams.get("call_id") || crypto.randomUUID();
  console.log("Client connected:", callId);

  // Create call row
  await fetch(`${SUPABASE_URL}/rest/v1/calls`, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal"
    },
    body: JSON.stringify([{ id: callId, source: "yeastar" }])
  });

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

  openaiWs.on("open", () => {
    console.log("OpenAI connected");

    openaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        modalities: ["text"],
        input_audio_format: AUDIO_FORMAT,
        input_audio_transcription: {
          model: "gpt-4o-mini-transcribe"
        }
      }
    }));
  });

  openaiWs.on("message", async msg => {
    let data;
    try { data = JSON.parse(msg); } catch { return; }

    const text =
      data.delta ||
      data.text ||
      data.transcript ||
      data.response?.output_text;

    if (text) {
      await fetch(`${SUPABASE_URL}/rest/v1/transcript_segments`, {
        method: "POST",
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Content-Type": "application/json",
          Prefer: "return=minimal"
        },
        body: JSON.stringify([{
          call_id: callId,
          text: String(text),
          is_final: false
        }])
      });
    }
  });

  // Yeastar -> relay -> OpenAI
  clientWs.on("message", buf => {
    const base64 = Buffer.isBuffer(buf)
      ? buf.toString("base64")
      : Buffer.from(buf).toString("base64");

    openaiWs.send(JSON.stringify({
      type: "input_audio_buffer.append",
      audio: base64
    }));

    openaiWs.send(JSON.stringify({ type: "input_audio_buffer.commit" }));
    openaiWs.send(JSON.stringify({
      type: "response.create",
      response: { modalities: ["text"] }
    }));
  });

  clientWs.on("close", () => {
    try { openaiWs.close(); } catch {}
  });

  openaiWs.on("close", () => {
    try { clientWs.close(); } catch {}
  });

  openaiWs.on("error", (e) => console.log("OpenAI WS error:", e?.message || e));
  clientWs.on("error", (e) => console.log("Client WS error:", e?.message || e));
});

server.listen(PORT, () => console.log("Listening on", PORT));
