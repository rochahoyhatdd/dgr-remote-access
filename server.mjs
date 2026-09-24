import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";

const port = Number(process.env.PORT || 8080);
const workerSecret = String(process.env.RELAY_WORKER_SECRET || "");
const ingressToken = String(process.env.RELAY_INGRESS_TOKEN || "");
const maxBodyBytes = 12 * 1024 * 1024;
const requestTimeoutMs = 180_000;

if (!workerSecret || !ingressToken) {
  throw new Error("RELAY_WORKER_SECRET and RELAY_INGRESS_TOKEN are required.");
}

let worker = null;
const pending = new Map();

function json(res, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": body.length,
    "cache-control": "no-store",
  });
  res.end(body);
}
function safeHeaders(headers) {
  const out = {};
  for (const [key, value] of Object.entries(headers)) {
    const lower = key.toLowerCase();
    if (!value) continue;
    if (["host", "connection", "content-length", "transfer-encoding", "accept-encoding"].includes(lower)) continue;
    out[lower] = Array.isArray(value) ? value.join(", ") : String(value);
  }
  return out;
}

async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxBodyBytes) throw new Error("body_too_large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function stripRelayPrefix(rawUrl) {
  const url = new URL(rawUrl, "http://relay.local");
  const prefix = `/v1/${ingressToken}`;
  if (!url.pathname.startsWith(prefix)) return null;
  const path = url.pathname.slice(prefix.length) || "/";
  return path + url.search;
}
const server = http.createServer(async (req, res) => {
  if (req.url === "/health") {
    return json(res, 200, { ok: true, workerConnected: worker?.readyState === 1 });
  }

  const targetUrl = stripRelayPrefix(req.url || "/");
  if (!targetUrl) return json(res, 404, { error: "not_found" });
  if (!worker || worker.readyState !== 1) {
    return json(res, 503, { error: "radar_worker_offline" });
  }

  let body;
  try {
    body = await readBody(req);
  } catch (error) {
    return json(res, error?.message === "body_too_large" ? 413 : 400, {
      error: error?.message || "bad_request",
    });
  }

  const id = crypto.randomUUID();
  const timer = setTimeout(() => {
    const item = pending.get(id);
    if (!item) return;
    pending.delete(id);
    if (!item.res.headersSent) json(item.res, 504, { error: "radar_worker_timeout" });
  }, requestTimeoutMs);

  pending.set(id, { res, timer });
  worker.send(JSON.stringify({
    type: "request",
    id,
    method: req.method || "GET",
    url: targetUrl,
    headers: safeHeaders(req.headers),
    bodyBase64: body.length ? body.toString("base64") : "",
  }));
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url || "/", "http://relay.local");
  if (url.pathname !== "/worker" || url.searchParams.get("secret") !== workerSecret) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws));
});

wss.on("connection", (ws) => {
  if (worker && worker.readyState === 1) worker.close(4001, "replaced");
  worker = ws;

  ws.on("message", (raw) => {
    let message;
    try { message = JSON.parse(String(raw)); } catch { return; }
    if (message.type !== "response" || !message.id) return;
    const item = pending.get(message.id);
    if (!item) return;
    pending.delete(message.id);
    clearTimeout(item.timer);
    const headers = message.headers || {};
    for (const [key, value] of Object.entries(headers)) {
      const lower = key.toLowerCase();
      if (["connection", "content-length", "transfer-encoding", "content-encoding"].includes(lower)) continue;
      if (lower === "set-cookie" && Array.isArray(value)) {
        item.res.setHeader("set-cookie", value);
      } else if (value != null) {
        item.res.setHeader(key, String(value));
      }
    }

    const body = message.bodyBase64
      ? Buffer.from(message.bodyBase64, "base64")
      : Buffer.alloc(0);
    item.res.statusCode = Number(message.status || 502);
    item.res.setHeader("content-length", body.length);
    item.res.end(body);
  });

  ws.on("close", () => {
    if (worker === ws) worker = null;
  });
});

setInterval(() => {
  if (worker?.readyState === 1) worker.ping();
}, 25_000).unref();

server.listen(port, "0.0.0.0", () => {
  console.log(`relay listening on ${port}`);
});
