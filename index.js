import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 4242;

// ── In-memory log store (last 500 entries) ────────────────────────────────────
const LOG_LIMIT = 500;
const logs = [];

function addLog(entry) {
  logs.push(entry);
  if (logs.length > LOG_LIMIT) logs.shift();
}

// ── HTTP server (serves dashboard + REST) ────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/logs") {
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    return res.end(JSON.stringify(logs));
  }
  if (req.method === "DELETE" && req.url === "/logs") {
    logs.length = 0;
    broadcast({ type: "clear" });
    res.writeHead(200, { "Access-Control-Allow-Origin": "*" });
    return res.end("ok");
  }
  if (req.method === "GET" && (req.url === "/" || req.url === "/dashboard")) {
    const html = fs.readFileSync(path.join(__dirname, "../dashboard/index.html"));
    res.writeHead(200, { "Content-Type": "text/html" });
    return res.end(html);
  }
  res.writeHead(404);
  res.end("not found");
});

// ── WebSocket server ─────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server });

// Track app clients vs dashboard clients
const dashboards = new Set();
const appClients = new Set();

function broadcast(msg, skip = null) {
  const data = JSON.stringify(msg);
  for (const ws of dashboards) {
    if (ws !== skip && ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}

wss.on("connection", (ws, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  const isDashboard = req.url === "/dashboard" || req.headers["x-client-type"] === "dashboard";

  if (isDashboard) {
    dashboards.add(ws);
    // Send all buffered logs on connect
    ws.send(JSON.stringify({ type: "history", logs }));
    ws.on("close", () => dashboards.delete(ws));
    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === "clear") { logs.length = 0; broadcast({ type: "clear" }); }
      } catch {}
    });
    return;
  }

  // App client
  appClients.add(ws);
  const connectedAt = new Date().toISOString();
  const connEntry = { type: "system", level: "info", message: `App connected from ${ip}`, ts: connectedAt };
  addLog(connEntry);
  broadcast(connEntry);

  ws.on("message", (raw) => {
    try {
      const entry = JSON.parse(raw);
      entry.ts = entry.ts || new Date().toISOString();
      entry.ip = ip;
      addLog(entry);
      broadcast(entry);
    } catch {}
  });

  ws.on("close", () => {
    appClients.delete(ws);
    const entry = { type: "system", level: "warn", message: `App disconnected (${ip})`, ts: new Date().toISOString() };
    addLog(entry);
    broadcast(entry);
  });

  ws.on("error", () => appClients.delete(ws));
});

server.listen(PORT, () => {
  console.log(`[magpie-debug] server running on port ${PORT}`);
  console.log(`[magpie-debug] dashboard → http://localhost:${PORT}/`);
});
