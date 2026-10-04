// PH102 quiz server: serves the page and relays each player's live state to everyone else.
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 3000, GRACE_MS = 10000, MAX_BYTES = 4096, MAX_PEERS = 200;
const page = path.join(__dirname, "index.html");

const server = http.createServer((req, res) => {
  const url = req.url.split("?")[0];
  if (url === "/healthz") { res.writeHead(200); return res.end("ok"); }
  if (url === "/" || url === "/index.html") {
    return fs.readFile(page, (err, buf) => {
      if (err) { res.writeHead(500); return res.end("Missing page"); }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
      res.end(buf);
    });
  }
  res.writeHead(404); res.end("Not found");
});

const peers = new Map(); // id -> { presence, ws, timer }
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 8192 });

function broadcast() {
  const list = [];
  for (const [id, p] of peers) if (Object.keys(p.presence).length) list.push({ peer: id, presence: p.presence });
  const msg = JSON.stringify({ t: "peers", peers: list });
  for (const p of peers.values()) if (p.ws && p.ws.readyState === 1) p.ws.send(msg);
}

wss.on("connection", (ws, req) => {
  let id = new URL(req.url, "http://x").searchParams.get("cid") || "";
  if (!/^[a-z0-9]{8,32}$/.test(id)) id = crypto.randomBytes(8).toString("hex");
  let p = peers.get(id);
  if (p) { clearTimeout(p.timer); if (p.ws && p.ws !== ws) p.ws.terminate(); }
  else {
    if (peers.size >= MAX_PEERS) return ws.close();
    p = { presence: {}, ws: null, timer: null }; peers.set(id, p);
  }
  p.ws = ws; ws.alive = true;
  ws.send(JSON.stringify({ t: "hello", id }));
  broadcast();
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || m.t !== "p" || typeof m.patch !== "object" || !m.patch || Array.isArray(m.patch)) return;
    const next = { ...p.presence };
    for (const k of Object.keys(m.patch)) { if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k]; }
    if (Buffer.byteLength(JSON.stringify(next)) > MAX_BYTES) return;
    p.presence = next; broadcast();
  });
  ws.on("close", () => {
    if (p.ws !== ws) return;
    p.ws = null;
    p.timer = setTimeout(() => { peers.delete(id); broadcast(); }, GRACE_MS);
  });
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); }
}, 25000);

server.listen(PORT, () => console.log("PH102 quiz listening on " + PORT));
