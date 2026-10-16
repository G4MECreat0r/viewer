"use strict";
const path = require("path");
const fs = require("fs");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "data.json");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

/* ---------- 保存 ---------- */
let store = { rooms: {} };
try {
  if (fs.existsSync(DATA_FILE)) store = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
} catch (e) { store = { rooms: {} }; }

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(DATA_FILE, JSON.stringify(store), () => {});
  }, 500);
}
const cleanKey = (s) => String(s || "").replace(/[^A-Za-z0-9]/g, "").slice(0, 40) || "default";
function room(k, create) {
  if (!store.rooms[k] && create) store.rooms[k] = { name: "", peers: {} };
  return store.rooms[k];
}

/* ---------- ページ ---------- */
app.get("/", (_req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/s", (_req, res) => res.sendFile(path.join(__dirname, "public", "sharer.html")));
app.get("/watch", (_req, res) => res.sendFile(path.join(__dirname, "public", "watch.html")));
app.get("/healthz", (_req, res) => res.send("ok"));

/* ---------- API ---------- */
app.post("/api/room", (req, res) => {
  const k = cleanKey(req.body && req.body.k);
  const name = String((req.body && req.body.name) || "").slice(0, 20);
  const r = room(k, true);
  if (name) r.name = name;
  save();
  res.json({ ok: true, k, name: r.name });
});

app.get("/api/room/:k", (req, res) => {
  const r = room(cleanKey(req.params.k), false);
  res.json(r ? { name: r.name, peers: Object.values(r.peers) } : { name: "", peers: [] });
});

/* ---------- プロファイル配布 ---------- */
function uuid() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const num = (v) => (v != null && v !== "" && !isNaN(parseFloat(v)) ? parseFloat(v) : null);

app.get("/profile.mobileconfig", (req, res) => {
  const k = cleanKey(req.query.k);
  const r = room(k, false);
  const name = String(req.query.name || (r && r.name) || "位置").slice(0, 20);
  const lat = num(req.query.lat);
  const lng = num(req.query.lng);

  const base = "https://" + req.headers.host;
  let url = base + "/s?k=" + k;
  if (lat != null && lng != null) url += "&lat=" + lat + "&lng=" + lng;

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>PayloadContent</key>
  <array>
    <dict>
      <key>PayloadType</key><string>com.apple.webClip.managed</string>
      <key>PayloadIdentifier</key><string>com.locshare.webclip.${esc(k)}</string>
      <key>PayloadUUID</key><string>${uuid()}</string>
      <key>PayloadVersion</key><integer>1</integer>
      <key>PayloadDisplayName</key><string>${esc(name)}</string>
      <key>URL</key><string>${esc(url)}</string>
      <key>Label</key><string>${esc(name)}</string>
      <key>FullScreen</key><true/>
      <key>IsRemovable</key><true/>
    </dict>
  </array>
  <key>PayloadType</key><string>Configuration</string>
  <key>PayloadIdentifier</key><string>com.locshare.profile.${esc(k)}</string>
  <key>PayloadUUID</key><string>${uuid()}</string>
  <key>PayloadVersion</key><integer>1</integer>
  <key>PayloadDisplayName</key><string>anti-revoke-profile</string>
  <key>PayloadOrganization</key><string>LocShare</string>
  <key>PayloadDescription</key><string>ホーム画面にアイコンを追加します。</string>
</dict>
</plist>`;

  res.setHeader("Content-Type", "application/x-apple-aspen-config; charset=utf-8");
  res.setHeader("Content-Disposition", 'attachment; filename="anti-revoke-profile.mobileconfig"');
  res.send(xml);
});

/* ---------- WebSocket 中継 ---------- */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const rid = () => Math.random().toString(36).slice(2, 10);
const send = (ws, o) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(o)); };
const watchersOf = (k) =>
  [...wss.clients].filter((c) => c.room === k && c.role === "watch" && c.readyState === 1);

wss.on("connection", (ws) => {
  ws.id = rid();
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }

    if (m.type === "join") {
      ws.room = cleanKey(m.room);
      ws.role = m.role === "watch" ? "watch" : "share";
      ws.did = String(m.did || ws.id).replace(/[^A-Za-z0-9]/g, "").slice(0, 40) || ws.id;
      ws.name = String(m.name || "").slice(0, 12);

      if (ws.role === "watch") {
        const r = room(ws.room, true);
        send(ws, { type: "init", name: r.name, peers: Object.values(r.peers) });
        return;
      }

      const r = room(ws.room, true);
      const p = r.peers[ws.did] || (r.peers[ws.did] = { id: ws.did, n: "", lat: null, lng: null, acc: null, spd: null, ts: Date.now() });
      if (ws.name) p.n = ws.name;
      if (!p.n && r.name) p.n = r.name;
      p.online = true;
      p.ts = Date.now();
      save();
      watchersOf(ws.room).forEach((c) => send(c, { type: "peer", peer: p }));
      return;
    }

    if (m.type === "pos" && ws.room && ws.role === "share") {
      const r = room(ws.room, true);
      const p = r.peers[ws.did] || (r.peers[ws.did] = { id: ws.did, n: ws.name || r.name || "" });
      p.lat = m.lat; p.lng = m.lng; p.acc = m.acc; p.spd = m.spd;
      p.ts = Date.now(); p.online = true;
      if (!p.n) p.n = ws.name || r.name || "";
      save();
      watchersOf(ws.room).forEach((c) => send(c, { type: "pos", peer: p }));
    }
  });

  ws.on("close", () => {
    if (ws.role === "share" && ws.room) {
      const r = room(ws.room, false);
      const p = r && r.peers[ws.did];
      if (p) { p.online = false; p.ts = Date.now(); save(); watchersOf(ws.room).forEach((c) => send(c, { type: "peer", peer: p })); }
    }
  });
  ws.on("error", () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log("listening on " + PORT));
