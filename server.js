"use strict";
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const app = express();
app.use(express.static(path.join(__dirname, "public")));

/* ---------- ページ ---------- */
app.get("/", (_req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.get("/s", (_req, res) => res.sendFile(path.join(__dirname, "public", "sharer.html")));
app.get("/watch", (_req, res) => res.sendFile(path.join(__dirname, "public", "watch.html")));
app.get("/setup", (_req, res) => res.sendFile(path.join(__dirname, "public", "setup.html")));
app.get("/healthz", (_req, res) => res.send("ok"));

/* ---------- プロファイル配布 ---------- */
function uuid() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}
const esc = (s) =>
  String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

app.get("/profile.mobileconfig", (req, res) => {
  const k = String(req.query.k || "").replace(/[^A-Za-z0-9]/g, "").slice(0, 40) || "default";
  const name = String(req.query.name || "位置").slice(0, 20);
  const base = "https://" + req.headers.host;
  const url = base + "/s?k=" + k;

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
  <key>PayloadDisplayName</key><string>位置共有プロファイル</string>
  <key>PayloadOrganization</key><string>LocShare</string>
  <key>PayloadDescription</key><string>ホーム画面に位置共有アイコンを追加します。</string>
</dict>
</plist>`;

  res.setHeader("Content-Type", "application/x-apple-aspen-config; charset=utf-8");
  res.setHeader("Content-Disposition", 'attachment; filename="locshare.mobileconfig"');
  res.send(xml);
});

/* ---------- WebSocket 中継 ---------- */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const rooms = new Map(); // room -> Map<id, client>
const rid = () => Math.random().toString(36).slice(2, 10);
const roomOf = (k) => { if (!rooms.has(k)) rooms.set(k, new Map()); return rooms.get(k); };
const send = (ws, o) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(o)); };

function leave(ws) {
  if (!ws.room) return;
  const r = rooms.get(ws.room);
  if (r) {
    r.delete(ws.id);
    for (const c of r.values()) if (c.role === "watch") send(c.ws, { type: "left", id: ws.id });
    if (r.size === 0) rooms.delete(ws.room);
  }
  ws.room = null;
}

wss.on("connection", (ws) => {
  ws.id = rid();
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }

    if (m.type === "join") {
      leave(ws);
      ws.room = String(m.room || "default").replace(/[^A-Za-z0-9]/g, "").slice(0, 40) || "default";
      ws.role = m.role === "watch" ? "watch" : "share";
      ws.name = String(m.name || "").slice(0, 12);
      const r = roomOf(ws.room);
      r.set(ws.id, { ws, id: ws.id, role: ws.role, name: ws.name });
      if (ws.role === "watch") {
        const peers = [...r.values()]
          .filter((c) => c.role === "share" && typeof c.lat === "number")
          .map((c) => ({ id: c.id, n: c.name, lat: c.lat, lng: c.lng, acc: c.acc, spd: c.spd, ts: c.ts }));
        send(ws, { type: "peers", peers });
      }
      return;
    }

    if (m.type === "pos" && ws.room) {
      const r = rooms.get(ws.room);
      if (!r) return;
      const c = r.get(ws.id);
      if (c) { c.lat = m.lat; c.lng = m.lng; c.acc = m.acc; c.spd = m.spd; c.ts = Date.now(); }
      for (const cl of r.values()) {
        if (cl.role === "watch") {
          send(cl.ws, {
            type: "pos", id: ws.id, n: ws.name,
            lat: m.lat, lng: m.lng, acc: m.acc, spd: m.spd, ts: Date.now()
          });
        }
      }
    }
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log("listening on " + PORT));
