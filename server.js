"use strict";
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(__dirname, "public")));
app.get("/healthz", (_req, res) => res.send("ok"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

/** rooms: Map<room, Map<id, client>> */
const rooms = new Map();
const rid = () => Math.random().toString(36).slice(2, 10);

function snapshot(room) {
  return [...room.values()]
    .filter((c) => typeof c.lat === "number")
    .map((c) => ({ id: c.id, n: c.name, lat: c.lat, lng: c.lng, acc: c.acc, spd: c.spd, ts: c.ts }));
}

const send = (ws, obj) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj)); };

function broadcast(roomName, fromId, obj) {
  const room = rooms.get(roomName);
  if (!room) return;
  for (const c of room.values()) if (c.id !== fromId) send(c.ws, obj);
}

function leave(ws) {
  if (!ws.room) return;
  const room = rooms.get(ws.room);
  if (room) {
    room.delete(ws.id);
    broadcast(ws.room, ws.id, { type: "left", id: ws.id });
    if (room.size === 0) rooms.delete(ws.room);
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
      const roomName = String(m.room || "PUBLIC").toUpperCase().slice(0, 16);
      ws.room = roomName;
      ws.name = String(m.name || "端末").slice(0, 8);
      if (!rooms.has(roomName)) rooms.set(roomName, new Map());
      const room = rooms.get(roomName);
      room.set(ws.id, { ws, id: ws.id, name: ws.name });
      send(ws, { type: "peers", peers: snapshot(room) });
      broadcast(roomName, ws.id, { type: "join", id: ws.id, n: ws.name });
      return;
    }

    if (m.type === "pos" && ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      const c = room.get(ws.id);
      if (c) { c.lat = m.lat; c.lng = m.lng; c.acc = m.acc; c.spd = m.spd; c.ts = Date.now(); }
      broadcast(ws.room, ws.id, {
        type: "pos", id: ws.id, n: ws.name,
        lat: m.lat, lng: m.lng, acc: m.acc, spd: m.spd, ts: Date.now()
      });
    }
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

// 死活監視（切れた接続を掃除）
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log("listening on " + PORT));
