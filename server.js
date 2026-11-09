// 나만의 방 - 실시간 서버
// 로그인 없이 링크로 들어온 사람들이 서로를 볼 수 있게 위치/이름/캐릭터를 중계하고,
// 방 주인의 상태(위치, 한마디 등)를 저장해서 모두에게 알려줍니다.
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 3000;
const OWNER_KEY = process.env.OWNER_KEY || "";
const MAX_CONNECTIONS = 60; // 방 안 10명 + 대기/구경하는 사람
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const STATUS_FILE = path.join(DATA_DIR, "status.json");
const GB_FILE = path.join(DATA_DIR, "guestbook.json");
const GB_MAX = 50; // 방명록은 최근 50개까지 보관하고 보여줘요

const ZONES = ["living", "read", "desk", "kitchen", "dining", "bed", "bath", "out"];
const AVAILS = ["free", "busy", "dnd"];
const CHARS = ["m", "f", "cat", "dog", "tiger", "rabbit", "mouse", "bear", "roach"];

if (!OWNER_KEY) console.warn("[경고] OWNER_KEY 환경변수가 없어서 아무도 방 주인 상태를 바꿀 수 없어요.");

// ---------- 방 주인 상태 저장 ----------
let status = { owner: process.env.OWNER_NAME || "", zone: "desk", msg: "", avail: "free", color: "#4f7cf0", char: "m", updatedAt: 0 };
try { status = Object.assign(status, JSON.parse(fs.readFileSync(STATUS_FILE, "utf8"))); } catch (e) { /* 처음 실행 */ }
function saveStatus() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(STATUS_FILE, JSON.stringify(status)); }
  catch (e) { console.error("상태 저장 실패:", e.message); }
}
// ---------- 방명록 ----------
let guestbook = [];
try { const g = JSON.parse(fs.readFileSync(GB_FILE, "utf8")); if (Array.isArray(g)) guestbook = g.slice(-GB_MAX); } catch (e) { /* 처음 실행 */ }
let gbSaveTimer = null;
function saveGuestbook() {
  if (gbSaveTimer) return;
  gbSaveTimer = setTimeout(() => {
    gbSaveTimer = null;
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(GB_FILE, JSON.stringify(guestbook)); }
    catch (e) { console.error("방명록 저장 실패:", e.message); }
  }, 300);
}
let gbLastGlobal = [];
// ---------- 대화 (메모리에만, 최근 30개) ----------
let chats = [];
const BAD_CHARS = new RegExp("[\\u0000-\\u001f\\u007f-\\u009f\\u200b-\\u200f\\u2028-\\u202e\\u2060-\\u206f\\ufeff]", "g");
function cleanText(s, n) {
  return String(s == null ? "" : s).replace(BAD_CHARS, "").trim().slice(0, n);
}
function validStatus(s) {
  if (!s || typeof s !== "object") return null;
  return {
    owner: cleanText(s.owner, 12),
    zone: ZONES.includes(s.zone) ? s.zone : "desk",
    msg: cleanText(s.msg, 40),
    avail: AVAILS.includes(s.avail) ? s.avail : "free",
    color: /^#[0-9a-fA-F]{6}$/.test(s.color) ? s.color : "#4f7cf0",
    char: CHARS.includes(s.char) ? s.char : "m",
    updatedAt: Date.now(),
  };
}
function sameKey(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

// ---------- 페이지 ----------
function buildPage() {
  const body = fs.readFileSync(path.join(__dirname, "room.html"), "utf8");
  return '<!doctype html><html lang="ko"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">' +
    "<style>:root{padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}" +
    "body{margin:0;font:14px system-ui,sans-serif}img{max-width:100%}[hidden]{display:none!important}</style>" +
    "<script>window.ROOM_SERVER=true</script></head><body>" + body + "</body></html>";
}
const PAGE = buildPage();

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/healthz") { res.writeHead(200, { "content-type": "text/plain" }); res.end("ok"); return; }
  if (url !== "/" && url !== "/index.html") { res.writeHead(404, { "content-type": "text/plain; charset=utf-8" }); res.end("없는 페이지예요"); return; }
  res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache" });
  res.end(PAGE);
});

// ---------- 실시간 ----------
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 8 * 1024 });
const clients = new Map(); // ws -> { id, presence, owner, msgs, windowStart, alive }
let nextId = 1;

function num(v, lo, hi) { const n = Number(v); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : undefined; }
function applyPresence(c, patch) {
  if (!patch || typeof patch !== "object") return;
  const p = c.presence;
  if ("name" in patch) p.name = cleanText(patch.name, 16);
  if ("color" in patch && /^#[0-9a-fA-F]{6}$/.test(patch.color)) p.color = patch.color;
  if ("char" in patch && CHARS.includes(patch.char)) p.char = patch.char;
  if ("wave" in patch) { const w = num(patch.wave, 0, 1e15); if (w !== undefined) p.wave = w; }
  if ("x" in patch) { const v = num(patch.x, -8, 11); if (v !== undefined) p.x = v; }
  if ("z" in patch) { const v = num(patch.z, -6, 6); if (v !== undefined) p.z = v; }
  if ("ry" in patch) { const v = num(patch.ry, -100, 100); if (v !== undefined) p.ry = v; }
  // 역할과 들어온 순서는 서버가 정해요 (조작 방지)
  p.role = c.owner ? "owner" : "guest";
  p.joinedAt = c.joinedAt;
}

let peersTimer = null;
function broadcastPeers() {
  if (peersTimer) return;
  peersTimer = setTimeout(() => {
    peersTimer = null;
    const peers = [];
    for (const c of clients.values()) peers.push({ id: c.id, presence: c.presence });
    const msg = JSON.stringify({ t: "peers", peers });
    for (const ws of clients.keys()) if (ws.readyState === 1) ws.send(msg);
  }, 50);
}
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const ws of clients.keys()) if (ws.readyState === 1) ws.send(msg);
}

wss.on("connection", (ws) => {
  if (clients.size >= MAX_CONNECTIONS) { ws.close(1013, "full"); return; }
  const c = { id: "p" + (nextId++), presence: {}, owner: false, joinedAt: Date.now(), msgs: 0, windowStart: Date.now(), alive: true, authTries: 0 };
  c.presence.role = "guest"; c.presence.joinedAt = c.joinedAt;
  clients.set(ws, c);
  ws.send(JSON.stringify({ t: "hello", id: c.id, status, gb: guestbook, chats }));
  broadcastPeers();

  ws.on("pong", () => { c.alive = true; });
  ws.on("message", (raw) => {
    const now = Date.now();
    if (now - c.windowStart > 1000) { c.windowStart = now; c.msgs = 0; }
    if (++c.msgs > 40) return; // 너무 많은 메시지는 무시
    let m; try { m = JSON.parse(String(raw)); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    if (m.t === "id") {
      // 같은 기기(브라우저)에서 다시 들어오면 예전 연결을 바로 정리해서 중복으로 안 보이게 해요
      const vid = String(m.vid || "");
      if (!/^[a-z0-9]{8,40}$/.test(vid) || c.vid) return;
      c.vid = vid;
      for (const [ows, oc] of clients) {
        if (ows !== ws && oc.vid === vid) { clients.delete(ows); try { ows.terminate(); } catch (e) {} }
      }
      broadcastPeers();
    }
    else if (m.t === "presence") { applyPresence(c, m.patch); broadcastPeers(); }
    else if (m.t === "auth") {
      if (c.authTries++ > 5) { ws.send(JSON.stringify({ t: "auth", ok: false })); return; }
      const ok = !!OWNER_KEY && sameKey(m.key, OWNER_KEY);
      c.owner = ok; c.presence.role = ok ? "owner" : "guest";
      ws.send(JSON.stringify({ t: "auth", ok }));
      broadcastPeers();
    }
    else if (m.t === "chat") {
      const text = cleanText(m.text, 20); // 한 번에 최대 20자
      if (!text) return;
      if (c.chatAt && now - c.chatAt < 800) { ws.send(JSON.stringify({ t: "chat_err" })); return; }
      c.chatAt = now;
      const name = c.owner ? (status.owner || "방 주인") : (cleanText(c.presence.name, 16) || "손님");
      const msg = { t: "chat", id: crypto.randomBytes(4).toString("hex"), from: c.id, name, owner: c.owner, text, at: now };
      chats.push(msg); if (chats.length > 30) chats.shift();
      broadcast(msg);
    }
    else if (m.t === "gb_add") {
      const text = cleanText(m.text, 200), name = cleanText(m.name, 16) || "손님";
      if (!text) return;
      // 한 사람은 10초에 한 번, 방 전체는 1분에 20개까지
      gbLastGlobal = gbLastGlobal.filter((t) => now - t < 60000);
      if ((c.gbAt && now - c.gbAt < 10000) || gbLastGlobal.length >= 20) { ws.send(JSON.stringify({ t: "gb_err", msg: "slow" })); return; }
      c.gbAt = now; gbLastGlobal.push(now);
      guestbook.push({ id: crypto.randomBytes(6).toString("hex"), name, text, at: now, owner: c.owner });
      if (guestbook.length > GB_MAX) guestbook = guestbook.slice(-GB_MAX);
      saveGuestbook();
      broadcast({ t: "gb", list: guestbook });
    }
    else if (m.t === "gb_del") {
      if (!c.owner) return;
      const before = guestbook.length;
      guestbook = guestbook.filter((g) => g.id !== String(m.id));
      if (guestbook.length !== before) { saveGuestbook(); broadcast({ t: "gb", list: guestbook }); }
    }
    else if (m.t === "status") {
      if (!c.owner) { ws.send(JSON.stringify({ t: "saved", ok: false })); return; }
      const s = validStatus(m.status);
      if (!s) { ws.send(JSON.stringify({ t: "saved", ok: false })); return; }
      status = s; saveStatus();
      broadcast({ t: "status", status });
      ws.send(JSON.stringify({ t: "saved", ok: true }));
    }
  });
  ws.on("close", () => { if (clients.delete(ws)) broadcastPeers(); });
  ws.on("error", () => {});
});

// 끊긴 연결 정리
setInterval(() => {
  for (const [ws, c] of clients) {
    if (!c.alive) { ws.terminate(); clients.delete(ws); continue; }
    c.alive = false; try { ws.ping(); } catch (e) {}
  }
  broadcastPeers();
}, 10000); // 10초마다 확인해서 창을 닫은 사람은 늦어도 20초 안에 정리

server.listen(PORT, () => console.log("나만의 방 서버가 열렸어요: http://localhost:" + PORT));
