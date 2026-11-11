'use strict';
// Сервер: раздаёт игру, пересылает данные игроков комнаты (WebSocket) и проксирует запросы к ИИ.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = process.env.AI_MODEL || 'claude-sonnet-5-5';
const PUB = path.join(__dirname, 'public');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css', '.ico': 'image/x-icon', '.png': 'image/png' };

// ---- простой лимит запросов по IP
const hits = new Map();
function limited(ip, max, win) {
  const n = Date.now(), a = (hits.get(ip) || []).filter(t => n - t < win);
  a.push(n); hits.set(ip, a); return a.length > max;
}
setInterval(() => { const n = Date.now(); for (const [k, a] of hits) if (!a.some(t => n - t < 60000)) hits.delete(k); }, 60000).unref();
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();

// ---- ИИ-прокси (ключ хранится только на сервере)
async function ai(req, res) {
  const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'access-control-allow-origin': '*' }); res.end(JSON.stringify(obj)); };
  if (!KEY) return send(503, { error: 'no_key' });
  if (limited(ipOf(req), 15, 60000)) return send(429, { error: 'rate_limited' });
  let body = '';
  for await (const ch of req) { body += ch; if (body.length > 30000) return send(413, { error: 'too_big' }); }
  let prompt;
  try { prompt = JSON.parse(body).prompt; } catch { return send(400, { error: 'bad_request' }); }
  if (typeof prompt !== 'string' || !prompt || prompt.length > 12000) return send(400, { error: 'bad_prompt' });
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: MODEL, max_tokens: 1500, messages: [{ role: 'user', content: prompt }] }),
      signal: AbortSignal.timeout(25000)
    });
    const j = await r.json();
    if (!r.ok) return send(502, { error: 'ai_http_' + r.status });
    send(200, { text: (j.content || []).filter(b => b.type === 'text').map(b => b.text).join('') });
  } catch { send(504, { error: 'ai_timeout' }); }
}

// ---- статические файлы
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type', 'access-control-allow-methods': 'POST' }); return res.end(); }
  if (req.method === 'POST' && url.pathname === '/api/ai') return ai(req, res);
  if (url.pathname === '/healthz') { res.writeHead(200); return res.end('ok'); }
  const rel = path.normalize(url.pathname === '/' ? '/index.html' : url.pathname);
  const full = path.join(PUB, rel);
  if (!full.startsWith(PUB)) { res.writeHead(403); return res.end(); }
  fs.readFile(full, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'content-type': MIME[path.extname(full)] || 'application/octet-stream' });
    res.end(data);
  });
});

// ---- комнаты
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16384 });
const rooms = new Map(); // имя -> Map(id -> ws)
const dirty = new Set();
let seq = 0;

function flush(name) {
  const r = rooms.get(name); if (!r) return;
  const msg = JSON.stringify({ t: 'peers', peers: [...r.values()].map(w => ({ peer: w.id, presence: w.pres })) });
  for (const w of r.values()) if (w.readyState === 1) w.send(msg);
}
function mark(name) { // не чаще 20 раз в секунду на комнату
  if (dirty.has(name)) return;
  dirty.add(name);
  setTimeout(() => { dirty.delete(name); flush(name); }, 50);
}

wss.on('connection', ws => {
  ws.id = null; ws.room = null; ws.pres = {}; ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  let cnt = 0, tm = Date.now();
  ws.on('message', buf => {
    const n = Date.now(); if (n - tm > 1000) { tm = n; cnt = 0; } if (++cnt > 60) return;
    let m; try { m = JSON.parse(buf.toString()); } catch { return; }
    if (m.t === 'join' && !ws.room) {
      const name = String(m.room || 'main').toLowerCase().replace(/[^a-z0-9а-яё_-]/gi, '').slice(0, 24) || 'main';
      let r = rooms.get(name); if (!r) { r = new Map(); rooms.set(name, r); }
      if (r.size >= 8) return ws.send(JSON.stringify({ t: 'full' }));
      ws.id = 'p' + String(++seq).padStart(8, '0'); ws.room = name; r.set(ws.id, ws);
      ws.send(JSON.stringify({ t: 'hello', id: ws.id })); mark(name);
    } else if (m.t === 'pres' && ws.room && m.patch && typeof m.patch === 'object' && !Array.isArray(m.patch)) {
      const next = Object.assign({}, ws.pres, m.patch);
      if (JSON.stringify(next).length > 12000) return;
      ws.pres = next; mark(ws.room);
    }
  });
  ws.on('close', () => {
    if (!ws.room) return;
    const r = rooms.get(ws.room); if (!r) return;
    r.delete(ws.id);
    if (r.size) mark(ws.room); else rooms.delete(ws.room);
  });
});

setInterval(() => { // убираем «мёртвые» соединения
  for (const w of wss.clients) { if (!w.alive) { w.terminate(); continue; } w.alive = false; w.ping(); }
}, 30000).unref();

server.listen(PORT, () => console.log('Игра запущена на порту ' + PORT + (KEY ? '' : ' (ИИ выключен: нет ANTHROPIC_API_KEY)')));
