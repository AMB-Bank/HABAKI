'use strict';
/* ХАБАКИ: Лабиринт — сервер: статика + аккаунты (SQLite) + статистика + онлайн-комнаты (WebSocket) */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
let WebSocketServer = null;
try { ({ WebSocketServer } = require('ws')); } catch { console.warn('Модуль ws не найден — онлайн-комнаты отключены (выполните npm install)'); }

const PORT = +process.env.PORT || 3000;
const DB_FILE_WANTED = process.env.DB_FILE || path.join(__dirname, 'habaki.db');
const PUB = path.join(__dirname, 'public');

/* ---------- БАЗА ДАННЫХ ----------
   Основной вариант — SQLite (node:sqlite). Если на хостинге старый Node или SQLite недоступен,
   автоматически используется простой JSON-файл — аккаунты работают в любом случае. */
const os = require('os');
function ensureWritable(file) {
  try { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.accessSync(path.dirname(file), fs.constants.W_OK); return file; }
  catch { const alt = path.join(os.tmpdir(), path.basename(file)); console.warn('Папка для БД недоступна для записи, использую', alt); return alt; }
}
function openSqlite(file) {
  const { DatabaseSync } = require('node:sqlite');
  const db = new DatabaseSync(file);
  try { db.exec('PRAGMA journal_mode=WAL;'); } catch {}
  db.exec(`
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE COLLATE NOCASE,
  username TEXT NOT NULL,
  username_lc TEXT NOT NULL UNIQUE,
  pass_hash TEXT NOT NULL,
  salt TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions(
  token_hash TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS stats(
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  games INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  hides INTEGER NOT NULL DEFAULT 0,
  picks INTEGER NOT NULL DEFAULT 0,
  deaths INTEGER NOT NULL DEFAULT 0,
  best_time INTEGER,
  play_time INTEGER NOT NULL DEFAULT 0
);
`);
  return {
    byEmail: db.prepare('SELECT * FROM users WHERE email = ?'),
    byName: db.prepare('SELECT * FROM users WHERE username_lc = ?'),
    byId: db.prepare('SELECT * FROM users WHERE id = ?'),
    insUser: db.prepare('INSERT INTO users(email,username,username_lc,pass_hash,salt,created_at) VALUES(?,?,?,?,?,?)'),
    insStats: db.prepare('INSERT INTO stats(user_id) VALUES(?)'),
    insSess: db.prepare('INSERT INTO sessions(token_hash,user_id,created_at) VALUES(?,?,?)'),
    sess: db.prepare('SELECT user_id FROM sessions WHERE token_hash = ?'),
    delSess: db.prepare('DELETE FROM sessions WHERE token_hash = ?'),
    stats: db.prepare('SELECT * FROM stats WHERE user_id = ?'),
    top: db.prepare(`SELECT u.username, s.wins, s.games, s.hides FROM stats s JOIN users u ON u.id = s.user_id
                     WHERE s.games > 0 ORDER BY s.wins DESC, s.hides DESC, s.games ASC LIMIT 10`),
    upd: db.prepare(`UPDATE stats SET games=games+1, wins=wins+?, hides=hides+?, picks=picks+?, deaths=deaths+?,
                     play_time=play_time+?, best_time=CASE WHEN ? = 1 AND (best_time IS NULL OR ? < best_time) THEN ? ELSE best_time END
                     WHERE user_id=?`),
  };
}
function openJson(file) {
  const jf = file.replace(/\.db$/i, '') + '.json';
  let d = { seq: 0, users: [], sessions: {}, stats: {} };
  try { d = Object.assign(d, JSON.parse(fs.readFileSync(jf, 'utf8'))); } catch {}
  let timer = null;
  const save = () => { if (timer) return; timer = setTimeout(() => { timer = null;
    try { fs.writeFileSync(jf + '.tmp', JSON.stringify(d)); fs.renameSync(jf + '.tmp', jf); } catch (e) { console.error('Не удалось сохранить БД:', e.message); } }, 200); };
  const flush = () => { try { fs.writeFileSync(jf + '.tmp', JSON.stringify(d)); fs.renameSync(jf + '.tmp', jf); } catch {} };
  process.on('exit', flush);
  for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { flush(); process.exit(0); });
  console.warn('SQLite недоступен — использую JSON-файл:', jf);
  return {
    byEmail: { get: e => d.users.find(u => u.email.toLowerCase() === String(e).toLowerCase()) },
    byName: { get: n => d.users.find(u => u.username_lc === n) },
    byId: { get: id => d.users.find(u => u.id === id) },
    insUser: { run: (email, username, username_lc, pass_hash, salt, created_at) => {
      const id = ++d.seq; d.users.push({ id, email, username, username_lc, pass_hash, salt, created_at }); save(); return { lastInsertRowid: id }; } },
    insStats: { run: uid => { d.stats[uid] = { user_id: uid, games: 0, wins: 0, hides: 0, picks: 0, deaths: 0, best_time: null, play_time: 0 }; save(); } },
    insSess: { run: (h, uid, t) => { d.sessions[h] = { user_id: uid, created_at: t }; save(); } },
    sess: { get: h => d.sessions[h] },
    delSess: { run: h => { delete d.sessions[h]; save(); } },
    stats: { get: uid => d.stats[uid] },
    top: { all: () => Object.values(d.stats).filter(s => s.games > 0).map(s => ({ username: d.users.find(u => u.id === s.user_id)?.username || '?', wins: s.wins, games: s.games, hides: s.hides }))
      .sort((a, b) => b.wins - a.wins || b.hides - a.hides || a.games - b.games).slice(0, 10) },
    upd: { run: (w, h, k, dth, t, win, t2, t3, uid) => { const s = d.stats[uid]; if (!s) return;
      s.games++; s.wins += w; s.hides += h; s.picks += k; s.deaths += dth; s.play_time += t;
      if (win === 1 && (s.best_time == null || t2 < s.best_time)) s.best_time = t3; save(); } },
  };
}
const DB_FILE = ensureWritable(DB_FILE_WANTED);
let q;
try { q = openSqlite(DB_FILE); } catch (e) { console.warn('SQLite не запустился (' + e.message + ')'); q = openJson(DB_FILE); }

const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const hashPass = (pw, salt) => crypto.scryptSync(pw, salt, 64).toString('hex');
const publicUser = u => ({ id: u.id, username: u.username, email: u.email });
const publicStats = uid => {
  const s = q.stats.get(uid) || {};
  return { games: s.games | 0, wins: s.wins | 0, hides: s.hides | 0, picks: s.picks | 0, deaths: s.deaths | 0, bestTime: s.best_time ?? null, playTime: s.play_time | 0 };
};

/* ---------- ВСПОМОГАТЕЛЬНОЕ ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.mp3': 'audio/mpeg', '.webmanifest': 'application/manifest+json' };

const hits = new Map(); // простая защита от перебора пароля
function limited(ip, key, max, windowMs) {
  const k = key + ip, now = Date.now(), a = (hits.get(k) || []).filter(t => now - t < windowMs);
  a.push(now); hits.set(k, a);
  return a.length > max;
}
setInterval(() => { const now = Date.now(); for (const [k, a] of hits) if (!a.some(t => now - t < 600000)) hits.delete(k); }, 60000).unref();

function readJson(req) {
  return new Promise((res, rej) => {
    let b = '';
    req.on('data', c => { b += c; if (b.length > 20000) { rej(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { res(b ? JSON.parse(b) : {}); } catch (e) { rej(e); } });
    req.on('error', rej);
  });
}
const send = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };
const authUser = req => {
  const m = /^Bearer (.+)$/.exec(req.headers.authorization || '');
  if (!m) return null;
  const s = q.sess.get(sha(m[1]));
  return s ? q.byId.get(s.user_id) : null;
};
function newSession(uid) {
  const token = crypto.randomBytes(32).toString('hex');
  q.insSess.run(sha(token), uid, Date.now());
  return token;
}
const clamp = (v, a, b) => Math.max(a, Math.min(b, Math.floor(+v) || 0));

/* ---------- API ---------- */
async function api(req, res, url) {
  const ip = (process.env.TRUST_PROXY !== '0' && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '';
  try {
    if (req.method === 'POST' && url === '/api/register') {
      if (limited(ip, 'reg', 20, 600000)) return send(res, 429, { error: 'Слишком много попыток. Подождите немного.' });
      const { email = '', username = '', password = '' } = await readJson(req);
      const e = String(email).trim(), n = String(username).trim(), p = String(password);
      if (!/^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,}$/.test(e) || e.length > 254) return send(res, 400, { error: 'Введите корректный email' });
      if (!/^[\p{L}\p{N}_.-]{3,16}$/u.test(n)) return send(res, 400, { error: 'Имя: 3–16 символов (буквы, цифры, _ . -)' });
      if (p.length < 6 || p.length > 100) return send(res, 400, { error: 'Пароль: минимум 6 символов' });
      if (q.byEmail.get(e)) return send(res, 409, { error: 'Этот email уже зарегистрирован' });
      if (q.byName.get(n.toLowerCase())) return send(res, 409, { error: 'Это имя пользователя занято' });
      const salt = crypto.randomBytes(16).toString('hex');
      const r = q.insUser.run(e, n, n.toLowerCase(), hashPass(p, salt), salt, Date.now());
      const uid = Number(r.lastInsertRowid);
      q.insStats.run(uid);
      return send(res, 200, { token: newSession(uid), user: publicUser(q.byId.get(uid)), stats: publicStats(uid) });
    }
    if (req.method === 'POST' && url === '/api/login') {
      if (limited(ip, 'log', 40, 600000)) return send(res, 429, { error: 'Слишком много попыток. Подождите немного.' });
      const { login = '', password = '' } = await readJson(req);
      const l = String(login).trim();
      const u = q.byEmail.get(l) || q.byName.get(l.toLowerCase());
      const bad = () => send(res, 401, { error: 'Неверный логин или пароль' });
      if (!u) { hashPass(String(password), 'x'.repeat(32)); return bad(); }
      const a = Buffer.from(hashPass(String(password), u.salt), 'hex'), b = Buffer.from(u.pass_hash, 'hex');
      if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return bad();
      return send(res, 200, { token: newSession(u.id), user: publicUser(u), stats: publicStats(u.id) });
    }
    if (req.method === 'GET' && url === '/api/me') {
      const u = authUser(req);
      return u ? send(res, 200, { user: publicUser(u), stats: publicStats(u.id) }) : send(res, 401, { error: 'Нужно войти' });
    }
    if (req.method === 'POST' && url === '/api/logout') {
      const m = /^Bearer (.+)$/.exec(req.headers.authorization || '');
      if (m) q.delSess.run(sha(m[1]));
      return send(res, 200, { ok: true });
    }
    if (req.method === 'POST' && url === '/api/stats') {
      const u = authUser(req);
      if (!u) return send(res, 401, { error: 'Нужно войти' });
      if (limited(ip, 'st', 120, 600000)) return send(res, 429, { error: 'Слишком часто' });
      const b = await readJson(req);
      const win = b.win ? 1 : 0, time = clamp(b.time, 0, 7200);
      // разумные пределы за одну партию
      q.upd.run(win, clamp(b.hides, 0, 60), clamp(b.picks, 0, 80), b.win ? 0 : 1, time, win, time, time, u.id);
      return send(res, 200, { stats: publicStats(u.id) });
    }
    if (req.method === 'GET' && url === '/api/leaderboard') return send(res, 200, { top: q.top.all() });
    return send(res, 404, { error: 'Не найдено' });
  } catch (e) {
    return send(res, 400, { error: 'Некорректный запрос' });
  }
}

/* ---------- HTTP (статика) ---------- */
const server = http.createServer((req, res) => {
  let url; try { url = decodeURIComponent((req.url || '/').split('?')[0]); } catch { res.writeHead(400); return res.end(); }
  if (url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (url.startsWith('/api/')) return api(req, res, url);
  let f = path.normalize(path.join(PUB, url === '/' ? 'index.html' : url));
  if (f !== PUB && !f.startsWith(PUB + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(404); return res.end('404'); }
    const ext = path.extname(f);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600' });
    res.end(data);
  });
});

/* ---------- ОНЛАЙН: комнаты по WebSocket ----------
   Протокол совместим с прежним API «room»: у каждого клиента есть presence (объект),
   сервер объединяет присланные поля и рассылает всем в комнате список участников. */
const rooms = new Map(); // name -> Map(peerId -> {ws, presence, joinedAt})
const wss = WebSocketServer ? new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 }) : { on() {}, clients: new Set() };
let peerSeq = 0;

function broadcast(name) {
  const room = rooms.get(name);
  if (!room) return;
  for (const [id, p] of room) {
    if (p.ws.readyState !== 1) continue;
    // свои данные обратно не отправляем — экономим трафик
    const peers = [...room.entries()].map(([pid, q]) => ({ peer: pid, presence: pid === id ? {} : q.presence }));
    p.ws.send(JSON.stringify({ t: 'peers', me: id, peers }));
  }
}
const pending = new Set(); // комнаты, ожидающие рассылки (склеиваем частые обновления)
setInterval(() => { for (const n of pending) broadcast(n); pending.clear(); }, 40);

wss.on('connection', (ws, req) => {
  const id = 'p' + (++peerSeq);
  let name = null;
  ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && typeof m.room === 'string' && /^[\w-]{1,40}$/.test(m.room) && !name) {
      const exist = rooms.get(m.room);
      if (m.host && exist && exist.size) { ws.send(JSON.stringify({ t: 'taken' })); return; }
      if (!m.host && (!exist || !exist.size)) { ws.send(JSON.stringify({ t: 'noroom' })); return; }
      const room = exist || new Map();
      if (room.size >= 2) { ws.send(JSON.stringify({ t: 'full' })); return; }
      rooms.set(m.room, room);
      room.set(id, { ws, presence: {}, joinedAt: Date.now() });
      name = m.room;
      ws.send(JSON.stringify({ t: 'joined', me: id }));
      pending.add(name);
    } else if (m.t === 'presence' && name && m.p && typeof m.p === 'object') {
      const p = rooms.get(name)?.get(id);
      if (p) { Object.assign(p.presence, m.p); pending.add(name); }
    }
  });
  const leave = () => {
    if (!name) return;
    const room = rooms.get(name);
    if (room) { room.delete(id); if (!room.size) rooms.delete(name); else pending.add(name); }
    name = null;
  };
  ws.on('close', leave);
  ws.on('error', leave);
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }), 20000).unref();

process.on('uncaughtException', e => console.error('Ошибка:', e));
process.on('unhandledRejection', e => console.error('Ошибка:', e));
server.listen(PORT, '0.0.0.0', () => console.log(`ХАБАКИ запущены: http://localhost:${PORT}  (БД: ${DB_FILE})`));
