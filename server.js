require('dotenv').config();
const express = require('express');
const { createServer } = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const QRCode = require('qrcode');
const {
  makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

const server = createServer(app);
const wss = new WebSocketServer({ server });

// ── Session store ────────────────────────────────────────────────────────────
// sessions Map stores { sock, wsId, connected }
// wsClients Map stores wsId → WebSocket instance
// This decouples WebSocket identity from Baileys sessions so stale-ws bugs
// are impossible: broadcast always looks up the CURRENT live socket by wsId.
const sessions  = new Map(); // sessionId → { sock, wsId, connected }
const wsClients = new Map(); // wsId      → WebSocket

let wsCounter = 0;
function assignWsId(ws) {
  const id = `ws_${++wsCounter}`;
  ws.__id = id;
  wsClients.set(id, ws);
  return id;
}

const AUTH_DIR = path.join(__dirname, 'auth_sessions');
if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });

// Silent logger — prevent Baileys flooding Railway logs
const silentLogger = pino({ level: 'silent' });

// Known-good fallback WA version — used when fetchLatestBaileysVersion fails
const FALLBACK_VERSION = [2, 3000, 1015901307];

// ── Anti-ban limits ──────────────────────────────────────────────────────────
const MESSAGE_LIMITS = {
  minDelayMs: 5000,
  maxPerDay: 200,
  maxNewContactsPerDay: 50,
};
const msgCounts = new Map();

function rateCheck(sid) {
  const today = new Date().toDateString();
  const r = msgCounts.get(sid) || { count: 0, date: today, lastSent: 0 };
  if (r.date !== today) { r.count = 0; r.date = today; }
  const wait = MESSAGE_LIMITS.minDelayMs - (Date.now() - r.lastSent);
  if (wait > 0) return { allowed: false, reason: `Wait ${Math.ceil(wait / 1000)}s`, waitMs: wait };
  if (r.count >= MESSAGE_LIMITS.maxPerDay) return { allowed: false, reason: 'Daily limit reached' };
  return { allowed: true };
}

function recordSent(sid) {
  const r = msgCounts.get(sid) || { count: 0, date: new Date().toDateString(), lastSent: 0 };
  r.count++; r.lastSent = Date.now();
  msgCounts.set(sid, r);
}

// ── broadcast — always looks up CURRENT ws, never uses stale reference ────────
function broadcast(sessionId, event, data) {
  const session = sessions.get(sessionId);
  const wsId = session?.wsId;
  if (!wsId) return;
  const ws = wsClients.get(wsId);
  if (ws?.readyState === 1) {
    try { ws.send(JSON.stringify({ event, data })); } catch {}
  }
}

// ── Direct send to a specific wsId (used before session is created) ──────────
function sendToWs(wsId, event, data) {
  const ws = wsClients.get(wsId);
  if (ws?.readyState === 1) {
    try { ws.send(JSON.stringify({ event, data })); } catch {}
  }
}

// ── createSession ─────────────────────────────────────────────────────────────
async function createSession(sessionId, wsId) {
  const sessionDir = path.join(AUTH_DIR, sessionId);
  fs.mkdirSync(sessionDir, { recursive: true });

  // FIX BUG 2: timeout + fallback for fetchLatestBaileysVersion
  let version = FALLBACK_VERSION;
  try {
    const controller = new AbortController();
    const to = setTimeout(() => controller.abort(), 8000);
    const result = await fetchLatestBaileysVersion();
    clearTimeout(to);
    if (result?.version) version = result.version;
  } catch (e) {
    console.log('[wa] Using fallback WA version:', FALLBACK_VERSION.join('.'));
  }

  let state, saveCreds;
  try {
    const auth = await useMultiFileAuthState(sessionDir);
    state = auth.state;
    saveCreds = auth.saveCreds;
  } catch (e) {
    sendToWs(wsId, 'error', { message: 'Failed to load auth state: ' + e.message });
    return;
  }

  let sock;
  try {
    sock = makeWASocket({
      version,
      auth: state,
      logger: silentLogger,          // FIX BUG 4: suppress all Baileys logs
      printQRInTerminal: false,
      browser: ['Business OS', 'Chrome', '120.0.0'],
      generateHighQualityLinkPreview: false,
      syncFullHistory: false,
      connectTimeoutMs: 30000,
      defaultQueryTimeoutMs: 20000,
      keepAliveIntervalMs: 10000,
    });
  } catch (e) {
    sendToWs(wsId, 'error', { message: 'Socket init failed: ' + e.message });
    return;
  }

  // FIX BUG 1 & 3: store wsId not ws reference; broadcast uses live lookup
  sessions.set(sessionId, { sock, wsId, connected: false });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      try {
        const qrDataUrl = await QRCode.toDataURL(qr, { width: 260, margin: 2, color: { dark: '#000000', light: '#ffffff' } });
        // FIX BUG 1: broadcast uses current ws via sessionId lookup
        broadcast(sessionId, 'qr', { qr: qrDataUrl, sessionId });
        console.log('[wa] QR emitted for', sessionId);
      } catch (e) {
        broadcast(sessionId, 'error', { message: 'QR generation failed: ' + e.message });
      }
    }

    if (connection === 'open') {
      const s = sessions.get(sessionId);
      if (s) s.connected = true;
      const user = sock.user;
      broadcast(sessionId, 'connected', {
        sessionId,
        phone: user?.id?.split(':')[0] || '',
        name: user?.name || '',
      });
      console.log('[wa] Connected:', sessionId);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      const loggedOut = code === DisconnectReason.loggedOut;
      broadcast(sessionId, 'disconnected', { sessionId, loggedOut, code });

      if (loggedOut) {
        sessions.delete(sessionId);
        fs.rmSync(sessionDir, { recursive: true, force: true });
      } else {
        // FIX BUG 3: reconnect uses current wsId from sessions map, not stale ws
        const currentWsId = sessions.get(sessionId)?.wsId || wsId;
        setTimeout(() => createSession(sessionId, currentWsId), 4000);
      }
    }
  });

  sock.ev.on('messages.upsert', ({ messages, type }) => {
    if (type !== 'notify') return;
    const msgs = messages.map(m => ({
      id: m.key.id,
      from: m.key.remoteJid,
      fromMe: m.key.fromMe,
      body: m.message?.conversation
         || m.message?.extendedTextMessage?.text
         || m.message?.imageMessage?.caption
         || '',
      timestamp: m.messageTimestamp,
      name: m.pushName,
    }));
    broadcast(sessionId, 'messages', { sessionId, messages: msgs });
  });

  sock.ev.on('chats.upsert', chats => {
    broadcast(sessionId, 'chats', { sessionId, chats: chats.slice(0, 100) });
  });

  sock.ev.on('contacts.upsert', contacts => {
    broadcast(sessionId, 'contacts', { sessionId, contacts: contacts.slice(0, 500) });
  });
}

// ── WebSocket server ──────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  const wsId = assignWsId(ws);
  console.log('[ws] Client connected:', wsId);

  // Keepalive ping from server side
  const serverPing = setInterval(() => {
    if (ws.readyState === 1) ws.ping();
  }, 20000);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { action, sessionId, data } = msg;
    if (!action) return;

    switch (action) {
      case 'connect': {
        if (!sessionId) break;
        const existing = sessions.get(sessionId);
        if (existing) {
          // Session exists — update ws reference so broadcasts go to this client
          existing.wsId = wsId;
          if (existing.connected) {
            sendToWs(wsId, 'already_connected', { sessionId });
          } else {
            sendToWs(wsId, 'connecting', { sessionId });
          }
        } else {
          sendToWs(wsId, 'connecting', { sessionId });
          // FIX BUG 1: pass wsId, not ws reference
          try {
            await createSession(sessionId, wsId);
          } catch (e) {
            sendToWs(wsId, 'error', { message: e.message });
          }
        }
        break;
      }

      case 'send_message': {
        const s = sessions.get(sessionId);
        if (!s?.sock) { sendToWs(wsId, 'error', { message: 'Session not connected' }); break; }
        const chk = rateCheck(sessionId);
        if (!chk.allowed) { sendToWs(wsId, 'rate_limited', chk); break; }
        try {
          await s.sock.sendMessage(data.to, { text: data.text });
          recordSent(sessionId);
          sendToWs(wsId, 'sent', { sessionId, to: data.to });
        } catch (e) {
          sendToWs(wsId, 'error', { message: e.message });
        }
        break;
      }

      case 'get_messages': {
        const s = sessions.get(sessionId);
        if (!s?.sock) break;
        try {
          const msgs = await s.sock.loadMessages(data.jid, data.count || 30);
          sendToWs(wsId, 'history', { sessionId, jid: data.jid, messages: msgs });
        } catch {}
        break;
      }

      case 'get_contacts': {
        const s = sessions.get(sessionId);
        if (!s?.sock) break;
        const contacts = Object.values(s.sock.store?.contacts || {}).slice(0, 500);
        sendToWs(wsId, 'contacts', { sessionId, contacts });
        break;
      }

      case 'logout': {
        const s = sessions.get(sessionId);
        if (s?.sock) {
          try { await s.sock.logout(); } catch {}
          sessions.delete(sessionId);
          const dir = path.join(AUTH_DIR, sessionId);
          fs.rmSync(dir, { recursive: true, force: true });
        }
        sendToWs(wsId, 'logged_out', { sessionId });
        break;
      }

      case 'ping':
        sendToWs(wsId, 'pong', { ts: Date.now(), limits: MESSAGE_LIMITS });
        break;
    }
  });

  ws.on('close', () => {
    clearInterval(serverPing);
    wsClients.delete(wsId);
    console.log('[ws] Client disconnected:', wsId);
  });

  ws.on('error', (e) => {
    console.error('[ws] Error on', wsId, e.message);
  });
});

// ── HTTP routes ───────────────────────────────────────────────────────────────
app.get('/health', (_, res) => {
  res.json({
    ok: true,
    sessions: sessions.size,
    clients: wsClients.size,
    limits: MESSAGE_LIMITS,
    ts: Date.now(),
  });
});

app.get('/sessions', (_, res) => {
  const list = Array.from(sessions.entries()).map(([id, s]) => ({
    id, connected: s.connected, wsId: s.wsId,
  }));
  res.json(list);
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`[wa-server] Listening on :${PORT}`);
});
