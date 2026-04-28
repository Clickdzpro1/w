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

// -- Session store ------------------------------------------------------------
// sessions Map stores { sock, wsId, connected }
// wsClients Map stores wsId ? WebSocket instance
// This decouples WebSocket identity from Baileys sessions so stale-ws bugs
// are impossible: broadcast always looks up the CURRENT live socket by wsId.
const sessions  = new Map(); // sessionId ? { sock, wsId, connected }
const wsClients = new Map(); // wsId      ? WebSocket
const chatStores = new Map(); // sessionId -> Map(jid -> chat summary)

let wsCounter = 0;
function assignWsId(ws) {
  const id = `ws_${++wsCounter}`;
  ws.__id = id;
  wsClients.set(id, ws);
  return id;
}

const AUTH_DIR = process.env.WA_AUTH_DIR || path.join(__dirname, 'auth_sessions');
if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });

// Silent logger — prevent Baileys flooding Railway logs
const silentLogger = pino({ level: 'silent' });

// Known-good fallback WA version — used when fetchLatestBaileysVersion fails
const FALLBACK_VERSION = [2, 3000, 1015901307];
const WA_WEB_VERSION_OVERRIDE = (process.env.WA_WEB_VERSION_OVERRIDE || '')
  .split('.')
  .map((part) => Number(part.trim()))
  .filter((part) => Number.isFinite(part));
const QR_AUTH_TIMEOUT_MS = Number(process.env.WA_QR_AUTH_TIMEOUT_MS || 45000);

// -- Anti-ban limits ----------------------------------------------------------
const MESSAGE_LIMITS = {
  minDelayMs: 5000,
  maxPerDay: 200,
  maxNewContactsPerDay: 50,
};
const msgCounts    = new Map();
const aiRateLimits = new Map(); // `ai_${sessionId}_${phone}` ? lastSentMs

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

// -- broadcast — always looks up CURRENT ws, never uses stale reference --------
function broadcast(sessionId, event, data) {
  const session = sessions.get(sessionId);
  const wsId = session?.wsId;
  if (!wsId) return;
  const ws = wsClients.get(wsId);
  if (ws?.readyState === 1) {
    try { ws.send(JSON.stringify({ event, data })); } catch {}
  }
}

// -- Direct send to a specific wsId (used before session is created) ----------
function sendToWs(wsId, event, data) {
  const ws = wsClients.get(wsId);
  if (ws?.readyState === 1) {
    try { ws.send(JSON.stringify({ event, data })); } catch {}
  }
}

function safeRmSessionFiles(sessionId) {
  if (!sessionId) return;
  const dir = path.join(AUTH_DIR, sessionId);
  fs.rmSync(dir, { recursive: true, force: true });
  try { fs.unlinkSync(path.join(AUTH_DIR, `${sessionId}.meta.json`)); } catch {}
}

function closeSessionSocket(sessionId) {
  const session = sessions.get(sessionId);
  if (!session?.sock) return;
  try { session.sock.end?.(); } catch {}
  try { session.sock.ws?.close?.(); } catch {}
}

function nowIso() {
  return new Date().toISOString();
}

function setSessionPhase(sessionId, phase, patch = {}) {
  const session = sessions.get(sessionId);
  if (!session) return null;
  const elapsedMs = Date.now() - (session.startedAt || Date.now());
  Object.assign(session, {
    phase,
    lastEvent: patch.event || phase,
    lastError: patch.error || session.lastError || '',
    lastCloseCode: patch.code || session.lastCloseCode || '',
    updatedAt: nowIso(),
    elapsedMs,
    ...patch,
  });
  const printable = {
    sessionId,
    phase: session.phase,
    event: session.lastEvent,
    code: session.lastCloseCode || '',
    elapsedMs,
  };
  if (patch.error) printable.error = patch.error;
  console.log('[wa:state]', JSON.stringify(printable));
  return session;
}

function emitSessionStatus(sessionId, status, message = '', patch = {}) {
  const session = setSessionPhase(sessionId, status, { event: status, ...patch });
  broadcast(sessionId, 'session_status', {
    sessionId,
    requestId: session?.requestId || null,
    status,
    message,
    phase: status,
    elapsedMs: session?.elapsedMs || 0,
  });
}

function emitAuthenticating(sessionId, source = 'authenticating') {
  const session = sessions.get(sessionId);
  if (!session || session.connected || session.phase === 'connected') return;
  if (!session.qrEmittedAt) return;
  setSessionPhase(sessionId, 'authenticating', { event: source, scannedAt: session.scannedAt || Date.now() });
  broadcast(sessionId, 'authenticated', {
    sessionId,
    requestId: session.requestId || null,
    status: 'authenticating',
    phase: 'login',
    source,
  });
}

function upsertSessionChat(sessionId, chat) {
  const jid = String(chat?.id || chat?.jid || chat?.remoteJid || chat?.key?.remoteJid || chat?.from || '').toLowerCase();
  if (!sessionId || !jid) return;
  const store = chatStores.get(sessionId) || new Map();
  const prev = store.get(jid) || {};
  store.set(jid, {
    ...prev,
    ...chat,
    id: jid,
    jid,
    name: chat?.name || chat?.pushName || chat?.notify || prev.name || jid.split('@')[0],
    updatedAt: Date.now(),
  });
  chatStores.set(sessionId, store);
}

function getSessionChats(sessionId) {
  const store = chatStores.get(sessionId);
  if (!store) return [];
  return Array.from(store.values())
    .sort((a, b) => Number(b.updatedAt || b.conversationTimestamp || 0) - Number(a.updatedAt || a.conversationTimestamp || 0))
    .slice(0, 100);
}

// -- createSession -------------------------------------------------------------
async function createSession(sessionId, wsId, userId = null, options = {}) {
  const { allowQr = !!wsId, restore = false, requestId = null } = options;
  const sessionDir = path.join(AUTH_DIR, sessionId);
  fs.mkdirSync(sessionDir, { recursive: true });

  let version = WA_WEB_VERSION_OVERRIDE.length === 3 ? WA_WEB_VERSION_OVERRIDE : FALLBACK_VERSION;
  if (WA_WEB_VERSION_OVERRIDE.length !== 3) {
    try {
      const result = await Promise.race([
        fetchLatestBaileysVersion(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('WA version lookup timed out')), 8000)),
      ]);
      if (result?.version) version = result.version;
    } catch (e) {
      console.log('[wa] Using fallback WA version:', FALLBACK_VERSION.join('.'), e.message || '');
    }
  } else {
    console.log('[wa] Using WA web version override:', version.join('.'));
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
  sessions.set(sessionId, {
    sock,
    wsId,
    userId,
    connected: false,
    allowQr,
    restore,
    requestId,
    connecting: true,
    phase: restore ? 'restoring' : 'connecting',
    startedAt: Date.now(),
    updatedAt: nowIso(),
    lastEvent: 'create_session',
    lastError: '',
    lastCloseCode: '',
    version: version.join('.'),
    qrEmittedAt: 0,
    scannedAt: 0,
  });
  setSessionPhase(sessionId, restore ? 'restoring' : 'connecting', { event: 'create_session' });

  sock.ev.on('creds.update', (...args) => {
    emitAuthenticating(sessionId, 'creds_update');
    return saveCreds(...args);
  });

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (connection) {
      if (connection === 'connecting') emitAuthenticating(sessionId, 'connection_connecting');
      else setSessionPhase(sessionId, connection, { event: `connection_${connection}` });
    }

    if (qr) {
      const current = sessions.get(sessionId);
      if (!current?.allowQr || !current?.wsId) {
        console.log('[wa] Stale QR ignored and session cleaned:', sessionId);
        closeSessionSocket(sessionId);
        sessions.delete(sessionId);
        safeRmSessionFiles(sessionId);
        return;
      }
      try {
        current.qrEmittedAt = Date.now();
        current.phase = 'qr_ready';
        current.lastEvent = 'qr';
        current.updatedAt = nowIso();
        const qrDataUrl = await QRCode.toDataURL(qr, { width: 260, margin: 2, color: { dark: '#000000', light: '#ffffff' } });
        // FIX BUG 1: broadcast uses current ws via sessionId lookup
        broadcast(sessionId, 'qr', { qr: qrDataUrl, sessionId, requestId: current.requestId || requestId || null, status: 'qr_ready', phase: 'login' });
        emitSessionStatus(sessionId, 'qr_ready', 'Scan the QR code to connect.');
        console.log('[wa] QR emitted for', sessionId);
      } catch (e) {
        setSessionPhase(sessionId, 'failed', { event: 'qr_error', error: e.message });
        broadcast(sessionId, 'error', { sessionId, requestId: current.requestId || requestId || null, phase: 'login', message: 'QR generation failed: ' + e.message });
      }
    }

    if (connection === 'open') {
      const s = sessions.get(sessionId);
      if (s) {
        s.connected = true;
        s.connecting = false;
        s.allowQr = false;
      }
      setSessionPhase(sessionId, 'connected', { event: 'connection_open' });
      const user = sock.user;
      // Persist meta so this session survives a Railway restart
      try {
        const metaPath = path.join(AUTH_DIR, `${sessionId}.meta.json`);
        fs.writeFileSync(metaPath, JSON.stringify({ sessionId, userId: s?.userId || userId, connectedAt: Date.now() }));
      } catch {}
      broadcast(sessionId, 'connected', {
        sessionId,
        requestId: s?.requestId || requestId || null,
        phone: user?.id?.split(':')[0] || '',
        name: user?.name || '',
        status: 'connected',
        phase: 'login',
      });
      console.log('[wa] Connected:', sessionId);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      const loggedOut = code === DisconnectReason.loggedOut;
      const cur = sessions.get(sessionId);
      setSessionPhase(sessionId, loggedOut ? 'logged_out' : 'closed', {
        event: 'connection_close',
        code: code || '',
        error: lastDisconnect?.error?.message || '',
      });
      broadcast(sessionId, 'disconnected', { sessionId, requestId: cur?.requestId || requestId || null, loggedOut, code, phase: loggedOut ? 'login' : 'socket' });

      if (loggedOut) {
        sessions.delete(sessionId);
        chatStores.delete(sessionId);
        safeRmSessionFiles(sessionId);
      } else if (!cur?.wsId || cur?.restore) {
        console.log('[wa] Stale restored session closed and cleaned:', sessionId, code || '');
        sessions.delete(sessionId);
        chatStores.delete(sessionId);
        safeRmSessionFiles(sessionId);
      } else if (cur?.phase === 'authenticating' || cur?.scannedAt) {
        console.log('[wa] Auth handshake closed after scan; keeping session for diagnostics:', sessionId, code || '');
        setTimeout(() => {
          const latest = sessions.get(sessionId);
          if (latest && !latest.connected && latest.phase !== 'connected') {
            setSessionPhase(sessionId, 'failed', { event: 'auth_timeout', code: latest.lastCloseCode || code || '', error: 'QR scanned but WhatsApp did not confirm the session.' });
            broadcast(sessionId, 'error', {
              sessionId,
              requestId: latest.requestId || requestId || null,
              phase: 'login',
              message: 'QR scanned but WhatsApp did not confirm the session. Generate a fresh code and try again.',
            });
          }
        }, QR_AUTH_TIMEOUT_MS);
      } else {
        // FIX BUG 3: reconnect uses current wsId from sessions map, not stale ws
        const currentWsId = cur?.wsId || wsId;
        const currentUserId = cur?.userId || userId;
        const currentRequestId = cur?.requestId || requestId || null;
        sessions.delete(sessionId);
        setTimeout(() => createSession(sessionId, currentWsId, currentUserId, {
          allowQr: false,
          restore: false,
          requestId: currentRequestId,
        }), 4000);
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
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
    for (const msg of msgs) {
      upsertSessionChat(sessionId, {
        id: msg.from,
        jid: msg.from,
        name: msg.name || msg.from,
        lastMessage: msg,
        conversationTimestamp: msg.timestamp,
        updatedAt: Date.now(),
      });
    }
    broadcast(sessionId, 'messages', { sessionId, messages: msgs });

    // AI auto-reply
    const currentSession = sessions.get(sessionId);
    if (!currentSession?.userId) return;
    const MAIN_APP  = process.env.MAIN_APP_URL         || 'https://clickdz.cloud';
    const WA_SECRET = process.env.WA_WEBHOOK_SECRET    || 'bdz-wa-secret-2025';

    for (const m of messages) {
      if (m.key.fromMe) continue;
      const text = m.message?.conversation || m.message?.extendedTextMessage?.text || '';
      if (!text) continue;
      const phone = (m.key.remoteJid || '').split('@')[0];
      if (!phone) continue;
      const aiKey = `ai_${sessionId}_${phone}`;
      if (Date.now() - (aiRateLimits.get(aiKey) || 0) < 10000) continue;
      try {
        const res = await fetch(`${MAIN_APP}/api/wa/ai-reply`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-wa-secret': WA_SECRET },
          body: JSON.stringify({
            userId: currentSession.userId,
            message: text,
            contactName: m.pushName || '',
            contactPhone: phone,
          }),
          signal: AbortSignal.timeout(20000),
        });
        const data = await res.json();
        if (data.ok && data.reply) {
          await new Promise(r => setTimeout(r, 5000));
          await sock.sendMessage(m.key.remoteJid, { text: data.reply });
          aiRateLimits.set(aiKey, Date.now());
          recordSent(sessionId);
        }
      } catch (e) {
        console.error('[ai-reply]', e.message);
      }
    }
  });

  sock.ev.on('chats.upsert', chats => {
    for (const chat of chats || []) upsertSessionChat(sessionId, chat);
    broadcast(sessionId, 'chats', { sessionId, chats: getSessionChats(sessionId) });
  });

  sock.ev.on('contacts.upsert', contacts => {
    broadcast(sessionId, 'contacts', { sessionId, contacts: contacts.slice(0, 500) });
  });
}

// -- Restore persisted sessions after Railway restart -------------------------
async function restorePersistedSessions() {
  let files;
  try { files = fs.readdirSync(AUTH_DIR); } catch { return; }
  const metas = files.filter(f => f.endsWith('.meta.json'));
  if (metas.length === 0) return;
  console.log(`[wa] Restoring ${metas.length} persisted session(s)…`);
  for (let i = 0; i < metas.length; i++) {
    const metaPath = path.join(AUTH_DIR, metas[i]);
    try {
      const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
      if (!meta.sessionId) { fs.unlinkSync(metaPath); continue; }
      const sessionDir = path.join(AUTH_DIR, meta.sessionId);
      if (!fs.existsSync(sessionDir)) { fs.unlinkSync(metaPath); continue; }
      if (sessions.has(meta.sessionId)) continue;
      await new Promise(r => setTimeout(r, 1000 * i));
      createSession(meta.sessionId, null, meta.userId || null, { allowQr: false, restore: true });
    } catch (e) {
      console.error('[wa] Restore failed for', metas[i], e.message);
    }
  }
}

// -- WebSocket server ----------------------------------------------------------
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
    const { action, sessionId, userId: msgUserId, data } = msg;
    const requestId = msg.requestId || data?.requestId || null;
    const restoreRequested = !!(msg.restore || data?.restore);
    if (!action) return;

    switch (action) {
      case 'connect': {
        if (!sessionId) break;
        const existing = sessions.get(sessionId);
        if (existing) {
          existing.wsId = wsId;
          if (msgUserId) existing.userId = msgUserId;
          existing.requestId = requestId || existing.requestId || null;
          if (existing.connected) {
            const user = existing.sock?.user;
            sendToWs(wsId, 'connected', {
              sessionId,
              requestId: existing.requestId,
              phone: user?.id?.split(':')[0] || '',
              name: user?.name || '',
            });
          } else {
            closeSessionSocket(sessionId);
            sessions.delete(sessionId);
            safeRmSessionFiles(sessionId);
            sendToWs(wsId, 'connecting', { sessionId, requestId });
            try {
              await createSession(sessionId, wsId, msgUserId || existing.userId || null, { allowQr: !restoreRequested, restore: restoreRequested, requestId });
            } catch (e) {
              sendToWs(wsId, 'error', { sessionId, requestId, phase: 'login', message: e.message });
            }
          }
        } else {
          sendToWs(wsId, 'connecting', { sessionId, requestId });
          try {
            await createSession(sessionId, wsId, msgUserId || null, { allowQr: !restoreRequested, restore: restoreRequested, requestId });
          } catch (e) {
            sendToWs(wsId, 'error', { sessionId, requestId, phase: 'login', message: e.message });
          }
        }
        break;
      }

      case 'sync_chats': {
        const s = sessions.get(sessionId);
        if (!s?.sock || !s.connected) {
          sendToWs(wsId, 'error', { sessionId, requestId, phase: 'sync', message: 'Session is not ready for chat sync' });
          break;
        }
        sendToWs(wsId, 'chats', { sessionId, requestId, chats: getSessionChats(sessionId), phase: 'sync' });
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
          chatStores.delete(sessionId);
          const dir = path.join(AUTH_DIR, sessionId);
          fs.rmSync(dir, { recursive: true, force: true });
          try { fs.unlinkSync(path.join(AUTH_DIR, `${sessionId}.meta.json`)); } catch {}
        }
        sendToWs(wsId, 'logged_out', { sessionId });
        break;
      }

      case 'delete_session': {
        const s = sessions.get(sessionId);
        if (s?.sock) { try { await s.sock.logout(); } catch {} }
        sessions.delete(sessionId);
        chatStores.delete(sessionId);
        const delDir = path.join(AUTH_DIR, sessionId);
        fs.rmSync(delDir, { recursive: true, force: true });
        try { fs.unlinkSync(path.join(AUTH_DIR, `${sessionId}.meta.json`)); } catch {}
        sendToWs(wsId, 'session_deleted', { sessionId });
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

// -- HTTP routes ---------------------------------------------------------------
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
    id, connected: s.connected, phase: s.phase, wsId: s.wsId,
  }));
  res.json(list);
});

app.get('/debug/sessions', (req, res) => {
  const secret = process.env.WA_DEBUG_SECRET || process.env.CRON_SECRET || '';
  const provided = String(req.headers['x-debug-secret'] || req.query.secret || '');
  if (!secret || provided !== secret) return res.status(404).json({ ok: false });
  const list = Array.from(sessions.entries()).map(([id, s]) => ({
    id,
    connected: !!s.connected,
    phase: s.phase || 'unknown',
    lastEvent: s.lastEvent || '',
    lastCloseCode: s.lastCloseCode || '',
    lastError: s.lastError || '',
    elapsedMs: s.elapsedMs || (Date.now() - (s.startedAt || Date.now())),
    qrAgeMs: s.qrEmittedAt ? Date.now() - s.qrEmittedAt : 0,
    scannedAgeMs: s.scannedAt ? Date.now() - s.scannedAt : 0,
    version: s.version || '',
    restore: !!s.restore,
    allowQr: !!s.allowQr,
    chats: getSessionChats(id).length,
  }));
  res.json({ ok: true, sessions: list, clients: wsClients.size, authDir: AUTH_DIR });
});

// -- Start ---------------------------------------------------------------------
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`[wa-server] Listening on :${PORT}`);
  setTimeout(restorePersistedSessions, 3000);
});
