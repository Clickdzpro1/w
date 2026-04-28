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
  downloadMediaMessage,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

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
const messageStores = new Map(); // sessionId -> Map(jid -> Map(messageId -> message))
const contactStores = new Map(); // sessionId -> Map(jid -> contact)
const mediaStores = new Map(); // sessionId -> Map(messageId -> { raw, token, mimetype, kind })

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
const MAX_STORED_CHATS = Number(process.env.WA_MAX_STORED_CHATS || 500);
const MAX_STORED_MESSAGES_PER_CHAT = Number(process.env.WA_MAX_STORED_MESSAGES_PER_CHAT || 120);
const SYNC_FULL_HISTORY = String(process.env.WA_SYNC_FULL_HISTORY || 'true') !== 'false';
const PUBLIC_BASE_URL = (
  process.env.PUBLIC_BASE_URL
  || process.env.WA_PUBLIC_URL
  || process.env.WA_SERVER_URL
  || process.env.NEXT_PUBLIC_WA_SERVER_URL
  || process.env.RAILWAY_STATIC_URL
  || (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : '')
).replace(/\/$/, '');

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
  broadcast(sessionId, 'authenticating', {
    sessionId,
    requestId: session.requestId || null,
    status: 'authenticating',
    phase: 'login',
    source,
  });
}

function unwrapMessageContent(message = {}) {
  let content = message || {};
  for (let i = 0; i < 4; i++) {
    if (content.ephemeralMessage?.message) content = content.ephemeralMessage.message;
    else if (content.viewOnceMessage?.message) content = content.viewOnceMessage.message;
    else if (content.viewOnceMessageV2?.message) content = content.viewOnceMessageV2.message;
    else if (content.documentWithCaptionMessage?.message) content = content.documentWithCaptionMessage.message;
    else break;
  }
  return content || {};
}

function messageBody(message = {}) {
  const content = unwrapMessageContent(message);
  return content.conversation
    || content.extendedTextMessage?.text
    || content.imageMessage?.caption
    || content.videoMessage?.caption
    || content.documentMessage?.caption
    || content.documentMessage?.fileName
    || content.contactMessage?.displayName
    || content.contactsArrayMessage?.displayName
    || content.locationMessage?.name
    || content.liveLocationMessage?.caption
    || content.pollCreationMessage?.name
    || content.reactionMessage?.text
    || content.buttonsResponseMessage?.selectedDisplayText
    || content.listResponseMessage?.title
    || '';
}

function messageKind(message = {}) {
  const content = unwrapMessageContent(message);
  if (content.conversation || content.extendedTextMessage) return 'text';
  if (content.imageMessage) return 'image';
  if (content.videoMessage) return 'video';
  if (content.audioMessage) return content.audioMessage.ptt ? 'voice' : 'audio';
  if (content.documentMessage) return 'document';
  if (content.stickerMessage) return 'sticker';
  if (content.contactMessage || content.contactsArrayMessage) return 'contact';
  if (content.locationMessage || content.liveLocationMessage) return 'location';
  if (content.pollCreationMessage) return 'poll';
  if (content.reactionMessage) return 'reaction';
  return 'media';
}

function createMediaToken() {
  return crypto.randomBytes(18).toString('hex');
}

function mediaPublicUrl(sessionId, messageId, token) {
  const urlPath = `/media/${encodeURIComponent(sessionId)}/${encodeURIComponent(messageId)}?token=${encodeURIComponent(token)}`;
  return PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}${urlPath}` : urlPath;
}

function firstUrl(text = '') {
  const match = String(text || '').match(/https?:\/\/[^\s<>"']+/i);
  return match ? match[0] : '';
}

function mediaInfo(message = {}) {
  const content = unwrapMessageContent(message);
  const variants = [
    ['image', content.imageMessage],
    ['video', content.videoMessage],
    [content.audioMessage?.ptt ? 'voice' : 'audio', content.audioMessage],
    ['document', content.documentMessage],
    ['sticker', content.stickerMessage],
  ];
  const picked = variants.find(([, value]) => value);
  const ext = content.extendedTextMessage;
  const text = ext?.text || content.conversation || '';
  const linkUrl = ext?.matchedText || ext?.canonicalUrl || firstUrl(text);
  const linkPreview = linkUrl ? {
    url: linkUrl,
    title: ext?.title || '',
    description: ext?.description || '',
  } : null;

  if (!picked) return { media: null, linkPreview };

  const [kind, value] = picked;
  const fileName = value.fileName || value.title || `${kind}-${Date.now()}`;
  return {
    media: {
      kind,
      mimetype: value.mimetype || (kind === 'sticker' ? 'image/webp' : 'application/octet-stream'),
      fileName,
      caption: value.caption || '',
      seconds: value.seconds || value.duration || null,
      width: value.width || null,
      height: value.height || null,
      size: value.fileLength ? Number(value.fileLength) : null,
    },
    linkPreview,
  };
}

function jidName(sessionId, jid, fallback = '') {
  const normalized = String(jid || '').toLowerCase();
  const contact = contactStores.get(sessionId)?.get(normalized);
  const label = contact?.name || contact?.notify || contact?.verifiedName || contact?.pushName || '';
  if (label && !looksLikeWaUid(label)) return label;
  return contact?.displayPhone || fallback || normalized.split('@')[0] || 'WhatsApp chat';
}

function looksLikeWaUid(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return true;
  if (/@(s\.whatsapp\.net|c\.us|g\.us|lid)$/i.test(raw)) return true;
  const digits = raw.replace(/\D/g, '');
  return digits.length >= 8 && raw.replace(/[^\d+()\-\s]/g, '') === raw;
}

function displayPhoneFromContact(contact = {}) {
  const raw = contact.phoneNumber || contact.phone || contact.waId || contact.id || contact.jid || '';
  const left = String(raw || '').split('@')[0].split(':')[0].replace(/[^\d+]/g, '');
  const digits = left.replace(/\D/g, '');
  if (!digits || digits.length < 8) return '';
  return left.startsWith('+') ? left : `+${digits}`;
}

function contactAliases(contact = {}) {
  const aliases = [
    contact.id,
    contact.jid,
    contact.lid,
    contact.phoneNumber,
    contact.phone,
    contact.waId,
  ];
  const out = new Set();
  for (const alias of aliases) {
    const raw = String(alias || '').trim().toLowerCase();
    if (!raw) continue;
    out.add(raw);
    const digits = raw.split('@')[0].split(':')[0].replace(/\D/g, '');
    if (digits.length >= 8) out.add(`${digits}@s.whatsapp.net`);
  }
  return Array.from(out);
}

function upsertSessionChat(sessionId, chat) {
  const jid = String(chat?.id || chat?.jid || chat?.remoteJid || chat?.key?.remoteJid || chat?.from || '').toLowerCase();
  if (!sessionId || !jid) return;
  const store = chatStores.get(sessionId) || new Map();
  const prev = store.get(jid) || {};
  const contact = contactStores.get(sessionId)?.get(jid);
  const candidateName = chat?.name || chat?.pushName || chat?.notify || prev.name || '';
  const resolvedName = looksLikeWaUid(candidateName) ? jidName(sessionId, jid) : candidateName;
  store.set(jid, {
    ...prev,
    ...chat,
    id: jid,
    jid,
    name: resolvedName || jidName(sessionId, jid),
    phone: contact?.displayPhone || prev.phone || '',
    conversationTimestamp: chat?.conversationTimestamp || chat?.timestamp || prev.conversationTimestamp || Date.now(),
    updatedAt: chat?.updatedAt || Date.now(),
  });
  if (store.size > MAX_STORED_CHATS) {
    const oldest = Array.from(store.values())
      .sort((a, b) => Number(a.updatedAt || a.conversationTimestamp || 0) - Number(b.updatedAt || b.conversationTimestamp || 0))
      .slice(0, store.size - MAX_STORED_CHATS);
    oldest.forEach((item) => store.delete(item.jid || item.id));
  }
  chatStores.set(sessionId, store);
}

function getSessionChats(sessionId) {
  const store = chatStores.get(sessionId);
  if (!store) return [];
  return Array.from(store.values())
    .sort((a, b) => Number(b.updatedAt || b.conversationTimestamp || 0) - Number(a.updatedAt || a.conversationTimestamp || 0))
    .slice(0, 100);
}

function normalizeMessage(sessionId, raw = {}) {
  const jid = String(raw.key?.remoteJid || raw.jid || raw.chatId || raw.from || raw.to || '').toLowerCase();
  const id = String(raw.key?.id || raw.id || `${jid}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
  const kind = messageKind(raw.message || raw);
  const body = messageBody(raw.message || raw);
  const info = mediaInfo(raw.message || raw);
  const timestamp = Number(raw.messageTimestamp || raw.timestamp || Date.now());
  return {
    id,
    jid,
    chatId: jid,
    from: jid,
    fromMe: raw.key?.fromMe !== undefined ? !!raw.key.fromMe : !!raw.fromMe,
    body: body || (kind === 'text' ? '' : `${kind[0].toUpperCase()}${kind.slice(1)} message`),
    text: body,
    kind,
    timestamp,
    messageTimestamp: timestamp,
    name: raw.pushName || jidName(sessionId, jid),
    pushName: raw.pushName || jidName(sessionId, jid),
    status: raw.status || 'received',
    media: info.media,
    linkPreview: info.linkPreview,
  };
}

function upsertSessionMessage(sessionId, raw = {}) {
  const msg = normalizeMessage(sessionId, raw);
  if (!sessionId || !msg.jid || !msg.id) return null;
  if (msg.media && raw?.message) {
    const mediaBySession = mediaStores.get(sessionId) || new Map();
    const prev = mediaBySession.get(msg.id) || {};
    const token = prev.token || createMediaToken();
    mediaBySession.set(msg.id, {
      raw,
      token,
      mimetype: msg.media.mimetype,
      kind: msg.media.kind,
      fileName: msg.media.fileName,
    });
    mediaStores.set(sessionId, mediaBySession);
    msg.media = {
      ...msg.media,
      url: mediaPublicUrl(sessionId, msg.id, token),
    };
  }
  const byChat = messageStores.get(sessionId) || new Map();
  const messages = byChat.get(msg.jid) || new Map();
  messages.set(msg.id, { ...(messages.get(msg.id) || {}), ...msg });
  if (messages.size > MAX_STORED_MESSAGES_PER_CHAT) {
    const extra = Array.from(messages.values())
      .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
      .slice(0, messages.size - MAX_STORED_MESSAGES_PER_CHAT);
    extra.forEach((item) => messages.delete(item.id));
  }
  byChat.set(msg.jid, messages);
  messageStores.set(sessionId, byChat);
  upsertSessionChat(sessionId, {
    id: msg.jid,
    jid: msg.jid,
    name: msg.name || jidName(sessionId, msg.jid),
    lastMessage: msg,
    conversationTimestamp: msg.timestamp,
    updatedAt: Date.now(),
  });
  return msg;
}

function getSessionMessages(sessionId, jid, limit = 50) {
  const normalized = String(jid || '').toLowerCase();
  const messages = messageStores.get(sessionId)?.get(normalized);
  if (!messages) return [];
  return Array.from(messages.values())
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
    .slice(-Math.max(1, Math.min(Number(limit) || 50, MAX_STORED_MESSAGES_PER_CHAT)));
}

function upsertSessionContact(sessionId, contact = {}) {
  const aliases = contactAliases(contact);
  const jid = aliases[0];
  if (!sessionId || !jid) return;
  const store = contactStores.get(sessionId) || new Map();
  const enriched = {
    ...contact,
    id: jid,
    displayPhone: displayPhoneFromContact(contact),
  };
  for (const alias of aliases) {
    store.set(alias, { ...(store.get(alias) || {}), ...enriched, id: alias });
  }
  contactStores.set(sessionId, store);
  const chats = chatStores.get(sessionId);
  const name = contact.name || contact.notify || contact.verifiedName || contact.pushName || enriched.displayPhone;
  for (const alias of aliases) {
    const chat = chats?.get(alias);
    if (chat && name) chats.set(alias, { ...chat, name, phone: enriched.displayPhone || chat.phone || '' });
  }
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
      syncFullHistory: SYNC_FULL_HISTORY,
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
        messageStores.delete(sessionId);
        contactStores.delete(sessionId);
        mediaStores.delete(sessionId);
        safeRmSessionFiles(sessionId);
      } else if (!cur?.wsId || cur?.restore) {
        console.log('[wa] Stale restored session closed and cleaned:', sessionId, code || '');
        sessions.delete(sessionId);
        chatStores.delete(sessionId);
        messageStores.delete(sessionId);
        contactStores.delete(sessionId);
        mediaStores.delete(sessionId);
        safeRmSessionFiles(sessionId);
      } else if (code === DisconnectReason.restartRequired || code === 515) {
        const currentWsId = cur?.wsId || wsId;
        const currentUserId = cur?.userId || userId;
        const currentRequestId = cur?.requestId || requestId || null;
        console.log('[wa] Restart required after auth; reconnecting saved session:', sessionId, code || '');
        emitSessionStatus(sessionId, 'authenticating', 'QR scanned. Finalizing WhatsApp session...', { event: 'restart_required', code: code || '' });
        sessions.delete(sessionId);
        setTimeout(() => createSession(sessionId, currentWsId, currentUserId, {
          allowQr: false,
          restore: false,
          requestId: currentRequestId,
        }), 1500);
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

  sock.ev.on('messaging-history.set', ({ chats = [], contacts = [], messages = [] } = {}) => {
    for (const contact of contacts || []) upsertSessionContact(sessionId, contact);
    for (const chat of chats || []) upsertSessionChat(sessionId, chat);
    for (const message of messages || []) upsertSessionMessage(sessionId, message);
    broadcast(sessionId, 'chats', { sessionId, chats: getSessionChats(sessionId), source: 'history_set' });
  });

  sock.ev.on('contacts.update', contacts => {
    for (const contact of contacts || []) upsertSessionContact(sessionId, contact);
  });

  sock.ev.on('messages.upsert', async ({ messages, type } = {}) => {
    const msgs = (messages || []).map(m => upsertSessionMessage(sessionId, m)).filter(Boolean);
    if (msgs.length === 0) return;
    broadcast(sessionId, 'messages', { sessionId, messages: msgs });

    // AI auto-reply
    if (type !== 'notify') return;
    const currentSession = sessions.get(sessionId);
    if (!currentSession?.userId) return;
    const MAIN_APP  = process.env.MAIN_APP_URL         || 'https://clickdz.cloud';
    const WA_SECRET = process.env.WA_WEBHOOK_SECRET    || 'bdz-wa-secret-2025';

    for (const m of messages) {
      if (m.key.fromMe) continue;
      const text = messageBody(m.message || m);
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

  sock.ev.on('chats.update', chats => {
    for (const chat of chats || []) upsertSessionChat(sessionId, chat);
    broadcast(sessionId, 'chats', { sessionId, chats: getSessionChats(sessionId) });
  });

  sock.ev.on('contacts.upsert', contacts => {
    for (const contact of contacts || []) upsertSessionContact(sessionId, contact);
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
          const jid = data?.jid || data?.chatId || data?.to || '';
          const msgs = getSessionMessages(sessionId, jid, data?.count || data?.limit || 50);
          sendToWs(wsId, 'history', { sessionId, jid, messages: msgs });
        } catch (e) {
          sendToWs(wsId, 'error', { sessionId, requestId, phase: 'message', message: 'Could not load message history.' });
        }
        break;
      }

      case 'get_contacts': {
        const s = sessions.get(sessionId);
        if (!s?.sock) break;
        const contacts = Array.from((contactStores.get(sessionId) || new Map()).values()).slice(0, 500);
        sendToWs(wsId, 'contacts', { sessionId, contacts });
        break;
      }

      case 'logout': {
        const s = sessions.get(sessionId);
        if (s?.sock) {
          try { await s.sock.logout(); } catch {}
          sessions.delete(sessionId);
          chatStores.delete(sessionId);
          messageStores.delete(sessionId);
          contactStores.delete(sessionId);
          mediaStores.delete(sessionId);
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
        messageStores.delete(sessionId);
        contactStores.delete(sessionId);
        mediaStores.delete(sessionId);
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
app.get('/media/:sessionId/:messageId', async (req, res) => {
  const { sessionId, messageId } = req.params;
  const token = String(req.query.token || '');
  const record = mediaStores.get(sessionId)?.get(messageId);
  if (!record || !record.token || token !== record.token) {
    return res.status(record ? 403 : 404).json({ ok: false, error: record ? 'forbidden' : 'not_found' });
  }
  const s = sessions.get(sessionId);
  if (!s?.sock) return res.status(503).json({ ok: false, error: 'session_not_ready' });

  try {
    const buffer = await downloadMediaMessage(
      record.raw,
      'buffer',
      {},
      { logger: silentLogger, reuploadRequest: s.sock.updateMediaMessage },
    );
    const filename = String(record.fileName || `${record.kind || 'whatsapp'}-media`).replace(/[^\w.\-() ]+/g, '_').slice(0, 120);
    res.setHeader('Content-Type', record.mimetype || 'application/octet-stream');
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
    return res.send(buffer);
  } catch (e) {
    console.error('[wa-media]', sessionId, messageId, e?.message || e);
    return res.status(500).json({ ok: false, error: 'media_unavailable' });
  }
});

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
  const secret = process.env.WA_DEBUG_SECRET || process.env.CRON_SECRET || process.env.WA_WEBHOOK_SECRET || '';
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
    messageThreads: messageStores.get(id)?.size || 0,
    contacts: contactStores.get(id)?.size || 0,
  }));
  res.json({ ok: true, sessions: list, clients: wsClients.size, authDir: AUTH_DIR });
});

// -- Start ---------------------------------------------------------------------
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`[wa-server] Listening on :${PORT}`);
  setTimeout(restorePersistedSessions, 3000);
});

