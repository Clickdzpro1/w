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
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors({ origin: process.env.ALLOWED_ORIGIN || '*' }));
app.use(express.json());

const server = createServer(app);
const wss = new WebSocketServer({ server });

// Sessions: Map<sessionId, { socket: WASocket, ws: WebSocket, state }>
const sessions = new Map();
const AUTH_DIR = path.join(__dirname, 'auth_sessions');
if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });

// Anti-ban rate limiting
const MESSAGE_LIMITS = {
  minDelayMs: 5000,          // 5 seconds between messages
  maxPerDay: 200,            // max 200 messages/day per account
  maxNewContactsPerDay: 50,  // max 50 new contacts/day
  cooldownNewAccount: 48,    // 48 hours before automation for new accounts
};

const messageCounts = new Map(); // sessionId -> { count, date, lastSent }

function rateCheck(sessionId) {
  const now = new Date();
  const today = now.toDateString();
  const record = messageCounts.get(sessionId) || { count: 0, date: today, lastSent: 0 };

  if (record.date !== today) {
    record.count = 0;
    record.date = today;
  }

  const timeSinceLast = Date.now() - record.lastSent;
  if (timeSinceLast < MESSAGE_LIMITS.minDelayMs) {
    return { allowed: false, reason: 'Too fast — minimum 5 second delay between messages', waitMs: MESSAGE_LIMITS.minDelayMs - timeSinceLast };
  }
  if (record.count >= MESSAGE_LIMITS.maxPerDay) {
    return { allowed: false, reason: 'Daily limit reached (200 messages/day). Resets at midnight.' };
  }

  return { allowed: true, record };
}

function recordSent(sessionId) {
  const record = messageCounts.get(sessionId) || { count: 0, date: new Date().toDateString(), lastSent: 0 };
  record.count++;
  record.lastSent = Date.now();
  messageCounts.set(sessionId, record);
}

function broadcast(ws, event, data) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify({ event, data }));
  }
}

async function createSession(sessionId, ws) {
  const sessionDir = path.join(AUTH_DIR, sessionId);
  fs.mkdirSync(sessionDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    browser: ['Business OS', 'Chrome', '120.0.0'],
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
  });

  sessions.set(sessionId, { sock, ws, connected: false });

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      const qrDataUrl = await QRCode.toDataURL(qr, { width: 280, margin: 2 });
      broadcast(ws, 'qr', { qr: qrDataUrl, sessionId });
    }

    if (connection === 'open') {
      const session = sessions.get(sessionId);
      if (session) session.connected = true;
      const user = sock.user;
      broadcast(ws, 'connected', {
        sessionId,
        phone: user?.id?.split(':')[0] || 'Unknown',
        name: user?.name || '',
      });
      await sendInitialData(sessionId, ws, sock);
    }

    if (connection === 'close') {
      const reason = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = reason !== DisconnectReason.loggedOut;
      broadcast(ws, 'disconnected', { sessionId, reason, shouldReconnect });

      if (shouldReconnect) {
        setTimeout(() => createSession(sessionId, ws), 3000);
      } else {
        sessions.delete(sessionId);
        // Clean auth state if logged out
        fs.rmSync(path.join(AUTH_DIR, sessionId), { recursive: true, force: true });
      }
    }
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    const msgs = messages.map(m => ({
      id: m.key.id,
      from: m.key.remoteJid,
      fromMe: m.key.fromMe,
      body: m.message?.conversation || m.message?.extendedTextMessage?.text || '',
      timestamp: m.messageTimestamp,
      pushName: m.pushName,
    }));
    broadcast(ws, 'messages', { sessionId, messages: msgs });
  });

  sock.ev.on('chats.upsert', (chats) => {
    broadcast(ws, 'chats', { sessionId, chats: chats.slice(0, 50) });
  });

  sock.ev.on('contacts.upsert', (contacts) => {
    broadcast(ws, 'contacts', { sessionId, contacts: contacts.slice(0, 100) });
  });

  return sock;
}

async function sendInitialData(sessionId, ws, sock) {
  try {
    const chats = await sock.groupFetchAllParticipating().catch(() => ({}));
    broadcast(ws, 'initial_data', { sessionId, chats: [] });
  } catch {}
}

wss.on('connection', (ws) => {
  console.log('Client connected');

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const { action, sessionId, data } = msg;

    switch (action) {
      case 'connect':
        if (!sessions.has(sessionId)) {
          broadcast(ws, 'connecting', { sessionId });
          await createSession(sessionId, ws);
        } else {
          const s = sessions.get(sessionId);
          if (s.connected) broadcast(ws, 'connected', { sessionId });
        }
        break;

      case 'send_message': {
        const session = sessions.get(sessionId);
        if (!session?.sock) { broadcast(ws, 'error', { message: 'Not connected' }); break; }

        const check = rateCheck(sessionId);
        if (!check.allowed) { broadcast(ws, 'rate_limited', check); break; }

        try {
          await session.sock.sendMessage(data.to, { text: data.text });
          recordSent(sessionId);
          broadcast(ws, 'message_sent', { sessionId, to: data.to, text: data.text });
        } catch (e) {
          broadcast(ws, 'error', { message: e.message });
        }
        break;
      }

      case 'get_messages': {
        const session = sessions.get(sessionId);
        if (!session?.sock) break;
        try {
          const msgs = await session.sock.loadMessages(data.jid, 20);
          broadcast(ws, 'message_history', { sessionId, jid: data.jid, messages: msgs });
        } catch {}
        break;
      }

      case 'get_contacts': {
        const session = sessions.get(sessionId);
        if (!session?.sock) break;
        try {
          const contacts = session.sock.store?.contacts || {};
          broadcast(ws, 'contacts', { sessionId, contacts: Object.values(contacts).slice(0, 200) });
        } catch {}
        break;
      }

      case 'disconnect': {
        const session = sessions.get(sessionId);
        if (session?.sock) {
          await session.sock.logout().catch(() => {});
          sessions.delete(sessionId);
        }
        broadcast(ws, 'disconnected', { sessionId });
        break;
      }

      case 'ping':
        broadcast(ws, 'pong', { ts: Date.now() });
        break;
    }
  });

  ws.on('close', () => console.log('Client disconnected'));
  ws.on('error', console.error);
});

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    sessions: sessions.size,
    limits: MESSAGE_LIMITS,
    ts: Date.now()
  });
});

// Sessions list (admin)
app.get('/sessions', (req, res) => {
  const list = Array.from(sessions.entries()).map(([id, s]) => ({
    id, connected: s.connected
  }));
  res.json(list);
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`WhatsApp WS server running on port ${PORT}`);
  console.log(`Anti-ban limits: ${JSON.stringify(MESSAGE_LIMITS)}`);
});
