const Database = require('better-sqlite3');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '../data/rsi_scanner.db');
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    tag TEXT DEFAULT 'personal',
    is_master INTEGER DEFAULT 0,
    is_enabled INTEGER DEFAULT 1,
    api_key_enc TEXT DEFAULT '',
    api_secret_enc TEXT DEFAULT '',
    size_ratio REAL DEFAULT 5,
    balance REAL DEFAULT 0,
    balance_at INTEGER,
    stats TEXT DEFAULT '{"opens":0,"closes":0,"wins":0,"losses":0,"tot":0}'
  );

  CREATE TABLE IF NOT EXISTS open_trades (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL DEFAULT 0,
    open_time TEXT,
    open_ts INTEGER,
    sl TEXT, tp1 TEXT, leverage TEXT, margin TEXT, label TEXT,
    executed INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS closed_trades (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL DEFAULT 0,
    exit_price REAL DEFAULT 0,
    pct REAL DEFAULT 0,
    result TEXT,
    open_time TEXT, close_time TEXT,
    open_ts INTEGER, close_ts INTEGER,
    sl TEXT, tp1 TEXT, leverage TEXT, margin TEXT, label TEXT
  );

  CREATE TABLE IF NOT EXISTS dca_orders (
    id INTEGER PRIMARY KEY,
    sym TEXT NOT NULL,
    price REAL NOT NULL,
    pct REAL NOT NULL,
    side TEXT NOT NULL,
    acc_ids TEXT DEFAULT '[]',
    lev INTEGER DEFAULT 20,
    done INTEGER DEFAULT 0,
    created_at TEXT
  );

  CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY,
    symbol TEXT, type TEXT, label TEXT, color TEXT,
    emoji TEXT, rsi TEXT, time TEXT, mode TEXT, side TEXT
  );
`);

// ── تشفير API Keys ──────────────────────────────
function getEncKey() {
  const s = process.env.JWT_SECRET || 'rsi_scanner_secret_2024';
  return crypto.createHash('sha256').update(s).digest();
}

function encrypt(text) {
  if (!text) return '';
  try {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', getEncKey(), iv);
    const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    return iv.toString('hex') + ':' + tag.toString('hex') + ':' + enc.toString('hex');
  } catch (e) { return ''; }
}

function decrypt(enc) {
  if (!enc) return '';
  try {
    const parts = enc.split(':');
    if (parts.length !== 3) return enc; // plain text fallback
    const [ivH, tagH, dataH] = parts;
    const decipher = crypto.createDecipheriv('aes-256-gcm', getEncKey(), Buffer.from(ivH, 'hex'));
    decipher.setAuthTag(Buffer.from(tagH, 'hex'));
    return Buffer.concat([decipher.update(Buffer.from(dataH, 'hex')), decipher.final()]).toString('utf8');
  } catch (e) { return ''; }
}

// ── الإعدادات ────────────────────────────────────
function saveSettings(settings) {
  db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
    .run('main', JSON.stringify(settings));
}

function loadSettings(defaults) {
  const row = db.prepare('SELECT value FROM settings WHERE key = ?').get('main');
  if (!row) return { ...defaults };
  try { return { ...defaults, ...JSON.parse(row.value) }; } catch (e) { return { ...defaults }; }
}

// ── الحسابات ─────────────────────────────────────
function saveAccounts(accounts) {
  const del = db.prepare('DELETE FROM accounts');
  const ins = db.prepare(`INSERT OR REPLACE INTO accounts
    (id, name, tag, is_master, is_enabled, api_key_enc, api_secret_enc, size_ratio, balance, balance_at, stats)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  db.transaction((accs) => {
    del.run();
    for (const a of accs) {
      ins.run(
        a.id, a.name, a.tag || 'personal',
        a.isMaster ? 1 : 0, a.isEnabled !== false ? 1 : 0,
        encrypt(a.apiKey || ''), encrypt(a.apiSecret || ''),
        parseFloat(a.sizeRatio) || 5,
        parseFloat(a.liveBalance ?? a.balance) || 0,
        a.balanceAt || null,
        JSON.stringify(a.stats || { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 })
      );
    }
  })(accounts);
}

function loadAccounts() {
  return db.prepare('SELECT * FROM accounts').all().map(r => ({
    id: r.id, name: r.name, tag: r.tag,
    isMaster: r.is_master === 1, isEnabled: r.is_enabled === 1,
    apiKey: decrypt(r.api_key_enc), apiSecret: decrypt(r.api_secret_enc),
    sizeRatio: r.size_ratio, balance: r.balance, balanceAt: r.balance_at,
    stats: (() => { try { return JSON.parse(r.stats); } catch (e) { return { opens:0,closes:0,wins:0,losses:0,tot:0 }; } })(),
    livePositions: [], liveBalance: null, apiOk: undefined, closedTrades: []
  }));
}

// ── الصفقات المفتوحة ──────────────────────────────
function saveOpenTrades(trades) {
  const del = db.prepare('DELETE FROM open_trades');
  const ins = db.prepare(`INSERT OR REPLACE INTO open_trades
    (id, symbol, side, entry_price, open_time, open_ts, sl, tp1, leverage, margin, label, executed)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  db.transaction((ts) => {
    del.run();
    for (const t of ts) ins.run(t.id, t.symbol, t.side, t.entryPrice, t.openTime, t.openTs, t.sl, t.tp1, t.leverage, t.margin, t.label, t.executed ? 1 : 0);
  })(trades);
}

function loadOpenTrades() {
  return db.prepare('SELECT * FROM open_trades').all().map(r => ({
    id: r.id, symbol: r.symbol, side: r.side, entryPrice: r.entry_price,
    openTime: r.open_time, openTs: r.open_ts, sl: r.sl, tp1: r.tp1,
    leverage: r.leverage, margin: r.margin, label: r.label, executed: r.executed === 1
  }));
}

// ── الصفقات المغلقة ───────────────────────────────
function saveClosedTrade(t) {
  db.prepare(`INSERT OR REPLACE INTO closed_trades
    (id, symbol, side, entry_price, exit_price, pct, result, open_time, close_time, open_ts, close_ts, sl, tp1, leverage, margin, label)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(t.id, t.symbol, t.side, t.entryPrice, t.exitPrice, t.pct, t.result,
      t.openTime, t.exitTime, t.openTs, t.closeTs, t.sl, t.tp1, t.leverage, t.margin, t.label);
  // احتفظ بآخر 500 فقط
  db.prepare('DELETE FROM closed_trades WHERE id NOT IN (SELECT id FROM closed_trades ORDER BY close_ts DESC LIMIT 500)').run();
}

function loadClosedTrades() {
  return db.prepare('SELECT * FROM closed_trades ORDER BY close_ts DESC LIMIT 500').all().map(r => ({
    id: r.id, symbol: r.symbol, side: r.side, entryPrice: r.entry_price,
    exitPrice: r.exit_price, pct: r.pct, result: r.result,
    openTime: r.open_time, exitTime: r.close_time, openTs: r.open_ts, closeTs: r.close_ts,
    sl: r.sl, tp1: r.tp1, leverage: r.leverage, margin: r.margin, label: r.label
  }));
}

// ── أوامر DCA ─────────────────────────────────────
function saveDcaOrders(orders) {
  const del = db.prepare('DELETE FROM dca_orders');
  const ins = db.prepare(`INSERT OR REPLACE INTO dca_orders
    (id, sym, price, pct, side, acc_ids, lev, done, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  db.transaction((os) => {
    del.run();
    for (const o of os) ins.run(o.id, o.sym, o.price, o.pct, o.side, JSON.stringify(o.accIds || []), o.lev || 20, o.done ? 1 : 0, o.createdAt);
  })(orders);
}

function loadDcaOrders() {
  return db.prepare('SELECT * FROM dca_orders').all().map(r => ({
    id: r.id, sym: r.sym, price: r.price, pct: r.pct, side: r.side,
    accIds: (() => { try { return JSON.parse(r.acc_ids); } catch (e) { return []; } })(),
    lev: r.lev, done: r.done === 1, createdAt: r.created_at
  }));
}

// ── الإشارات ──────────────────────────────────────
function saveAlert(a) {
  db.prepare(`INSERT OR REPLACE INTO alerts (id, symbol, type, label, color, emoji, rsi, time, mode, side)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(a.id, a.symbol, a.type, a.label, a.color, a.emoji, a.rsi, a.time, a.mode, a.side);
  db.prepare('DELETE FROM alerts WHERE id NOT IN (SELECT id FROM alerts ORDER BY id DESC LIMIT 200)').run();
}

function loadAlerts() {
  return db.prepare('SELECT * FROM alerts ORDER BY id DESC LIMIT 200').all().map(r => ({
    id: r.id, symbol: r.symbol, type: r.type, label: r.label, color: r.color,
    emoji: r.emoji, rsi: r.rsi, time: r.time, mode: r.mode, side: r.side
  }));
}

module.exports = {
  saveSettings, loadSettings,
  saveAccounts, loadAccounts,
  saveOpenTrades, loadOpenTrades,
  saveClosedTrade, loadClosedTrades,
  saveDcaOrders, loadDcaOrders,
  saveAlert, loadAlerts,
};
