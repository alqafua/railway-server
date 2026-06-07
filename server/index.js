require('dotenv').config();
const express = require('express');
const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');
const fetch = require('node-fetch');
const crypto = require('crypto');
const cors = require('cors');
const db = require('./db');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || (() => { console.warn('⚠️ JWT_SECRET not set, using default (insecure)'); return 'rsi_scanner_secret_2024'; })();
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS || '*';
const BASE = 'https://fapi.binance.com';
const WS_BASE = 'wss://fstream.binance.com/stream';

// CORS
app.use(cors({
  origin: ALLOWED_ORIGINS === '*' ? '*' : ALLOWED_ORIGINS.split(','),
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));
app.options('*', cors());

// ══════════════════════════════════════════════
//  STATE
// ══════════════════════════════════════════════
const DEFAULT_SETTINGS = {
  mode: 'SMA', maPeriod: 14, interval: '1h',
  autoSend: false, enableDiv: true, blockOpen: true,
  sigFilters: { ob: true, os: true, conf: true, trail: true },
  cxMargin: 'Cross', cxLev: '20', cxAmt: '1%',
  cxSLon: false, cxSL: '2',
  cxTP1: '3', cxTP1Amt: '50', cxTP2on: false, cxTP2: '6', cxTP2Amt: '50',
  cxTrailTp: 'on', cxTrailPct: '0.5', cxEntryTrail: '0.5%',
  cxToken: process.env.TG_TOKEN || '',
  cxChat: process.env.TG_CHAT || '',
  cxChatClose: process.env.TG_CHAT_CLOSE || '',
  cxEntry2on: true, cxEntry2Dist: '0.2', cxEntry2Amt: '50',
  cxEntry3on: false, cxEntry3Dist: '4', cxEntry3Amt: '50',
  cxBEon: false,
  trSon: false, trSstart: 75, trSgap: 3,
  trLon: false, trLstart: 25, trLgap: 3,
  liqVon: false, liqVmin: 50000000,
  liqOon: false, liqOmin: 10000000,
  revMode: 'candles', revCount: 1, rsiGap: 1,
  dataMode: 'ws', soundEnabled: true,
  maxOpenTrades: 0,
  sigQueueFilters: { ob: true, os: true, conf: true, trail: true },
  ema200TF: '4h',
  ema200FilterOn: false,
  dirFilter: 'all',
};

const STATE = {
  symbols: [],
  symbolData: {},
  symbolMeta: {},
  alerts: [],
  openTrades: [],
  masterOrders: {},
  closedTrades: [],
  copyAccounts: [],
  copyLog: [],
  masterPositions: {},
  settings: { ...DEFAULT_SETTINGS },
  dcaOrders: [],
  pendingOrders: [],     // أوامر Limit/Trailing محفوظة (يدوي + DCA)
  cooldowns: {},
  sentSigs: {},
  copyOn: false,
  waitQueue: [],
  ema200: { value: null, direction: null, btcPrice: null, updatedAt: null },
  rsiPeaks: {},          // إصلاح #1 — كان غير معرَّف
  sysStatus: { ok: true, lastError: null, errorLoc: null, errorTs: null },
};

// ══════════════════════════════════════════════
//  AUTH + RATE LIMIT
// ══════════════════════════════════════════════
const ADMIN_USER = process.env.ADMIN_USER || 'Alqafua';
const ADMIN_PASS_HASH = bcrypt.hashSync(process.env.ADMIN_PASS || '7007', 10);
const USERS = [{ id: 1, username: ADMIN_USER, passwordHash: ADMIN_PASS_HASH }];

const loginAttempts = new Map(); // IP -> { count, lockUntil }

function authMiddleware(req, res, next) {
  const token = (req.headers.authorization?.replace('Bearer ', '')) || req.query.token;
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch (e) { res.status(401).json({ error: 'Invalid token' }); }
}

// ══════════════════════════════════════════════
//  MIDDLEWARE
// ══════════════════════════════════════════════
app.use(express.json());
app.use(express.static(path.join(__dirname, '../client/dist')));
app.use(express.static(path.join(__dirname, '../client/public')));

// ══════════════════════════════════════════════
//  RSI CALCULATIONS
// ══════════════════════════════════════════════
const RSI_P = 14;

function calcRSI(c, p = 14) {
  if (c.length < p + 1) return null;
  let g = 0, l = 0;
  for (let i = 1; i <= p; i++) { const d = c[i] - c[i - 1]; d >= 0 ? g += d : l -= d; }
  let ag = g / p, al = l / p;
  for (let i = p + 1; i < c.length; i++) {
    const d = c[i] - c[i - 1], gi = d >= 0 ? d : 0, li = d < 0 ? -d : 0;
    ag = (ag * (p - 1) + gi) / p; al = (al * (p - 1) + li) / p;
  }
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

function calcRSISeries(c, p = 14) {
  const s = []; if (c.length < p + 1) return s;
  let g = 0, l = 0;
  for (let i = 1; i <= p; i++) { const d = c[i] - c[i - 1]; d >= 0 ? g += d : l -= d; }
  let ag = g / p, al = l / p; s.push(al === 0 ? 100 : 100 - 100 / (1 + ag / al));
  for (let i = p + 1; i < c.length; i++) {
    const d = c[i] - c[i - 1], gi = d >= 0 ? d : 0, li = d < 0 ? -d : 0;
    ag = (ag * (p - 1) + gi) / p; al = (al * (p - 1) + li) / p;
    s.push(al === 0 ? 100 : 100 - 100 / (1 + ag / al));
  }
  return s;
}

function calcSMA(s, p) { if (s.length < p) return null; return s.slice(-p).reduce((a, b) => a + b, 0) / p; }
function calcEMA(s, p) {
  if (s.length < p) return null; const k = 2 / (p + 1); let e = s[0];
  for (let i = 1; i < s.length; i++) e = s[i] * k + e * (1 - k); return e;
}

function computeInd(cls, mode, ma) {
  if (mode === 'RSI') return calcRSI(cls, RSI_P);
  const rs = calcRSISeries(cls, RSI_P);
  return mode === 'SMA' ? calcSMA(rs, ma) : calcEMA(rs, ma);
}

function computeIndSeries(cls, mode, ma) {
  const rs = calcRSISeries(cls, RSI_P);
  if (mode === 'RSI') return rs;
  const out = [];
  if (mode === 'SMA') { for (let i = ma; i <= rs.length; i++) out.push(rs.slice(i - ma, i).reduce((a, b) => a + b, 0) / ma); }
  else { if (rs.length >= ma) { const k = 2 / (ma + 1); let e = rs[0]; out.push(e); for (let i = 1; i < rs.length; i++) { e = rs[i] * k + e * (1 - k); if (i >= ma - 1) out.push(e); } } }
  return out;
}

function checkDiv(cls, ind, type) {
  const n = ind.length; if (n < 15 || cls.length < 15) return false;
  const cl = cls.slice(-n);
  if (type === 'LONG') {
    const ci = ind[n - 1], cp = cl[n - 1];
    for (let i = n - 5; i >= n - 25 && i >= 1; i--) { if (ind[i] < ind[i - 1] && ind[i] < ind[i + 1] && ci > ind[i] && cp < cl[i]) return true; }
  } else {
    const ci = ind[n - 1], cp = cl[n - 1];
    for (let i = n - 5; i >= n - 25 && i >= 1; i--) { if (ind[i] > ind[i - 1] && ind[i] > ind[i + 1] && ci < ind[i] && cp > cl[i]) return true; }
  }
  return false;
}

function detectSignal(pv, cu, cls, id, ed) {
  if (pv === null || cu === null) return null;
  const rm = STATE.settings.revMode, rv = parseInt(STATE.settings.revCount) || 1;
  if (rm === 'candles' && rv > 1 && id.length >= rv) {
    const n = id.slice(-rv);
    if (!n.every((v, i) => i === 0 || v > n[i - 1]) && !n.every((v, i) => i === 0 || v < n[i - 1])) return null;
  }
  if (pv <= 70 && cu > 70) { if (ed && !checkDiv(cls, id, 'SHORT')) return null; return { type: 'a70', label: 'تجاوز 70 🔴', color: 'red', emoji: '🔴', side: 'SHORT' }; }
  if (pv >= 70 && cu < 70) { if (ed && !checkDiv(cls, id, 'SHORT')) return null; if (rm === 'rsi') return null; return { type: 'b70', label: 'نزل من 70 🟠', color: 'orange', emoji: '🟠', side: 'SHORT' }; }
  if (pv >= 30 && cu < 30) { if (ed && !checkDiv(cls, id, 'LONG')) return null; return { type: 'b30', label: 'نزل من 30 🔵', color: 'blue', emoji: '🔵', side: 'LONG' }; }
  if (pv <= 30 && cu > 30) { if (ed && !checkDiv(cls, id, 'LONG')) return null; if (rm === 'rsi') return null; return { type: 'a30', label: 'صعد من 30 🟢', color: 'green', emoji: '🟢', side: 'LONG' }; }
  return null;
}

function detectConf(pv, cu, cls, id, ed) {
  if (pv === null || cu === null) return null;
  if (pv <= 30 && cu > pv && cu <= 30) { if (ed && !checkDiv(cls, id, 'LONG')) return null; return { type: 'cl', label: '⚡ شراء أكيد', color: 'confirmed', emoji: '⚡🟢', side: 'LONG' }; }
  if (pv >= 70 && cu < pv && cu >= 70) { if (ed && !checkDiv(cls, id, 'SHORT')) return null; return { type: 'cs', label: '⚡ بيع أكيد', color: 'confirmed', emoji: '⚡🔴', side: 'SHORT' }; }
  return null;
}

function detectTrail(sym, cu, cls, id, ed) {
  const st = STATE.settings;
  if (!STATE.rsiPeaks[sym]) STATE.rsiPeaks[sym] = { sp: null, sf: false, lp: null, lf: false };
  const pk = STATE.rsiPeaks[sym];
  if (st.trSon) {
    const sl = parseFloat(st.trSstart) || 75, sg = parseFloat(st.trSgap) || 3;
    if (cu >= sl) { if (pk.sp === null || cu > pk.sp) { pk.sp = cu; pk.sf = false; } }
    if (!pk.sf && pk.sp !== null && pk.sp >= sl && cu <= pk.sp - sg) {
      pk.sf = true; if (ed && !checkDiv(cls, id, 'SHORT')) return null;
      return { type: 'ts', label: `🟣 Trail SHORT (${pk.sp.toFixed(1)}→${cu.toFixed(1)})`, color: 'purple', emoji: '🟣📉', side: 'SHORT' };
    }
    if (pk.sf && cu < sl - sg * 2) { pk.sp = null; pk.sf = false; }
  }
  if (st.trLon) {
    const ll = parseFloat(st.trLstart) || 25, lg = parseFloat(st.trLgap) || 3;
    if (cu <= ll) { if (pk.lp === null || cu < pk.lp) { pk.lp = cu; pk.lf = false; } }
    if (!pk.lf && pk.lp !== null && pk.lp <= ll && cu >= pk.lp + lg) {
      pk.lf = true; if (ed && !checkDiv(cls, id, 'LONG')) return null;
      return { type: 'tl', label: `🟣 Trail LONG (${pk.lp.toFixed(1)}→${cu.toFixed(1)})`, color: 'purple', emoji: '🟣📈', side: 'LONG' };
    }
    if (pk.lf && cu > ll + lg * 2) { pk.lp = null; pk.lf = false; }
  }
  return null;
}

// ══════════════════════════════════════════════
//  BINANCE API
// ══════════════════════════════════════════════
const livePrices = {};
const candleCache = {};
let alertId = 0;

async function fetchBinance(p) {
  const res = await fetch(`${BASE}${p}`);
  if (!res.ok) throw new Error(`Binance ${res.status}`);
  return res.json();
}

const maxLevCache = {};
async function getMaxLev(sym) {
  if (maxLevCache[sym]) return maxLevCache[sym];
  try {
    const master = STATE.copyAccounts?.find(a => a.isMaster);
    let d;
    if (master?.apiKey && master?.apiSecret) {
      d = await bFetch(master.apiKey, master.apiSecret, 'GET', '/fapi/v1/leverageBracket', { symbol: sym });
    } else {
      d = await fetchBinance(`/fapi/v1/leverageBracket?symbol=${sym}`);
    }
    const l = (Array.isArray(d) ? d[0] : d)?.brackets?.[0]?.initialLeverage || 20;
    maxLevCache[sym] = l; return l;
  } catch (e) { return 20; }
}

const lotSizeCache = {}; // sym -> stepSize (from exchangeInfo)

// جلب stepSize ديناميكياً إذا ما كان في الكاش (للعملات التي لم تُحمَّل عند البدء)
async function ensureLotSize(sym) {
  if (lotSizeCache[sym]) return;
  try {
    const info = await fetchBinance(`/fapi/v1/exchangeInfo?symbol=${sym}`);
    const lot = info.symbols?.[0]?.filters?.find(f => f.filterType === 'LOT_SIZE');
    if (lot) lotSizeCache[sym] = parseFloat(lot.stepSize);
  } catch (e) {}
}

function roundQty(qty, sym) {
  const step = lotSizeCache[sym] || 0.001;
  const precision = step >= 1 ? 0 : Math.max(0, -Math.floor(Math.log10(step)));
  return parseFloat((Math.floor(qty / step) * step).toFixed(precision));
}

// قفل لمنع التنفيذ المتزامن لنفس أمر DCA
const dcaLocks = new Set();

async function hmac256(secret, msg) {
  return crypto.createHmac('sha256', secret).update(msg).digest('hex');
}

async function bFetch(apiKey, apiSecret, method, ep, params = {}) {
  const t = Date.now();
  const q = Object.entries({ ...params, timestamp: t }).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  const sig = await hmac256(apiSecret, q);
  const url = `${BASE}${ep}?${q}&signature=${sig}`;
  const res = await fetch(url, { method, headers: { 'X-MBX-APIKEY': apiKey, 'Content-Type': 'application/x-www-form-urlencoded' } });
  const d = await res.json();
  if (d.code && d.code < 0) throw new Error(d.msg || `code:${d.code}`);
  return d;
}

async function getBalance(acc) {
  const d = await bFetch(acc.apiKey, acc.apiSecret, 'GET', '/fapi/v2/balance');
  const u = Array.isArray(d) ? d.find(b => b.asset === 'USDT') : null;
  return u ? parseFloat(u.availableBalance) : 0;
}

async function getPositions(acc) {
  const d = await bFetch(acc.apiKey, acc.apiSecret, 'GET', '/fapi/v2/positionRisk');
  return Array.isArray(d) ? d.filter(p => parseFloat(p.positionAmt) !== 0) : [];
}

// ══════════════════════════════════════════════
//  TELEGRAM
// ══════════════════════════════════════════════
async function tgSend(text, chat) {
  const st = STATE.settings;
  if (!st.cxToken || !chat) return;
  try {
    await fetch(`https://api.telegram.org/bot${st.cxToken}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text })
    });
  } catch (e) {}
}

function buildMsg(sym, side) {
  const st = STATE.settings;
  const p = livePrices[sym], pair = sym.replace('USDT', '/USDT');
  const fp = n => { if (!n && n !== 0) return 'N/A'; if (n >= 100) return n.toFixed(2); if (n >= 1) return n.toFixed(3); if (n >= 0.1) return n.toFixed(4); return n.toFixed(6); };
  let tp1 = null, tp2 = null, sll = null, e2 = null, e3 = null;
  if (p) {
    const t1 = parseFloat(st.cxTP1) / 100, s = parseFloat(st.cxSL) / 100;
    tp1 = side === 'LONG' ? p * (1 + t1) : p * (1 - t1);
    sll = side === 'LONG' ? p * (1 - s) : p * (1 + s);
    if (st.cxTP2on) { const t2 = parseFloat(st.cxTP2) / 100; tp2 = side === 'LONG' ? p * (1 + t2) : p * (1 - t2); }
    if (st.cxEntry2on) { const d2 = parseFloat(st.cxEntry2Dist || '2') / 100; e2 = side === 'LONG' ? p * (1 - d2) : p * (1 + d2); }
    if (st.cxEntry2on && st.cxEntry3on) { const d3 = parseFloat(st.cxEntry3Dist || '4') / 100; e3 = side === 'LONG' ? p * (1 - d3) : p * (1 + d3); }
  }
  const L = [`#${pair}`, 'Exchanges: Binance Futures', `Signal Type: Regular (${side === 'LONG' ? 'Long' : 'Short'})`,
    `Leverage: ${st.cxMargin} (${st.cxLev}X)`, `Amount: ${st.cxAmt}`, '', 'Entry Targets:', '1) Market'];
  if (e2) L.push(`2) ${fp(e2)}${st.cxEntry2Amt ? ` (${st.cxEntry2Amt}%)` : ''}`);
  if (e3) L.push(`3) ${fp(e3)}${st.cxEntry3Amt ? ` (${st.cxEntry3Amt}%)` : ''}`);
  L.push('', 'Take-Profit Targets:');
  if (st.cxTP2on) { L.push(`1) ${fp(tp1)} (${st.cxTP1Amt}%)`); L.push(`2) ${fp(tp2)} (${st.cxTP2Amt}%)`); }
  else L.push(`1) ${fp(tp1)}`);
  L.push('');
  if (st.cxSLon) L.push('Stop Targets:', `1) ${fp(sll)}`, '');
  L.push('Trailing Configuration:', `Entry: Percentage (${st.cxEntryTrail})`);
  if (st.cxTrailTp === 'on') L.push(`Take-Profit: Percentage (${st.cxTrailPct}%)`);
  if (st.cxBEon) L.push('Stop: Breakeven - Trigger: Target (1)');
  return L.join('\n');
}

async function sendSignal(sym, side, overridePrice, fromQueue = false, queueLabel = '') {
  const st = STATE.settings;
  if (fromQueue) {
    // القائمة الذكية: تعتمد على sigFilters.queue فقط، مش autoSend
    if (st.sigFilters?.queue === false) return;
    if (!st.cxToken || !st.cxChat) return;
  } else {
    if (!st.autoSend || !st.cxToken || !st.cxChat) return;
  }
  if (!overridePrice && STATE.sentSigs[sym]) return;
  STATE.sentSigs[sym] = Date.now();

  let lv = parseInt(st.cxLev) || 20;
  const mx = await getMaxLev(sym);
  let note = '';
  if (lv > mx) { lv = Math.min(10, mx); note = `\n⚠️ رافعة عُدّلت إلى ${lv}X (الحد ${mx}X)`; }
  const origLev = st.cxLev; st.cxLev = String(lv);
  const origPrice = overridePrice ? livePrices[sym] : null;
  if (overridePrice) livePrices[sym] = overridePrice;
  const prefix = fromQueue && queueLabel ? `⏳ قائمة الانتظار | ${queueLabel}\n` : '';
  const text = prefix + buildMsg(sym, side) + note;
  if (origPrice !== null) livePrices[sym] = origPrice;
  st.cxLev = origLev;
  await tgSend(text, st.cxChat);
}

// ══════════════════════════════════════════════
//  SCANNER
// ══════════════════════════════════════════════
const BATCH = 5, BDEL = 300;
let scanRunning = false;

function nowStr() {
  return new Date().toLocaleTimeString('ar-EG', { hour12: false, timeZone: 'Asia/Aden' });
}

function isLiquid(sym) {
  const st = STATE.settings, m = STATE.symbolMeta[sym] || {};
  if (st.liqVon && (m.vol || 0) < parseFloat(st.liqVmin)) return false;
  if (st.liqOon && (m.oi || 0) < parseFloat(st.liqOmin)) return false;
  return true;
}

function countOpenPositions() {
  const master = STATE.copyAccounts.find(a => a.isMaster);
  // إذا الماستر موجود بـ API بس المراكز لم تُجلب بعد → افترض أن الحد ممتلئ (أكثر أماناً)
  if (master?.apiKey && !master?.apiOk) {
    return parseInt(STATE.settings.maxOpenTrades) || 999;
  }
  const liveSyms = (master?.livePositions || [])
    .filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0)
    .map(p => p.symbol);
  return new Set([...STATE.openTrades.map(t => t.symbol), ...liveSyms]).size;
}

function triggerAlert(sym, sig, val) {
  const st = STATE.settings;
  const key = `${sym}_${sig.type}`, now = Date.now();
  if (STATE.cooldowns[key]) return;
  const master = STATE.copyAccounts.find(a => a.isMaster);
  const hasLivePos = master?.livePositions?.some(p => p.symbol === sym && Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  if (st.blockOpen && (STATE.openTrades.some(t => t.symbol === sym) || hasLivePos)) return;
  if (STATE.sentSigs[sym]) return;
  if (!isLiquid(sym)) return;

  const isOB = ['a70', 'b70'].includes(sig.type);
  const isOS = ['b30', 'a30'].includes(sig.type);
  const isConf = ['cl', 'cs'].includes(sig.type);
  const isTrail = ['ts', 'tl'].includes(sig.type);
  const typeKey = isOB ? 'ob' : isOS ? 'os' : isConf ? 'conf' : isTrail ? 'trail' : null;

  // فلتر الاتجاه اليدوي — يوقف كل شي (سجل + قائمة + تلغرام)
  if (st.dirFilter && st.dirFilter !== 'all' && sig.side) {
    const required = st.dirFilter === 'long' ? 'LONG' : 'SHORT';
    if (sig.side !== required) return;
  }

  // sigQueueFilters يتحكم بما يظهر في السجل والقائمة
  const sqFilters = { ob: true, os: true, conf: true, trail: true, ...(st.sigQueueFilters || {}) };
  if (typeKey && !sqFilters[typeKey]) return;

  STATE.cooldowns[key] = now;
  alertId++;
  const item = {
    id: alertId, symbol: sym, type: sig.type, label: sig.label,
    color: sig.color, emoji: sig.emoji, rsi: val.toFixed(2),
    time: nowStr(), mode: `${st.mode}(${st.mode === 'RSI' ? RSI_P : st.maPeriod}) ${st.interval}`, side: sig.side
  };
  STATE.alerts = [item, ...STATE.alerts].slice(0, 200);
  db.saveAlert(item);
  broadcast({ type: 'alert', data: item });

  // فلتر EMA 200 — يؤثر على القائمة والإرسال لكن ليس السجل
  if (st.ema200FilterOn && STATE.ema200?.direction) {
    const dir = STATE.ema200.direction;
    if ((sig.side === 'LONG' && dir !== 'up') || (sig.side === 'SHORT' && dir !== 'down')) return;
  }

  // فحص حد الصفقات — إضافة للقائمة إذا وصل الحد
  const maxOT = parseInt(st.maxOpenTrades) || 0;
  if (maxOT > 0 && countOpenPositions() >= maxOT) {
    if (typeKey && !STATE.waitQueue.some(q => q.symbol === sym)) {
      STATE.waitQueue.push({
        id: Date.now() + Math.random(), symbol: sym, side: sig.side,
        signalType: typeKey, signalPrice: livePrices[sym] || 0,
        addedTs: Date.now(), addedTime: nowStr(),
        label: sig.label, emoji: sig.emoji, color: sig.color
      });
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
    }
    return;
  }

  // sigFilters يتحكم فقط بالإرسال المباشر للتلغرام (بدون قائمة)
  const sigFilters = { ob: true, os: true, conf: true, trail: true, ...st.sigFilters };
  if (isOB && !sigFilters.ob) return;
  if (isOS && !sigFilters.os) return;
  if (isConf && !sigFilters.conf) return;
  if (isTrail && !sigFilters.trail) return;

  sendSignal(sym, sig.side);
}

function queueWithReversals() {
  return STATE.waitQueue.map(q => {
    const cur = livePrices[q.symbol] || q.signalPrice;
    const rev = q.signalPrice > 0 ? Math.abs((cur - q.signalPrice) / q.signalPrice) * 100 : 0;
    return { ...q, reversalPct: parseFloat(rev.toFixed(3)) };
  }).sort((a, b) => b.reversalPct - a.reversalPct);
}

async function sendQueueItemNow(qItem, currentPrice) {
  STATE.waitQueue = STATE.waitQueue.filter(q => q.id !== qItem.id);
  broadcast({ type: 'waitQueue', data: queueWithReversals() });
  const label = qItem.emoji ? `${qItem.emoji} ${qItem.label}` : qItem.label || qItem.signalType || '';
  await sendSignal(qItem.symbol, qItem.side, currentPrice || livePrices[qItem.symbol], true, label);
}

function autoSendFromQueue() {
  if (!STATE.waitQueue.length) return;
  if (STATE.settings.sigFilters?.queue === false) return;
  const maxOT = parseInt(STATE.settings.maxOpenTrades) || 0;
  if (maxOT > 0 && countOpenPositions() >= maxOT) return; // الحد لازال ممتلئ
  const scored = queueWithReversals();
  const top = scored[0];
  if (top) sendQueueItemNow(top, livePrices[top.symbol]);
}

function calcEMA(data, period) {
  const k = 2 / (period + 1);
  let ema = data[0];
  for (let i = 1; i < data.length; i++) ema = data[i] * k + ema * (1 - k);
  return ema;
}

async function updateEMA200() {
  try {
    const tf = STATE.settings.ema200TF || '4h';
    const res = await fetch(`${BASE}/fapi/v1/klines?symbol=BTCUSDT&interval=${tf}&limit=210`);
    if (!res.ok) return;
    const klines = await res.json();
    if (!Array.isArray(klines) || klines.length < 201) return;
    const closes = klines.map(k => parseFloat(k[4]));
    const ema200 = calcEMA(closes, 200);
    const currentPrice = closes[closes.length - 1];
    STATE.ema200 = {
      value: parseFloat(ema200.toFixed(2)),
      direction: currentPrice > ema200 ? 'up' : 'down',
      btcPrice: parseFloat(currentPrice.toFixed(2)),
      updatedAt: new Date().toISOString()
    };
    broadcast({ type: 'ema200', data: STATE.ema200 });
  } catch (e) {}
}

async function scanSym(sym, candles) {
  try {
    const cls = candles || candleCache[sym];
    if (!cls || cls.length < RSI_P + 2) return;
    livePrices[sym] = cls[cls.length - 1];
    const st = STATE.settings;
    const cu = computeInd(cls, st.mode, st.maPeriod);
    const pv = computeInd(cls.slice(0, -1), st.mode, st.maPeriod);
    const id = computeIndSeries(cls, st.mode, st.maPeriod);
    const sig = detectSignal(pv, cu, cls, id, st.enableDiv);
    const conf = detectConf(pv, cu, cls, id, st.enableDiv);
    const trail = detectTrail(sym, cu, cls, id, st.enableDiv);
    const zone = cu >= 70 ? 'ob' : cu <= 30 ? 'os' : 'neutral';
    const old = STATE.symbolData[sym] || {};
    const oldZone = old.zone || 'neutral';
    if (oldZone !== 'neutral' && zone === 'neutral') {
      Object.keys(STATE.cooldowns).forEach(k => { if (k.startsWith(sym + '_')) delete STATE.cooldowns[k]; });
    }
    const fSig = trail || sig;
    STATE.symbolData[sym] = { rsi: cu, prevRsi: pv, signal: fSig, conf, zone, error: false, trailActive: !!trail };
    if (fSig && (!old.signal || old.signal.type !== fSig.type)) triggerAlert(sym, fSig, cu);
    if (conf && (!old.conf || old.conf.type !== conf.type)) triggerAlert(sym, conf, cu);

    // فحص أوامر التعزيز
    const price = livePrices[sym];
    if (price && STATE.dcaOrders.length) {
      for (const order of STATE.dcaOrders.filter(o => o.sym === sym && !o.done)) {
        if (dcaLocks.has(order.id)) continue; // منع التنفيذ المتزامن
        const hit = !order.price || order.side === 'LONG' ? price <= order.price || !order.price : price >= order.price;
        if (!hit) continue;
        dcaLocks.add(order.id);
        try {
        const master = STATE.copyAccounts.find(a => a.isMaster);
        const hasPos = master?.livePositions?.some(p => p.symbol === sym);
        if (!hasPos) { order.done = true; addCopyLog('info', `⏭ DCA ${sym}: ما في صفقة`); continue; }
        await ensureLotSize(sym); // تأكد من وجود stepSize
        const targets = STATE.copyAccounts.filter(a => order.accIds.includes(a.id) && a.isEnabled !== false);
        for (const acc of targets) {
          if (!acc.apiKey) continue;
          try {
            const bal = await getBalance(acc);
            const rawQty = order.useAmt
              ? (parseFloat(order.amt || order.pct) * parseInt(order.lev || 20)) / price
              : (bal * (parseFloat(order.pct) / 100) * parseInt(order.lev || 20)) / price;
            const qty = roundQty(rawQty, sym);
            if (qty <= 0) continue;
            const mode = await getPositionMode(acc);
            const isBuy = order.side === 'LONG';
            const oParams = {
              symbol: sym, side: isBuy ? 'BUY' : 'SELL', quantity: qty,
              ...(mode === 'hedge' ? { positionSide: order.side } : { positionSide: 'BOTH' })
            };
            if (order.orderType === 'LIMIT' && order.price) {
              if (order.trailingPct > 0) {
                oParams.type = 'TRAILING_STOP_MARKET';
                oParams.callbackRate = parseFloat(order.trailingPct);
                oParams.activationPrice = parseFloat(order.price);
              } else {
                oParams.type = 'LIMIT';
                oParams.price = parseFloat(order.price);
                oParams.timeInForce = 'GTC';
              }
            } else {
              oParams.type = 'MARKET';
            }
            await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', oParams);
            addCopyLog('success', `✅ DCA ${sym} ${order.side} × ${qty} [${oParams.type}] — ${acc.name}`);
          } catch (e) {
            addCopyLog('fail', `❌ DCA ${acc.name}: ${e.message}`);
            reportError(`DCA ${sym}`, e.message);
          }
        }
        order.done = true;
        db.saveDcaOrders(STATE.dcaOrders);
        tgSend(`🔄 تعزيز\n#${sym.replace('USDT', '/USDT')}\n${order.side} عند $${price}\nنوع: ${order.orderType || 'MARKET'}\nحسابات: ${targets.length}`, STATE.settings.cxChat);
        broadcast({ type: 'dcaOrders', data: STATE.dcaOrders });
        } finally { dcaLocks.delete(order.id); }
      }
    }
  } catch (e) {
    STATE.symbolData[sym] = { rsi: null, prevRsi: null, signal: null, conf: null, zone: 'neutral', error: true };
  }
}

async function fetchCandles(sym) {
  try {
    const st = STATE.settings;
    const d = await fetchBinance(`/fapi/v1/klines?symbol=${sym}&interval=${st.interval}&limit=200`);
    if (!Array.isArray(d) || d.length < RSI_P + 2) return false;
    candleCache[sym] = d.map(k => parseFloat(k[4]));
    return true;
  } catch (e) { return false; }
}

async function scanAll() {
  if (scanRunning || !STATE.symbols.length) return;
  scanRunning = true;
  broadcast({ type: 'scanning', data: true });
  let done = 0;
  for (let i = 0; i < STATE.symbols.length; i += BATCH) {
    const batch = STATE.symbols.slice(i, i + BATCH);
    await Promise.all(batch.map(async s => { await fetchCandles(s); await scanSym(s); }));
    done += batch.length;
    broadcast({ type: 'progress', data: Math.round(done / STATE.symbols.length * 100) });
    if (i + BATCH < STATE.symbols.length) await new Promise(r => setTimeout(r, BDEL));
  }
  broadcast({ type: 'scanning', data: false });
  broadcast({ type: 'symbolData', data: STATE.symbolData });
  scanRunning = false;
}

// ══════════════════════════════════════════════
//  WEBSOCKET — Binance Live
// ══════════════════════════════════════════════
let binanceWs = null, binanceReconn = null;

function startBinanceWS() {
  if (!STATE.symbols.length) return;
  stopBinanceWS();
  const streams = STATE.symbols.map(s => `${s.toLowerCase()}@kline_${STATE.settings.interval}`).join('/');
  binanceWs = new WebSocket(`${WS_BASE}?streams=${streams}`);
  binanceWs.on('open', () => {
    console.log('✅ Binance WS connected');
    broadcast({ type: 'wsStatus', data: 'connected' });
  });
  binanceWs.on('message', (data) => {
    try {
      const m = JSON.parse(data);
      if (!m.data?.k) return;
      const k = m.data.k, sym = k.s, close = parseFloat(k.c);
      livePrices[sym] = close;
      if (!candleCache[sym]) candleCache[sym] = [];

      if (k.x) {
        // شمعة مغلقة — تحديث الكاش وفحص كامل
        candleCache[sym].push(close);
        if (candleCache[sym].length > 200) candleCache[sym] = candleCache[sym].slice(-200);
        scanSym(sym);
      } else {
        // إصلاح #1 (تأخر الإشارات) — كشف الإشارات على الشمعة الحية بدون انتظار إغلاقها
        const a = candleCache[sym];
        if (a.length >= RSI_P + 2) {
          const st = STATE.settings;
          const tmp = [...a.slice(0, -1), close]; // استبدل آخر شمعة بالسعر الحي
          const cu = computeInd(tmp, st.mode, st.maPeriod);
          const pv = computeInd(a.slice(0, -1), st.mode, st.maPeriod);

          if (cu !== null && STATE.symbolData[sym]) {
            const old = STATE.symbolData[sym];
            const oldZone = old.zone || 'neutral';
            STATE.symbolData[sym].rsi = cu;
            STATE.symbolData[sym].error = false; // مسح حالة الخطأ عند نجاح الحساب
            const newZone = cu >= 70 ? 'ob' : cu <= 30 ? 'os' : 'neutral';
            STATE.symbolData[sym].zone = newZone;

            // مسح cooldown عند الخروج من المنطقة
            if (oldZone !== 'neutral' && newZone === 'neutral') {
              Object.keys(STATE.cooldowns).forEach(ck => { if (ck.startsWith(sym + '_')) delete STATE.cooldowns[ck]; });
            }

            // كشف الإشارات على الشمعة الحية
            if (pv !== null) {
              const id = computeIndSeries(tmp, st.mode, st.maPeriod);
              const sig = detectSignal(pv, cu, tmp, id, st.enableDiv);
              const conf = detectConf(pv, cu, tmp, id, st.enableDiv);
              const trail = detectTrail(sym, cu, tmp, id, st.enableDiv);
              const fSig = trail || sig;
              STATE.symbolData[sym].trailActive = !!trail;
              if (fSig && (!old.signal || old.signal.type !== fSig.type)) {
                STATE.symbolData[sym].signal = fSig;
                triggerAlert(sym, fSig, cu);
              }
              if (conf && (!old.conf || old.conf.type !== conf.type)) {
                STATE.symbolData[sym].conf = conf;
                triggerAlert(sym, conf, cu);
              }
            }
          }
        }
      }

      if (STATE.symbolData[sym]) {
        broadcastThrottled({ type: 'priceUpdate', data: { sym, rsi: STATE.symbolData[sym].rsi, zone: STATE.symbolData[sym].zone, price: close, error: STATE.symbolData[sym].error } });
      }
    } catch (e) {}
  });
  binanceWs.on('close', () => { broadcast({ type: 'wsStatus', data: 'disconnected' }); scheduleReconn(); });
  binanceWs.on('error', () => {});
}

function stopBinanceWS() {
  if (binanceReconn) { clearTimeout(binanceReconn); binanceReconn = null; }
  if (binanceWs) { try { binanceWs.terminate(); } catch (e) {} binanceWs = null; }
}

function scheduleReconn() {
  if (binanceReconn) return;
  binanceReconn = setTimeout(() => { binanceReconn = null; startBinanceWS(); }, 3000);
}

let throttleTimer = null, pendingPrices = {};
function broadcastThrottled(msg) {
  if (msg.type === 'priceUpdate') {
    pendingPrices[msg.data.sym] = msg.data;
    if (!throttleTimer) {
      throttleTimer = setTimeout(() => {
        if (Object.keys(pendingPrices).length > 0) {
          broadcast({ type: 'priceUpdates', data: pendingPrices });
          pendingPrices = {};
        }
        throttleTimer = null;
      }, 3000);
    }
    return;
  }
  broadcast(msg);
}

// ══════════════════════════════════════════════
//  COPY TRADING
// ══════════════════════════════════════════════
let copyTimer = null;

function addCopyLog(type, text) {
  STATE.copyLog = [{ type, text, time: nowStr() }, ...STATE.copyLog].slice(0, 300);
  broadcast({ type: 'copyLog', data: STATE.copyLog[0] });
}

async function getPositionMode(acc) {
  try {
    const d = await bFetch(acc.apiKey, acc.apiSecret, 'GET', '/fapi/v1/positionSide/dual');
    return d.dualSidePosition ? 'hedge' : 'oneway';
  } catch (e) { return 'oneway'; }
}

async function openFollower(acc, mPos) {
  try {
    const bal = await getBalance(acc);
    if (bal <= 0) throw new Error('رصيد غير كافٍ');
    const ratio = parseFloat(acc.sizeRatio) || 5;
    const sym = mPos.symbol, amt = parseFloat(mPos.positionAmt);
    const isLong = amt > 0;
    const side = isLong ? 'BUY' : 'SELL';
    const price = livePrices[sym] || parseFloat(mPos.markPrice) || 1;
    const lev = Math.min(parseFloat(mPos.leverage) || 20, 125);
    await ensureLotSize(sym);
    const qty = roundQty((bal * (ratio / 100) * lev) / price, sym);
    if (qty <= 0) throw new Error('الكمية صغيرة جداً');
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: lev });
    const mode = await getPositionMode(acc);
    const orderParams = { symbol: sym, side, type: 'MARKET', quantity: qty };
    if (mode === 'hedge') orderParams.positionSide = isLong ? 'LONG' : 'SHORT';
    else orderParams.positionSide = 'BOTH';
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', orderParams);
    if (!acc.stats) acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
    acc.stats.opens++;
    addCopyLog('success', `✅ ${acc.name}: فُتحت ${sym} ${isLong ? 'LONG' : 'SHORT'} × ${qty}`);
    return true;
  } catch (e) {
    addCopyLog('fail', `❌ ${acc.name}/${mPos.symbol}: ${e.message}`);
    reportError(`Copy فتح ${mPos.symbol}`, e.message);
    return false;
  }
}

async function closeFollower(acc, sym, posAmt) {
  try {
    const isLong = posAmt > 0;
    const side = isLong ? 'SELL' : 'BUY';
    await ensureLotSize(sym);
    const qty = roundQty(Math.abs(posAmt), sym);
    const mode = await getPositionMode(acc);
    const orderParams = { symbol: sym, side, type: 'MARKET', quantity: qty };
    if (mode === 'hedge') orderParams.positionSide = isLong ? 'LONG' : 'SHORT';
    else { orderParams.positionSide = 'BOTH'; orderParams.reduceOnly = true; }
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', orderParams);
    if (!acc.stats) acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
    acc.stats.closes++;
    const entry = acc.livePositions?.find(p => p.symbol === sym);
    if (entry) {
      const pnl = parseFloat(entry.unRealizedProfit) || 0;
      const entryPrice = parseFloat(entry.entryPrice) || 0;
      const markPrice = parseFloat(entry.markPrice) || livePrices[sym] || entryPrice;
      const pct = entryPrice ? ((markPrice - entryPrice) / entryPrice * 100 * (isLong ? 1 : -1)) : 0;
      if (pct >= 0) acc.stats.wins++; else acc.stats.losses++;
      acc.stats.tot = (acc.stats.tot || 0) + pct;
      if (!acc.closedTrades) acc.closedTrades = [];
      acc.closedTrades = [{ symbol: sym, side: isLong ? 'LONG' : 'SHORT', entryPrice, exitPrice: markPrice, pnl, pct, closeTs: Date.now(), closeTime: nowStr() }, ...acc.closedTrades].slice(0, 200);
    }
    addCopyLog('success', `🔒 ${acc.name}: أُغلقت ${sym}`);
    return true;
  } catch (e) {
    addCopyLog('fail', `❌ إغلاق ${acc.name}/${sym}: ${e.message}`);
    reportError(`Copy إغلاق ${sym}`, e.message);
    return false;
  }
}

async function syncOrders(master, followers) {
  try {
    const orders = await bFetch(master.apiKey, master.apiSecret, 'GET', '/fapi/v1/openOrders', {});
    if (!Array.isArray(orders)) return;
    const prevOrders = STATE.masterOrders || {};
    const newOrderMap = {};
    for (const o of orders) newOrderMap[o.orderId] = o;

    for (const o of orders) {
      if (prevOrders[o.orderId]) continue; // سبق نسخه
      if (!['STOP_MARKET', 'TAKE_PROFIT_MARKET', 'STOP', 'TAKE_PROFIT'].includes(o.type)) continue;
      const sym = o.symbol;
      const masterPos = master.livePositions?.find(p => p.symbol === sym);
      if (!masterPos) continue;
      const masterAmt = Math.abs(parseFloat(masterPos.positionAmt));
      if (masterAmt === 0) continue;
      const orderFrac = Math.min(parseFloat(o.origQty) / masterAmt, 1);

      for (const f of followers) {
        if (!f.apiKey || !f.apiSecret || !f.apiOk) continue;
        const fPos = f.livePositions?.find(p => p.symbol === sym);
        if (!fPos) continue;
        const fAmt = Math.abs(parseFloat(fPos.positionAmt));
        if (fAmt === 0) continue;
        const isLong = parseFloat(fPos.positionAmt) > 0;
        const closeQty = roundQty(fAmt * orderFrac, sym);
        if (closeQty <= 0) continue;
        const params = {
          symbol: sym,
          side: isLong ? 'SELL' : 'BUY',
          type: o.type,
          quantity: closeQty,
          positionSide: 'BOTH',
          reduceOnly: true,
          stopPrice: o.stopPrice,
        };
        if (['STOP', 'TAKE_PROFIT'].includes(o.type)) params.price = o.price || o.stopPrice;
        try {
          await bFetch(f.apiKey, f.apiSecret, 'POST', '/fapi/v1/order', params);
          addCopyLog('success', `📋 ${f.name}: ${o.type} ${sym} @ ${o.stopPrice}`);
        } catch (e) {
          addCopyLog('fail', `❌ ${f.name}: فشل نسخ أمر ${o.type} ${sym}: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 200));
      }
    }
    STATE.masterOrders = newOrderMap;
  } catch (e) {
    console.error('syncOrders error:', e.message);
  }
}

let rateLimitPause = 0;
async function syncCopy() {
  if (!STATE.copyOn) return;
  if (Date.now() < rateLimitPause) return;
  const master = STATE.copyAccounts.find(a => a.isMaster);
  if (!master?.apiKey || !master?.apiSecret) return;

  let curr;
  try {
    const p = await getPositions(master);
    curr = {}; p.forEach(x => curr[x.symbol] = x);
    master.livePositions = p;
    master.liveBalance = await getBalance(master);
    master.apiOk = true;
  } catch (e) {
    const em = e.message.toLowerCase();
    if (em.includes('too many') || em.includes('1003') || em.includes('banned') || em.includes('rate limit')) {
      rateLimitPause = Date.now() + 60000;
      addCopyLog('fail', `⚠️ Binance حجب IP — انتظر 60 ثانية`);
      return;
    }
    addCopyLog('fail', `❌ ماستر: ${e.message}`);
    master.apiOk = false; return;
  }

  const prev = STATE.masterPositions || {};
  const followers = STATE.copyAccounts.filter(a => !a.isMaster && a.isEnabled !== false);

  // تحقق إذا تغيرت مراكز الماستر (رموز أو كميات)
  const currSig = Object.keys(curr).sort().map(s => `${s}:${parseFloat(curr[s].positionAmt).toFixed(4)}`).join(',');
  const prevSig = Object.keys(prev).sort().map(s => `${s}:${parseFloat(prev[s].positionAmt).toFixed(4)}`).join(',');
  if (currSig === prevSig) {
    STATE.masterPositions = curr;
    broadcast({ type: 'accounts', data: getSafeAccounts() });
    return;
  }

  // مراكز الماستر تغيرت — اجلب بيانات المتابعين الآن فقط
  for (const f of followers) {
    try { f.livePositions = await getPositions(f); f.liveBalance = await getBalance(f); f.apiOk = true; }
    catch (e) { f.apiOk = false; }
  }

  // إغلاق جزئي — كمية الماستر نقصت بدون إغلاق كامل
  for (const [sym, pos] of Object.entries(curr)) {
    if (!prev[sym]) continue;
    const prevAmt = Math.abs(parseFloat(prev[sym].positionAmt));
    const currAmt = Math.abs(parseFloat(pos.positionAmt));
    if (prevAmt > 0 && currAmt < prevAmt * 0.98) {
      const closeFrac = (prevAmt - currAmt) / prevAmt;
      addCopyLog('info', `📉 ماستر أغلق ${(closeFrac * 100).toFixed(0)}% من ${sym}`);
      for (const f of followers) {
        const fPos = f.livePositions?.find(p => p.symbol === sym);
        if (!fPos) continue;
        const fAmt = parseFloat(fPos.positionAmt);
        const closeQty = roundQty(Math.abs(fAmt) * closeFrac, sym);
        if (closeQty <= 0) continue;
        try { await closeFollower(f, sym, fAmt > 0 ? closeQty : -closeQty); }
        catch (e) { addCopyLog('fail', `❌ إغلاق جزئي ${f.name}/${sym}: ${e.message}`); }
        await new Promise(r => setTimeout(r, 300));
      }
    }
  }

  // فتح صفقات جديدة
  for (const [sym, pos] of Object.entries(curr)) {
    const isNew = !prev[sym];
    if (isNew) {
      addCopyLog('info', `📡 ماستر فتح: ${sym} ${parseFloat(pos.positionAmt) > 0 ? 'LONG' : 'SHORT'}`);

      // إصلاح #4 — مزامنة الصفقات المفتوحة فوراً
      if (!STATE.openTrades.some(t => t.symbol === sym)) {
        const newTrade = {
          id: Date.now() + Math.random(),
          symbol: sym,
          side: parseFloat(pos.positionAmt) > 0 ? 'LONG' : 'SHORT',
          entryPrice: parseFloat(pos.entryPrice) || livePrices[sym] || 0,
          openTime: nowStr(), openTs: Date.now(),
          sl: STATE.settings.cxSL, tp1: STATE.settings.cxTP1,
          leverage: pos.leverage || STATE.settings.cxLev,
          margin: STATE.settings.cxMargin,
          label: '🪞 Copy', executed: true
        };
        STATE.openTrades = [newTrade, ...STATE.openTrades];
        db.saveOpenTrades(STATE.openTrades);
        broadcast({ type: 'trades', data: STATE.openTrades });
      }
    }
    for (const f of followers) {
      if (!f.apiOk) continue;
      const fPos = (f.livePositions || []).find(p => p.symbol === sym);
      if (!fPos) { await openFollower(f, pos); await new Promise(r => setTimeout(r, 300)); }
    }
    if (isNew) tgSend(`🪞 Copy\n#${sym.replace('USDT', '/USDT')}\nماستر فتح: ${parseFloat(pos.positionAmt) > 0 ? 'Long' : 'Short'}\nحسابات: ${followers.length}`, STATE.settings.cxChat);
  }

  // إغلاق صفقات أغلقها الماستر
  for (const [sym] of Object.entries(prev)) {
    if (!curr[sym]) {
      addCopyLog('info', `📡 ماستر أغلق: ${sym}`);

      const prevPos = prev[sym];
      const isLongPos = parseFloat(prevPos.positionAmt) > 0;
      const side = isLongPos ? 'LONG' : 'SHORT';
      const entryPrice = parseFloat(prevPos.entryPrice) || 0;
      const exitPrice = livePrices[sym] || entryPrice;
      const lev = parseFloat(prevPos.leverage) || 1;
      const rawPct = entryPrice ? ((exitPrice - entryPrice) / entryPrice) * 100 : 0;
      const pct = parseFloat(((isLongPos ? rawPct : -rawPct) * lev).toFixed(2));

      // ابحث في openTrades أو أنشئ سجل جديد
      const t = STATE.openTrades.find(x => x.symbol === sym);
      const closed = t
        ? { ...t, exitPrice, exitTime: nowStr(), closeTs: Date.now(), pct, result: pct >= 0 ? 'win' : 'loss' }
        : { id: Date.now() + Math.random(), symbol: sym, side, entryPrice, exitPrice,
            pct, result: pct >= 0 ? 'win' : 'loss',
            openTime: '', exitTime: nowStr(), openTs: 0, closeTs: Date.now(),
            sl: '', tp1: '', leverage: String(prevPos.leverage || 20),
            margin: prevPos.marginType || 'Cross', label: '🪞 Binance', executed: true };

      STATE.closedTrades = [closed, ...STATE.closedTrades].slice(0, 500);
      STATE.openTrades = STATE.openTrades.filter(x => x.symbol !== sym);
      delete STATE.sentSigs[sym];
      db.saveClosedTrade(closed);
      db.saveOpenTrades(STATE.openTrades);

      // تحديث إحصائيات الماستر
      if (!master.stats) master.stats = { opens:0, closes:0, wins:0, losses:0, tot:0 };
      master.stats.closes++;
      if (pct >= 0) master.stats.wins++; else master.stats.losses++;
      master.stats.tot = parseFloat(((master.stats.tot || 0) + pct).toFixed(2));
      // تسجيل الصفقة في سجل الماستر المغلقة (مع USD PnL تقديري)
      if (!master.closedTrades) master.closedTrades = [];
      const posAmt = Math.abs(parseFloat(prevPos.positionAmt));
      const pnlUsd = posAmt * (exitPrice - entryPrice) * (isLongPos ? 1 : -1);
      master.closedTrades = [{ symbol: sym, side, entryPrice, exitPrice, pnl: pnlUsd, pct, closeTs: Date.now(), closeTime: nowStr() }, ...master.closedTrades].slice(0, 200);

      broadcast({ type: 'trades', data: STATE.openTrades });
      broadcast({ type: 'closedTrades', data: STATE.closedTrades.slice(0, 100) });
      // أرسل تلقائياً من قائمة الانتظار عند إغلاق صفقة
      setTimeout(autoSendFromQueue, 1000);

      for (const f of followers) {
        const fp = (f.livePositions || []).find(p => p.symbol === sym);
        if (fp) { await closeFollower(f, sym, parseFloat(fp.positionAmt)); await new Promise(r => setTimeout(r, 300)); }
      }
      tgSend(`🔒 Copy أُغلقت ${sym.replace('USDT', '/USDT')} ${pct >= 0 ? '✅' : '❌'} ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`, STATE.settings.cxChatClose || STATE.settings.cxChat);
    }
  }

  STATE.masterPositions = curr;
  db.saveAccounts(STATE.copyAccounts);
  broadcast({ type: 'accounts', data: getSafeAccounts() });

  // نسخ أوامر SL/TP الجديدة من الماستر
  await syncOrders(master, followers);
}

async function startCopy() {
  if (copyTimer) clearInterval(copyTimer);
  STATE.copyOn = true;

  // جلب خط الأساس أولاً قبل بدء الـ timer — بـ await لمنع race condition
  const master = STATE.copyAccounts.find(a => a.isMaster);
  if (master?.apiKey) {
    try {
      const p = await getPositions(master);
      const baseline = {};
      p.forEach(x => baseline[x.symbol] = x);
      STATE.masterPositions = baseline;
      master.livePositions = p;
      addCopyLog('info', `📌 خط الأساس: ${Object.keys(baseline).length} صفقة موجودة — لن تُنسخ`);
    } catch (e) {
      STATE.masterPositions = {};
      addCopyLog('fail', `⚠️ تعذر جلب خط الأساس: ${e.message}`);
    }
  } else {
    STATE.masterPositions = {};
  }

  copyTimer = setInterval(syncCopy, 5000);
  addCopyLog('info', '▶️ بدأ النسخ — فقط الصفقات الجديدة من الآن');
  broadcast({ type: 'copyOn', data: true });
}

function stopCopy() {
  if (copyTimer) { clearInterval(copyTimer); copyTimer = null; }
  STATE.copyOn = false;
  addCopyLog('info', '⏸ توقف النسخ');
  broadcast({ type: 'copyOn', data: false });
}

async function emergencyStop() {
  stopCopy();
  addCopyLog('fail', '🚨 إيقاف طارئ...');
  for (const acc of STATE.copyAccounts) {
    if (!acc.apiKey || !acc.apiSecret) continue;
    try {
      const pos = await getPositions(acc);
      for (const p of pos) {
        await closeFollower(acc, p.symbol, parseFloat(p.positionAmt));
        await new Promise(r => setTimeout(r, 150));
      }
    } catch (e) {
      // إصلاح — تسجيل الأخطاء بدل ابتلاعها
      addCopyLog('fail', `❌ إيقاف طارئ ${acc.name}: ${e.message}`);
    }
  }
  addCopyLog('info', '✅ اكتمل الإيقاف');
  db.saveAccounts(STATE.copyAccounts);
  broadcast({ type: 'accounts', data: getSafeAccounts() });
}

function getSafeAccounts() {
  return STATE.copyAccounts.map(a => ({
    id: a.id, name: a.name, tag: a.tag, isMaster: a.isMaster,
    isEnabled: a.isEnabled, sizeRatio: a.sizeRatio,
    apiKeyPreview: a.apiKey ? a.apiKey.slice(0, 8) + '••••' + a.apiKey.slice(-4) : '',
    balance: a.liveBalance ?? a.balance ?? null,
    balanceAt: a.balanceAt,
    livePositions: a.livePositions || [],
    apiOk: a.apiOk,
    stats: a.stats || { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 },
    closedTrades: (a.closedTrades || []).slice(0, 50)
  }));
}

// ══════════════════════════════════════════════
//  WEBSOCKET — Clients
// ══════════════════════════════════════════════
const clients = new Set();

function broadcast(msg) {
  const str = JSON.stringify(msg);
  clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(str); });
}

let sysErrorTimer = null;
function reportError(loc, errMsg) {
  STATE.sysStatus = { ok: false, lastError: errMsg, errorLoc: loc, errorTs: Date.now() };
  broadcast({ type: 'sysStatus', data: STATE.sysStatus });
  if (sysErrorTimer) clearTimeout(sysErrorTimer);
  sysErrorTimer = setTimeout(() => {
    STATE.sysStatus = { ok: true, lastError: null, errorLoc: null, errorTs: null };
    broadcast({ type: 'sysStatus', data: STATE.sysStatus });
    sysErrorTimer = null;
  }, 60000);
}

function getPublicState() {
  return {
    symbols: STATE.symbols,
    symbolData: STATE.symbolData,
    symbolMeta: STATE.symbolMeta,
    alerts: STATE.alerts,
    openTrades: STATE.openTrades,
    closedTrades: STATE.closedTrades,
    settings: STATE.settings,
    copyOn: STATE.copyOn,
    copyLog: STATE.copyLog.slice(0, 50),
    accounts: getSafeAccounts(),
    dcaOrders: STATE.dcaOrders,
    pendingOrders: STATE.pendingOrders,
    waitQueue: queueWithReversals(),
    sentSigs: STATE.sentSigs,
    sysStatus: STATE.sysStatus,
    ema200: STATE.ema200,
    lastUpdate: nowStr(),
  };
}

wss.on('connection', (ws, req) => {
  // إصلاح أمني — التحقق من التوكن قبل قبول الاتصال
  try {
    const url = new URL(req.url, `http://localhost`);
    const token = url.searchParams.get('token');
    if (!token) { ws.close(4001, 'Unauthorized'); return; }
    jwt.verify(token, JWT_SECRET);
  } catch (e) { ws.close(4001, 'Invalid token'); return; }

  clients.add(ws);
  ws.send(JSON.stringify({ type: 'init', data: getPublicState() }));
  ws.on('message', async (raw) => {
    try { await handleClientMsg(JSON.parse(raw)); } catch (e) {}
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

async function handleClientMsg(msg) {
  switch (msg.type) {
    case 'updateSettings': {
      const oldInterval = STATE.settings.interval;
      if (msg.data.sigFilters) {
        msg.data.sigFilters = { ...STATE.settings.sigFilters, ...msg.data.sigFilters };
      }
      if (msg.data.sigQueueFilters) {
        msg.data.sigQueueFilters = { ...STATE.settings.sigQueueFilters, ...msg.data.sigQueueFilters };
      }
      Object.assign(STATE.settings, msg.data);
      db.saveSettings(STATE.settings);
      broadcast({ type: 'settings', data: STATE.settings });
      if (msg.data.ema200TF !== undefined) updateEMA200();
      // إعادة تشغيل WS عند تغيير الفريم الزمني
      if (msg.data.interval && msg.data.interval !== oldInterval) {
        Object.keys(candleCache).forEach(k => delete candleCache[k]);
        startBinanceWS();
        broadcast({ type: 'scanning', data: true });
        scanAll().then(() => broadcast({ type: 'scanning', data: false }));
      }
      break;
    }

    case 'scanNow': scanAll(); break;

    case 'forceRescan':
      Object.keys(candleCache).forEach(k => delete candleCache[k]);
      startBinanceWS();
      broadcast({ type: 'scanning', data: true });
      scanAll().then(() => broadcast({ type: 'scanning', data: false }));
      break;

    case 'refreshAllAccounts': {
      const refreshed = await Promise.all(STATE.copyAccounts.map(async acc => {
        if (!acc.apiKey) return;
        try {
          [acc.liveBalance, acc.livePositions] = await Promise.all([getBalance(acc), getPositions(acc)]);
          acc.balanceAt = Date.now(); acc.apiOk = true;
        } catch (e) { acc.apiOk = false; }
      }));
      db.saveAccounts(STATE.copyAccounts);
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }
    case 'toggleCopy': STATE.copyOn ? stopCopy() : startCopy(); break;
    case 'emergencyStop': await emergencyStop(); break;
    case 'clearSysError': {
      if (sysErrorTimer) { clearTimeout(sysErrorTimer); sysErrorTimer = null; }
      STATE.sysStatus = { ok: true, lastError: null, errorLoc: null, errorTs: null };
      broadcast({ type: 'sysStatus', data: STATE.sysStatus });
      break;
    }

    case 'addAccount': {
      const acc = msg.data;
      const copyExisting = !!acc.copyExisting;
      delete acc.copyExisting;
      try {
        const bal = await getBalance(acc);
        acc.id = Date.now();
        acc.balance = bal; acc.balanceAt = Date.now();
        acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
        acc.apiOk = true;
        // إصلاح — الأول يصير Master تلقائياً، وتبقى واحدة فقط
        if (acc.isMaster || STATE.copyAccounts.length === 0) {
          STATE.copyAccounts.forEach(a => a.isMaster = false);
          acc.isMaster = true;
        }
        STATE.copyAccounts.push(acc);
        db.saveAccounts(STATE.copyAccounts);
        addCopyLog('success', `➕ أُضيف: ${acc.name} — $${bal.toFixed(2)}`);
        // نسخ الصفقات الحالية من الماستر إذا طُلب ذلك
        if (copyExisting && !acc.isMaster) {
          const master = STATE.copyAccounts.find(a => a.isMaster);
          if (master?.apiKey) {
            try {
              const masterPos = await getPositions(master);
              let copied = 0;
              for (const pos of masterPos) {
                const ok = await openFollower(acc, pos);
                if (ok) copied++;
                await new Promise(r => setTimeout(r, 400));
              }
              addCopyLog('success', `🪞 نُسخت ${copied} صفقة للحساب الجديد: ${acc.name}`);
            } catch (e) {
              addCopyLog('fail', `⚠️ فشل نسخ الصفقات: ${e.message}`);
            }
          }
        }
        broadcast({ type: 'accounts', data: getSafeAccounts() });
        broadcast({ type: 'addAccountResult', data: { success: true, balance: bal } });
      } catch (e) {
        broadcast({ type: 'addAccountResult', data: { success: false, error: e.message } });
      }
      break;
    }

    case 'testAccount': {
      try {
        const bal = await getBalance(msg.data);
        broadcast({ type: 'testAccountResult', data: { success: true, balance: bal } });
      } catch (e) {
        broadcast({ type: 'testAccountResult', data: { success: false, error: e.message } });
      }
      break;
    }

    case 'updateAccount': {
      const idx = STATE.copyAccounts.findIndex(a => a.id === msg.data.id);
      if (idx >= 0) {
        // إصلاح — منع تعدد الـ Master
        if (msg.data.isMaster) STATE.copyAccounts.forEach(a => a.isMaster = false);
        Object.assign(STATE.copyAccounts[idx], msg.data);
        db.saveAccounts(STATE.copyAccounts);
        broadcast({ type: 'accounts', data: getSafeAccounts() });
      }
      break;
    }

    case 'deleteAccount':
      STATE.copyAccounts = STATE.copyAccounts.filter(a => a.id !== msg.data.id);
      db.saveAccounts(STATE.copyAccounts);
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;

    case 'toggleAccount': {
      const acc = STATE.copyAccounts.find(a => a.id === msg.data.id);
      if (acc) { acc.isEnabled = !acc.isEnabled; db.saveAccounts(STATE.copyAccounts); broadcast({ type: 'accounts', data: getSafeAccounts() }); }
      break;
    }

    case 'refreshAccount': {
      const acc = STATE.copyAccounts.find(a => a.id === msg.data.id);
      if (acc?.apiKey) {
        try { acc.liveBalance = await getBalance(acc); acc.livePositions = await getPositions(acc); acc.balanceAt = Date.now(); acc.apiOk = true; }
        catch (e) { acc.apiOk = false; }
        broadcast({ type: 'accounts', data: getSafeAccounts() });
      }
      break;
    }

    case 'closeAllAccount': {
      const acc = STATE.copyAccounts.find(a => a.id === msg.data.id);
      if (acc?.apiKey) {
        try {
          const pos = await getPositions(acc);
          for (const p of pos) await closeFollower(acc, p.symbol, parseFloat(p.positionAmt));
        } catch (e) {}
        broadcast({ type: 'accounts', data: getSafeAccounts() });
      }
      break;
    }

    case 'sendReport': {
      const acc = STATE.copyAccounts.find(a => a.id === msg.data.id);
      if (!acc) break;
      const st = acc.stats || {};
      const pos = acc.livePositions || [];
      const pnlOpen = pos.reduce((s, p) => s + parseFloat(p.unRealizedProfit || 0), 0);
      const wr = st.closes ? (st.wins / st.closes * 100).toFixed(0) : 0;
      const closed = acc.closedTrades || [];
      const totalPnlUsd = closed.reduce((s, t) => s + (t.pnl || 0), 0);
      const lines = [
        `📊 تقرير: ${acc.name}`, `━━━━━━━━━━━━━━━`,
        `💵 الرصيد: $${parseFloat(acc.liveBalance || acc.balance || 0).toFixed(2)}`,
        `📈 صفقات مفتوحة: ${pos.length}`,
        `💹 PnL مفتوح: ${pnlOpen >= 0 ? '+' : ''}$${pnlOpen.toFixed(2)}`, ``,
        `📋 الصفقات المغلقة: ${st.closes || 0}`,
        `✅ رابحة: ${st.wins || 0}`, `❌ خاسرة: ${st.losses || 0}`,
        `🎯 نسبة النجاح: ${wr}%`,
        `📊 الأداء الكلي: ${(st.tot || 0) >= 0 ? '+' : ''}${(st.tot || 0).toFixed(2)}%`,
        `💰 إجمالي PnL: ${totalPnlUsd >= 0 ? '+' : ''}$${totalPnlUsd.toFixed(2)}`,
      ];
      if (closed.length) {
        lines.push('', '📜 آخر الصفقات:');
        closed.slice(0, 10).forEach(t => {
          lines.push(`${t.pnl >= 0 ? '✅' : '❌'} ${t.symbol.replace('USDT','')} ${t.side} → ${t.pnl >= 0 ? '+' : ''}$${(t.pnl || 0).toFixed(2)} (${t.pct >= 0 ? '+' : ''}${(t.pct || 0).toFixed(2)}%)`);
        });
      }
      await tgSend(lines.join('\n'), STATE.settings.cxChatClose || STATE.settings.cxChat);
      broadcast({ type: 'reportSent', data: { id: acc.id } });
      break;
    }

    case 'sendQueueItem': {
      const q = STATE.waitQueue.find(x => x.id === msg.data.id);
      if (q) await sendQueueItemNow(q, livePrices[q.symbol]);
      break;
    }

    case 'removeQueueItem': {
      STATE.waitQueue = STATE.waitQueue.filter(x => x.id !== msg.data.id);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      break;
    }

    case 'clearQueue': {
      STATE.waitQueue = [];
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      break;
    }

    case 'closeOnePosition': {
      const { accId, symbol, posAmt } = msg.data;
      const acc = STATE.copyAccounts.find(a => a.id === accId);
      if (acc?.apiKey) {
        await closeFollower(acc, symbol, posAmt);
        try { acc.livePositions = await getPositions(acc); acc.liveBalance = await getBalance(acc); } catch (e) {}
        broadcast({ type: 'accounts', data: getSafeAccounts() });
      }
      break;
    }

    // ── أمر يدوي على حساب محدد (إضافة جديدة) ──────────────
    case 'manualOrder': {
      let { accId, sym, side, amt, pct, useAmt, lev, orderType, limitPrice, trailingPct } = msg.data;
      // إصلاح: منع تكرار USDT
      sym = sym.replace(/USDT$/i, '').toUpperCase() + 'USDT';
      const acc = STATE.copyAccounts.find(a => a.id === accId);
      if (!acc?.apiKey) { broadcast({ type: 'manualOrderResult', data: { success: false, error: 'الحساب غير موجود أو بدون API' } }); break; }
      try {
        const bal = await getBalance(acc);
        if (bal <= 0) throw new Error(`الرصيد صفر — تأكد من الـ API`);
        const price = livePrices[sym] || parseFloat(limitPrice) || 1;
        await ensureLotSize(sym);
        const leverage = Math.min(parseInt(lev) || 20, await getMaxLev(sym));
        const amtPct = parseFloat(pct || 5) / 100;
        const rawQty = useAmt
          ? (parseFloat(amt || 0) * leverage) / price
          : (bal * amtPct * leverage) / price;
        const qty = roundQty(rawQty, sym);
        if (qty <= 0) throw new Error(`الكمية صغيرة جداً (رصيد: $${bal.toFixed(2)})`);
        await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage });
        const mode = await getPositionMode(acc);
        const isBuy = side === 'LONG';
        const orderParams = {
          symbol: sym, side: isBuy ? 'BUY' : 'SELL', quantity: qty,
          ...(mode === 'hedge' ? { positionSide: side } : { positionSide: 'BOTH' })
        };
        if (orderType === 'LIMIT' && limitPrice) {
          orderParams.type = 'LIMIT';
          orderParams.price = parseFloat(limitPrice);
          orderParams.timeInForce = 'GTC';
          if (trailingPct > 0) {
            // trailing limit: نستخدم TRAILING_STOP_MARKET كأمر مرافق
            orderParams.type = 'TRAILING_STOP_MARKET';
            delete orderParams.price;
            orderParams.callbackRate = parseFloat(trailingPct);
            orderParams.activationPrice = parseFloat(limitPrice);
          }
        } else {
          orderParams.type = 'MARKET';
        }
        const result = await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', orderParams);
        if (!acc.stats) acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
        acc.stats.opens++;
        addCopyLog('success', `✅ يدوي: ${sym} ${side} × ${qty} (${leverage}x) [${orderParams.type}] — ${acc.name}`);
        // حفظ أوامر Limit/Trailing في القائمة
        if (orderParams.type !== 'MARKET') {
          const po = { id: result.orderId || Date.now(), sym, side, qty, type: orderParams.type, price: orderParams.price || orderParams.activationPrice, acc: acc.name, accId, lev: leverage, status: 'NEW', createdAt: nowStr(), source: 'manual' };
          STATE.pendingOrders = [po, ...STATE.pendingOrders].slice(0, 200);
          broadcast({ type: 'pendingOrders', data: STATE.pendingOrders });
        }
        [acc.livePositions, acc.liveBalance] = await Promise.all([getPositions(acc), getBalance(acc)]);
        db.saveAccounts(STATE.copyAccounts);
        broadcast({ type: 'accounts', data: getSafeAccounts() });
        broadcast({ type: 'manualOrderResult', data: { success: true, sym, side, qty, lev: leverage, acc: acc.name, type: orderParams.type } });
      } catch (e) {
        console.error('manualOrder error:', e);
        addCopyLog('fail', `❌ يدوي ${sym} — ${acc.name}: ${e.message}`);
        reportError(`أمر يدوي ${sym}`, e.message);
        broadcast({ type: 'manualOrderResult', data: { success: false, error: e.message } });
      }
      break;
    }

    case 'confirmTrade': {
      const a = msg.data;
      const t = { id: Date.now(), symbol: a.symbol, side: a.side, entryPrice: livePrices[a.symbol] || 0, openTime: a.time, openTs: Date.now(), sl: STATE.settings.cxSL, tp1: STATE.settings.cxTP1, leverage: STATE.settings.cxLev, margin: STATE.settings.cxMargin, label: a.label };
      STATE.openTrades = [t, ...STATE.openTrades];
      db.saveOpenTrades(STATE.openTrades);
      broadcast({ type: 'trades', data: STATE.openTrades });
      break;
    }

    case 'executeSignal': {
      const { sym, side, accId } = msg.data;
      const st = STATE.settings;
      const accs = accId
        ? STATE.copyAccounts.filter(a => a.id === accId)
        : STATE.copyAccounts.filter(a => a.isEnabled !== false);
      const results = [];
      for (const acc of accs) {
        if (!acc.apiKey || !acc.apiSecret) continue;
        try {
          const bal = await getBalance(acc);
          const price = livePrices[sym] || 1;
          // إصلاح #5 — رافعة مع حد أقصى
          const lev = Math.min(parseInt(st.cxLev) || 20, await getMaxLev(sym));
          const amtPct = parseFloat(st.cxAmt) / 100;
          const qty = roundQty((bal * amtPct * lev) / price, sym);
          if (qty <= 0) throw new Error('الكمية صغيرة');
          await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: lev });
          await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', {
            symbol: sym, side: side === 'LONG' ? 'BUY' : 'SELL',
            type: 'MARKET', quantity: qty, positionSide: 'BOTH'
          });
          results.push({ acc: acc.name, ok: true, qty });
          addCopyLog('success', `✅ تنفيذ ${sym} ${side} × ${qty} — ${acc.name}`);
        } catch (e) {
          results.push({ acc: acc.name, ok: false, error: e.message });
          addCopyLog('fail', `❌ فشل تنفيذ ${sym} — ${acc.name}: ${e.message}`);
        }
      }
      const ep = livePrices[sym] || 0;
      const t = { id: Date.now(), symbol: sym, side, entryPrice: ep, openTime: nowStr(), openTs: Date.now(), sl: st.cxSL, tp1: st.cxTP1, leverage: st.cxLev, margin: st.cxMargin, label: 'تنفيذ مباشر', executed: true };
      STATE.openTrades = [t, ...STATE.openTrades];
      db.saveOpenTrades(STATE.openTrades);
      await tgSend(buildMsg(sym, side), st.cxChat);
      broadcast({ type: 'executeResult', data: results });
      broadcast({ type: 'trades', data: STATE.openTrades });
      // تحديث المراكز الحية من Binance مباشرة بعد التنفيذ
      await Promise.all(accs.filter(a => a.apiKey && a.apiSecret).map(async acc => {
        try {
          const [pos, bal] = await Promise.all([getPositions(acc), getBalance(acc)]);
          acc.livePositions = pos;
          acc.liveBalance = bal;
        } catch (e) {}
      }));
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    case 'closePartial': {
      const { tradeId, pct } = msg.data;
      const t = STATE.openTrades.find(x => x.id === tradeId);
      if (!t) break;
      for (const acc of STATE.copyAccounts.filter(a => a.isEnabled !== false)) {
        if (!acc.apiKey || !acc.apiSecret) continue;
        try {
          const pos = (await getPositions(acc)).find(p => p.symbol === t.symbol);
          if (!pos) continue;
          const closeAmt = roundQty(Math.abs(parseFloat(pos.positionAmt)) * (pct / 100), t.symbol);
          if (closeAmt <= 0) continue;
          await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', {
            symbol: t.symbol, side: t.side === 'LONG' ? 'SELL' : 'BUY',
            type: 'MARKET', quantity: closeAmt, positionSide: 'BOTH', reduceOnly: true
          });
          addCopyLog('success', `🔒 إغلاق ${pct}% من ${t.symbol} — ${acc.name}`);
        } catch (e) { addCopyLog('fail', `❌ إغلاق جزئي ${acc.name}: ${e.message}`); }
      }
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    case 'closeTrade': {
      const t = STATE.openTrades.find(x => x.id === msg.data.id);
      if (t) {
        const ep = livePrices[t.symbol] || t.entryPrice;
        const pct = t.side === 'LONG' ? ((ep - t.entryPrice) / t.entryPrice) * 100 : ((t.entryPrice - ep) / t.entryPrice) * 100;
        const closed = { ...t, exitPrice: ep, exitTime: nowStr(), closeTs: Date.now(), pct, result: pct >= 0 ? 'win' : 'loss' };
        STATE.closedTrades = [closed, ...STATE.closedTrades].slice(0, 500);
        STATE.openTrades = STATE.openTrades.filter(x => x.id !== t.id);
        delete STATE.sentSigs[t.symbol];
        db.saveClosedTrade(closed);
        db.saveOpenTrades(STATE.openTrades);
        const dur = Math.round((Date.now() - t.openTs) / 60000);
        tgSend(`${pct >= 0 ? '✅' : '❌'} ${t.symbol.replace('USDT', '/USDT')}\n${t.side === 'LONG' ? '🟢' : '🔴'} ${t.side}\nالنتيجة: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%\nالمدة: ${dur}m`, STATE.settings.cxChatClose || STATE.settings.cxChat);
        broadcast({ type: 'trades', data: STATE.openTrades });
        broadcast({ type: 'closedTrades', data: STATE.closedTrades.slice(0, 100) });
        setTimeout(autoSendFromQueue, 1000);
      }
      break;
    }

    case 'closeBySymbol': {
      const { sym, side, pct = 100 } = msg.data;
      for (const acc of STATE.copyAccounts.filter(a => a.isEnabled !== false)) {
        if (!acc.apiKey || !acc.apiSecret) continue;
        try {
          const pos = (await getPositions(acc)).find(p => p.symbol === sym);
          if (!pos) continue;
          const closeAmt = roundQty(Math.abs(parseFloat(pos.positionAmt)) * (pct / 100), sym);
          if (closeAmt <= 0) continue;
          await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', {
            symbol: sym, side: side === 'LONG' ? 'SELL' : 'BUY',
            type: 'MARKET', quantity: closeAmt, positionSide: 'BOTH', reduceOnly: true
          });
          addCopyLog('success', `🔒 إغلاق ${pct}% من ${sym} — ${acc.name}`);
        } catch (e) { addCopyLog('fail', `❌ إغلاق ${sym} — ${acc.name}: ${e.message}`); }
      }
      await Promise.all(STATE.copyAccounts.filter(a => a.apiKey && a.apiSecret).map(async acc => {
        try { [acc.livePositions, acc.liveBalance] = await Promise.all([getPositions(acc), getBalance(acc)]); } catch (e) {}
      }));
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    case 'exportData': {
      const payload = {
        settings: STATE.settings,
        accounts: STATE.copyAccounts.map(a => ({ ...a, livePositions: undefined, liveBalance: undefined, apiOk: undefined, closedTrades: undefined })),
        openTrades: STATE.openTrades,
        closedTrades: STATE.closedTrades,
        exportedAt: new Date().toISOString(),
      };
      ws.send(JSON.stringify({ type: 'exportData', data: payload }));
      break;
    }

    case 'importData': {
      const d = msg.data;
      if (d.settings) { STATE.settings = { ...DEFAULT_SETTINGS, ...d.settings }; db.saveSettings(STATE.settings); }
      if (Array.isArray(d.accounts) && d.accounts.length) { db.saveAccounts(d.accounts); STATE.copyAccounts = db.loadAccounts(); }
      if (Array.isArray(d.openTrades)) { STATE.openTrades = d.openTrades; db.saveOpenTrades(STATE.openTrades); }
      if (Array.isArray(d.closedTrades)) {
        STATE.closedTrades = d.closedTrades;
        d.closedTrades.forEach(t => db.saveClosedTrade(t));
      }
      broadcast({ type: 'settings', data: STATE.settings });
      broadcast({ type: 'trades', data: STATE.openTrades });
      broadcast({ type: 'closedTrades', data: STATE.closedTrades.slice(0, 100) });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      ws.send(JSON.stringify({ type: 'importDone' }));
      break;
    }

    case 'clearAlerts':
      STATE.alerts = [];
      broadcast({ type: 'alerts', data: [] });
      break;

    case 'unlockSym':
      delete STATE.sentSigs[msg.data.sym];
      break;

    case 'addDCA': {
      let { sym, price, amt, pct, side, accIds, lev, orderType, trailingPct, useAmt } = msg.data;
      sym = sym.replace(/USDT$/i, '').toUpperCase() + 'USDT';
      if (!accIds?.length) { broadcast({ type: 'dcaError', data: 'اختر حساباً واحداً على الأقل' }); break; }
      const id = Date.now();
      STATE.dcaOrders.push({ id, sym, price: parseFloat(price), amt: parseFloat(amt || 0), pct: parseFloat(pct || 5), useAmt: !!useAmt, side, accIds, lev: lev || 20, orderType: orderType || 'MARKET', trailingPct: trailingPct || 0, done: false, createdAt: nowStr() });
      db.saveDcaOrders(STATE.dcaOrders);
      broadcast({ type: 'dcaOrders', data: STATE.dcaOrders });
      addCopyLog('info', `📌 DCA: ${sym} ${side} عند $${price} — ${pct}%`);
      break;
    }

    case 'removeDCA': {
      STATE.dcaOrders = STATE.dcaOrders.filter(o => o.id !== msg.data.id);
      db.saveDcaOrders(STATE.dcaOrders);
      broadcast({ type: 'dcaOrders', data: STATE.dcaOrders });
      break;
    }

    case 'cancelPendingOrder': {
      const { orderId, accId: cancelAccId, sym: cancelSym } = msg.data;
      const cancelAcc = STATE.copyAccounts.find(a => a.id === cancelAccId);
      if (cancelAcc?.apiKey && orderId && cancelSym) {
        try {
          await bFetch(cancelAcc.apiKey, cancelAcc.apiSecret, 'DELETE', '/fapi/v1/order', { symbol: cancelSym, orderId });
          addCopyLog('info', `🗑 أُلغي الأمر #${orderId} ${cancelSym} — ${cancelAcc.name}`);
        } catch (e) { addCopyLog('fail', `❌ إلغاء: ${e.message}`); }
      }
      STATE.pendingOrders = STATE.pendingOrders.filter(o => o.id !== orderId);
      broadcast({ type: 'pendingOrders', data: STATE.pendingOrders });
      break;
    }

    case 'sendSignalManual': {
      const { sym, side } = msg.data;
      const st = STATE.settings;
      let lv = parseInt(st.cxLev) || 20;
      const mx = await getMaxLev(sym);
      let note = '';
      if (lv > mx) { lv = Math.min(10, mx); note = `\n⚠️ رافعة عُدّلت إلى ${lv}X (الحد ${mx}X)`; }
      const origLev = st.cxLev; st.cxLev = String(lv);
      const text = buildMsg(sym, side) + note;
      st.cxLev = origLev;
      await tgSend(text, st.cxChat);
      break;
    }
  }
}

// ══════════════════════════════════════════════
//  REST API
// ══════════════════════════════════════════════
app.get('/api/ping', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.post('/api/login', async (req, res) => {
  // إصلاح أمني — Rate limiting بعد 5 محاولات فاشلة
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  if (!loginAttempts.has(ip)) loginAttempts.set(ip, { count: 0, lockUntil: 0 });
  const att = loginAttempts.get(ip);
  if (att.lockUntil > now) {
    return res.status(429).json({ error: `حاول بعد ${Math.ceil((att.lockUntil - now) / 1000)} ثانية` });
  }

  const { username, password } = req.body;
  const user = USERS.find(u => u.username === username);
  if (!user || !bcrypt.compareSync(password, user.passwordHash)) {
    att.count++;
    if (att.count >= 5) att.lockUntil = now + 300000; // 5 دقائق
    return res.status(401).json({ error: 'بيانات الدخول غير صحيحة' });
  }

  att.count = 0; att.lockUntil = 0;
  const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, username: user.username });
});

app.get('/api/state', authMiddleware, (req, res) => res.json(getPublicState()));
app.get('/api/accounts', authMiddleware, (req, res) => res.json(getSafeAccounts()));

app.get('/api/export', authMiddleware, (req, res) => {
  const payload = {
    settings: STATE.settings,
    accounts: STATE.copyAccounts.map(a => ({ ...a, livePositions: undefined, liveBalance: undefined, apiOk: undefined, closedTrades: undefined })),
    openTrades: STATE.openTrades,
    closedTrades: STATE.closedTrades,
    exportedAt: new Date().toISOString(),
  };
  res.setHeader('Content-Disposition', 'attachment; filename*=UTF-8\'\'%D9%86%D8%B3%D8%AE%D9%87%20%D8%A7%D8%AD%D8%AA%D9%8A%D8%A7%D8%B7%D9%8A%D9%87.json');
  res.setHeader('Content-Type', 'application/json');
  res.json(payload);
});

app.get('/api/binance/*', authMiddleware, async (req, res) => {
  try {
    const p = req.path.replace('/api/binance', '');
    const query = new URLSearchParams(req.query).toString();
    const data = await fetchBinance(p + (query ? '?' + query : ''));
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('*', (req, res) => {
  const fs = require('fs');
  const distIndex = path.join(__dirname, '../client/dist/index.html');
  const pubIndex = path.join(__dirname, '../client/public/index.html');
  if (fs.existsSync(distIndex)) return res.sendFile(distIndex);
  if (fs.existsSync(pubIndex)) return res.sendFile(pubIndex);
  res.send('<h1>RSI Scanner Pro</h1>');
});

// ══════════════════════════════════════════════
//  STARTUP
// ══════════════════════════════════════════════
async function init() {
  console.log('🚀 RSI Scanner Pro v3.1 starting...');

  // تحميل الحالة من قاعدة البيانات
  STATE.settings = db.loadSettings(DEFAULT_SETTINGS);
  // تحديث إعدادات التلغرام من env vars عند كل تشغيل
  if (process.env.TG_TOKEN) STATE.settings.cxToken = process.env.TG_TOKEN;
  if (process.env.TG_CHAT) STATE.settings.cxChat = process.env.TG_CHAT;
  if (process.env.TG_CHAT_CLOSE) STATE.settings.cxChatClose = process.env.TG_CHAT_CLOSE;
  db.saveSettings(STATE.settings);

  STATE.copyAccounts = db.loadAccounts();
  // seed الحسابات من env vars إذا كانت القائمة فارغة
  if (STATE.copyAccounts.length === 0) {
    const seed = [];
    if (process.env.MASTER_KEY && process.env.MASTER_SECRET)
      seed.push({ id: 1, name: process.env.MASTER_NAME || 'هيثم', tag: 'personal', isMaster: true, isEnabled: true, apiKey: process.env.MASTER_KEY, apiSecret: process.env.MASTER_SECRET, sizeRatio: 5, balance: 0, balanceAt: null, stats: { opens:0,closes:0,wins:0,losses:0,tot:0 } });
    if (process.env.FOLLOWER1_KEY && process.env.FOLLOWER1_SECRET)
      seed.push({ id: 2, name: process.env.FOLLOWER1_NAME || 'محمد', tag: 'personal', isMaster: false, isEnabled: true, apiKey: process.env.FOLLOWER1_KEY, apiSecret: process.env.FOLLOWER1_SECRET, sizeRatio: parseFloat(process.env.FOLLOWER1_RATIO || '1'), balance: 0, balanceAt: null, stats: { opens:0,closes:0,wins:0,losses:0,tot:0 } });
    if (seed.length) { db.saveAccounts(seed); STATE.copyAccounts = db.loadAccounts(); console.log(`🌱 Seeded ${seed.length} accounts from env vars`); }
  }

  STATE.openTrades = db.loadOpenTrades();
  STATE.closedTrades = db.loadClosedTrades();
  STATE.dcaOrders = db.loadDcaOrders();
  STATE.alerts = db.loadAlerts();
  alertId = STATE.alerts.reduce((m, a) => Math.max(m, a.id || 0), 0);
  console.log(`📦 DB loaded: ${STATE.copyAccounts.length} accounts, ${STATE.openTrades.length} trades, ${STATE.dcaOrders.length} DCA orders`);

  try {
    const d = await fetchBinance('/fapi/v1/exchangeInfo');
    STATE.symbols = d.symbols
      .filter(s => s.quoteAsset === 'USDT' && s.contractType === 'PERPETUAL' && s.status === 'TRADING')
      .map(s => {
        const lot = s.filters.find(f => f.filterType === 'LOT_SIZE');
        if (lot) lotSizeCache[s.symbol] = parseFloat(lot.stepSize);
        return s.symbol;
      }).sort();
    console.log(`✅ Loaded ${STATE.symbols.length} symbols`);
    STATE.symbols.forEach(s => {
      if (!STATE.symbolData[s]) STATE.symbolData[s] = { rsi: null, prevRsi: null, signal: null, conf: null, zone: 'neutral', error: false };
    });
    try {
      const v = await fetchBinance('/fapi/v1/ticker/24hr');
      if (Array.isArray(v)) v.forEach(t => {
        if (!STATE.symbolMeta[t.symbol]) STATE.symbolMeta[t.symbol] = {};
        STATE.symbolMeta[t.symbol].vol = parseFloat(t.quoteVolume) || 0;
      });
    } catch (e) {}
    await scanAll();
    startBinanceWS();
    setInterval(async () => {
      if (!binanceWs || binanceWs.readyState !== WebSocket.OPEN) await scanAll();
    }, 120000);
    updateEMA200();
    setInterval(updateEMA200, 10 * 60 * 1000);
  } catch (e) {
    console.error('❌ Init failed:', e.message);
  }

  // heartbeat log
  setInterval(() => {
    console.log(`💓 ${new Date().toISOString()} | Symbols:${STATE.symbols.length} | Clients:${clients.size} | Accounts:${STATE.copyAccounts.length}`);
  }, 300000);

  // تحديث نسب الانعكاس في قائمة الانتظار كل 10 ثوانٍ
  setInterval(() => {
    if (STATE.waitQueue.length && clients.size)
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
  }, 10000);

  // تحديث مراكز الماستر كل 30 ثانية حتى لو النسخ متوقف
  // يكتشف إغلاق صفقة → يرسل من القائمة تلقائياً
  let prevMasterCount = -1;
  setInterval(async () => {
    if (STATE.copyOn) return; // syncCopy يتولى هذا عند تشغيل النسخ
    const master = STATE.copyAccounts.find(a => a.isMaster);
    if (!master?.apiKey || !master?.apiSecret) return;
    try {
      master.livePositions = await getPositions(master);
      master.liveBalance = await getBalance(master);
      master.apiOk = true;
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      const openCount = master.livePositions.filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0).length;
      if (prevMasterCount > 0 && openCount < prevMasterCount) {
        // أُغلقت صفقة — حرّر خانة → أرسل من القائمة
        setTimeout(autoSendFromQueue, 1000);
      }
      prevMasterCount = openCount;
    } catch {}
  }, 30000);

  // self-ping كل 25 ثانية لمنع النوم
  const selfHost = process.env.RAILWAY_PUBLIC_DOMAIN || process.env.RAILWAY_STATIC_URL?.replace('https://', '');
  if (selfHost) {
    setInterval(() => {
      https.get(`https://${selfHost}/api/ping`, res => {
        res.resume();
      }).on('error', () => {});
    }, 25000);
    console.log(`🔁 Self-ping active → https://${selfHost}/api/ping`);
  }
}

server.listen(PORT, () => {
  console.log(`✅ Server on port ${PORT}`);
  init();
});
