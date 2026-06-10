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
const BT = require('./backtest');
const btState = { busy: false, cancel: false };

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
  stTF: '4h',
  stPeriod: 10,
  stMult: 3,
  stFilterOn: false,
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
  superTrend: { value: null, direction: null, btcPrice: null, updatedAt: null },
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

  // فلتر EMA 200 و SuperTrend — يؤثر على القائمة والإرسال لكن ليس السجل (AND logic عند تفعيل الاثنين)
  {
    const emaOn = st.ema200FilterOn && STATE.ema200?.direction;
    const stOn = st.stFilterOn && STATE.superTrend?.direction;
    if (emaOn || stOn) {
      let pass = true;
      if (emaOn) {
        const up = STATE.ema200.direction === 'up';
        pass = pass && (sig.side === 'LONG' ? up : !up);
      }
      if (stOn) {
        const up = STATE.superTrend.direction === 'up';
        pass = pass && (sig.side === 'LONG' ? up : !up);
      }
      if (!pass) return;
    }
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
    const klines = await fetchBinance(`/fapi/v1/klines?symbol=BTCUSDT&interval=${tf}&limit=210`);
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

function calcSuperTrend(klines, period, mult) {
  const n = klines.length;
  if (n < period + 2) return null;
  const H = klines.map(k => parseFloat(k[2]));
  const L = klines.map(k => parseFloat(k[3]));
  const C = klines.map(k => parseFloat(k[4]));
  const atrArr = new Array(n).fill(0);
  let sumTR = 0;
  for (let i = 1; i <= period; i++) {
    sumTR += Math.max(H[i] - L[i], Math.abs(H[i] - C[i - 1]), Math.abs(L[i] - C[i - 1]));
  }
  atrArr[period] = sumTR / period;
  for (let i = period + 1; i < n; i++) {
    const tr = Math.max(H[i] - L[i], Math.abs(H[i] - C[i - 1]), Math.abs(L[i] - C[i - 1]));
    atrArr[i] = (atrArr[i - 1] * (period - 1) + tr) / period;
  }
  let prevUB = 0, prevLB = 0, prevDir = 1, finalDir = 1, finalST = 0;
  for (let i = period; i < n; i++) {
    const hl2 = (H[i] + L[i]) / 2;
    const basicUB = hl2 + mult * atrArr[i];
    const basicLB = hl2 - mult * atrArr[i];
    const ub = i === period ? basicUB : (basicUB < prevUB || C[i - 1] > prevUB ? basicUB : prevUB);
    const lb = i === period ? basicLB : (basicLB > prevLB || C[i - 1] < prevLB ? basicLB : prevLB);
    let dir;
    if (i === period) dir = C[i] > lb ? 1 : -1;
    else if (prevDir === -1 && C[i] > prevUB) dir = 1;
    else if (prevDir === 1 && C[i] < prevLB) dir = -1;
    else dir = prevDir;
    finalDir = dir; finalST = dir === 1 ? lb : ub;
    prevUB = ub; prevLB = lb; prevDir = dir;
  }
  return { direction: finalDir === 1 ? 'up' : 'down', value: parseFloat(finalST.toFixed(2)) };
}

async function updateSuperTrend() {
  try {
    const tf = STATE.settings.stTF || '4h';
    const period = parseInt(STATE.settings.stPeriod) || 10;
    const mult = parseFloat(STATE.settings.stMult) || 3;
    const limit = Math.max(100, period * 4);
    const klines = await fetchBinance(`/fapi/v1/klines?symbol=BTCUSDT&interval=${tf}&limit=${limit}`);
    if (!Array.isArray(klines) || klines.length < period + 2) return;
    const result = calcSuperTrend(klines, period, mult);
    if (!result) return;
    STATE.superTrend = {
      direction: result.direction,
      value: result.value,
      btcPrice: parseFloat(parseFloat(klines[klines.length - 1][4]).toFixed(2)),
      updatedAt: new Date().toISOString()
    };
    broadcast({ type: 'superTrend', data: STATE.superTrend });
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