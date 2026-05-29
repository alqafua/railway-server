require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');
const fetch = require('node-fetch');
const crypto = require('crypto');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'rsi_scanner_secret_2024';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS || '*';
const BASE = 'https://fapi.binance.com';
const WS_BASE = 'wss://fstream.binance.com/stream';

// CORS — يسمح لـ Vercel يتصل بالسيرفر
app.use(cors({
  origin: ALLOWED_ORIGINS === '*' ? '*' : ALLOWED_ORIGINS.split(','),
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true
}));
app.options('*', cors());

// ══════════════════════════════════════════════
//  STATE (in-memory — كل شي في الذاكرة)
// ══════════════════════════════════════════════
const STATE = {
  symbols: [],
  symbolData: {},
  symbolMeta: {},
  alerts: [],
  openTrades: [],
  closedTrades: [],
  copyAccounts: [],
  copyLog: [],
  masterPositions: {},
  settings: {
    mode: 'SMA', maPeriod: 14, interval: '1h',
    autoSend: false, enableDiv: true, blockOpen: true,
    cxMargin: 'Cross', cxLev: '20', cxAmt: '5%',
    cxSLon: true, cxSL: '2',
    cxTP1: '3', cxTP1Amt: '50', cxTP2on: false, cxTP2: '6', cxTP2Amt: '50',
    cxTrailTp: 'off', cxTrailPct: '0.5', cxEntryTrail: '0.5%',
    cxToken: '', cxChat: '', cxChatClose: '',
    cxEntry2on: false, cxEntry2Dist: '2', cxEntry2Amt: '50',
    cxEntry3on: false, cxEntry3Dist: '4', cxEntry3Amt: '50',
    cxBEon: false,
    trSon: false, trSstart: 75, trSgap: 3,
    trLon: false, trLstart: 25, trLgap: 3,
    liqVon: false, liqVmin: 50000000,
    liqOon: false, liqOmin: 10000000,
    revMode: 'candles', revCount: 1, rsiGap: 1,
    dataMode: 'ws', soundEnabled: true,
  },
  rsiPeaks: {},
  cooldowns: {},
  sentSigs: {},
  copyOn: false,
};

// ══════════════════════════════════════════════
//  AUTH
// ══════════════════════════════════════════════
const USERS = [
  { id: 1, username: 'Alqafua', passwordHash: bcrypt.hashSync('7007', 10) }
];

function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    res.status(401).json({ error: 'Invalid token' });
  }
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

async function fetchBinance(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`Binance ${res.status}`);
  return res.json();
}

async function getMaxLev(sym) {
  if (STATE.symbolMeta[sym]?.ml) return STATE.symbolMeta[sym].ml;
  try {
    const d = await fetchBinance(`/fapi/v1/leverageBracket?symbol=${sym}`);
    const l = d[0]?.brackets[0]?.initialLeverage || 125;
    if (!STATE.symbolMeta[sym]) STATE.symbolMeta[sym] = {};
    STATE.symbolMeta[sym].ml = l; return l;
  } catch (e) { return 125; }
}

async function hmac256(secret, msg) {
  const key = crypto.createHmac('sha256', secret);
  key.update(msg);
  return key.digest('hex');
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
  if (st.cxSLon) { L.push('Stop Targets:', `1) ${fp(sll)}`, ''); }
  L.push('Trailing Configuration:', `Entry: Percentage (${st.cxEntryTrail})`);
  if (st.cxTrailTp === 'on') L.push(`Take-Profit: Percentage (${st.cxTrailPct}%)`);
  if (st.cxBEon) L.push('Stop: Breakeven - Trigger: Target (1)');
  return L.join('\n');
}

async function sendSignal(sym, side) {
  const st = STATE.settings;
  if (!st.autoSend || !st.cxToken || !st.cxChat) return;
  if (STATE.sentSigs[sym]) return;
  STATE.sentSigs[sym] = Date.now();
  let lv = parseInt(st.cxLev) || 20;
  const mx = await getMaxLev(sym);
  let note = '';
  if (lv > mx) { lv = Math.min(10, mx); note = `\n⚠️ رافعة عُدّلت إلى ${lv}X`; }
  const origLev = st.cxLev; st.cxLev = String(lv);
  const text = buildMsg(sym, side) + note;
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

function triggerAlert(sym, sig, val) {
  const st = STATE.settings;
  const key = `${sym}_${sig.type}`, now = Date.now();
  if (STATE.cooldowns[key] && now - STATE.cooldowns[key] < 900000) return;
  if (st.blockOpen && STATE.openTrades.some(t => t.symbol === sym)) return;
  if (STATE.sentSigs[sym]) return;
  if (!isLiquid(sym)) return;
  STATE.cooldowns[key] = now;
  alertId++;
  const item = {
    id: alertId, symbol: sym, type: sig.type, label: sig.label,
    color: sig.color, emoji: sig.emoji, rsi: val.toFixed(2),
    time: nowStr(), mode: `${st.mode}(${st.mode === 'RSI' ? RSI_P : st.maPeriod})`, side: sig.side
  };
  STATE.alerts = [item, ...STATE.alerts].slice(0, 200);
  sendSignal(sym, sig.side);
  broadcast({ type: 'alert', data: item });
  broadcast({ type: 'state', data: getPublicState() });
}

async function scanSym(sym) {
  try {
    const cls = candleCache[sym];
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
    const fSig = trail || sig;
    STATE.symbolData[sym] = { rsi: cu, prevRsi: pv, signal: fSig, conf, zone, error: false, trailActive: !!trail };
    if (fSig && (!old.signal || old.signal.type !== fSig.type)) triggerAlert(sym, fSig, cu);
    if (conf && (!old.conf || old.conf.type !== conf.type)) triggerAlert(sym, conf, cu);
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
  broadcast({ type: 'state', data: getPublicState() });
  scanRunning = false;
}

// ══════════════════════════════════════════════
//  WEBSOCKET — Binance Live
// ══════════════════════════════════════════════
let binanceWs = null;
let binanceReconn = null;

function startBinanceWS() {
  if (!STATE.symbols.length) return;
  stopBinanceWS();
  const streams = STATE.symbols.map(s => `${s.toLowerCase()}@kline_${STATE.settings.interval}`).join('/');
  binanceWs = new WebSocket(`${WS_BASE}?streams=${streams}`);
  binanceWs.on('open', () => {
    console.log('✅ Binance WebSocket connected');
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
        candleCache[sym].push(close);
        if (candleCache[sym].length > 200) candleCache[sym] = candleCache[sym].slice(-200);
        scanSym(sym);
      } else {
        const a = candleCache[sym];
        if (a.length > 0) {
          const st = STATE.settings;
          const tmp = [...a.slice(0, -1), close];
          if (tmp.length >= RSI_P + 2) {
            const cu = computeInd(tmp, st.mode, st.maPeriod);
            if (STATE.symbolData[sym]) {
              STATE.symbolData[sym].rsi = cu;
              STATE.symbolData[sym].zone = cu >= 70 ? 'ob' : cu <= 30 ? 'os' : 'neutral';
            }
          }
        }
      }
      // broadcast price update
      if (STATE.symbolData[sym]) {
        broadcastThrottled({ type: 'priceUpdate', data: { sym, rsi: STATE.symbolData[sym].rsi, zone: STATE.symbolData[sym].zone, price: close } });
      }
    } catch (e) {}
  });
  binanceWs.on('close', () => {
    console.log('⚠️ Binance WS closed, reconnecting...');
    broadcast({ type: 'wsStatus', data: 'disconnected' });
    scheduleReconn();
  });
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

// Throttle broadcast لتجنب إرسال كثير
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
      }, 500);
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

async function openFollower(acc, mPos) {
  try {
    const bal = await getBalance(acc);
    if (bal <= 0) throw new Error('رصيد غير كافٍ');
    const ratio = parseFloat(acc.sizeRatio) || 5;
    const sym = mPos.symbol, amt = parseFloat(mPos.positionAmt);
    const side = amt > 0 ? 'BUY' : 'SELL';
    const price = livePrices[sym] || parseFloat(mPos.markPrice) || 1;
    const lev = Math.min(parseFloat(mPos.leverage) || 20, 125);
    const qty = parseFloat(((bal * (ratio / 100) * lev) / price).toFixed(3));
    if (qty <= 0) throw new Error('الكمية صغيرة جداً');
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/leverage', { symbol: sym, leverage: lev });
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', { symbol: sym, side, type: 'MARKET', quantity: qty, positionSide: 'BOTH' });
    if (!acc.stats) acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
    acc.stats.opens++;
    addCopyLog('success', `✅ ${acc.name}: فُتحت ${sym} ${amt > 0 ? 'LONG' : 'SHORT'} × ${qty}`);
    return true;
  } catch (e) { addCopyLog('fail', `❌ ${acc.name}/${mPos.symbol}: ${e.message}`); return false; }
}

async function closeFollower(acc, sym, posAmt) {
  try {
    const side = posAmt > 0 ? 'SELL' : 'BUY', qty = Math.abs(posAmt).toFixed(3);
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', { symbol: sym, side, type: 'MARKET', quantity: qty, positionSide: 'BOTH', reduceOnly: true });
    addCopyLog('success', `🔒 ${acc.name}: أُغلقت ${sym}`);
    return true;
  } catch (e) { addCopyLog('fail', `❌ إغلاق ${acc.name}/${sym}: ${e.message}`); return false; }
}

async function syncCopy() {
  if (!STATE.copyOn) return;
  const master = STATE.copyAccounts.find(a => a.isMaster);
  if (!master?.apiKey || !master?.apiSecret) return;
  let curr;
  try {
    const p = await getPositions(master);
    curr = {}; p.forEach(x => curr[x.symbol] = x);
    master.livePositions = p; master.liveBalance = await getBalance(master);
    master.apiOk = true;
  } catch (e) { addCopyLog('fail', `❌ ماستر: ${e.message}`); master.apiOk = false; return; }
  const prev = STATE.masterPositions || {};
  const followers = STATE.copyAccounts.filter(a => !a.isMaster && a.isEnabled);

  // فتح صفقات — سواء جديدة أو موجودة ما عند التابع
  for (const [sym, pos] of Object.entries(curr)) {
    const isNew = !prev[sym];
    if (isNew) addCopyLog('info', `📡 ماستر فتح: ${sym} ${parseFloat(pos.positionAmt) > 0 ? 'LONG' : 'SHORT'}`);
    for (const f of followers) {
      const fPos = (f.livePositions || []).find(p => p.symbol === sym);
      if (!fPos) {
        await openFollower(f, pos);
        await new Promise(r => setTimeout(r, 200));
      }
    }
    if (isNew) {
      tgSend(`🪞 Copy\n#${sym.replace('USDT', '/USDT')}\nماستر فتح: ${parseFloat(pos.positionAmt) > 0 ? 'Long' : 'Short'}\nحسابات: ${followers.length}`, STATE.settings.cxChat);
    }
  }

  // إغلاق صفقات أغلقها الماستر
  for (const [sym] of Object.entries(prev)) {
    if (!curr[sym]) {
      addCopyLog('info', `📡 ماستر أغلق: ${sym}`);
      for (const f of followers) {
        const fp = (f.livePositions || []).find(p => p.symbol === sym);
        if (fp) await closeFollower(f, sym, parseFloat(fp.positionAmt));
        await new Promise(r => setTimeout(r, 200));
      }
      tgSend(`🔒 Copy أُغلقت ${sym.replace('USDT', '/USDT')}`, STATE.settings.cxChatClose || STATE.settings.cxChat);
    }
  }

  // تحديث بيانات التابعين
  for (const acc of followers) {
    try { acc.livePositions = await getPositions(acc); acc.liveBalance = await getBalance(acc); acc.apiOk = true; }
    catch (e) { acc.apiOk = false; }
  }
  STATE.masterPositions = curr;
  broadcast({ type: 'accounts', data: getSafeAccounts() });
}

function startCopy() {
  if (copyTimer) clearInterval(copyTimer);
  STATE.copyOn = true;
  syncCopy();
  copyTimer = setInterval(syncCopy, 1500);
  addCopyLog('info', '▶️ بدأ النسخ');
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
      for (const p of pos) { await closeFollower(acc, p.symbol, parseFloat(p.positionAmt)); await new Promise(r => setTimeout(r, 150)); }
    } catch (e) {}
  }
  addCopyLog('info', '✅ اكتمل الإيقاف');
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
    stats: a.stats || { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 }
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
    lastUpdate: nowStr(),
  };
}

wss.on('connection', (ws) => {
  clients.add(ws);
  // إرسال الحالة الكاملة فوراً
  ws.send(JSON.stringify({ type: 'init', data: getPublicState() }));
  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);
      await handleClientMsg(msg);
    } catch (e) {}
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

async function handleClientMsg(msg) {
  switch (msg.type) {
    case 'updateSettings':
      Object.assign(STATE.settings, msg.data);
      broadcast({ type: 'settings', data: STATE.settings });
      break;
    case 'scanNow':
      scanAll();
      break;
    case 'toggleCopy':
      STATE.copyOn ? stopCopy() : startCopy();
      break;
    case 'emergencyStop':
      await emergencyStop();
      break;
    case 'addAccount': {
      const acc = msg.data;
      try {
        const bal = await getBalance(acc);
        acc.id = Date.now();
        acc.balance = bal; acc.balanceAt = Date.now();
        acc.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
        acc.apiOk = true;
        if (acc.isMaster || STATE.copyAccounts.length === 0) {
          STATE.copyAccounts.forEach(a => a.isMaster = false);
          acc.isMaster = true;
        }
        STATE.copyAccounts.push(acc);
        addCopyLog('success', `➕ أُضيف: ${acc.name} — $${bal.toFixed(2)}`);
        broadcast({ type: 'accounts', data: getSafeAccounts() });
        broadcast({ type: 'addAccountResult', data: { success: true, balance: bal } });
      } catch (e) {
        broadcast({ type: 'addAccountResult', data: { success: false, error: e.message } });
      }
      break;
    }
    case 'testAccount': {
      const acc = msg.data;
      try {
        const bal = await getBalance(acc);
        broadcast({ type: 'testAccountResult', data: { success: true, balance: bal } });
      } catch (e) {
        broadcast({ type: 'testAccountResult', data: { success: false, error: e.message } });
      }
      break;
    }
    case 'updateAccount': {
      const idx = STATE.copyAccounts.findIndex(a => a.id === msg.data.id);
      if (idx >= 0) { Object.assign(STATE.copyAccounts[idx], msg.data); broadcast({ type: 'accounts', data: getSafeAccounts() }); }
      break;
    }
    case 'deleteAccount':
      STATE.copyAccounts = STATE.copyAccounts.filter(a => a.id !== msg.data.id);
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    case 'toggleAccount': {
      const acc = STATE.copyAccounts.find(a => a.id === msg.data.id);
      if (acc) { acc.isEnabled = !acc.isEnabled; broadcast({ type: 'accounts', data: getSafeAccounts() }); }
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
    case 'confirmTrade': {
      const a = msg.data;
      STATE.openTrades = [{ id: Date.now(), symbol: a.symbol, side: a.side, entryPrice: livePrices[a.symbol] || 0, openTime: a.time, openTs: Date.now(), sl: STATE.settings.cxSL, tp1: STATE.settings.cxTP1, leverage: STATE.settings.cxLev, margin: STATE.settings.cxMargin, label: a.label }, ...STATE.openTrades];
      broadcast({ type: 'trades', data: STATE.openTrades });
      break;
    }

    // ─── تنفيذ مباشر من الإشارة ───
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
          const lev = Math.min(parseInt(st.cxLev) || 20, await getMaxLev(sym));
          const amtStr = st.cxAmt || '5%';
          const amtPct = parseFloat(amtStr) / 100;
          const qty = parseFloat(((bal * amtPct * lev) / price).toFixed(3));
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
      // أضف للصفقات المفتوحة
      const ep = livePrices[sym] || 0;
      STATE.openTrades = [{ id: Date.now(), symbol: sym, side, entryPrice: ep, openTime: nowStr(), openTs: Date.now(), sl: st.cxSL, tp1: st.cxTP1, leverage: st.cxLev, margin: st.cxMargin, label: `تنفيذ مباشر`, executed: true }, ...STATE.openTrades];
      // إرسال تلغرام
      await tgSend(buildMsg(sym, side), st.cxChat);
      broadcast({ type: 'executeResult', data: results });
      broadcast({ type: 'trades', data: STATE.openTrades });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    // ─── إغلاق جزئي ───
    case 'closePartial': {
      const { tradeId, pct } = msg.data; // pct = نسبة الإغلاق (50 = 50%)
      const t = STATE.openTrades.find(x => x.id === tradeId);
      if (!t) break;
      const accs = STATE.copyAccounts.filter(a => a.isEnabled !== false);
      for (const acc of accs) {
        if (!acc.apiKey || !acc.apiSecret) continue;
        try {
          const pos = (await getPositions(acc)).find(p => p.symbol === t.symbol);
          if (!pos) continue;
          const totalAmt = Math.abs(parseFloat(pos.positionAmt));
          const closeAmt = parseFloat((totalAmt * (pct / 100)).toFixed(3));
          if (closeAmt <= 0) continue;
          await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', {
            symbol: t.symbol, side: t.side === 'LONG' ? 'SELL' : 'BUY',
            type: 'MARKET', quantity: closeAmt, positionSide: 'BOTH', reduceOnly: true
          });
          addCopyLog('success', `🔒 إغلاق ${pct}% من ${t.symbol} — ${acc.name}`);
        } catch (e) {
          addCopyLog('fail', `❌ إغلاق جزئي ${acc.name}: ${e.message}`);
        }
      }
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }
    case 'closeTrade': {
      const t = STATE.openTrades.find(x => x.id === msg.data.id);
      if (t) {
        const ep = livePrices[t.symbol] || t.entryPrice;
        const pct = t.side === 'LONG' ? ((ep - t.entryPrice) / t.entryPrice) * 100 : ((t.entryPrice - ep) / t.entryPrice) * 100;
        STATE.closedTrades = [{ ...t, exitPrice: ep, exitTime: nowStr(), closeTs: Date.now(), pct, result: pct >= 0 ? 'win' : 'loss' }, ...STATE.closedTrades].slice(0, 500);
        STATE.openTrades = STATE.openTrades.filter(x => x.id !== t.id);
        delete STATE.sentSigs[t.symbol];
        const dur = Math.round((Date.now() - t.openTs) / 60000);
        tgSend(`${pct >= 0 ? '✅' : '❌'} ${t.symbol.replace('USDT', '/USDT')}\n${t.side === 'LONG' ? '🟢' : '🔴'} ${t.side}\nالنتيجة: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%\nالمدة: ${dur}m`, STATE.settings.cxChatClose || STATE.settings.cxChat);
        broadcast({ type: 'trades', data: STATE.openTrades });
        broadcast({ type: 'closedTrades', data: STATE.closedTrades.slice(0, 100) });
      }
      break;
    }
    case 'clearAlerts':
      STATE.alerts = [];
      broadcast({ type: 'alerts', data: [] });
      break;
    case 'unlockSym':
      delete STATE.sentSigs[msg.data.sym];
      break;
    case 'sendSignalManual':
      await tgSend(buildMsg(msg.data.sym, msg.data.side), STATE.settings.cxChat);
      break;
  }
}

// ══════════════════════════════════════════════
//  REST API
// ══════════════════════════════════════════════
app.get('/api/ping', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  const user = USERS.find(u => u.username === username);
  if (!user || !bcrypt.compareSync(password, user.passwordHash)) {
    return res.status(401).json({ error: 'بيانات الدخول غير صحيحة' });
  }
  const token = jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, username: user.username });
});

app.get('/api/state', authMiddleware, (req, res) => {
  res.json(getPublicState());
});

app.get('/api/accounts', authMiddleware, (req, res) => {
  res.json(getSafeAccounts());
});

// Proxy لـ Binance (يحل مشكلة CORS)
app.get('/api/binance/*', authMiddleware, async (req, res) => {
  try {
    const path = req.path.replace('/api/binance', '');
    const query = new URLSearchParams(req.query).toString();
    const url = `${BASE}${path}${query ? '?' + query : ''}`;
    const data = await fetchBinance(path + (query ? '?' + query : ''));
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Serve frontend
app.get('*', (req, res) => {
  const distIndex = path.join(__dirname, '../client/dist/index.html');
  const pubIndex = path.join(__dirname, '../client/public/index.html');
  const fs = require('fs');
  if (fs.existsSync(distIndex)) return res.sendFile(distIndex);
  if (fs.existsSync(pubIndex)) return res.sendFile(pubIndex);
  res.send('<h1>RSI Scanner Pro</h1><p>Please build the client or add index.html to client/public/</p>');
});

// ══════════════════════════════════════════════
//  STARTUP
// ══════════════════════════════════════════════
async function init() {
  console.log('🚀 RSI Scanner Pro v3 starting...');
  try {
    const d = await fetchBinance('/fapi/v1/exchangeInfo');
    STATE.symbols = d.symbols
      .filter(s => s.quoteAsset === 'USDT' && s.contractType === 'PERPETUAL' && s.status === 'TRADING')
      .map(s => s.symbol).sort();
    console.log(`✅ Loaded ${STATE.symbols.length} symbols`);
    STATE.symbols.forEach(s => {
      STATE.symbolData[s] = { rsi: null, prevRsi: null, signal: null, conf: null, zone: 'neutral', error: false };
    });
    // تحميل الأحجام
    try {
      const v = await fetchBinance('/fapi/v1/ticker/24hr');
      if (Array.isArray(v)) v.forEach(t => {
        if (!STATE.symbolMeta[t.symbol]) STATE.symbolMeta[t.symbol] = {};
        STATE.symbolMeta[t.symbol].vol = parseFloat(t.quoteVolume) || 0;
      });
    } catch (e) {}
    // بدء الفحص
    await scanAll();
    startBinanceWS();
    // فحص دوري للـ REST fallback
    setInterval(async () => {
      if (!binanceWs || binanceWs.readyState !== WebSocket.OPEN) {
        await scanAll();
      }
    }, 120000);
  } catch (e) {
    console.error('❌ Init failed:', e.message);
  }
}

server.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
  init();
});
