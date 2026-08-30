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
  mode: 'SMA', maPeriod: 14, interval: '1h', extraIntervals: [],
  autoSend: false, enableDiv: true, blockOpen: true,
  sigFilters: { ob: true, os: true, conf: true, trail: true },
  cxMargin: 'Cross', cxLev: '20', cxAmt: '1%', cxAmtMax: '0',
  cxSLon: false, cxSL: '2', cxSLMax: '0',
  cxTP1: '3', cxTP1Amt: '50', cxTP2on: false, cxTP2: '6', cxTP2Amt: '50',
  cxTrailTp: 'on', cxTrailPct: '0.5', cxEntryTrail: '0.5%',
  cxToken: process.env.TG_TOKEN || '',
  // قناة الإشارات الثابتة — تُستخدم ما لم يُضبط TG_CHAT أو تُحفظ قيمة من الواجهة
  cxChat: process.env.TG_CHAT || '-1004495709499',
  cxChatClose: process.env.TG_CHAT_CLOSE || '',
  // لا تضع معرّفات قنوات ثابتة هنا — على خادم جديد تصبح قنوات لا يملكها البوت
  // فيفشل الإرسال بـ "chat not found" مع كل إشارة
  cxChatBT: process.env.TG_CHAT_BT || '',
  cxChatSettings: process.env.TG_CHAT_SETTINGS || '',
  cxEntry2on: true, cxEntry2Dist: '0.2', cxEntry2Amt: '50',
  cxBEon: false,
  trSon: false, trSstart: 75, trSgap: 3,
  trLon: false, trLstart: 25, trLgap: 3,
  liqVon: false, liqVmin: 50000000,
  liqOon: false, liqOmin: 10000000,
  revMode: 'candles', revCount: 1, rsiGap: 1,
  dataMode: 'ws', soundEnabled: true,
  maxOpenTrades: 0,
  // إرسال تلقائي من قائمة الانتظار عند بلوغ الانعكاس نسبةً محدّدة.
  // استثناء صريح من حدّ الصفقات المفتوحة — النسبة الكبيرة هي المبرّر.
  queueRevAutoOn: false,
  queueRevAutoPct: 10,
  sigQueueFilters: { ob: true, os: true, conf: true, trail: true },
  ema200TF: '4h',
  ema200FilterOn: false,
  stTF: '4h',
  stPeriod: 10,
  stMult: 3,
  stFilterOn: false,
  respectFilterOn: false,
  respectMin: 50,
  perSymSTon: false,
  perSymSTtf: '4h',
  perSymSigTF: '5m',
  perSymSigMode: 'RSI',
  deadZoneOn: false,
  deadZoneMode: 'wait',
  deadZonePct: 1,
  stSLon: false,
  stSLmode: 'flip',
  stSLpct: 0.5,
  cxChatSTSim: '',
  // ── نظام القفل ─────────────────────────────────────────
  lockOn: false,            // المفتاح العام لنظام القفل
  lockMaster: false,        // القفل العام — يمنع تعديل الحد اليومي
  lockMasterHours: 24,      // مدّته بالساعات (٠ = دائم بلا انتهاء)
  lockAllSettings: false,   // يوسّع القفل ليشمل كل إعدادات البوت لا الحد اليومي وحده
  lockAutoSLon: false,      // (1) ستوب تلقائي للصفقات اليدوية
  lockAutoSLpct: 2,         // نسبة الستوب من السعر %
  lockDailyOn: false,       // (2)+(3) الحد اليومي
  lockDailyAmt: 10,         // الحد بالدولار (هامش الصفقة + سقف الخسارة اليومية)
  lockDailyHours: 24,       // مدة النافذة / الانتظار بالساعات
  lockAutoBEon: false,      // (4) بريك إيفن تلقائي عند اقتراب/انعكاس الاتجاه
  lockBEtrig: 'near',       // متى يشتغل: near = قرب الانعكاس · flip = انعكاس فعلي · both
  lockBEnearPct: 1,         // قرب السعر من خط السوبر/EMA لاعتباره "قرب انعكاس" %
  lockBEoffsetPct: 0.1,     // نسبة البريك إيفن فوق الدخول (تغطية العمولات) %
  // أقل ربح (PnL على الهامش) لتطبيق البريك إيفن — نفس النسبة التي تظهر في بايننس.
  // دونه تُترك الصفقة وشأنها، فمستوى التعادل يقع عندها فوق السعر فتُقفل فور تسجيلها.
  lockBEminPnl: 50,
  // تريلنج تلقائي عند شرط الاتجاه (وإلا فهو يدوي من الأزرار)
  lockAutoTrailOn: false,
  lockAutoTrailPct: 10,
  lockAutoTrailTrig: 'near',
  // وقف خسارة تلقائي للخاسرات عند شرط الاتجاه
  lockAutoLossOn: false,
  lockAutoLossPct: 2,
  lockAutoLossTrig: 'flip',
  lockTgChat: process.env.TG_CHAT_LOCK || '-1004312421634',   // قناة إشعارات نظام القفل
  lockCloseCmd: '/close',   // أمر إغلاق الصفقة عبر الرد على الإشارة (مؤكَّد عملياً)
  // مزامنة كورنكس بتعديل الرسالة — معطّلة افتراضياً:
  // التعديل يمسح أزرار كورنكس من الإشارة، والإغلاق يتم بأمر /close بدلاً عنه.
  lockCornixSync: false,
  lockCornixMode: 'edit',   // edit | reply | both
  lockCxBEtpl: 'SL to entry',
  lockCxSLtpl: 'New stop loss: {price}',
  lockCxTrailTpl: 'Trailing stop: {pct}%',
  dirFilter: 'all',
  useSymbolSettings: true,
  // فلتر إرسال إشارات التلغرام حسب وجود إعدادات خاصة للعملة: all = الكل، star = العملات ⭐ فقط، other = الباقي فقط
  tgStarFilter: 'all',
  lockFields: { amt: false, lev: false, sl: false, targets: false, entries: false, trailing: false, be: false },
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
  respectData: {},
  perSymST: {},
  simTrades: [],
  // نظام القفل — نافذة يومية + تتبّع الصفقات اليدوية
  lockState: {
    windowStart: 0,      // بداية النافذة الحالية
    realizedLoss: 0,     // الخسارة المحققة داخل النافذة ($)
    lockedUntil: 0,      // مقفل حتى هذا الوقت
    enabledAt: 0,        // وقت تفعيل النظام (حماية الصفقات القديمة)
    manualSyms: {},      // sym -> { ts, margin } الصفقات اليدوية المعروفة
    beDone: {},          // sym -> true (بريك إيفن مطبّق)
    ourStops: {},        // sym -> [orderId] أوامر الوقف التي وضعها النظام (لا نلغي غيرها)
    manualOverride: {},  // sym -> 'manual' | 'bot' — تصنيف يدوي يغلب الاستنتاج التلقائي
    vStops: {},          // sym -> وقف افتراضي يتابعه البوت ويغلق بأمر close عند بلوغه
    msgFate: {},         // sym -> مصير رسالة الإشارة بعد إرسالها (هل بقيت؟ هل يملكها البوت؟)
    trades: [],          // سجل مختصر لصفقات النافذة
  },
  sentMsgIds: {},        // sym -> message_id لرسالة الإشارة (لمزامنة كورنكس)
  rsiPeaks: {},          // إصلاح #1 — كان غير معرَّف
  sysStatus: { ok: true, lastError: null, errorLoc: null, errorTs: null },
  symbolSettings: {},    // إعدادات خاصة لكل عملة — تُدمج فوق STATE.settings عند توليد إشاراتها
};

// الحقول المسموح بتخصيصها لكل عملة على حدة (لا تشمل ema200TF/stTF/stPeriod/stMult لأنها
// تتطلب حساب مؤشرات BTC منفصلة لكل توليفة — تبقى عامة لكل البوت)
const SYMBOL_OVERRIDE_FIELDS = [
  'interval',
  'mode', 'maPeriod', 'revMode', 'revCount', 'enableDiv', 'dirFilter',
  'sigQueueFilters', 'sigFilters',
  'trSon', 'trSstart', 'trSgap', 'trLon', 'trLstart', 'trLgap',
  'ema200FilterOn', 'stFilterOn',
  'cxMargin', 'cxLev', 'cxAmt',
  'cxSLon', 'cxSL',
  'cxTP1', 'cxTP1Amt', 'cxTP2on', 'cxTP2', 'cxTP2Amt',
  'cxEntry2on', 'cxEntry2Dist', 'cxEntry2Amt',
  'cxTrailTp', 'cxTrailPct', 'cxEntryTrail', 'cxBEon', 'cxBEonAuto',
];

// مجموعات الحقول التي يمكن "تثبيتها" على القيم العامة دائمًا عبر STATE.settings.lockFields
const LOCK_FIELD_GROUPS = {
  amt: ['cxAmt'],
  lev: ['cxLev'],
  sl: ['cxSLon', 'cxSL'],
  targets: ['cxTP1', 'cxTP1Amt', 'cxTP2on', 'cxTP2', 'cxTP2Amt'],
  entries: ['cxEntry2on', 'cxEntry2Dist', 'cxEntry2Amt'],
  trailing: ['cxEntryTrail', 'cxTrailPct'],
  be: ['cxBEon'],
};

// يفرض على `merged` قيم الإعدادات العامة للمجموعات المُثبَّتة في lockFields، بحيث لا
// يتجاوزها أي إعداد خاص بالعملة (من الواجهة أو من ملف مرفوع)
function applyLockFields(merged) {
  const lf = STATE.settings.lockFields;
  if (!lf) return merged;
  for (const [group, fields] of Object.entries(LOCK_FIELD_GROUPS)) {
    if (lf[group]) for (const f of fields) merged[f] = STATE.settings[f];
  }
  return merged;
}

// يُرجع valStr كما هي إن لم يكن هناك حد أقصى (maxStr فاضي/٠) أو لم تتجاوزه،
// وإلا يُرجع الحد الأقصى نفسه (بنفس صيغة valStr — مع % أو بدونها)
function capNumeric(valStr, maxStr) {
  const max = parseFloat(maxStr);
  if (!max || max <= 0) return valStr;
  const num = parseFloat(valStr);
  if (isNaN(num) || num <= max) return valStr;
  return (typeof valStr === 'string' && valStr.includes('%')) ? (max + '%') : String(max);
}

// يطبّق الحد الأقصى العام (cxAmtMax/cxSLMax) على الإعدادات العامة نفسها — يُرجع
// STATE.settings كما هي (بنفس المرجع) إن لم يكن هناك حد أقصى أو لم تتجاوزه القيم الحالية
function capGlobal(settings) {
  const cxAmt = capNumeric(settings.cxAmt, settings.cxAmtMax);
  const cxSL = capNumeric(settings.cxSL, settings.cxSLMax);
  if (cxAmt === settings.cxAmt && cxSL === settings.cxSL) return settings;
  return { ...settings, cxAmt, cxSL };
}

// هل لهذه العملة إعدادات خاصة محفوظة في تبويب "لكل العملات"؟ — تُستخدم لإظهار ⭐
function hasSymOverride(sym) {
  return !!(STATE.symbolSettings[sym] && Object.keys(STATE.symbolSettings[sym]).length);
}

// يدمج إعدادات العملة الخاصة (إن وُجدت) فوق الإعدادات العامة — يُرجع STATE.settings
// كما هي (بنفس المرجع) إذا لم تكن للعملة إعدادات خاصة، لضمان عدم تغيير السلوك الحالي
function settingsFor(sym) {
  if (STATE.settings.useSymbolSettings === false) return capGlobal(STATE.settings);
  let base = STATE.settings;
  const ov = STATE.symbolSettings[sym];
  if (!ov || !Object.keys(ov).length) return capGlobal(base);
  const merged = { ...base, ...ov };
  if (ov.sigQueueFilters) merged.sigQueueFilters = { ...STATE.settings.sigQueueFilters, ...ov.sigQueueFilters };
  if (ov.sigFilters) merged.sigFilters = { ...STATE.settings.sigFilters, ...ov.sigFilters };
  // البريك ايفن: إن فعّل المستخدم "استخدام الإعداد العام" لهذه العملة، يتبع البريك ايفن
  // القيمة العامة الحالية دائماً (حتى لو غُيّرت لاحقاً)، متجاهلاً قيمة cxBEon المحفوظة للعملة
  if (ov.cxBEonAuto) merged.cxBEon = STATE.settings.cxBEon;
  // الحد الأقصى العام لحجم الصفقة ووقف الخسارة: يُقصّ عليه أي قيمة (خاصة بالعملة أو عامة) تتجاوزه
  merged.cxAmt = capNumeric(merged.cxAmt, STATE.settings.cxAmtMax);
  merged.cxSL = capNumeric(merged.cxSL, STATE.settings.cxSLMax);
  return applyLockFields(merged);
}

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

function calcRespect(cls, mode, ma) {
  if (!cls || cls.length < 30) return null;
  const series = computeIndSeries(cls, mode, ma);
  if (!series || series.length < 20) return null;
  const offset = cls.length - series.length;
  const lookAhead = 5;
  let total = 0, respected = 0;
  for (let i = 1; i < series.length - lookAhead; i++) {
    const pv = series[i - 1], cu = series[i];
    let side = null;
    if (pv <= 70 && cu > 70) side = 'SHORT';
    else if (pv >= 70 && cu < 70) side = 'SHORT';
    else if (pv >= 30 && cu < 30) side = 'LONG';
    else if (pv <= 30 && cu > 30) side = 'LONG';
    else if (pv <= 30 && cu > pv && cu <= 30) side = 'LONG';
    else if (pv >= 70 && cu < pv && cu >= 70) side = 'SHORT';
    if (!side) continue;
    total++;
    const entry = cls[offset + i];
    const future = cls[offset + i + lookAhead];
    if (side === 'LONG' && future > entry) respected++;
    else if (side === 'SHORT' && future < entry) respected++;
  }
  if (total < 3) return null;
  return { rate: Math.round((respected / total) * 100), total, respected };
}

async function updateRespect() {
  const total = STATE.symbols.length;
  if (!total) return;
  broadcast({ type: 'respectProgress', data: { current: 0, total, done: false } });
  for (let i = 0; i < total; i += 3) {
    const batch = STATE.symbols.slice(i, i + 3);
    await Promise.all(batch.map(async sym => {
      try {
        const st = settingsFor(sym);
        const d = await fetchBinance(`/fapi/v1/klines?symbol=${sym}&interval=${st.interval}&limit=1500`);
        if (!Array.isArray(d) || d.length < 30) return;
        const cls = d.map(k => parseFloat(k[4]));
        const r = calcRespect(cls, st.mode, st.maPeriod);
        if (r) STATE.respectData[sym] = r;
      } catch (e) {}
    }));
    broadcast({ type: 'respectProgress', data: { current: Math.min(i + 3, total), total, done: false } });
    if (i + 3 < total) await new Promise(r => setTimeout(r, 1500));
  }
  STATE.respectAt = Date.now();
  db.saveRespectData({ _at: STATE.respectAt, ...STATE.respectData });
  broadcast({ type: 'respectData', data: STATE.respectData });
  broadcast({ type: 'respectProgress', data: { current: total, total, done: true } });
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

function detectSignal(pv, cu, cls, id, ed, st = STATE.settings) {
  if (pv === null || cu === null) return null;
  const rm = st.revMode, rv = parseInt(st.revCount) || 1;
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

function detectTrail(sym, cu, cls, id, ed, st = STATE.settings) {
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
let levBracketsLoaded = false;

// يجلب جميع حدود الرافعة دفعة واحدة من واجهة Binance العامة (لا تتطلب مفاتيح API) — احتياطي
async function fetchPublicLevBrackets() {
  const res = await fetch('https://www.binance.com/bapi/futures/v1/public/future/leverage-bracket');
  const d = await res.json();
  if (!Array.isArray(d?.data)) throw new Error('bad response');
  for (const item of d.data) {
    const l = item.brackets?.[0]?.initialLeverage;
    if (l) maxLevCache[item.symbol] = l;
  }
  levBracketsLoaded = true;
}

async function getMaxLev(sym) {
  if (maxLevCache[sym]) return maxLevCache[sym];
  // أي حساب لديه مفاتيح API صالحة يكفي لجلب حدود الرافعة (/fapi/v1/leverageBracket مُوثَّق ولا يتطلب أن يكون الحساب "ماستر")
  const withKeys = STATE.copyAccounts?.find(a => a.apiKey && a.apiSecret);
  if (withKeys) {
    try {
      const d = await bFetch(withKeys.apiKey, withKeys.apiSecret, 'GET', '/fapi/v1/leverageBracket', { symbol: sym });
      const l = (Array.isArray(d) ? d[0] : d)?.brackets?.[0]?.initialLeverage;
      if (l) { maxLevCache[sym] = l; return l; }
    } catch (e) {}
  }
  if (!levBracketsLoaded) {
    try { await fetchPublicLevBrackets(); } catch (e) {}
  }
  return maxLevCache[sym] || 20;
}

// يضبط الرافعة المطلوبة بحيث لا تتجاوز الحد الأقصى المسموح للعملة — بالتدرّج (-10) في كل مرة
// (مثلاً: رافعة مطلوبة ٥٠ وحد العملة ٢٠ → ينزل ٤٠ ثم ٣٠ ثم ٢٠)
async function resolveLeverage(sym, requestedLev) {
  const orig = parseInt(requestedLev) || 20;
  const mx = await getMaxLev(sym);
  const lv = Math.min(orig, mx);
  const note = lv !== orig ? `\n⚠️ رافعة عُدّلت إلى ${lv}X (الحد ${mx}X)` : '';
  return { lv, note };
}

const lotSizeCache = {}; // sym -> stepSize (from exchangeInfo)
const tickSizeCache = {}; // sym -> tickSize (PRICE_FILTER) — لازم لأوامر الوقف

// جلب stepSize ديناميكياً إذا ما كان في الكاش (للعملات التي لم تُحمَّل عند البدء)
async function ensureLotSize(sym) {
  if (lotSizeCache[sym] && tickSizeCache[sym]) return;
  try {
    const info = await fetchBinance(`/fapi/v1/exchangeInfo?symbol=${sym}`);
    const filters = info.symbols?.[0]?.filters || [];
    const lot = filters.find(f => f.filterType === 'LOT_SIZE');
    if (lot) lotSizeCache[sym] = parseFloat(lot.stepSize);
    const pf = filters.find(f => f.filterType === 'PRICE_FILTER');
    if (pf) tickSizeCache[sym] = parseFloat(pf.tickSize);
  } catch (e) {}
}

function roundQty(qty, sym) {
  const step = lotSizeCache[sym] || 0.001;
  const precision = step >= 1 ? 0 : Math.max(0, -Math.floor(Math.log10(step)));
  return parseFloat((Math.floor(qty / step) * step).toFixed(precision));
}

// تقريب السعر إلى أقرب مضاعف لـ tickSize — بايننس يرفض أوامر الوقف بأسعار غير مطابقة
function roundPrice(price, sym) {
  const tick = tickSizeCache[sym];
  if (!tick || !isFinite(price)) return parseFloat(Number(price).toFixed(8));
  const precision = tick >= 1 ? 0 : Math.max(0, -Math.floor(Math.log10(tick)));
  return parseFloat((Math.round(price / tick) * tick).toFixed(precision));
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
const tgQueue = [];
let tgSending = false;

// opts.trackSym  → يحفظ message_id للرسالة تحت هذا الرمز (لمزامنة كورنكس لاحقاً)
// opts.replyTo   → يرسلها كرد على رسالة موجودة (أوامر تحديث كورنكس)
async function tgSend(text, chat, opts = {}) {
  const st = STATE.settings;
  if (!st.cxToken || !chat) return;
  tgQueue.push({ text, chat, token: st.cxToken, ...opts });
  if (!tgSending) drainTgQueue();
}

// أي خانة إعدادات يخصّها هذا الـ chat id — ليظهر اسمها في رسالة الخطأ
function chatLabel(chat) {
  const s = STATE.settings, c = String(chat);
  if (c === String(s.cxChat)) return 'Chat ID الأساسي (الإشارات)';
  if (c === String(s.cxChatClose)) return 'Chat ID — إغلاق الصفقات';
  if (c === String(s.cxChatSettings)) return 'Chat ID — إعدادات الصفقات';
  if (c === String(s.cxChatBT)) return 'Chat ID — الباك تيست';
  if (c === String(s.cxChatSTSim)) return 'Chat ID — محاكاة سوبر تريند';
  if (c === String(s.lockTgChat)) return 'Chat ID — نظام القفل';
  return 'قناة غير معروفة';
}

// قنوات فشلت بـ "chat not found" — نتوقف عن مراسلتها بدل تكرار الخطأ مع كل إشارة.
// لكن نعيد المحاولة بعد فترة: قد يُضاف البوت إلى القناة بعد أول فشل،
// وبدون هذه المهلة تبقى القناة معطّلة إلى أن يُعاد تشغيل السيرفر.
const deadChats = new Map();   // chat -> { at, label }
const DEAD_CHAT_RETRY_MS = 10 * 60000;
function isDeadChat(chat) {
  const rec = deadChats.get(String(chat));
  if (!rec) return false;
  if (Date.now() - rec.at > DEAD_CHAT_RETRY_MS) { deadChats.delete(String(chat)); return false; }
  return true;
}

async function drainTgQueue() {
  tgSending = true;
  while (tgQueue.length) {
    const item = tgQueue.shift();
    const { text, chat, token, trackSym, replyTo, editId } = item;
    if (isDeadChat(chat)) continue;   // قناة معطوبة — تخطَّ (تُعاد المحاولة بعد ١٠ دقائق)
    try {
      // editId → تعديل رسالة قائمة بدل إرسال جديدة
      const method = editId ? 'editMessageText' : 'sendMessage';
      const payload = editId ? { chat_id: chat, message_id: editId, text } : { chat_id: chat, text };
      if (replyTo) payload.reply_to_message_id = replyTo;
      const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.ok) {
        if (trackSym) {
          try {
            const d = await res.json();
            const mid = d?.result?.message_id;
            if (mid) {
              // نحفظ النص أيضاً — لازم لتعديل الرسالة لاحقاً بنفس صيغتها
              STATE.sentMsgIds[trackSym] = { id: mid, chat, ts: Date.now(), text };
              saveSentMsgIdsDebounced();
              // لا نعدّل رسالة الإشارة تلقائياً بعد إرسالها:
              // editMessageText يمسح الأزرار المضافة (أزرار كورنكس) ما لم تُعَد معه،
              // فكان الفحص الدوري يجرّد الإشارات من أزرار المتابعة.
            }
          } catch (e) {}
        }
      } else {
        const body = await res.text().catch(() => '');
        if (res.status === 429) {
          const wait = parseInt(body.match(/"retry_after":(\d+)/)?.[1] || '5') * 1000;
          tgQueue.unshift(item);
          await new Promise(r => setTimeout(r, wait));
        } else {
          const label = chatLabel(chat);
          const notFound = /chat not found|chat_id is empty|bot was kicked|not a member/i.test(body);
          if (notFound) {
            // لن تنجح أبداً حتى يُصلَّح الإعداد — أوقف الإرسال لها وبلّغ مرة واحدة
            deadChats.set(String(chat), { at: Date.now(), label });
            reportError('تلغرام', `القناة «${label}» غير موجودة (${chat}) — صحّح الرقم في صفحة الإعدادات أو امسحه. أُوقف الإرسال لها.`);
            addCopyLog('fail', `❌ تلغرام: «${label}» غير موجودة (${chat}) — أُوقف الإرسال لها`);
          } else {
            reportError('تلغرام', `فشل الإرسال إلى «${label}» (${res.status}): ${body.slice(0, 160)}`);
          }
        }
      }
    } catch (e) {
      reportError('تلغرام', `فشل الإرسال: ${e.message}`);
    }
    if (tgQueue.length) await new Promise(r => setTimeout(r, 1000));
  }
  tgSending = false;
}

let msgIdsTimer = null;
function saveSentMsgIdsDebounced() {
  if (msgIdsTimer) clearTimeout(msgIdsTimer);
  msgIdsTimer = setTimeout(() => {
    // تنظيف: احتفظ فقط بمعرّفات آخر ٧ أيام
    const cutoff = Date.now() - 7 * 86400000;
    for (const k of Object.keys(STATE.sentMsgIds)) {
      if ((STATE.sentMsgIds[k]?.ts || 0) < cutoff) delete STATE.sentMsgIds[k];
    }
    db.saveSentMsgIds(STATE.sentMsgIds);
  }, 3000);
}

// إرسال ملف (نتائج فحص/باك تيست بصيغة JSON) كمستند تلغرام
async function tgSendDocument(buf, filename, caption, chat) {
  const st = STATE.settings;
  if (!st.cxToken || !chat) return;
  try {
    const form = new FormData();
    form.append('chat_id', chat);
    if (caption) form.append('caption', caption.slice(0, 1024));
    form.append('document', new Blob([buf], { type: 'application/json' }), filename);
    // ملاحظة: استخدام fetch الأصلي (Node) لأن node-fetch v2 لا يدعم FormData/Blob الأصليين بشكل صحيح
    await globalThis.fetch(`https://api.telegram.org/bot${st.cxToken}/sendDocument`, { method: 'POST', body: form });
  } catch (e) {}
}

// فجوة الدخول "القريب من السعر" — يعتمده كورنكس كدخول أول فعلي (راجع buildMsg)
const NEAR_ENTRY_GAP = 0.0001; // 0.01%

// تنسيق أسعار رسالة كورنكس — مصدر واحد كي تبقى الرسالة المعدَّلة مطابقة للأصلية
function fmtSignalPrice(n) {
  if (!n && n !== 0) return 'N/A';
  if (n >= 100) return n.toFixed(2);
  if (n >= 1) return n.toFixed(3);
  if (n >= 0.1) return n.toFixed(4);
  return n.toFixed(6);
}

// يحوّل قيمة "تريلنج الدخول" (مهما كانت صيغتها: مع % أو بدونها، بمسافات، أو بأرقام عربية)
// إلى نسبة نظيفة بصيغة "N%" — لضمان عدم خروج رسالة كورنكس بصيغة فاسدة أو فارغة لهذا البند
function entryTrailPct(v) {
  const ascii = String(v ?? '').replace(/[٠-٩]/g, d => '٠١٢٣٤٥٦٧٨٩'.indexOf(d));
  const n = parseFloat(ascii);
  return (isNaN(n) ? 0 : n) + '%';
}

function buildMsg(sym, side, st = STATE.settings) {
  const p = livePrices[sym], pair = sym.replace('USDT', '/USDT');
  const star = hasSymOverride(sym) ? ' ⭐' : '';
  const fp = fmtSignalPrice;
  let tp1 = null, tp2 = null, sll = null, eNear = null, e2 = null;
  if (p) {
    const t1 = parseFloat(st.cxTP1) / 100;
    tp1 = side === 'LONG' ? p * (1 + t1) : p * (1 - t1);
    if (st.cxTP2on) { const t2 = parseFloat(st.cxTP2) / 100; tp2 = side === 'LONG' ? p * (1 + t2) : p * (1 - t2); }
    // وقف الخسارة: حسب إعدادات العملة، وإن كان معطّلاً نضع ٥٠٪ (قيمة ثابتة لإرضاء شرط كورنكس)
    const s = st.cxSLon ? parseFloat(st.cxSL) / 100 : 0.5;
    sll = side === 'LONG' ? Math.max(p * (1 - s), p * 0.0001) : p * (1 + s);
    // الدخول "القريب من السعر" — كورنكس يعتمد الدخول الثاني كدخول أول فعلي، فهذا يمثّل نية الدخول بسعر السوق
    eNear = side === 'LONG' ? p * (1 - NEAR_ENTRY_GAP) : p * (1 + NEAR_ENTRY_GAP);
    if (st.cxEntry2on) { const d2 = parseFloat(st.cxEntry2Dist || '2') / 100; e2 = side === 'LONG' ? p * (1 - d2) : p * (1 + d2); }
  }
  const L = [`#${pair}${star}`, 'Exchanges: Binance Futures', `Signal Type: Regular (${side === 'LONG' ? 'Long' : 'Short'})`,
    `Leverage: ${st.cxMargin} (${st.cxLev}X)`, `Amount: ${st.cxAmt}`, '', 'Entry Targets:', '1) Market'];
  // كورنكس يتجاهل "1) Market" ويعتمد البند ٢ كدخول أول فعلي، والبند ٣ كدخول ثاني فعلي
  const nearAmt = st.cxEntry2on ? (100 - (parseFloat(st.cxEntry2Amt) || 0)) : 100;
  L.push(`2) ${fp(eNear)} (${nearAmt}%)`);
  if (st.cxEntry2on) L.push(`3) ${fp(e2)}${st.cxEntry2Amt ? ` (${st.cxEntry2Amt}%)` : ''}`);
  L.push('', 'Take-Profit Targets:');
  if (st.cxTP2on) { L.push(`1) ${fp(tp1)} (${st.cxTP1Amt}%)`); L.push(`2) ${fp(tp2)} (${st.cxTP2Amt}%)`); }
  else L.push(`1) ${fp(tp1)}`);
  // بند وقف الخسارة لازم يكون موجود دائماً (كورنكس يرفض الرسائل بدونه)
  L.push('', 'Stop Targets:', `1) ${fp(sll)}`, '');
  L.push('Trailing Configuration:', `Entry: Percentage (${entryTrailPct(st.cxEntryTrail)})`);
  if (st.cxTrailTp === 'on') L.push(`Take-Profit: Percentage (${st.cxTrailPct}%)`);
  if (st.cxBEon) L.push('Stop: Breakeven - Trigger: Target (1)');
  return L.join('\n');
}

// رسالة مبسّطة بأهم إعدادات الصفقة — تُرسل لقناة "إعدادات الصفقات" مع كل إشارة
function buildSettingsMsg(sym, side, st, lv) {
  const pair = sym.replace('USDT', '/USDT');
  const entryDists = [`السوق ${(NEAR_ENTRY_GAP * 100).toFixed(2)}%`];
  if (st.cxEntry2on) entryDists.push(`${st.cxEntry2Dist}%`);
  const tpDists = [`${st.cxTP1}%`];
  if (st.cxTP2on) tpDists.push(`${st.cxTP2}%`);
  return [
    `⚙️ إعدادات الصفقة — #${pair}${hasSymOverride(sym) ? ' ⭐' : ''}`,
    '',
    `الفريم الزمني: ${st.interval}`,
    `الاتجاه: ${side === 'LONG' ? 'لونج 🟢' : 'شورت 🔴'}`,
    `الدخولات (المسافة): ${entryDists.join(' / ')}`,
    `الأهداف (المسافة): ${tpDists.join(' / ')}`,
    `وقف الخسارة: ${st.cxSLon ? `${st.cxSL}%` : '50% (افتراضي - الإعداد معطّل)'}`,
    `Entry Trailing: ${entryTrailPct(st.cxEntryTrail)}`,
    `Take-Profit Trailing: ${st.cxTrailTp === 'on' ? `${st.cxTrailPct}%` : 'معطّل'}`,
    `Break Even: ${st.cxBEon ? 'مفعّل' : 'معطّل'}`,
    `الرافعة: ${lv}X`,
    `حجم الصفقة: ${st.cxAmt}`,
  ].join('\n');
}

async function sendSignal(sym, side, overridePrice, fromQueue = false, queueLabel = '', st = STATE.settings, tf = null) {
  if (fromQueue) {
    // القائمة الذكية: تعتمد على sigFilters.queue فقط، مش autoSend
    if (st.sigFilters?.queue === false) return;
    if (!st.cxToken || !st.cxChat) return;
  } else {
    if (!st.autoSend || !st.cxToken || !st.cxChat) return;
  }
  // فلتر إرسال التلغرام حسب وجود إعدادات خاصة للعملة (⭐) — لا يؤثر على السجل أو القائمة أو الصفقة، فقط على إرسال التلغرام
  const tgFilter = STATE.settings.tgStarFilter;
  if (tgFilter === 'star' && !hasSymOverride(sym)) return;
  if (tgFilter === 'other' && hasSymOverride(sym)) return;
  if (!overridePrice && STATE.sentSigs[sym]) return;
  STATE.sentSigs[sym] = Date.now();
  saveSentSigsDebounced();
  if (STATE.settings.stSLon && STATE._stslTracked) STATE._stslTracked[sym] = true;

  const { lv, note } = await resolveLeverage(sym, st.cxLev);
  const origLev = st.cxLev; st.cxLev = String(lv);
  const origPrice = overridePrice ? livePrices[sym] : null;
  if (overridePrice) livePrices[sym] = overridePrice;
  const prefix = fromQueue && queueLabel ? `⏳ قائمة الانتظار | ${queueLabel}\n` : '';
  // الفريم يوضع في ذيل الرسالة بعد بنود كورنكس، حيث لا يدخل في التحليل
  const tfTag = tf ? `\n(${tf})` : '';
  const text = prefix + buildMsg(sym, side, st) + note + tfTag;
  if (origPrice !== null) livePrices[sym] = origPrice;
  st.cxLev = origLev;
  // trackSym: نحفظ message_id لنستطيع الرد عليها لاحقاً بأوامر تحديث كورنكس (بريك إيفن/وقف/تريلنج)
  await tgSend(text, st.cxChat, { trackSym: sym });

  // إرسال ملخص إعدادات الصفقة لقناة "إعدادات الصفقات" — لكل الصفقات
  if (STATE.settings.cxChatSettings) {
    await tgSend(buildSettingsMsg(sym, side, st, lv), STATE.settings.cxChatSettings);
  }
}

// ══════════════════════════════════════════════
//  SCANNER
// ══════════════════════════════════════════════
const BATCH = 5, BDEL = 300;
let scanRunning = false;

const TZ = 'Asia/Aden';   // توقيت صنعاء

function nowStr() {
  return new Date().toLocaleTimeString('ar-EG', { hour12: false, timeZone: TZ });
}

// تاريخ ووقت بالميلادي وبتوقيت صنعاء.
// ملاحظة: ar-SA يستخدم التقويم الهجري ويتجاهل المنطقة الزمنية إن لم تُحدَّد،
// فكان الوقت يظهر بتوقيت الخادم وبتاريخ هجري.
function fmtDateTime(ts) {
  return new Date(ts).toLocaleString('ar-EG', {
    timeZone: TZ, calendar: 'gregory', hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
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

async function triggerAlert(sym, sig, val, st = STATE.settings, tfOverride) {
  const tf = tfOverride || st.interval;
  const coolKey = tfOverride ? `${sym}_${sig.type}_${tf}` : `${sym}_${sig.type}`;
  const sentKey = tfOverride ? `${sym}_${tf}` : sym;
  const now = Date.now();
  if (STATE.cooldowns[coolKey]) return;
  const master = STATE.copyAccounts.find(a => a.isMaster);
  const hasLivePos = master?.livePositions?.some(p => p.symbol === sym && Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  if (st.blockOpen && (STATE.openTrades.some(t => t.symbol === sym) || hasLivePos)) return;
  if (STATE.sentSigs[sentKey]) return;
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

  // فحص سوبر تريند العملة مبكراً — لإضافة العلامة على الإشارة
  let stCheckResult = null;
  if (st.perSymSTon) {
    stCheckResult = await checkSymST(sym);
  }

  STATE.cooldowns[coolKey] = now;
  alertId++;
  const item = {
    id: alertId, symbol: sym, type: sig.type, label: sig.label,
    color: sig.color, emoji: sig.emoji, rsi: val.toFixed(2),
    time: nowStr(), mode: `${st.mode}(${st.mode === 'RSI' ? RSI_P : st.maPeriod})`, tf, side: sig.side,
    stDir: stCheckResult?.direction || null
  };
  STATE.alerts = [item, ...STATE.alerts].slice(0, 200);
  db.saveAlert(item);
  broadcast({ type: 'alert', data: item });

  // فلتر EMA 200 و SuperTrend — يؤثر على القائمة والإرسال لكن ليس السجل (AND logic عند تفعيل الاثنين)
  {
    // فلتر مفعّل بلا بيانات = حجب، لا تخطٍّ. تخطّيه كان يمرّر إشارات معاكسة
    // في الفترة التي تسبق تحميل البيانات.
    if (st.ema200FilterOn && !STATE.ema200?.direction) return;
    if (st.stFilterOn && !STATE.superTrend?.direction) return;
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

  // فلتر سوبر تريند العملة — فحص فوري للعملة عند وصول الإشارة
  if (st.perSymSTon) {
    if (!stCheckResult?.direction) return;
    const pst = stCheckResult;
    const price = livePrices[sym] || 0;
    const dirTxt = pst.direction === 'up' ? '🟢 صاعد' : '🔴 نازل';
    // منطقة ميتة — السعر قريب من خط السوبر تريند
    if (st.deadZoneOn) {
      const dist = price && pst.value ? Math.abs((price - pst.value) / price) * 100 : 999;
      if (dist < parseFloat(st.deadZonePct || 1)) {
        if (STATE.settings.cxChatSTSim) {
          tgSend(`⚠️ منطقة ميتة\n#${sym.replace('USDT', '/USDT')} ${tf}\n${sig.side === 'LONG' ? '🟢 LONG' : '🔴 SHORT'}\nسوبر: ${dirTxt}\nمسافة: ${dist.toFixed(2)}% < ${st.deadZonePct}%`, STATE.settings.cxChatSTSim);
        }
        if (st.deadZoneMode === 'wait') return;
        if (st.deadZoneMode === 'reverse') {
          if (sig.side === 'LONG' && pst.direction === 'up') return;
          if (sig.side === 'SHORT' && pst.direction === 'down') return;
        } else return;
      }
    }
    const blocked = (sig.side === 'LONG' && pst.direction !== 'up') || (sig.side === 'SHORT' && pst.direction !== 'down');
    if (blocked) {
      if (STATE.settings.cxChatSTSim) {
        tgSend(`❌ حُجبت\n#${sym.replace('USDT', '/USDT')} ${tf}\n${sig.side === 'LONG' ? '🟢 LONG' : '🔴 SHORT'}\nسوبر: ${dirTxt} ⛔ عكس الإشارة\n💰 سعر: ${price} | ST: ${pst.value}`, STATE.settings.cxChatSTSim);
      }
      return;
    }
    if (STATE.settings.cxChatSTSim) {
      tgSend(`✅ مرّت\n#${sym.replace('USDT', '/USDT')} ${tf}\n${sig.side === 'LONG' ? '🟢 LONG' : '🔴 SHORT'}\nسوبر: ${dirTxt}\n💰 سعر: ${price} | ST: ${pst.value}`, STATE.settings.cxChatSTSim);
    }
  }

  // فلتر احترام المؤشر — يؤثر على القائمة والإرسال لكن ليس السجل
  if (st.respectFilterOn) {
    const rd = STATE.respectData[sym];
    if (rd && rd.rate < (parseInt(st.respectMin) || 50)) return;
  }

  // فحص حد الصفقات — إضافة للقائمة إذا وصل الحد
  const maxOT = parseInt(st.maxOpenTrades) || 0;
  if (maxOT > 0 && countOpenPositions() >= maxOT) {
    if (typeKey && !STATE.waitQueue.some(q => q.symbol === sym)) {
      STATE.waitQueue.push({
        id: Date.now() + Math.random(), symbol: sym, side: sig.side,
        signalType: typeKey, signalPrice: livePrices[sym] || 0,
        addedTs: Date.now(), addedTime: nowStr(), tf,
        label: sig.label, emoji: sig.emoji, color: sig.color
      });
      db.saveWaitQueue(STATE.waitQueue);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
    }
    return;
  }

  // sigFilters يتحكم فقط بالإرسال المباشر للتلغرام — المحجوبة تروح القائمة
  const sigFilters = { ob: true, os: true, conf: true, trail: true, ...st.sigFilters };
  const blocked = (isOB && !sigFilters.ob) || (isOS && !sigFilters.os) || (isConf && !sigFilters.conf) || (isTrail && !sigFilters.trail);
  if (blocked) {
    if (typeKey && !STATE.waitQueue.some(q => q.symbol === sym)) {
      STATE.waitQueue.push({
        id: Date.now() + Math.random(), symbol: sym, side: sig.side,
        signalType: typeKey, signalPrice: livePrices[sym] || 0,
        addedTs: Date.now(), addedTime: nowStr(), tf,
        label: sig.label, emoji: sig.emoji, color: sig.color
      });
      db.saveWaitQueue(STATE.waitQueue);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      setTimeout(() => autoSendFromQueue(), 1000);
    }
    return;
  }

  // محاكاة سوبر تريند — صفقات وهمية كاملة مع أهداف وستوب
  if (!st.perSymSTon && STATE.settings.cxChatSTSim) {
    try {
      const pst = await checkSymST(sym);
      if (pst?.direction) {
        const wouldPass = (sig.side === 'LONG' && pst.direction === 'up') || (sig.side === 'SHORT' && pst.direction === 'down');
        const price = livePrices[sym] || 0;
        const dirTxt = pst.direction === 'up' ? '🟢 صاعد' : '🔴 نازل';
        if (wouldPass && price > 0) {
          if (STATE.simTrades.some(t => t.symbol === sym && !t.closed)) {
            // عملة مفتوحة بالفعل
          } else {
            const lev = parseInt(st.cxLev) || 20;
            const isLong = sig.side === 'LONG';
            const sl = pst.value;
            const slPct = Math.abs((price - sl) / price) * 100;
            const tp1Pct = parseFloat(st.cxTP1) || 3;
            const tp1 = isLong ? price * (1 + tp1Pct / 100) : price * (1 - tp1Pct / 100);
            const tp2on = st.cxTP2on;
            const tp2Pct = parseFloat(st.cxTP2) || 6;
            const tp2 = tp2on ? (isLong ? price * (1 + tp2Pct / 100) : price * (1 - tp2Pct / 100)) : null;
            const simT = {
              id: Date.now() + Math.random(),
              symbol: sym, side: sig.side, tf,
              entry: price, sl, tp1, tp2,
              tp1Hit: false, closed: false,
              beSl: st.cxBEon ? price : null,
              lev, stValue: pst.value,
              openTime: nowStr(), openTs: Date.now(),
            };
            STATE.simTrades.push(simT);
            db.saveSimTrades(STATE.simTrades);
            const dec = countDecimals(price);
            const amt = st.cxAmt || '2%';
            const entry2 = st.cxEntry2on ? price * (isLong ? (1 - parseFloat(st.cxEntry2Dist || 0.2) / 100) : (1 + parseFloat(st.cxEntry2Dist || 0.2) / 100)) : null;
            const trailEntry = st.cxEntryTrail || '0.5%';
            const trailTp = st.cxTrailTp === 'on' ? st.cxTrailPct || '0.5' : null;
            const tp1Amt = st.cxTP1Amt || '50';
            const tp2Amt = st.cxTP2Amt || '50';
            tgSend(
              `#${sym.replace('USDT', '/USDT')}\n` +
              `Exchanges: Binance Futures\n` +
              `Signal Type: Regular (${isLong ? 'Long' : 'Short'})\n` +
              `Leverage: ${st.cxMargin || 'Cross'} (${lev}X)\n` +
              `Amount: ${amt}\n\n` +
              `Entry Targets:\n1) Market` +
              (entry2 ? `\n2) ${entry2.toFixed(dec)} (100%)` : '') +
              `\n\nTake-Profit Targets:\n1) ${tp1.toFixed(dec)} (${tp1Amt}%)` +
              (tp2 ? `\n2) ${tp2.toFixed(dec)} (${tp2Amt}%)` : '') +
              `\n\nStop Targets:\n1) ${sl.toFixed(dec)}\n\n` +
              `Trailing Configuration:\n` +
              `Entry: Percentage (${trailEntry})\n` +
              `Take-Profit: ${trailTp ? 'Percentage (' + trailTp + '%)' : 'Off'}\n` +
              `Stop: ${st.cxBEon ? 'Breakeven - Trigger: Target (1)' : 'Off'}\n\n` +
              `📊 محاكاة ST | سوبر: ${dirTxt}`,
              STATE.settings.cxChatSTSim
            );
          }
        } else if (!wouldPass) {
          tgSend(
            `❌ محجوبة\n#${sym.replace('USDT', '/USDT')} ${tf}\n${sig.side === 'LONG' ? '🟢 LONG' : '🔴 SHORT'}\nسوبر: ${dirTxt} ⛔ عكس الإشارة\n💰 سعر: ${price} | ST: ${pst.value}`,
            STATE.settings.cxChatSTSim
          );
        }
      }
    } catch (e) {}
  }

  sendSignal(sym, sig.side, null, false, '', st, tf);
}

function queueWithReversals() {
  return STATE.waitQueue.map(q => {
    const cur = livePrices[q.symbol] || q.signalPrice;
    const diff = q.signalPrice > 0 ? ((cur - q.signalPrice) / q.signalPrice) * 100 : 0;
    const rev = q.side === 'LONG' ? -diff : diff;
    const reversalLabel = rev > 0 ? `انعكاس ${rev.toFixed(2)}%` : `مع الإشارة ${Math.abs(rev).toFixed(2)}%`;
    return { ...q, reversalPct: parseFloat(rev.toFixed(3)), reversalLabel };
  }).sort((a, b) => b.reversalPct - a.reversalPct);
}

// يرسل كل عنصر بلغ انعكاسه النسبة المحدّدة.
// لا يفحص حدّ الصفقات المفتوحة: القائمة نشأت أصلاً بسبب الحد، والغرض هنا
// اقتناص الدخول الأفضل بعد الارتداد بغضّ النظر عن امتلاء الحد.
let revAutoBusy = false;
async function autoSendOnReversal() {
  const st = STATE.settings;
  if (!st.queueRevAutoOn || revAutoBusy) return;
  const need = parseFloat(st.queueRevAutoPct);
  if (!isFinite(need) || need <= 0) return;
  const ready = queueWithReversals().filter(q => q.reversalPct >= need);
  if (!ready.length) return;
  revAutoBusy = true;
  try {
    for (const q of ready) {
      // قد يكون أُرسل في دورة سابقة
      if (!STATE.waitQueue.some(x => x.id === q.id)) continue;
      await sendQueueItemNow(q, livePrices[q.symbol]);
      addCopyLog('success', `📤 انعكاس ${q.reversalPct}% ≥ ${need}% — أُرسلت ${q.symbol}`);
      await new Promise(r => setTimeout(r, 1200));   // مباعدة تحترم حدود تلغرام
    }
  } catch (e) {
    addCopyLog('fail', `❌ إرسال الانعكاس: ${e.message}`);
  } finally {
    revAutoBusy = false;
  }
}

async function sendQueueItemNow(qItem, currentPrice) {
  STATE.waitQueue = STATE.waitQueue.filter(q => q.id !== qItem.id);
  db.saveWaitQueue(STATE.waitQueue);
  broadcast({ type: 'waitQueue', data: queueWithReversals() });
  const label = qItem.emoji ? `${qItem.emoji} ${qItem.label}` : qItem.label || qItem.signalType || '';
  await sendSignal(qItem.symbol, qItem.side, currentPrice || livePrices[qItem.symbol], true, label, settingsFor(qItem.symbol), qItem.tf || null);
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

async function updatePerSymST() {
  const total = STATE.symbols.length;
  if (!total) return;
  const tf = STATE.settings.perSymSTtf || '4h';
  const period = parseInt(STATE.settings.stPeriod) || 10;
  const mult = parseFloat(STATE.settings.stMult) || 3;
  const limit = Math.max(100, period * 4);
  broadcast({ type: 'perSymSTProgress', data: { current: 0, total, done: false } });
  for (let i = 0; i < total; i += 3) {
    const batch = STATE.symbols.slice(i, i + 3);
    await Promise.all(batch.map(async sym => {
      try {
        const klines = await fetchBinance(`/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=${limit}`);
        if (!Array.isArray(klines) || klines.length < period + 2) return;
        const result = calcSuperTrend(klines, period, mult);
        if (result) STATE.perSymST[sym] = result;
      } catch (e) {}
    }));
    broadcast({ type: 'perSymSTProgress', data: { current: Math.min(i + 3, total), total, done: false } });
    if (i + 3 < total) await new Promise(r => setTimeout(r, 1500));
  }
  broadcast({ type: 'perSymST', data: STATE.perSymST });
  broadcast({ type: 'perSymSTProgress', data: { current: total, total, done: true } });
}

async function checkSymST(sym) {
  try {
    const tf = STATE.settings.perSymSTtf || '4h';
    const period = parseInt(STATE.settings.stPeriod) || 10;
    const mult = parseFloat(STATE.settings.stMult) || 3;
    const limit = Math.max(100, period * 4);
    const klines = await fetchBinance(`/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=${limit}`);
    if (!Array.isArray(klines) || klines.length < period + 2) return null;
    const result = calcSuperTrend(klines, period, mult);
    if (result) STATE.perSymST[sym] = result;
    return result;
  } catch (e) { return null; }
}

async function monitorSTSL() {
  if (!STATE.settings.stSLon) return;
  if (!STATE._stslEnabledAt) STATE._stslEnabledAt = Date.now();
  const master = STATE.copyAccounts.find(a => a.isMaster);
  if (!master?.apiKey) return;
  const positions = (master.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  if (!positions.length) return;

  for (const pos of positions) {
    const sym = pos.symbol;
    const isLong = parseFloat(pos.positionAmt) > 0;
    const entryPrice = parseFloat(pos.entryPrice) || 0;

    // حماية: لا تقفل صفقات كانت مفتوحة قبل تفعيل الخاصية
    const trade = STATE.openTrades.find(t => t.symbol === sym);
    const tradeOpenTs = trade?.openTs || 0;
    if (tradeOpenTs > 0 && tradeOpenTs < STATE._stslEnabledAt) continue;
    // لو ما فيه سجل بالصفقة، تحقق إنها فُتحت بعد التفعيل
    if (!trade && !STATE._stslTracked?.[sym]) continue;

    try {
      const pst = await checkSymST(sym);
      if (!pst?.direction || !pst.value) continue;

      const mode = STATE.settings.stSLmode || 'flip';
      const pctBuf = parseFloat(STATE.settings.stSLpct || 0.5);

      if (mode === 'flip') {
        if ((isLong && pst.direction === 'down') || (!isLong && pst.direction === 'up')) {
          addCopyLog('info', `🛑 ST SL: ${sym} سوبر تريند انقلب — إغلاق`);
          const amt = parseFloat(pos.positionAmt);
          await closeFollower(master, sym, amt);
        }
      } else if (mode === 'moving') {
        const slPrice = isLong
          ? parseFloat((pst.value * (1 + pctBuf / 100)).toFixed(countDecimals(entryPrice)))
          : parseFloat((pst.value * (1 - pctBuf / 100)).toFixed(countDecimals(entryPrice)));
        await placeOrUpdateSTSL(master, sym, pos, slPrice);
      } else if (mode === 'fixed') {
        const tradeKey = `stsl_${sym}`;
        if (!STATE._stslPlaced) STATE._stslPlaced = {};
        if (STATE._stslPlaced[tradeKey]) continue;
        const slPrice = isLong
          ? parseFloat((pst.value * (1 + pctBuf / 100)).toFixed(countDecimals(entryPrice)))
          : parseFloat((pst.value * (1 - pctBuf / 100)).toFixed(countDecimals(entryPrice)));
        await placeOrUpdateSTSL(master, sym, pos, slPrice);
        STATE._stslPlaced[tradeKey] = true;
      }
    } catch (e) {
      addCopyLog('fail', `❌ ST SL ${sym}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 300));
  }
}

function countDecimals(num) {
  const s = String(num);
  const dot = s.indexOf('.');
  return dot === -1 ? 2 : s.length - dot - 1;
}

function checkSimTrades() {
  if (!STATE.settings.cxChatSTSim || !STATE.simTrades.length) return;
  const chatId = STATE.settings.cxChatSTSim;
  for (const t of STATE.simTrades) {
    if (t.closed) continue;
    const price = livePrices[t.symbol];
    if (!price) continue;
    const isLong = t.side === 'LONG';
    const dec = countDecimals(t.entry);
    const rawPct = isLong ? ((price - t.entry) / t.entry) * 100 : ((t.entry - price) / t.entry) * 100;
    const pctLev = rawPct * t.lev;

    // فحص ستوب لوز
    const slHit = isLong ? price <= t.sl : price >= t.sl;
    if (slHit) {
      t.closed = true; t.closePrice = price; t.closeTs = Date.now(); t.result = 'sl';
      tgSend(
        `🔴 محاكاة — ستوب لوز\n#${t.symbol.replace('USDT', '/USDT')} ${t.tf}\n${isLong ? '🟢 LONG' : '🔴 SHORT'} x${t.lev}\n` +
        `💰 دخول: ${t.entry} → خروج: ${price.toFixed(dec)}\n` +
        `📉 النتيجة: ${rawPct.toFixed(2)}% (${pctLev.toFixed(1)}% بالرافعة)\n` +
        `⏱ المدة: ${formatDuration(Date.now() - t.openTs)}`,
        chatId
      );
      continue;
    }

    // فحص الهدف الأول
    if (!t.tp1Hit) {
      const tp1Hit = isLong ? price >= t.tp1 : price <= t.tp1;
      if (tp1Hit) {
        t.tp1Hit = true;
        if (t.beSl !== null) t.sl = t.entry;
        const tp1Pct = isLong ? ((t.tp1 - t.entry) / t.entry) * 100 : ((t.entry - t.tp1) / t.entry) * 100;
        tgSend(
          `🟡 محاكاة — TP1 ✅\n#${t.symbol.replace('USDT', '/USDT')} ${t.tf}\n${isLong ? '🟢 LONG' : '🔴 SHORT'} x${t.lev}\n` +
          `💰 دخول: ${t.entry} → TP1: ${t.tp1.toFixed(dec)}\n` +
          `📈 +${tp1Pct.toFixed(2)}% (+${(tp1Pct * t.lev).toFixed(1)}%)` +
          (t.beSl !== null ? '\n🔄 SL → بريك إيفن' : '') +
          (!t.tp2 ? '\n✅ الصفقة مقفلة بالكامل' : ''),
          chatId
        );
        if (!t.tp2) {
          t.closed = true; t.closePrice = t.tp1; t.closeTs = Date.now(); t.result = 'tp1';
        }
        continue;
      }
    }

    // فحص الهدف الثاني
    if (t.tp1Hit && t.tp2) {
      const tp2Hit = isLong ? price >= t.tp2 : price <= t.tp2;
      if (tp2Hit) {
        t.closed = true; t.closePrice = price; t.closeTs = Date.now(); t.result = 'tp2';
        const tp2Pct = isLong ? ((price - t.entry) / t.entry) * 100 : ((t.entry - price) / t.entry) * 100;
        tgSend(
          `🟢 محاكاة — TP2 ✅✅\n#${t.symbol.replace('USDT', '/USDT')} ${t.tf}\n${isLong ? '🟢 LONG' : '🔴 SHORT'} x${t.lev}\n` +
          `💰 دخول: ${t.entry} → TP2: ${price.toFixed(dec)}\n` +
          `📈 +${tp2Pct.toFixed(2)}% (+${(tp2Pct * t.lev).toFixed(1)}%)\n` +
          `⏱ المدة: ${formatDuration(Date.now() - t.openTs)}\n` +
          `✅ الصفقة مقفلة بالكامل`,
          chatId
        );
        continue;
      }
    }
  }
  // تنظيف الصفقات المقفلة القديمة (أكثر من يوم)
  STATE.simTrades = STATE.simTrades.filter(t => !t.closed || (Date.now() - t.closeTs) < 86400000);
  db.saveSimTrades(STATE.simTrades);
}

function formatDuration(ms) {
  const m = Math.floor(ms / 60000);
  if (m < 60) return m + ' دقيقة';
  const h = Math.floor(m / 60);
  if (h < 24) return h + ' ساعة ' + (m % 60) + ' د';
  const d = Math.floor(h / 24);
  return d + ' يوم ' + (h % 24) + ' س';
}

async function placeOrUpdateSTSL(acc, sym, pos, slPrice) {
  try {
    const orders = await bFetch(acc.apiKey, acc.apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym });
    if (Array.isArray(orders)) {
      for (const o of orders) {
        if (o.type === 'STOP_MARKET' && o.origType !== 'TRAILING_STOP_MARKET') {
          const existingSL = parseFloat(o.stopPrice);
          if (Math.abs(existingSL - slPrice) / slPrice < 0.001) return;
          await bFetch(acc.apiKey, acc.apiSecret, 'DELETE', '/fapi/v1/order', { symbol: sym, orderId: o.orderId });
          addCopyLog('info', `🔄 ST SL: ألغي أمر قديم ${sym} @ ${existingSL}`);
        }
      }
    }
    const isLong = parseFloat(pos.positionAmt) > 0;
    const qty = roundQty(Math.abs(parseFloat(pos.positionAmt)), sym);
    await ensureLotSize(sym);
    const params = {
      symbol: sym,
      side: isLong ? 'SELL' : 'BUY',
      type: 'STOP_MARKET',
      stopPrice: String(slPrice),
      quantity: qty,
      positionSide: 'BOTH',
      reduceOnly: 'true',
    };
    await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', params);
    addCopyLog('success', `🛡️ ST SL: ${sym} وقف عند ${slPrice}`);
    // حدّث رسالة الإشارة بالوقف الجديد كي تبقى مطابقة لما على المنصّة
    await cornixSync(sym, {
      cmd: (STATE.settings.lockCxSLtpl || 'New stop loss: {price}').replace('{price}', String(slPrice)),
      stop: slPrice,
    });
  } catch (e) {
    addCopyLog('fail', `❌ ST SL أمر ${sym}: ${e.message}`);
  }
}

// ══════════════════════════════════════════════
//  نظام القفل — حدّ يومي + ستوب تلقائي + بريك إيفن + تريلنج
// ══════════════════════════════════════════════

const HOUR_MS = 3600000;

function lockSave() { db.saveLockState(STATE.lockState); }

function fmtHours(h) {
  if (h < 1) return `${Math.round(h * 60)} دقيقة`;
  if (h < 24) return `${h} ساعة`;
  const d = Math.floor(h / 24), r = h % 24;
  return `${d} يوم${r ? ` و${r} ساعة` : ''}`;
}

// يستعيد أي إعداد انحرف عن النسخة المحفوظة وقت القفل.
// الرفض وحده لا يكفي: قد يتغيّر إعداد من مسار لم نغطّه أو من الخادم مباشرة،
// فهذه الطبقة تُرجع الحالة لما كانت عليه وتُبلّغ بما تغيّر.
function restoreSnapshot() {
  const snap = STATE.lockState.snapshot;
  if (!snap) return null;
  const changed = [];
  for (const k of Object.keys(snap)) {
    if (JSON.stringify(STATE.settings[k]) !== JSON.stringify(snap[k])) {
      changed.push({ k, from: STATE.settings[k], to: snap[k] });
      STATE.settings[k] = snap[k];
    }
  }
  if (!changed.length) return null;
  db.saveSettings(STATE.settings);
  broadcast({ type: 'settings', data: STATE.settings });
  return changed;
}

// القفل العام: نشط طوال مدّته، وينتهي وحده بعدها.
// المدّة صفر = دائم (لا ينتهي إلا بتغيير الإعداد من الخادم).
function masterLockActive() {
  if (!STATE.settings.lockMaster) return false;
  const until = STATE.lockState.masterUntil || 0;
  if (!until) return true;                 // دائم
  if (Date.now() >= until) {
    // انتهت المدّة — نرفع القفل تلقائياً
    STATE.settings.lockMaster = false;
    STATE.lockState.masterUntil = 0;
    STATE.lockState.snapshot = null;
    db.saveSettings(STATE.settings);
    lockSave();
    lockNotify('🔓 انتهت مدّة القفل العام — عادت إعدادات الحد اليومي قابلة للتعديل');
    broadcast({ type: 'settings', data: STATE.settings });
    return false;
  }
  return true;
}

// إشعار لقناة نظام القفل. إن تعذّر الوصول إليها نُحوّل لقناة الإشارات
// بدل ضياع الإشعار بصمت — مع تنبيه يوضّح السبب.
function lockNotify(text) {
  const lock = STATE.settings.lockTgChat;
  const main = STATE.settings.cxChat;
  if (lock && !isDeadChat(lock)) { tgSend('🔒 نظام القفل\n' + text, lock); return; }
  if (lock && main) {
    tgSend(`🔒 نظام القفل\n${text}\n\n⚠️ تعذّر الوصول لقناة القفل (${lock}) — تأكّد أن البوت مشرف فيها وله صلاحية نشر الرسائل.`, main);
    return;
  }
  if (main) tgSend('🔒 نظام القفل\n' + text, main);
}

// يدوّر النافذة اليومية إذا انتهت مدّتها، ويُرجع الحالة الحالية
function lockWindow() {
  const L = STATE.lockState;
  const hrs = Math.max(1, parseFloat(STATE.settings.lockDailyHours) || 24);
  const now = Date.now();
  if (!L.windowStart) { L.windowStart = now; lockSave(); }
  // انتهت فترة القفل → نافذة جديدة نظيفة
  if (L.lockedUntil && now >= L.lockedUntil) {
    L.lockedUntil = 0; L.realizedLoss = 0; L.windowStart = now; L.trades = [];
    lockSave();
    lockNotify(`✅ انتهت فترة الانتظار — التداول اليدوي مفتوح من جديد\nالحد اليومي: $${STATE.settings.lockDailyAmt}`);
  } else if (!L.lockedUntil && now - L.windowStart >= hrs * HOUR_MS) {
    L.realizedLoss = 0; L.windowStart = now; L.trades = [];
    lockSave();
  }
  return L;
}

function lockIsLocked() {
  if (!STATE.settings.lockOn || !STATE.settings.lockDailyOn) return false;
  const L = lockWindow();
  return !!(L.lockedUntil && Date.now() < L.lockedUntil);
}

function lockRemaining() {
  const L = lockWindow();
  const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
  return Math.max(0, cap - (L.realizedLoss || 0));
}

// تسجيل نتيجة صفقة يدوية مُغلقة — يفعّل القفل عند بلوغ الحد
function lockRecordClose(sym, pnlUsd) {
  if (!STATE.settings.lockOn || !STATE.settings.lockDailyOn) return;
  const L = lockWindow();
  // حماية من التسجيل المزدوج (سجل الرمز يبقى ١٠ دقائق بعد الإغلاق)
  const rec = L.manualSyms[sym];
  if (rec?.recorded) return;
  if (rec) rec.recorded = true;
  L.trades = [{ sym, pnl: pnlUsd, ts: Date.now() }, ...(L.trades || [])].slice(0, 100);
  if (pnlUsd < 0) L.realizedLoss = parseFloat(((L.realizedLoss || 0) + Math.abs(pnlUsd)).toFixed(4));
  const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
  const hrs = Math.max(1, parseFloat(STATE.settings.lockDailyHours) || 24);
  if (cap > 0 && L.realizedLoss >= cap && !L.lockedUntil) {
    L.lockedUntil = Date.now() + hrs * HOUR_MS;
    lockNotify(
      `⛔ بلغت الحد اليومي للخسارة\n` +
      `الخسارة: $${L.realizedLoss.toFixed(2)} من $${cap}\n` +
      `أي صفقة يدوية تُفتح الآن ستُغلق فوراً\n` +
      `⏳ يفتح بعد ${hrs} ساعة`
    );
  } else {
    lockNotify(
      `${pnlUsd >= 0 ? '✅' : '❌'} أُغلقت ${sym.replace('USDT', '/USDT')} — ${pnlUsd >= 0 ? '+' : ''}$${pnlUsd.toFixed(2)}\n` +
      `المتبقي من الحد اليومي: $${lockRemaining().toFixed(2)} من $${cap}`
    );
  }
  lockSave();
  broadcast({ type: 'lockState', data: lockPublic() });
}

// مجموع الخسارة العائمة على الصفقات اليدوية المفتوحة.
// نحسبها من السعر اللحظي لا من تقرير بايننس، فالأخير يتأخّر ١٥ ثانية —
// وبرافعة عالية تكفي حركة صغيرة لتجاوز الحد في تلك الفترة.
function manualFloatingLoss() {
  const master = STATE.copyAccounts.find(a => a.isMaster);
  if (!master) return 0;
  let total = 0;
  for (const p of (master.livePositions || [])) {
    const amt = parseFloat(p.positionAmt);
    if (!amt) continue;
    const sym = p.symbol;
    if (STATE.lockState.manualSyms?.[sym]?.baseline) continue;
    if (!isManualPosition(sym, p)) continue;
    const entry = parseFloat(p.entryPrice) || 0;
    const px = livePrices[sym] || parseFloat(p.markPrice) || 0;
    if (!entry || !px) continue;
    const pnl = (px - entry) * amt;   // amt سالب للشورت فيصحّ الاتجاه تلقائياً
    if (pnl < 0) total += pnl;
  }
  return Math.abs(total);
}

// الحدّ اليومي يشمل المحقّق والعائم معاً. عند بلوغه تُغلق كل الصفقات اليدوية
// فوراً ويبدأ الانتظار — لا ينتظر إغلاقها يدوياً.
let dailyCheckBusy = false, lastDailyCheck = 0, lastRevCheck = 0;
async function checkDailyLossLimit() {
  const S = STATE.settings;
  if (!S.lockOn || !S.lockDailyOn || dailyCheckBusy) return;
  const cap = parseFloat(S.lockDailyAmt) || 0;
  if (cap <= 0) return;
  if (lockIsLocked()) return;
  const L = lockWindow();
  const floating = manualFloatingLoss();
  const total = (L.realizedLoss || 0) + floating;
  if (total < cap) return;

  dailyCheckBusy = true;
  try {
    const master = STATE.copyAccounts.find(a => a.isMaster);
    const hrs = Math.max(1, parseFloat(S.lockDailyHours) || 24);
    L.lockedUntil = Date.now() + hrs * HOUR_MS;
    L.realizedLoss = cap;          // بلغ الحد
    lockSave();

    const closed = [], failed = [];
    for (const p of (master?.livePositions || [])) {
      const amt = parseFloat(p.positionAmt);
      if (!amt) continue;
      const sym = p.symbol;
      if (STATE.lockState.manualSyms?.[sym]?.baseline) continue;
      if (!isManualPosition(sym, p)) continue;
      try { await closeFollower(master, sym, amt); closed.push(sym.replace('USDT', '')); }
      catch (e) { failed.push(`${sym}: ${e.message}`); }
      await new Promise(r => setTimeout(r, 250));
    }
    lockNotify(
      `⛔ بلغت الحد اليومي — أُغلقت الصفقات اليدوية\n` +
      `الخسارة: محقّقة $${(L.realizedLoss - floating > 0 ? L.realizedLoss - floating : 0).toFixed(2)} + عائمة $${floating.toFixed(2)} = $${total.toFixed(2)}\n` +
      `الحد: $${cap}\n` +
      (closed.length ? `✅ أُغلقت: ${closed.join(' · ')}\n` : 'ℹ️ لا توجد صفقات يدوية مفتوحة\n') +
      (failed.length ? `❌ فشل: ${failed.join(' · ')}\n` : '') +
      `⏳ يفتح بعد ${hrs} ساعة`
    );
    broadcast({ type: 'lockState', data: lockPublic() });
  } catch (e) {
    addCopyLog('fail', `❌ الحد اليومي: ${e.message}`);
  } finally {
    dailyCheckBusy = false;
  }
}

function lockPublic() {
  const L = STATE.lockState;
  const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
  const hrs = Math.max(1, parseFloat(STATE.settings.lockDailyHours) || 24);
  return {
    windowStart: L.windowStart, realizedLoss: L.realizedLoss || 0,
    lockedUntil: L.lockedUntil || 0, remaining: Math.max(0, cap - (L.realizedLoss || 0)),
    cap, hours: hrs, locked: lockIsLocked(),
    windowEnds: L.lockedUntil || (L.windowStart + hrs * HOUR_MS),
    trades: (L.trades || []).slice(0, 20),
    floating: manualFloatingLoss(),
    diag: L.diag || {},
    diagAt: L.diagAt || 0,
    msgFate: L.msgFate || {},
    vStops: L.vStops || {},
    booted: bootBaselineDone,
    masterUntil: STATE.lockState.masterUntil || 0,
    masterActive: masterLockActive(),   // يفحص الانتهاء ويرفع القفل عند حلوله
    allLocked: !!(STATE.settings.lockMaster && STATE.settings.lockAllSettings),
  };
}

// يبني تشخيصاً لكل مركز مفتوح: يدوية؟ خط أساس؟ لها وقف؟ الهامش؟
async function buildLockDiag(master, positions) {
  const L = STATE.lockState;
  const diag = {};
  for (const pos of positions) {
    const sym = pos.symbol;
    const chk = manualCheck(sym, pos);
    const rec = L.manualSyms[sym] || null;
    const amt = Math.abs(parseFloat(pos.positionAmt));
    const mark = parseFloat(pos.markPrice) || livePrices[sym] || 0;
    const lev = parseFloat(pos.leverage) || 1;
    const margin = mark && lev ? (amt * mark) / lev : 0;
    let hasStop = null, ourStop = null, stopPx = null;
    try {
      const orders = await bFetch(master.apiKey, master.apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym });
      if (Array.isArray(orders)) {
        const stops = orders.filter(o => o.type === 'STOP_MARKET' || o.type === 'STOP' || o.type === 'TRAILING_STOP_MARKET');
        hasStop = stops.length > 0;
        const ourIds = (L.ourStops?.[sym] || []).map(String);
        const mine = stops.find(o => ourIds.includes(String(o.orderId)));
        ourStop = !!mine;
        // أقرب وقف للسعر هو الذي سيُنفَّذ فعلياً
        const prices = stops.map(o => parseFloat(o.stopPrice)).filter(p => p > 0);
        if (prices.length && mark) {
          stopPx = prices.reduce((a, b) => Math.abs(b - mark) < Math.abs(a - mark) ? b : a);
        }
      }
    } catch (e) {}
    // سبب عدم التصرّف (إن وُجد) — baseline يمنع التقليم/الإغلاق فقط، لا الستوب
    let blocked = null;
    if (!chk.manual) blocked = chk.why;
    else if (rec?.baseline) blocked = 'خط أساس — لا تُقلَّم ولا تُغلق (الستوب يُطبَّق)';
    diag[sym] = {
      manual: chk.manual, why: chk.why, baseline: !!rec?.baseline,
      slPlaced: !!rec?.slPlaced, hasStop, ourStop, stopPx, margin: parseFloat(margin.toFixed(2)),
      lev, pnl: parseFloat(pos.unRealizedProfit) || 0, blocked,
      lastErr: rec?.lastErr || null,
      override: L.manualOverride?.[sym] || null,
    };
    await new Promise(r => setTimeout(r, 120));
  }
  L.diag = diag;
  L.diagAt = Date.now();
  return diag;
}

// سجلّات openTrades التي تُنشأ تلقائياً لأي مركز جديد على الماستر —
// لا تدل على أن الصفقة من إشارة البوت (تُنشأ حتى للصفقات اليدوية عند تشغيل النسخ)
const AUTO_TRADE_LABELS = ['🪞 Copy', '🪞 Binance', '📊 مراقبة'];

// هل هذه الصفقة يدوية؟ (ليست من إشارة البوت/كورنكس)
// نُرجع السبب أيضاً ليظهر في لوحة التشخيص
// كورنكس ينفّذ الإشارة خلال ثوانٍ/دقائق من وصولها. فإن كان فارق الوقت بين
// فتح المركز وإرسال الإشارة أكبر من هذا الحد، فالمركز فُتح يدوياً لا من الإشارة.
const SIGNAL_MATCH_WINDOW_MS = 15 * 60 * 1000;

function fmtGap(ms) {
  const m = Math.round(ms / 60000);
  if (m < 60) return `${m} د`;
  const h = Math.floor(m / 60);
  return h < 24 ? `${h} س` : `${Math.floor(h / 24)} ي`;
}

// pos اختياري — بوجوده نطابق وقت فتح المركز بوقت الإشارة بدل الاكتفاء
// بوجود إشارة قديمة للرمز، فلا تُحسب صفقة يدوية على أنها صفقة بوت
function manualCheck(sym, pos) {
  // تجاوز يدوي من المستخدم — يغلب كل استنتاج
  const ov = STATE.lockState.manualOverride?.[sym];
  if (ov === 'manual') return { manual: true, why: 'يدوية (تحديد يدوي)' };
  if (ov === 'bot') return { manual: false, why: 'صفقة بوت (تحديد يدوي)' };

  const posTs = pos ? (parseFloat(pos.updateTime) || 0) : 0;
  const rec = STATE.sentMsgIds[sym];
  if (rec?.ts && posTs) {
    const gap = Math.abs(posTs - rec.ts);
    return gap <= SIGNAL_MATCH_WINDOW_MS
      ? { manual: false, why: `فُتحت مع الإشارة (فارق ${fmtGap(gap)})` }
      : { manual: true, why: `يدوية — الإشارة قبل ${fmtGap(gap)} من فتح المركز` };
  }
  // بلا وقت للمقارنة: نرجع للاستنتاج القديم
  if (rec) return { manual: false, why: 'أُرسلت إشارة تلغرام لهذا الرمز' };
  if (STATE.sentSigs[sym]) return { manual: false, why: 'إشارة بوت نشطة لهذا الرمز' };
  const t = STATE.openTrades.find(x => x.symbol === sym);
  if (t && !AUTO_TRADE_LABELS.includes(t.label)) return { manual: false, why: `صفقة بوت (${t.label})` };
  return { manual: true, why: 'يدوية' };
}
function isManualPosition(sym, pos) { return manualCheck(sym, pos).manual; }

// ── مزامنة كورنكس ─────────────────────────────────────
// وضعان: رد على رسالة الإشارة بأمر تحديث، أو تعديل الرسالة الأصلية نفسها
// بنفس صيغتها مع تغيير الأرقام فقط (كي يستطيع كورنكس قراءتها من جديد).

// تلغرام لا يسمح للبوت بتعديل رسائله بعد ٤٨ ساعة
const TG_EDIT_WINDOW_MS = 48 * HOUR_MS;

// تعديل مباشر (خارج الطابور) — يُرجع نتيجة تلغرام الحقيقية بدل ابتلاع الخطأ
// تحذير: editMessageText يمسح لوحة الأزرار المرفقة بالرسالة ما لم تُمرَّر معه.
// أزرار كورنكس (View Signal / One Click Follow) تُضاف بعد النشر، فأي تعديل
// لاحق على الإشارة يجرّدها منها. لا تستدعِ هذه الدالة على رسالة إشارة
// إلا بنيّة صريحة وقبولٍ لفقد الأزرار.
async function tgEditDirect(chat, messageId, text) {
  const token = STATE.settings.cxToken;
  if (!token) return { ok: false, error: 'لا يوجد توكن بوت' };
  // مهلة صريحة: بدونها قد يعلّق الطلب إلى الأبد فيبقى الزر ينتظر بلا رد
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 15000);
  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, message_id: messageId, text }),
      signal: ac.signal,
    });
    const body = await res.text().catch(() => '');
    if (res.ok) return { ok: true };
    let desc = body;
    try { desc = JSON.parse(body).description || body; } catch (e) {}
    return { ok: false, error: desc, status: res.status };
  } catch (e) {
    return { ok: false, error: e.name === 'AbortError' ? 'انتهت المهلة (١٥ ثانية) بلا رد من تلغرام' : e.message };
  } finally {
    clearTimeout(timer);
  }
}

// يقرأ سعر الوقف الحالي واتجاه الصفقة من نص الإشارة
function parseSignalMsg(text) {
  const stopM = text.match(/Stop Targets:[^\n]*\n\s*1\)\s*([\d.]+)/);
  const sideM = text.match(/Signal Type:\s*Regular\s*\((Long|Short)\)/i);
  const entryM = text.match(/Entry Targets:[\s\S]*?2\)\s*([\d.]+)/);
  return {
    stop: stopM ? parseFloat(stopM[1]) : null,
    side: sideM ? sideM[1].toUpperCase() : null,
    entry: entryM ? parseFloat(entryM[1]) : null,
  };
}

// سعر وقف تجريبي منطقي: يزحزح الوقف الحالي ٢٪ باتجاه الدخول — تغيير واضح وغير عشوائي
function suggestTestStop(text) {
  const { stop, side } = parseSignalMsg(text);
  if (!stop) return null;
  return side === 'SHORT' ? stop * 0.98 : stop * 1.02;
}

// يستبدل سعر الوقف في نص إشارة كورنكس، مع الحفاظ على باقي الرسالة كما هي
function replaceStopInMsg(text, newStop) {
  // "Stop Targets:" ثم سطر "1) <سعر>"
  const re = /(Stop Targets:[^\n]*\n\s*1\)\s*)([^\n]*)/;
  if (!re.test(text)) return null;
  return text.replace(re, `$1${newStop}`);
}

// يستبدل نسبة تريلنج جني الأرباح — "Take-Profit: Percentage (N%)" داخل قسم Trailing
// (لا يمسّ "Take-Profit Targets:" لأن الصيغة هناك بلا نقطتين قبل Percentage)
function replaceTrailInMsg(text, newPct) {
  const re = /(Take-Profit:\s*Percentage\s*\()([^)]*)(\))/;
  if (!re.test(text)) return null;
  return text.replace(re, `$1${newPct}%$3`);
}

// يعدّل رسالة الإشارة الأصلية بالأرقام الجديدة
async function cornixEditSignal(sym, { stop, trailPct } = {}) {
  const rec = STATE.sentMsgIds[sym];
  if (!rec?.id || !rec.text) return { ok: false, why: 'لا توجد رسالة إشارة محفوظة لهذا الرمز' };
  const age = Date.now() - (rec.ts || 0);
  if (age > TG_EDIT_WINDOW_MS) {
    return { ok: false, why: `عمر الرسالة ${Math.floor(age / HOUR_MS)} ساعة — تلغرام يمنع تعديل رسائل البوت بعد ٤٨ ساعة` };
  }
  let t = rec.text;
  const changes = [];
  if (stop != null) {
    const nt = replaceStopInMsg(t, fmtSignalPrice(stop));
    if (nt) { t = nt; changes.push(`الوقف → ${fmtSignalPrice(stop)}`); }
  }
  if (trailPct != null) {
    const nt = replaceTrailInMsg(t, trailPct);
    if (nt) { t = nt; changes.push(`التريلنج → ${trailPct}%`); }
  }
  if (!changes.length) return { ok: false, why: 'تعذّر إيجاد البند المطلوب في نص الرسالة' };
  if (t === rec.text) return { ok: false, why: 'لا تغيير' };
  const chat = rec.chat || STATE.settings.cxChat;
  const r = await tgEditDirect(chat, rec.id, t);
  if (!r.ok) {
    // تفاصيل تكفي للتشخيص بدل رسالة تلغرام المبهمة
    const info = `رسالة#${rec.id} · قناة ${chat} · عمر ${Math.floor(age / 60000)}د`;
    if (/message to edit not found/i.test(r.error || '')) {
      delete STATE.sentMsgIds[sym];      // سجل قديم لا يطابق أي رسالة — أزله
      saveSentMsgIdsDebounced();
      return { ok: false, why: `تلغرام لا يجد الرسالة (${info}). غالباً حُذفت، أو أُرسلت بتوكن/قناة مختلفة قبل النقل. أُزيل السجل — أرسل إشارة جديدة وجرّب عليها.` };
    }
    return { ok: false, why: `${r.error} (${info})` };
  }
  rec.text = t;                 // النص المحفوظ يبقى مطابقاً للرسالة المنشورة
  saveSentMsgIdsDebounced();
  return { ok: true, changes };
}

// فحص وجود رسالة بلا أي أثر جانبي:
// نطلب تحويلها إلى قناة وهمية غير موجودة. تلغرام يتحقق من الرسالة المصدر أولاً،
// فيكون ردّه:
//   "message to forward not found" → الرسالة غير موجودة
//   "chat not found"               → الرسالة موجودة (فشل عند الوجهة فقط)
// وبهذا لا تُحوَّل أي رسالة ولا تتلوّث أي قناة.
const PROBE_DEST = '-1000000000001';
async function messageExists(fromChat, msgId) {
  const token = STATE.settings.cxToken;
  if (!token) return null;
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), 12000);
  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/forwardMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: PROBE_DEST, from_chat_id: fromChat, message_id: msgId }),
      signal: ac.signal,
    });
    if (res.ok) return true;                       // نجح فعلاً (وجهة صالحة بالصدفة)
    const body = await res.text().catch(() => '');
    if (/message to forward not found/i.test(body)) return false;
    if (/chat not found/i.test(body)) return true;
    return null;                                   // غير حاسم
  } catch (e) { return null; }
  finally { clearTimeout(timer); }
}

// يبحث عن رسالة كورنكس البديلة بعد رقم رسالتنا مباشرة
async function findReplacementMsg(chat, ourId) {
  for (let off = 1; off <= 5; off++) {
    const ex = await messageExists(chat, ourId + off);
    if (ex === true) return ourId + off;
    await new Promise(r => setTimeout(r, 300));
  }
  return null;
}

// بعد إرسال إشارة بمدّة قصيرة نتحقق: هل بقيت رسالتنا؟ وهل ما زال البوت يملكها؟
// نعدّلها بنفس نصها تماماً، فردّ تلغرام وحده يكفي للحكم دون تغيير أي شيء.
// هذا يحسم السؤال آلياً بدل الاعتماد على توقيت المستخدم أو على رسائل قد تُحذف يدوياً.
async function verifySignalMsg(sym) {
  const rec = STATE.sentMsgIds[sym];
  if (!rec?.id || !rec.text) return;
  const chat = rec.chat || STATE.settings.cxChat;
  const r = await tgEditDirect(chat, rec.id, rec.text);
  const err = String(r.error || '');
  let verdict, detail;
  if (r.ok || /not modified/i.test(err)) {
    verdict = 'alive';
    detail = 'الرسالة باقية والبوت يملكها — التعديل ممكن';
  } else if (/not found/i.test(err)) {
    verdict = 'gone';
    detail = 'اختفت خلال دقيقة من إرسالها — حُذفت أو أعاد كورنكس نشرها';
    // ابحث عن البديل: أرقام الرسائل متتابعة فرسالة كورنكس تلي رسالتنا
    const alt = await findReplacementMsg(chat, rec.id);
    if (alt) {
      rec.cornixId = alt;
      saveSentMsgIdsDebounced();
      detail += ` · البديل على الأرجح #${alt}`;
    }
  } else if (/can't be edited|MESSAGE_AUTHOR_REQUIRED|MESSAGE_ID_INVALID|not enough rights/i.test(err)) {
    verdict = 'foreign';
    detail = 'موجودة لكن ليست رسالة البوت — لا يمكن تعديلها';
  } else {
    verdict = 'unknown';
    detail = err.slice(0, 80);
  }
  const store = STATE.lockState.msgFate || (STATE.lockState.msgFate = {});
  store[sym] = { at: Date.now(), id: rec.id, chat, verdict, detail, cornixId: rec.cornixId || null };
  // نحتفظ بآخر ١٠ فقط
  const keys = Object.keys(store).sort((a, b) => (store[b].at || 0) - (store[a].at || 0));
  for (const k of keys.slice(10)) delete store[k];
  lockSave();
  addCopyLog(verdict === 'alive' ? 'success' : 'info', `🔎 رسالة ${sym}: ${detail}`);
  broadcast({ type: 'lockState', data: lockPublic() });
}

// ══════════════════════════════════════════════
//  الوقف الافتراضي — يتابعه البوت ويغلق بأمر close
// ══════════════════════════════════════════════
// كورنكس لا يقبل أمراً نصياً لتغيير الوقف، ولا يقرأ تعديل الرسالة.
// الأمر الوحيد المدعوم هو الإغلاق (close/cancel/exit). لذا يتتبّع البوت
// المستوى بنفسه على السعر اللحظي، وعند بلوغه يردّ على الإشارة بأمر إغلاق
// فيُغلق كورنكس الصفقة لدى كل المتابعين.

const VSTOP_KINDS = {
  trail: 'تريلنج',
  be: 'بريك إيفن',
  sl: 'وقف خسارة',
};

function vStopsStore() {
  return STATE.lockState.vStops || (STATE.lockState.vStops = {});
}

// يسجّل وقفاً افتراضياً لعملة. للتريلنج نبدأ التتبّع من السعر الحالي.
function armVStop(sym, { kind, side, pct, price, entry, reason }) {
  const store = vStopsStore();
  const isLong = side === 'LONG';
  const v = {
    kind, side, pct: parseFloat(pct), entry,
    startPrice: price, peak: price,
    armedAt: Date.now(), reason: reason || '',
    triggered: false,
  };
  // مستوى الإغلاق: ثابت للبريك إيفن والوقف، ومتحرّك للتريلنج
  if (kind === 'be') {
    v.level = isLong ? entry * (1 + v.pct / 100) : entry * (1 - v.pct / 100);
  } else if (kind === 'sl') {
    v.level = isLong ? price * (1 - v.pct / 100) : price * (1 + v.pct / 100);
  } else {
    v.level = isLong ? price * (1 - v.pct / 100) : price * (1 + v.pct / 100);
  }
  // حارس: مستوىً مُبلَّغ عند التسجيل يعني إغلاقاً فورياً — نرفضه بدل تنفيذه.
  // يحمي من خطأ في الحساب أو من تسجيل بريك إيفن على صفقة لم تربح كفاية.
  const hitNow = isLong ? price <= v.level : price >= v.level;
  if (hitNow) {
    return { rejected: true, why: `المستوى ${fmtSignalPrice(v.level)} مُبلَّغ أصلاً عند ${fmtSignalPrice(price)} — لم يُسجَّل` };
  }
  store[sym] = v;
  lockSave();
  return v;
}

// يردّ على رسالة الإشارة بأمر الإغلاق.
// الصيغة "/close" مؤكَّدة عملياً على هذه القناة (جُرِّبت يدوياً فأغلقت الصفقة).
async function cornixClose(sym) {
  const rec = STATE.sentMsgIds[sym];
  if (!rec?.id) return { ok: false, why: 'لا توجد رسالة إشارة محفوظة لهذا الرمز' };
  const chat = rec.chat || STATE.settings.cxChat;
  const cmd = STATE.settings.lockCloseCmd || '/close';
  await tgSend(cmd, chat, { replyTo: rec.id });
  // نردّ على رسالة كورنكس أيضاً إن عُرف رقمها — لا نعرف أيّهما يعتبرها الأصلية،
  // وأمر إغلاق مكرّر على صفقة مغلقة لا يضرّ
  if (rec.cornixId) await tgSend(cmd, chat, { replyTo: rec.cornixId });
  return { ok: true, ids: [rec.id, rec.cornixId].filter(Boolean), cmd };
}

// يُنفَّذ عند بلوغ المستوى: إغلاق على بايننس + أمر إغلاق لكورنكس
async function fireVStop(sym, v, price) {
  v.triggered = true;
  lockSave();
  const pair = sym.replace('USDT', '/USDT');
  const lines = [];
  const isLong = v.side === 'LONG';
  const movePct = v.entry ? ((price - v.entry) / v.entry) * 100 * (isLong ? 1 : -1) : 0;

  // ١) إغلاق فعلي على بايننس — لا ينتظر استجابة كورنكس
  const master = STATE.copyAccounts.find(a => a.isMaster);
  const pos = (master?.livePositions || []).find(p => p.symbol === sym && Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  if (master?.apiKey && pos) {
    try {
      await closeFollower(master, sym, parseFloat(pos.positionAmt));
      lines.push('✅ أُغلقت على بايننس');
    } catch (e) {
      lines.push(`❌ إغلاق بايننس فشل: ${e.message}`);
    }
  } else {
    lines.push('ℹ️ لا يوجد مركز مفتوح على الماستر');
  }

  // ٢) أمر إغلاق لكورنكس ليغلق لدى المتابعين
  const cr = await cornixClose(sym);
  lines.push(cr.ok ? `📨 أُرسل «${cr.cmd}» رداً على ${cr.ids.map(i => '#' + i).join(' و ')}`
                   : `⚠️ لم يُرسل لكورنكس: ${cr.why}`);

  lockNotify(
    `🔔 ${VSTOP_KINDS[v.kind] || v.kind} — #${pair}\n` +
    `${isLong ? '🟢 LONG' : '🔴 SHORT'} · دخول ${fmtSignalPrice(v.entry)}\n` +
    (v.kind === 'trail'
      ? `📈 أعلى نقطة: ${fmtSignalPrice(v.peak)} · ارتد ${v.pct}% → ${fmtSignalPrice(v.level)}\n`
      : `🎯 المستوى: ${fmtSignalPrice(v.level)} (${v.pct}%)\n`) +
    `💰 سعر الإغلاق: ${fmtSignalPrice(price)} · ${movePct >= 0 ? '+' : ''}${movePct.toFixed(2)}%\n` +
    (v.reason ? `📝 ${v.reason}\n` : '') +
    lines.join('\n')
  );

  delete vStopsStore()[sym];
  lockSave();
  broadcast({ type: 'lockState', data: lockPublic() });
}

// يُستدعى مع كل تحديث سعر — خفيف لأنه يمرّ على العملات المسجّلة فقط
let vStopBusy = false;
function checkVStops(sym, price) {
  const store = STATE.lockState.vStops;
  if (!store) return;
  const v = store[sym];
  if (!v || v.triggered || !price) return;
  const isLong = v.side === 'LONG';

  if (v.kind === 'trail') {
    // لاحق القمة (أو القاع للشورت) وحرّك المستوى معها
    const better = isLong ? price > v.peak : price < v.peak;
    if (better) {
      v.peak = price;
      v.level = isLong ? price * (1 - v.pct / 100) : price * (1 + v.pct / 100);
      return;   // لا نحفظ على القرص مع كل تحديث — يُحفظ عند الإطلاق
    }
  }

  const hit = isLong ? price <= v.level : price >= v.level;
  if (!hit) return;
  if (vStopBusy) return;
  vStopBusy = true;
  fireVStop(sym, v, price).catch(e => addCopyLog('fail', `❌ وقف افتراضي ${sym}: ${e.message}`))
    .finally(() => { vStopBusy = false; });
}

// قائمة الإشارات المحفوظة مع عمرها وقابليتها للتعديل
function msgSymsList() {
  const now = Date.now();
  return Object.entries(STATE.sentMsgIds)
    .filter(([, r]) => r?.id && r?.text)
    .map(([sym, r]) => {
      const age = now - (r.ts || 0);
      const p = parseSignalMsg(r.text);
      return {
        sym, ts: r.ts, ageH: Math.floor(age / HOUR_MS),
        editable: age < TG_EDIT_WINDOW_MS,
        stop: p.stop, side: p.side,
        suggest: p.stop ? parseFloat(fmtSignalPrice(suggestTestStop(r.text))) : null,
      };
    })
    .sort((a, b) => (b.ts || 0) - (a.ts || 0)).slice(0, 30);
}

// ينفّذ المزامنة حسب الوضع المختار — يُرجع وصفاً لما تم لعرضه في الإشعار
async function cornixSync(sym, { cmd, stop, trailPct } = {}) {
  if (!STATE.settings.lockCornixSync) return null;
  const rec = STATE.sentMsgIds[sym];
  if (!rec?.id) return null;
  const mode = STATE.settings.lockCornixMode || 'edit';
  const out = [];
  if (mode === 'reply' || mode === 'both') {
    if (cmd) { await tgSend(cmd, rec.chat || STATE.settings.cxChat, { replyTo: rec.id }); out.push('رد'); }
  }
  if (mode === 'edit' || mode === 'both') {
    const r = await cornixEditSignal(sym, { stop, trailPct });
    out.push(r.ok ? 'تعديل الرسالة' : `تعديل فشل (${r.why})`);
  }
  return out.length ? out.join(' + ') : null;
}

// يلغي أوامر الوقف التي وضعها هذا النظام فقط.
// لا نمسّ أوامر كورنكس: وقفه بعيد (٣٠٪ مثلاً) ووقفنا أقرب، فالأقرب يضرب أولاً
// والاثنان يتعايشان على المنصّة. إلغاء وقف كورنكس كان يترك الصفقة بلا حماية
// احتياطية وقد يدفع كورنكس لإعادة إنشائه.
async function cancelOurStops(acc, sym) {
  const store = STATE.lockState.ourStops || (STATE.lockState.ourStops = {});
  const ids = store[sym] || [];
  for (const id of ids) {
    // قد يكون الأمر نُفِّذ أو أُلغي مسبقاً — الفشل هنا غير مهم
    try { await bFetch(acc.apiKey, acc.apiSecret, 'DELETE', '/fapi/v1/order', { symbol: sym, orderId: id }); }
    catch (e) {}
  }
  if (ids.length) { delete store[sym]; lockSave(); }
}

function rememberOurStop(sym, orderId, px) {
  if (!orderId) return;
  const store = STATE.lockState.ourStops || (STATE.lockState.ourStops = {});
  store[sym] = [orderId];
  // نحفظ السعر أيضاً كي يستطيع الحارس إعادة الأمر إن ألغاه كورنكس
  if (px != null) {
    const pxs = STATE.lockState.stopPx || (STATE.lockState.stopPx = {});
    pxs[sym] = px;
  }
  lockSave();
}

// تقليص جزئي لمركز — بدون تسجيله كصفقة مغلقة في الإحصائيات (يُستخدم للتقليم)
async function reducePosition(acc, sym, posAmt, qtyToClose) {
  const isLong = posAmt > 0;
  await ensureLotSize(sym);
  const qty = roundQty(Math.abs(qtyToClose), sym);
  if (qty <= 0) throw new Error('الكمية صغيرة جداً');
  const mode = await getPositionMode(acc);
  const p = { symbol: sym, side: isLong ? 'SELL' : 'BUY', type: 'MARKET', quantity: qty };
  if (mode === 'hedge') p.positionSide = isLong ? 'LONG' : 'SHORT';
  else { p.positionSide = 'BOTH'; p.reduceOnly = 'true'; }
  await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', p);
  return qty;
}

// وضع أمر وقف حدّي فعلي على المنصّة (يبقى قائماً حتى لو توقّف البوت)
async function placeStop(acc, sym, pos, stopPrice, tag) {
  await ensureLotSize(sym);
  const px = roundPrice(stopPrice, sym);
  const amt = parseFloat(pos.positionAmt);
  const isLong = amt > 0;
  const mark = parseFloat(pos.markPrice) || livePrices[sym] || 0;
  // بايننس يرفض وقفاً سيُفعَّل فوراً — تحقّق من الاتجاه أولاً
  if (mark > 0) {
    if (isLong && px >= mark) throw new Error(`سعر الوقف ${px} فوق السعر الحالي ${mark}`);
    if (!isLong && px <= mark) throw new Error(`سعر الوقف ${px} تحت السعر الحالي ${mark}`);
  }
  await cancelOurStops(acc, sym);
  const mode = await getPositionMode(acc);
  const side = isLong ? 'SELL' : 'BUY';
  const posSide = mode === 'hedge' ? (isLong ? 'LONG' : 'SHORT') : 'BOTH';

  // بعض الحسابات/العملات ترفض closePosition على /fapi/v1/order وتردّ
  // "Order type not supported for this endpoint" — نسقط عندها إلى صيغة
  // الكمية + reduceOnly، وهي الصيغة المستخدمة في باقي البوت.
  const attempts = [
    { symbol: sym, side, type: 'STOP_MARKET', stopPrice: String(px), closePosition: 'true', positionSide: posSide },
    (() => {
      const q = { symbol: sym, side, type: 'STOP_MARKET', stopPrice: String(px), quantity: roundQty(Math.abs(amt), sym), positionSide: posSide };
      if (mode !== 'hedge') q.reduceOnly = 'true';
      return q;
    })(),
  ];

  let lastErr = null;
  for (const params of attempts) {
    try {
      const r = await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', params);
      rememberOurStop(sym, r?.orderId, px);
      addCopyLog('success', `🛡️ ${tag}: ${sym} وقف حدّي @ ${px}`);
      return px;
    } catch (e) {
      lastErr = e;
      // أخطاء لا تُصلحها إعادة المحاولة بصيغة أخرى
      if (!/not supported for this endpoint|Algo Order/i.test(e.message)) throw e;
    }
  }
  throw lastErr || new Error('تعذّر وضع الوقف');
}

// (4)+(5) بريك إيفن للصفقات الرابحة — سعر الوقف فوق الدخول بقليل لتغطية العمولات
async function applyBreakEven(acc, syms, offsetPct, reason) {
  const results = { done: [], skipped: [], failed: [], synced: [] };
  const positions = (acc.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  const off = parseFloat(offsetPct);
  const offset = isFinite(off) ? off : 0.1;
  for (const pos of positions) {
    const sym = pos.symbol;
    if (syms && syms.length && !syms.includes(sym)) continue;
    const pnl = parseFloat(pos.unRealizedProfit) || 0;
    if (pnl <= 0) { results.skipped.push(`${sym}: خاسرة`); continue; }
    const isLong = parseFloat(pos.positionAmt) > 0;
    const entry = parseFloat(pos.entryPrice) || 0;
    if (!entry) { results.skipped.push(`${sym}: لا يوجد سعر دخول`); continue; }
    const bePrice = isLong ? entry * (1 + offset / 100) : entry * (1 - offset / 100);

    // الغرض حماية ربح قائم، لا إقفال صفقة بالكاد دخلت الربح.
    // نقيس الربح على الهامش (نفس نسبة بايننس) ونقارنه بالحدّ الذي يحدّده المستخدم.
    const mark = parseFloat(pos.markPrice) || livePrices[sym] || 0;
    const lev = parseFloat(pos.leverage) || 1;
    const margin = mark ? (Math.abs(parseFloat(pos.positionAmt)) * mark) / lev : 0;
    const roi = margin ? (pnl / margin) * 100 : 0;
    const minPnl = parseFloat(STATE.settings.lockBEminPnl);
    const needPnl = isFinite(minPnl) ? minPnl : 0;
    // حدّ ضمني: مستوى التعادل يجب أن يبقى تحت السعر بهامش، وإلا أُقفلت على أول حركة
    const movePct = mark ? ((mark - entry) / entry) * 100 * (isLong ? 1 : -1) : 0;
    if (roi < needPnl || movePct < offset * 1.5) {
      results.skipped.push(`${sym}: ربح ${roi.toFixed(0)}% (المطلوب ${needPnl}%)`);
      continue;
    }
    try {
      const px = await placeStop(acc, sym, pos, bePrice, 'بريك إيفن');
      STATE.lockState.beDone[sym] = Date.now();
      results.done.push(`${sym} @ ${px}`);
      // وقف افتراضي موازٍ: يردّ على الإشارة بأمر إغلاق ليتبعه كورنكس والمشتركون
      const av = armVStop(sym, { kind: 'be', side: isLong ? 'LONG' : 'SHORT', pct: offset,
                      price: mark || entry, entry, reason: `بريك إيفن (${reason})` });
      if (av?.rejected) results.skipped.push(`${sym}: ${av.why}`);
      else results.armed.push(`${sym} @ ${fmtSignalPrice(bePrice)}`);
      const s = await cornixSync(sym, { cmd: STATE.settings.lockCxBEtpl || 'SL to entry', stop: px });
      if (s) results.synced.push(`${sym}: ${s}`);
    } catch (e) {
      results.failed.push(`${sym}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 250));
  }
  if (results.done.length || results.failed.length) {
    lockSave();
    lockNotify(
      `🟡 بريك إيفن (${reason})\nنسبة فوق الدخول: ${offset}%\n` +
      (results.done.length ? `✅ طُبّق: ${results.done.join(' · ')}\n` : '') +
      (results.failed.length ? `❌ فشل: ${results.failed.join(' · ')}\n` : '') +
      (results.armed?.length ? `👁 يتابعها البوت: ${results.armed.join(' · ')}\n` : '') +
      (results.skipped.length ? `⏭ تُخطّيت: ${results.skipped.length}` : '')
    );
  }
  return results;
}

// (6) وقف خسارة للصفقات الخاسرة — بنسبة من السعر الحالي
async function applyLossStop(acc, syms, pct) {
  const results = { done: [], skipped: [], failed: [], synced: [] };
  const positions = (acc.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  const d = parseFloat(pct);
  if (!isFinite(d) || d <= 0) return results;
  for (const pos of positions) {
    const sym = pos.symbol;
    if (syms && syms.length && !syms.includes(sym)) continue;
    const pnl = parseFloat(pos.unRealizedProfit) || 0;
    if (pnl >= 0) { results.skipped.push(`${sym}: رابحة`); continue; }
    const isLong = parseFloat(pos.positionAmt) > 0;
    const mark = parseFloat(pos.markPrice) || livePrices[sym] || 0;
    if (!mark) { results.skipped.push(`${sym}: لا يوجد سعر`); continue; }
    const slPrice = isLong ? mark * (1 - d / 100) : mark * (1 + d / 100);
    try {
      const px = await placeStop(acc, sym, pos, slPrice, 'وقف خسارة');
      results.done.push(`${sym} @ ${px}`);
      const av2 = armVStop(sym, { kind: 'sl', side: isLong ? 'LONG' : 'SHORT', pct: d,
                      price: mark, entry: parseFloat(pos.entryPrice) || mark,
                      reason: `وقف ${d}% من ${fmtSignalPrice(mark)}` });
      if (av2?.rejected) results.skipped.push(`${sym}: ${av2.why}`);
      else results.armed.push(`${sym} @ ${fmtSignalPrice(slPrice)}`);
      const s2 = await cornixSync(sym, {
        cmd: (STATE.settings.lockCxSLtpl || 'New stop loss: {price}').replace('{price}', String(px)),
        stop: px,
      });
      if (s2) results.synced.push(`${sym}: ${s2}`);
    } catch (e) {
      results.failed.push(`${sym}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 250));
  }
  if (results.done.length || results.failed.length) {
    lockNotify(
      `🛑 وقف الخسارة (${d}% من السعر الحالي)\n` +
      (results.done.length ? `✅ طُبّق: ${results.done.join(' · ')}\n` : '') +
      (results.failed.length ? `❌ فشل: ${results.failed.join(' · ')}\n` : '') +
      (results.armed?.length ? `👁 يتابعها البوت: ${results.armed.join(' · ')}\n` : '') +
      (results.skipped.length ? `⏭ تُخطّيت: ${results.skipped.length}` : '')
    );
  }
  return results;
}

// (7) تعديل التريلنج للصفقات الرابحة
// ملاحظة: بايننس يقبل callbackRate بين 0.1% و5% فقط — ما فوقها يُطبَّق على كورنكس فقط
const BINANCE_TRAIL_MAX = 5;
async function applyTrailing(acc, syms, pct) {
  const results = { done: [], skipped: [], failed: [], cornixOnly: [], synced: [], armed: [] };
  const positions = (acc.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
  const rate = parseFloat(pct);
  if (!isFinite(rate) || rate <= 0) return results;
  const overMax = rate > BINANCE_TRAIL_MAX;
  for (const pos of positions) {
    const sym = pos.symbol;
    if (syms && syms.length && !syms.includes(sym)) continue;
    const pnl = parseFloat(pos.unRealizedProfit) || 0;
    if (pnl <= 0) { results.skipped.push(`${sym}: خاسرة`); continue; }
    const tpl = (STATE.settings.lockCxTrailTpl || 'Trailing stop: {pct}%').replace('{pct}', String(rate));
    // تريلنج يتابعه البوت: يبدأ من السعر الحالي ويلاحق القمة،
    // وعند الارتداد بالنسبة يردّ على الإشارة بأمر إغلاق.
    // هذا يتجاوز حدّ بايننس (٥٪) ويعمل بأي نسبة.
    const isLong = parseFloat(pos.positionAmt) > 0;
    const mk = parseFloat(pos.markPrice) || livePrices[sym] || 0;
    const ent = parseFloat(pos.entryPrice) || mk;
    if (mk) {
      const av3 = armVStop(sym, { kind: 'trail', side: isLong ? 'LONG' : 'SHORT', pct: rate,
                      price: mk, entry: ent, reason: `تريلنج ${rate}% من ${fmtSignalPrice(mk)}` });
      if (av3?.rejected) results.skipped.push(`${sym}: ${av3.why}`);
      else results.armed.push(`${sym} @ ${rate}% من ${fmtSignalPrice(mk)}`);
    }
    if (overMax) {
      // فوق حدّ بايننس — التتبّع الافتراضي وحده يكفي
      continue;
    }
    const qty = roundQty(Math.abs(parseFloat(pos.positionAmt)), sym);
    try {
      await ensureLotSize(sym);
      await cancelOurStops(acc, sym);
      const tmode = await getPositionMode(acc);
      const tp = {
        symbol: sym, side: isLong ? 'SELL' : 'BUY', type: 'TRAILING_STOP_MARKET',
        quantity: qty, callbackRate: rate,
      };
      if (tmode === 'hedge') tp.positionSide = isLong ? 'LONG' : 'SHORT';
      else { tp.positionSide = 'BOTH'; tp.reduceOnly = 'true'; }
      const tr = await bFetch(acc.apiKey, acc.apiSecret, 'POST', '/fapi/v1/order', tp);
      rememberOurStop(sym, tr?.orderId);
      results.done.push(`${sym} @ ${rate}%`);
      addCopyLog('success', `📉 تريلنج: ${sym} @ ${rate}%`);
      const s3 = await cornixSync(sym, { cmd: tpl, trailPct: rate });
      if (s3) results.synced.push(`${sym}: ${s3}`);
    } catch (e) {
      results.failed.push(`${sym}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 250));
  }
  if (results.done.length || results.failed.length || results.cornixOnly.length) {
    lockNotify(
      `📉 تعديل التريلنج → ${rate}%\n` +
      (results.done.length ? `✅ على بايننس: ${results.done.join(' · ')}\n` : '') +
      (results.cornixOnly.length ? `📨 كورنكس فقط (>${BINANCE_TRAIL_MAX}% غير مدعوم ببايننس): ${results.cornixOnly.join(' · ')}\n` : '') +
      (results.failed.length ? `❌ فشل: ${results.failed.join(' · ')}\n` : '') +
      (results.armed?.length ? `👁 يتابعها البوت: ${results.armed.join(' · ')}\n` : '') +
      (results.skipped.length ? `⏭ تُخطّيت: ${results.skipped.length}` : '')
    );
  }
  return results;
}

// حالة الاتجاه العام (BTC): نفصل "قرب الانعكاس" عن "الانعكاس الفعلي"
// كي تختار كل ميزة أيّهما يشغّلها.
function trendReversalStatus() {
  const nearPct = parseFloat(STATE.settings.lockBEnearPct) || 1;
  const btc = STATE.superTrend?.btcPrice || STATE.ema200?.btcPrice || livePrices['BTCUSDT'] || 0;
  if (!btc) return { near: false, flipped: false, trigger: false, reasons: [] };
  const nearReasons = [], flipReasons = [];

  const stVal = STATE.superTrend?.value;
  if (stVal) {
    const dist = Math.abs((btc - stVal) / btc) * 100;
    if (dist <= nearPct) nearReasons.push(`على بُعد ${dist.toFixed(2)}% من خط السوبر`);
  }
  const emaVal = STATE.ema200?.value;
  if (emaVal) {
    const dist = Math.abs((btc - emaVal) / btc) * 100;
    if (dist <= nearPct) nearReasons.push(`على بُعد ${dist.toFixed(2)}% من EMA200`);
  }
  // انعكاس فعلي: تغيّر اتجاه السوبر العام عن آخر قراءة
  const prevDir = STATE._lastGlobalSTdir;
  const curDir = STATE.superTrend?.direction;
  if (curDir && prevDir && curDir !== prevDir) {
    flipReasons.push(`السوبر العام انقلب إلى ${curDir === 'up' ? 'صاعد' : 'نازل'}`);
  }
  if (curDir) STATE._lastGlobalSTdir = curDir;

  const near = nearReasons.length > 0, flipped = flipReasons.length > 0;
  return {
    near, flipped, trigger: near || flipped,
    nearReasons, flipReasons,
    reasons: [...flipReasons, ...nearReasons],
  };
}

// هل يشغّل هذا الحدث ميزةً مضبوطة على 'near' أو 'flip' أو 'both'؟
function trigMatches(trig, st) {
  if (trig === 'flip') return st.flipped;
  if (trig === 'both') return st.near || st.flipped;
  return st.near || st.flipped;   // 'near' الافتراضي: القرب أو الانعكاس
}

// المراقب الدوري لنظام القفل
let lockBusy = false;
let bootBaselineDone = false;   // أول دورة بعد الإقلاع تُسجّل الصفقات القائمة فقط
async function monitorLock() {
  if (!STATE.settings.lockOn || lockBusy) return;
  lockBusy = true;
  try {
    const L = lockWindow();
    if (!L.enabledAt) { L.enabledAt = Date.now(); lockSave(); }
    const master = STATE.copyAccounts.find(a => a.isMaster);
    if (!master?.apiKey) return;
    let positions = (master.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);

    // ── حماية بعد الإقلاع: أي صفقة قائمة في أول دورة تُسجَّل كخط أساس ──
    // (تحمي من التقليم/الإغلاق الخاطئ لو ضاع ملف الحالة أو أُعيد تشغيل السيرفر)
    if (!bootBaselineDone) {
      bootBaselineDone = true;
      let seeded = 0;
      for (const p of positions) {
        if (!L.manualSyms[p.symbol]) {
          // baseline يحمي من التقليم والإغلاق فقط — الستوب يُطبَّق لأنه حماية لا خطر
          L.manualSyms[p.symbol] = { ts: Date.now(), margin: 0, baseline: true };
          seeded++;
        }
      }
      if (seeded) {
        lockSave();
        addCopyLog('info', `🔒 نظام القفل: ${seeded} صفقة قائمة سُجّلت كخط أساس (لا تُقلَّم ولا تُغلق)`);
      }
      try { await buildLockDiag(master, positions); } catch (e) {}
      broadcast({ type: 'lockState', data: lockPublic() });
      return; // لا نتصرّف في أول دورة — نكتفي بالتسجيل
    }

    // ── (3) القفل نشط: أغلق أي صفقة يدوية فُتحت بعد القفل ──
    if (lockIsLocked()) {
      for (const pos of positions) {
        const sym = pos.symbol;
        if (!isManualPosition(sym, pos)) continue;
        const known = L.manualSyms[sym];
        // لا نلمس صفقات خط الأساس (كانت مفتوحة قبل التفعيل/الإقلاع)
        if (known?.baseline) continue;
        // ولا صفقة فُتحت قبل لحظة بدء القفل
        const lockStartedAt = L.lockedUntil - (parseFloat(STATE.settings.lockDailyHours) || 24) * HOUR_MS;
        if (known && known.ts < lockStartedAt) continue;
        if (!known) { L.manualSyms[sym] = { ts: Date.now(), margin: 0 }; lockSave(); }
        try {
          await closeFollower(master, sym, parseFloat(pos.positionAmt));
          lockNotify(`⛔ صفقة يدوية أُغلقت فوراً — القفل نشط\n#${sym.replace('USDT', '/USDT')}\n⏳ يفتح بعد ${Math.ceil((L.lockedUntil - Date.now()) / 60000)} دقيقة`);
        } catch (e) {
          addCopyLog('fail', `❌ قفل ${sym}: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 300));
      }
      try { master.livePositions = await getPositions(master); } catch (e) {}
      positions = (master.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
    }

    // ── حارس الوقف: كورنكس يعيد فرض أوامره وقد يلغي وقفنا ──
    // نتحقّق أن كل وقف وضعناه ما زال قائماً، ونعيده إن اختفى والصفقة مفتوحة.
    // بلا هذا قد تبقى الصفقة بلا حماية دون أن يشعر أحد.
    if (L.ourStops && Object.keys(L.ourStops).length) {
      for (const pos of positions) {
        const sym = pos.symbol;
        const ids = (L.ourStops[sym] || []).map(String);
        if (!ids.length) continue;
        try {
          const orders = await bFetch(master.apiKey, master.apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym });
          if (!Array.isArray(orders)) continue;
          const stillThere = orders.some(o => ids.includes(String(o.orderId)));
          if (stillThere) continue;
          // اختفى وقفنا والصفقة ما زالت مفتوحة — أعده بنفس السعر المسجّل
          const px = L.stopPx?.[sym];
          if (!px) { delete L.ourStops[sym]; lockSave(); continue; }
          const re = await placeStop(master, sym, pos, px, 'إعادة وقف');
          lockNotify(
            `♻️ أُعيد وضع الوقف — #${sym.replace('USDT', '/USDT')}\n` +
            `اختفى أمر الوقف الذي وضعه النظام (غالباً ألغاه كورنكس) فأُعيد عند ${re}`
          );
        } catch (e) {
          addCopyLog('fail', `❌ حارس الوقف ${sym}: ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 250));
      }
    }

    // ── (2) تقليم الزيادة فوق الحد اليومي للصفقات اليدوية ──
    if (STATE.settings.lockDailyOn && !lockIsLocked()) {
      const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
      if (cap > 0) {
        for (const pos of positions) {
          const sym = pos.symbol;
          if (!isManualPosition(sym, pos)) continue;
          const amt = Math.abs(parseFloat(pos.positionAmt));
          const mark = parseFloat(pos.markPrice) || livePrices[sym] || 0;
          const lev = parseFloat(pos.leverage) || 1;
          if (!mark || !amt) continue;
          const margin = (amt * mark) / lev;
          // حماية: تجاهل الصفقات المفتوحة قبل تفعيل النظام
          if (!L.manualSyms[sym]) {
            const openedBefore = (STATE.lockState.enabledAt || 0) > 0 && !L._seededBaseline;
            L.manualSyms[sym] = { ts: Date.now(), margin, baseline: openedBefore };
            lockSave();
            if (openedBefore) continue;
          }
          if (L.manualSyms[sym].baseline) continue;
          if (margin <= cap * 1.02) continue;   // هامش تسامح 2% لتفادي التقليم المتكرر
          const excessMargin = margin - cap;
          const closeQty = roundQty((excessMargin * lev) / mark, sym);
          if (closeQty <= 0) continue;
          try {
            await reducePosition(master, sym, parseFloat(pos.positionAmt), closeQty);
            lockNotify(
              `✂️ تقليم الزيادة — #${sym.replace('USDT', '/USDT')}\n` +
              `الهامش كان $${margin.toFixed(2)} → أصبح $${cap.toFixed(2)}\n` +
              `أُخرج: $${excessMargin.toFixed(2)}`
            );
          } catch (e) {
            addCopyLog('fail', `❌ تقليم ${sym}: ${e.message}`);
          }
          await new Promise(r => setTimeout(r, 300));
        }
      }
    }

    // ── (1) ستوب تلقائي لأي صفقة يدوية بلا وقف ──
    if (STATE.settings.lockAutoSLon) {
      const slPct = parseFloat(STATE.settings.lockAutoSLpct) || 2;
      for (const pos of positions) {
        const sym = pos.symbol;
        if (!isManualPosition(sym, pos)) continue;
        if (L.manualSyms[sym]?.slPlaced) continue;
        try {
          const orders = await bFetch(master.apiKey, master.apiSecret, 'GET', '/fapi/v1/openOrders', { symbol: sym });
          const hasStop = Array.isArray(orders) && orders.some(o => o.type === 'STOP_MARKET' || o.type === 'STOP' || o.type === 'TRAILING_STOP_MARKET');
          if (hasStop) {
            L.manualSyms[sym] = { ...(L.manualSyms[sym] || { ts: Date.now() }), slPlaced: true };
            lockSave();
            continue;
          }
          const isLong = parseFloat(pos.positionAmt) > 0;
          const entry = parseFloat(pos.entryPrice) || parseFloat(pos.markPrice) || 0;
          if (!entry) continue;
          const slPrice = isLong ? entry * (1 - slPct / 100) : entry * (1 + slPct / 100);
          const px = await placeStop(master, sym, pos, slPrice, 'ستوب يدوي تلقائي');
          L.manualSyms[sym] = { ...(L.manualSyms[sym] || { ts: Date.now() }), slPlaced: true };
          lockSave();
          const lev = parseFloat(pos.leverage) || 1;
          lockNotify(
            `🛡️ ستوب تلقائي — #${sym.replace('USDT', '/USDT')}\n` +
            `${isLong ? '🟢 LONG' : '🔴 SHORT'} · دخول ${entry}\n` +
            `وقف @ ${px} (${slPct}% من السعر ≈ ${(slPct * lev).toFixed(0)}% من الهامش)`
          );
        } catch (e) {
          addCopyLog('fail', `❌ ستوب تلقائي ${sym}: ${e.message}`);
          // نحفظ آخر خطأ ليظهر في لوحة التشخيص بدل الصمت
          L.manualSyms[sym] = { ...(L.manualSyms[sym] || { ts: Date.now() }), lastErr: e.message, lastErrAt: Date.now() };
          lockSave();
        }
        await new Promise(r => setTimeout(r, 300));
      }
    }

    await checkDailyLossLimit();

    // ── حارس النسخة: أي إعداد انحرف عن لحظة القفل يُستعاد ──
    const mActive = masterLockActive();   // يفحص الانتهاء في كل دورة
    if (STATE.settings.lockAllSettings && mActive) {
      const drift = restoreSnapshot();
      if (drift) {
        lockNotify(
          `↩️ استُعيدت إعدادات تغيّرت أثناء القفل\n` +
          drift.slice(0, 6).map(d => `${d.k}: ${JSON.stringify(d.from)} ← ${JSON.stringify(d.to)}`).join('\n') +
          (drift.length > 6 ? `\n(و${drift.length - 6} أخرى)` : '')
        );
      }
    }

    // ── التنفيذ التلقائي عند شرط الاتجاه العام ──
    // كل ميزة تختار محفّزها: قرب الانعكاس أو الانعكاس الفعلي أو كليهما.
    // ما كان له وقف افتراضي نشط لا يُعاد تسجيله.
    {
      const S = STATE.settings;
      if (S.lockAutoBEon || S.lockAutoTrailOn || S.lockAutoLossOn) {
        const status = trendReversalStatus();
        if (status.trigger) {
          const vs = STATE.lockState.vStops || {};
          const why = status.reasons.join(' · ');
          // الرابحة = ربح موجب. أما البريك إيفن فيشترط حركة كافية إضافةً لذلك،
          // وإلا أقفل صفقةً بالكاد دخلت الربح
          const minPnl = parseFloat(S.lockBEminPnl) || 0;
          const movedEnough = (p) => {
            const m = parseFloat(p.markPrice) || livePrices[p.symbol] || 0;
            const lv = parseFloat(p.leverage) || 1;
            const mg = m ? (Math.abs(parseFloat(p.positionAmt)) * m) / lv : 0;
            if (!mg) return false;
            return ((parseFloat(p.unRealizedProfit) || 0) / mg) * 100 >= minPnl;
          };
          const winners = positions.filter(p => (parseFloat(p.unRealizedProfit) || 0) > 0).map(p => p.symbol);
          const beReady = positions.filter(p => (parseFloat(p.unRealizedProfit) || 0) > 0 && movedEnough(p)).map(p => p.symbol);
          const losers = positions.filter(p => (parseFloat(p.unRealizedProfit) || 0) < 0).map(p => p.symbol);

          // بريك إيفن للرابحات
          if (S.lockAutoBEon && trigMatches(S.lockBEtrig, status)) {
            const pend = beReady.filter(s => !L.beDone[s] && !vs[s]);
            if (pend.length) await applyBreakEven(master, pend, S.lockBEoffsetPct, `تلقائي — ${why}`);
          }
          // تريلنج للرابحات
          if (S.lockAutoTrailOn && trigMatches(S.lockAutoTrailTrig, status)) {
            const pend = winners.filter(s => !vs[s]);
            if (pend.length) {
              lockNotify(`📉 تريلنج تلقائي ${S.lockAutoTrailPct}%\nالسبب: ${why}\nالعملات: ${pend.map(x => x.replace('USDT', '')).join(' · ')}`);
              await applyTrailing(master, pend, S.lockAutoTrailPct);
            }
          }
          // وقف خسارة للخاسرات
          if (S.lockAutoLossOn && trigMatches(S.lockAutoLossTrig, status)) {
            const pend = losers.filter(s => !vs[s]);
            if (pend.length) {
              lockNotify(`🛑 وقف تلقائي ${S.lockAutoLossPct}%\nالسبب: ${why}\nالعملات: ${pend.map(x => x.replace('USDT', '')).join(' · ')}`);
              await applyLossStop(master, pend, S.lockAutoLossPct);
            }
          }
        }
      }
    }

    // تنظيف: نُعلّم المغلقة بوقت الإغلاق ولا نحذفها فوراً —
    // لأن كاشف الإغلاق (syncCopy/المراقب) يحتاجها ليسجّل الخسارة على الحد اليومي
    const openSyms = new Set(positions.map(p => p.symbol));
    const now2 = Date.now();
    let changed = false;
    for (const s of Object.keys(L.manualSyms)) {
      const rec = L.manualSyms[s];
      if (openSyms.has(s)) {
        if (rec.closedAt) { delete rec.closedAt; changed = true; }
      } else if (!rec.closedAt) {
        rec.closedAt = now2; changed = true;
      } else if (now2 - rec.closedAt > 600000) {
        delete L.manualSyms[s]; changed = true;
      }
    }
    for (const s of Object.keys(L.beDone)) if (!openSyms.has(s)) { delete L.beDone[s]; changed = true; }
    if (L.ourStops) for (const s of Object.keys(L.ourStops)) if (!openSyms.has(s)) { delete L.ourStops[s]; changed = true; }
    if (L.stopPx) for (const s of Object.keys(L.stopPx)) if (!openSyms.has(s)) { delete L.stopPx[s]; changed = true; }
    if (L.vStops) for (const s of Object.keys(L.vStops)) if (!openSyms.has(s)) { delete L.vStops[s]; changed = true; }
    if (changed) lockSave();

    // تشخيص كل دورة ثانية (كل ٣٠ ثانية) لتخفيف الضغط على الـ API
    if (!L.diagAt || Date.now() - L.diagAt > 30000) {
      await buildLockDiag(master, positions);
    }
    broadcast({ type: 'lockState', data: lockPublic() });
  } catch (e) {
    addCopyLog('fail', `❌ نظام القفل: ${e.message}`);
  } finally {
    lockBusy = false;
  }
}

async function scanSym(sym, candles) {
  try {
    const cls = candles || candleCache[sym];
    if (!cls || cls.length < RSI_P + 2) return;
    livePrices[sym] = cls[cls.length - 1];
    const st = settingsFor(sym);
    const cu = computeInd(cls, st.mode, st.maPeriod);
    const pv = computeInd(cls.slice(0, -1), st.mode, st.maPeriod);
    const id = computeIndSeries(cls, st.mode, st.maPeriod);
    const sig = detectSignal(pv, cu, cls, id, st.enableDiv, st);
    const conf = detectConf(pv, cu, cls, id, st.enableDiv);
    const trail = detectTrail(sym, cu, cls, id, st.enableDiv, st);
    const zone = cu >= 70 ? 'ob' : cu <= 30 ? 'os' : 'neutral';
    const old = STATE.symbolData[sym] || {};
    const oldZone = old.zone || 'neutral';
    if (oldZone !== 'neutral' && zone === 'neutral') {
      Object.keys(STATE.cooldowns).forEach(k => { if (k.startsWith(sym + '_')) delete STATE.cooldowns[k]; });
      delete STATE.sentSigs[sym]; saveSentSigsDebounced();
    }
    const fSig = trail || sig;
    STATE.symbolData[sym] = { rsi: cu, prevRsi: pv, signal: fSig, conf, zone, error: false, trailActive: !!trail };
    if (fSig && (!old.signal || old.signal.type !== fSig.type)) await triggerAlert(sym, fSig, cu, st);
    if (conf && (!old.conf || old.conf.type !== conf.type)) await triggerAlert(sym, conf, cu, st);

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
    const st = settingsFor(sym);
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
  // فحص الفريمات الإضافية
  const extras = STATE.settings.extraIntervals || [];
  for (const tf of extras) {
    if (tf === STATE.settings.interval) continue;
    await scanAllForInterval(tf);
  }
}

async function scanAllForInterval(tf) {
  for (let i = 0; i < STATE.symbols.length; i += BATCH) {
    const batch = STATE.symbols.slice(i, i + BATCH);
    await Promise.all(batch.map(async sym => {
      try {
        const d = await fetchBinance(`/fapi/v1/klines?symbol=${sym}&interval=${tf}&limit=200`);
        if (!Array.isArray(d) || d.length < RSI_P + 2) return;
        const cls = d.map(k => parseFloat(k[4]));
        livePrices[sym] = cls[cls.length - 1];
        const st = settingsFor(sym);
        const cu = computeInd(cls, st.mode, st.maPeriod);
        const pv = computeInd(cls.slice(0, -1), st.mode, st.maPeriod);
        const id = computeIndSeries(cls, st.mode, st.maPeriod);
        const sig = detectSignal(pv, cu, cls, id, st.enableDiv, st);
        const conf = detectConf(pv, cu, cls, id, st.enableDiv);
        const trail = detectTrail(sym + '_' + tf, cu, cls, id, st.enableDiv, st);
        const fSig = trail || sig;
        const oldKey = sym + '_' + tf;
        const old = extraSymData[oldKey] || {};
        const zone = cu >= 70 ? 'ob' : cu <= 30 ? 'os' : 'neutral';
        const oldZone = old.zone || 'neutral';
        if (oldZone !== 'neutral' && zone === 'neutral') {
          Object.keys(STATE.cooldowns).forEach(k => { if (k.startsWith(oldKey + '_')) delete STATE.cooldowns[k]; });
          delete STATE.sentSigs[oldKey]; saveSentSigsDebounced();
        }
        extraSymData[oldKey] = { zone };
        const stOverride = { ...st, interval: tf };
        if (fSig && (!old.lastSigType || old.lastSigType !== (fSig.type + fSig.side))) {
          extraSymData[oldKey].lastSigType = fSig.type + fSig.side;
          await triggerAlert(sym, fSig, cu, stOverride, tf);
        }
        if (conf && (!old.lastConfType || old.lastConfType !== conf.type)) {
          extraSymData[oldKey].lastConfType = conf.type;
          await triggerAlert(sym, conf, cu, stOverride, tf);
        }
      } catch (e) {}
    }));
    if (i + BATCH < STATE.symbols.length) await new Promise(r => setTimeout(r, BDEL));
  }
}

const extraSymData = {};

// ══════════════════════════════════════════════
//  WEBSOCKET — Binance Live
// ══════════════════════════════════════════════
let binanceSockets = {}, binanceReconns = {}; // interval -> WebSocket / reconnect timeout

// الفريم الزمني الفعّال لعملة معينة (إعدادها الخاص إن وُجد، وإلا الفريم العام)
function symInterval(sym) {
  return settingsFor(sym).interval || STATE.settings.interval;
}

// يجمع العملات حسب فريمها الفعّال، ويفتح اتصال WS مجمّع لكل فريم على حدة
function startBinanceWS() {
  if (!STATE.symbols.length) return;
  stopBinanceWS();
  const groups = {};
  for (const sym of STATE.symbols) {
    const iv = symInterval(sym);
    (groups[iv] = groups[iv] || []).push(sym);
  }
  for (const [interval, syms] of Object.entries(groups)) startBinanceWSGroup(interval, syms);
}

function startBinanceWSGroup(interval, syms) {
  const streams = syms.map(s => `${s.toLowerCase()}@kline_${interval}`).join('/');
  const binanceWs = new WebSocket(`${WS_BASE}?streams=${streams}`);
  binanceSockets[interval] = binanceWs;
  binanceWs.on('open', () => {
    console.log(`✅ Binance WS connected (${interval}, ${syms.length} عملة)`);
    broadcast({ type: 'wsStatus', data: 'connected' });
  });
  binanceWs.on('message', async (data) => {
    try {
      const m = JSON.parse(data);
      if (!m.data?.k) return;
      const k = m.data.k, sym = k.s, close = parseFloat(k.c);
      livePrices[sym] = close;
      // الوقف الافتراضي يُفحص على السعر اللحظي لا على دورة كل ١٥ ثانية
      checkVStops(sym, close);
      // الحدّ اليومي (محقّق + عائم) — مخنوق لثانيتين فالحساب يمرّ على كل المراكز
      if (Date.now() - lastDailyCheck > 2000) {
        lastDailyCheck = Date.now();
        checkDailyLossLimit().catch(() => {});
      }
      // إرسال ما بلغ نسبة الانعكاس — مخنوق ٣ ثوانٍ
      if (STATE.settings.queueRevAutoOn && Date.now() - lastRevCheck > 3000) {
        lastRevCheck = Date.now();
        autoSendOnReversal().catch(() => {});
      }
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
          const st = settingsFor(sym);
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

            // مسح cooldown و sentSigs عند الخروج من المنطقة
            if (oldZone !== 'neutral' && newZone === 'neutral') {
              Object.keys(STATE.cooldowns).forEach(ck => { if (ck.startsWith(sym + '_')) delete STATE.cooldowns[ck]; });
              delete STATE.sentSigs[sym]; saveSentSigsDebounced();
            }

            // كشف الإشارات على الشمعة الحية
            if (pv !== null) {
              const id = computeIndSeries(tmp, st.mode, st.maPeriod);
              const sig = detectSignal(pv, cu, tmp, id, st.enableDiv, st);
              const conf = detectConf(pv, cu, tmp, id, st.enableDiv);
              const trail = detectTrail(sym, cu, tmp, id, st.enableDiv, st);
              const fSig = trail || sig;
              STATE.symbolData[sym].trailActive = !!trail;
              if (fSig && (!old.signal || old.signal.type !== fSig.type)) {
                STATE.symbolData[sym].signal = fSig;
                await triggerAlert(sym, fSig, cu, st);
              }
              if (conf && (!old.conf || old.conf.type !== conf.type)) {
                STATE.symbolData[sym].conf = conf;
                await triggerAlert(sym, conf, cu, st);
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
  binanceWs.on('close', () => { broadcast({ type: 'wsStatus', data: 'disconnected' }); scheduleReconn(interval); });
  binanceWs.on('error', () => {});
}

function stopBinanceWS() {
  for (const iv of Object.keys(binanceReconns)) { clearTimeout(binanceReconns[iv]); delete binanceReconns[iv]; }
  for (const iv of Object.keys(binanceSockets)) {
    try { binanceSockets[iv].removeAllListeners(); binanceSockets[iv].terminate(); } catch (e) {}
    delete binanceSockets[iv];
  }
}

function scheduleReconn(interval) {
  if (binanceReconns[interval]) return;
  binanceReconns[interval] = setTimeout(() => {
    delete binanceReconns[interval];
    const syms = STATE.symbols.filter(s => symInterval(s) === interval);
    if (syms.length) startBinanceWSGroup(interval, syms);
  }, 3000);
}

// تُستخدم عند تغييرات تؤثر على بيانات الشموع (الفريم الزمني العام أو لعملة معينة):
// تفريغ الكاش، إعادة تجميع اتصالات WS حسب الفريم الفعّال لكل عملة، وإعادة فحص شامل
function rescanWithFreshCandles() {
  Object.keys(candleCache).forEach(k => delete candleCache[k]);
  startBinanceWS();
  broadcast({ type: 'scanning', data: true });
  scanAll().then(() => broadcast({ type: 'scanning', data: false }));
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
let sentSigsTimer = null;
function saveSentSigsDebounced() {
  if (sentSigsTimer) clearTimeout(sentSigsTimer);
  sentSigsTimer = setTimeout(() => db.saveSentSigs(STATE.sentSigs), 2000);
}

function addCopyLog(type, text) {
  STATE.copyLog = [{ type, text, time: nowStr() }, ...STATE.copyLog].slice(0, 300);
  db.saveCopyLog(STATE.copyLog);
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
        if (STATE.settings.stSLon && STATE._stslTracked) STATE._stslTracked[sym] = true;
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
      const posAmt = Math.abs(parseFloat(prevPos.positionAmt));
      const pnlUsd = parseFloat((posAmt * (exitPrice - entryPrice) * (isLongPos ? 1 : -1)).toFixed(4));
      const t = STATE.openTrades.find(x => x.symbol === sym);
      const closed = t
        ? { ...t, exitPrice, exitTime: nowStr(), closeTs: Date.now(), pct, pnl: pnlUsd, result: pct >= 0 ? 'win' : 'loss' }
        : { id: Date.now() + Math.random(), symbol: sym, side, entryPrice, exitPrice,
            pct, pnl: pnlUsd, result: pct >= 0 ? 'win' : 'loss',
            openTime: '', exitTime: nowStr(), openTs: 0, closeTs: Date.now(),
            sl: '', tp1: '', leverage: String(prevPos.leverage || 20),
            margin: prevPos.marginType || 'Cross', label: '🪞 Binance', executed: true };

      STATE.closedTrades = [closed, ...STATE.closedTrades].slice(0, 500);
      STATE.openTrades = STATE.openTrades.filter(x => x.symbol !== sym);
      delete STATE.sentSigs[sym]; saveSentSigsDebounced();
      db.saveClosedTrade(closed);
      db.saveOpenTrades(STATE.openTrades);

      // تحديث إحصائيات الماستر
      if (!master.stats) master.stats = { opens:0, closes:0, wins:0, losses:0, tot:0 };
      master.stats.closes++;
      if (pct >= 0) master.stats.wins++; else master.stats.losses++;
      master.stats.tot = parseFloat(((master.stats.tot || 0) + pct).toFixed(2));
      if (!master.closedTrades) master.closedTrades = [];
      master.closedTrades = [{ symbol: sym, side, entryPrice, exitPrice, pnl: pnlUsd, pct, closeTs: Date.now(), closeTime: nowStr() }, ...master.closedTrades].slice(0, 200);
      if (STATE.lockState.manualSyms[sym] && !STATE.lockState.manualSyms[sym].baseline) lockRecordClose(sym, pnlUsd);

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

  if (STATE.settings.stSLon) await monitorSTSL();

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
    symbolSettings: STATE.symbolSettings,
    copyOn: STATE.copyOn,
    copyLog: STATE.copyLog.slice(0, 50),
    accounts: getSafeAccounts(),
    dcaOrders: STATE.dcaOrders,
    pendingOrders: STATE.pendingOrders,
    waitQueue: queueWithReversals(),
    sentSigs: STATE.sentSigs,
    sysStatus: STATE.sysStatus,
    ema200: STATE.ema200,
    superTrend: STATE.superTrend,
    respectData: STATE.respectData,
    perSymST: STATE.perSymST,
    lockState: lockPublic(),
    lastUpdate: nowStr(),
    btBusy: btState.busy,
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
    try { await handleClientMsg(JSON.parse(raw), ws); } catch (e) { reportError('handleClientMsg', e.message); }
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

async function handleClientMsg(msg, ws) {
  switch (msg.type) {
    case 'updateSettings': {
      const oldInterval = STATE.settings.interval;
      const oldUseSym = STATE.settings.useSymbolSettings;
      if (msg.data.sigFilters) {
        msg.data.sigFilters = { ...STATE.settings.sigFilters, ...msg.data.sigFilters };
      }
      if (msg.data.sigQueueFilters) {
        msg.data.sigQueueFilters = { ...STATE.settings.sigQueueFilters, ...msg.data.sigQueueFilters };
      }
      if (msg.data.stSLon === true && !STATE.settings.stSLon) {
        STATE._stslEnabledAt = Date.now();
        STATE._stslTracked = {};
      }
      // ── القفل العام: يحمي الإعدادات طوال مدّته ──
      // الوضع الشامل يمنع أي تعديل على أي إعداد؛ وإلا فالحماية على الحد اليومي وحده.
      if (masterLockActive()) {
        if (STATE.settings.lockAllSettings) {
          const changed = Object.keys(msg.data).filter(k => JSON.stringify(msg.data[k]) !== JSON.stringify(STATE.settings[k]));
          if (changed.length) {
            const until = STATE.lockState.masterUntil || 0;
            broadcast({ type: 'lockResult', data: { ok: false, error:
              `🔒 القفل الشامل مفعّل — كل الإعدادات محميّة${until ? ` (ينتهي ${fmtDateTime(until)})` : ' (بلا مدّة)'}` } });
            // أعد بث الإعدادات الحقيقية كي تتراجع الواجهة عمّا أظهرته
            broadcast({ type: 'settings', data: STATE.settings });
            lockNotify(
              `🔒 مُنع تعديل إعدادات\n` +
              `المحاولة: ${changed.slice(0, 6).map(k => `${k} → ${JSON.stringify(msg.data[k])}`).join('\n')}` +
              (changed.length > 6 ? `\n(و${changed.length - 6} أخرى)` : '') +
              `\n↩️ أُعيدت القيم كما كانت`
            );
            break;   // لا نطبّق أي تغيير
          }
        } else {
          const PROTECTED = ['lockDailyOn', 'lockDailyAmt', 'lockDailyHours', 'lockMaster', 'lockOn', 'lockAllSettings'];
          const blocked = PROTECTED.filter(k => msg.data[k] !== undefined && msg.data[k] !== STATE.settings[k]);
          for (const k of blocked) delete msg.data[k];
          if (blocked.length) {
            broadcast({ type: 'lockResult', data: { ok: false, error: '🔒 القفل العام مفعّل — إعدادات الحد اليومي محميّة' } });
          }
        }
      }
      // القفل العام لا يُسلَّح من هنا إطلاقاً.
      // الواجهة ترسل كل الإعدادات مع أي تعديل، فبعد انتهاء القفل كانت نسخة
      // المتصفح القديمة (lockMaster=true) تعيد تسليحه من تلقاء نفسه.
      // التسليح صار عبر رسالة مخصّصة (lockArm) لا تُرسَل إلا بضغطة صريحة.
      delete msg.data.lockMaster;

      // عند تفعيل نظام القفل نسجّل الصفقات القائمة كخط أساس فلا تُقلَّم ولا تُغلق
      if (msg.data.lockOn === true && !STATE.settings.lockOn) {
        STATE.lockState.enabledAt = Date.now();
        STATE.lockState.manualSyms = {};
        STATE.lockState.beDone = {};
        const master = STATE.copyAccounts.find(a => a.isMaster);
        for (const p of (master?.livePositions || [])) {
          if (Math.abs(parseFloat(p.positionAmt || 0)) > 0) {
            STATE.lockState.manualSyms[p.symbol] = { ts: Date.now(), margin: 0, baseline: true, slPlaced: true };
          }
        }
        STATE.lockState._seededBaseline = true;
        if (!STATE.lockState.windowStart) STATE.lockState.windowStart = Date.now();
        lockSave();
        lockNotify(`🔒 تم تفعيل نظام القفل\nالصفقات المفتوحة حالياً (${Object.keys(STATE.lockState.manualSyms).length}) لن تتأثر`);
      }
      // تعديل أي Chat ID يعيد تفعيل الإرسال له (لو كان معطوباً سابقاً)
      for (const k of ['cxChat', 'cxChatClose', 'cxChatSettings', 'cxChatBT', 'cxChatSTSim', 'lockTgChat']) {
        if (msg.data[k] !== undefined && msg.data[k] !== STATE.settings[k]) {
          deadChats.delete(String(STATE.settings[k]));
          deadChats.delete(String(msg.data[k]));
        }
      }
      Object.assign(STATE.settings, msg.data);
      db.saveSettings(STATE.settings);
      broadcast({ type: 'settings', data: STATE.settings });
      if (msg.data.ema200TF !== undefined) updateEMA200();
      if (msg.data.stTF !== undefined || msg.data.stPeriod !== undefined || msg.data.stMult !== undefined) updateSuperTrend();
      // إعادة تشغيل WS عند تغيير الفريم الزمني العام، أو تفعيل/تعطيل استخدام إعدادات العملات
      // (يؤثر على الفريم الفعّال لكل العملات ذات الإعدادات الخاصة)
      if ((msg.data.interval && msg.data.interval !== oldInterval) ||
          (msg.data.useSymbolSettings !== undefined && msg.data.useSymbolSettings !== oldUseSym)) {
        rescanWithFreshCandles();
      }
      break;
    }

    // إعدادات خاصة لعملة واحدة — تُدمج فوق الإعدادات العامة عند توليد إشارات هذه العملة فقط
    case 'setSymbolSettings': {
      if (masterLockActive() && STATE.settings.lockAllSettings) {
        broadcast({ type: 'lockResult', data: { ok: false, error: '🔒 القفل الشامل مفعّل — إعدادات العملات محميّة' } });
        break;
      }
      const { symbol, settings } = msg.data || {};
      if (!symbol || !settings || typeof settings !== 'object') break;
      const clean = {};
      for (const k of SYMBOL_OVERRIDE_FIELDS) if (settings[k] !== undefined) clean[k] = settings[k];
      if (clean.sigQueueFilters) clean.sigQueueFilters = { ...clean.sigQueueFilters };
      if (clean.sigFilters) clean.sigFilters = { ...clean.sigFilters };
      STATE.symbolSettings[symbol] = clean;
      db.saveSymbolSettings(STATE.symbolSettings);
      broadcast({ type: 'symbolSettings', data: STATE.symbolSettings });
      rescanWithFreshCandles();
      break;
    }

    // تطبيق إعدادات مخصصة لعدة عملات دفعة واحدة (ملف bySymbol من نتائج الباكتيست)
    case 'setSymbolSettingsBulk': {
      const { bySymbol } = msg.data || {};
      if (!bySymbol || typeof bySymbol !== 'object') break;
      for (const [symbol, entry] of Object.entries(bySymbol)) {
        const settings = entry && entry.settings;
        if (!settings || typeof settings !== 'object') continue;
        const clean = {};
        for (const k of SYMBOL_OVERRIDE_FIELDS) if (settings[k] !== undefined) clean[k] = settings[k];
        if (clean.sigQueueFilters) clean.sigQueueFilters = { ...clean.sigQueueFilters };
        if (clean.sigFilters) clean.sigFilters = { ...clean.sigFilters };
        STATE.symbolSettings[symbol] = clean;
      }
      db.saveSymbolSettings(STATE.symbolSettings);
      broadcast({ type: 'symbolSettings', data: STATE.symbolSettings });
      rescanWithFreshCandles();
      break;
    }

    case 'removeSymbolSettings': {
      const { symbol } = msg.data || {};
      if (symbol && STATE.symbolSettings[symbol]) {
        delete STATE.symbolSettings[symbol];
        db.saveSymbolSettings(STATE.symbolSettings);
        broadcast({ type: 'symbolSettings', data: STATE.symbolSettings });
        rescanWithFreshCandles();
      }
      break;
    }

    case 'scanNow': scanAll(); break;

    // ══ Backtest ══════════════════════════════════════════════
    case 'btInfo': {
      const stored = {}, storedSymbols = {};
      for (const tf of BT.ALLOWED_TF) {
        const syms = BT.listStoredSymbols(tf).filter(s => s !== 'BTCUSDT').sort();
        stored[tf] = syms.length; storedSymbols[tf] = syms;
      }
      broadcast({ type: 'btInfo', data: { symbolsScanned: STATE.symbols.length, stored, storedSymbols, busy: btState.busy } });
      break;
    }
    case 'btDownload': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      btState.busy = true; btState.cancel = false;
      const days = parseInt(msg.data?.days) || 90;
      const toMs = Date.now(), fromMs = toMs - days * 86400000;
      const baseSyms = (msg.data?.symbols?.length ? msg.data.symbols : STATE.symbols);
      const syms = baseSyms.includes('BTCUSDT') ? baseSyms : ['BTCUSDT', ...baseSyms];
      (async () => {
        try {
          await BT.downloadData(syms, BT.ALLOWED_TF, fromMs, toMs, p => {
            if (btState.cancel) throw new Error('أُلغي التنزيل');
            broadcast({ type: 'btProgress', data: { ...p, kind: 'download' } });
          });
          broadcast({ type: 'btDownloadDone', data: { ok: true } });
          tgSend('✅ انتهى تنزيل بيانات الباك تيست', STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btDownloadDone', data: { ok: false, error: e.message } });
          tgSend('❌ فشل تنزيل بيانات الباك تيست: ' + e.message, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btUpdate': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      btState.busy = true; btState.cancel = false;
      const baseSyms = (msg.data?.symbols?.length ? msg.data.symbols : STATE.symbols);
      const syms = baseSyms.includes('BTCUSDT') ? baseSyms : ['BTCUSDT', ...baseSyms];
      (async () => {
        try {
          await BT.updateData(syms, BT.ALLOWED_TF, p => {
            if (btState.cancel) throw new Error('أُلغي التحديث');
            broadcast({ type: 'btProgress', data: { ...p, kind: 'update' } });
          });
          broadcast({ type: 'btDownloadDone', data: { ok: true } });
          tgSend('✅ انتهى تحديث بيانات الباك تيست', STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btDownloadDone', data: { ok: false, error: e.message } });
          tgSend('❌ فشل تحديث بيانات الباك تيست: ' + e.message, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btRun': {
      try {
        const set = { ...STATE.settings, ...(msg.data?.settings || {}) };
        const tf = BT.ALLOWED_TF.includes(set.interval) ? set.interval : '1h';
        const syms = BT.listStoredSymbols(tf).filter(s => s !== 'BTCUSDT');
        if (!syms.length) { broadcast({ type: 'btResult', data: { error: 'لا توجد بيانات مخزّنة لهذا الفريم — نزّل أولاً' } }); break; }
        const ds = BT.loadDatasetByTf(syms, [tf])[tf];
        const ftf = BT.ALLOWED_TF.includes(set.ema200TF) ? set.ema200TF : (BT.ALLOWED_TF.includes(set.stTF) ? set.stTF : tf);
        const btc = BT.loadCandles('BTCUSDT', ftf) || BT.loadCandles('BTCUSDT', tf);
        const regime = btc ? BT.buildBtcRegime(btc, ftf, ftf, parseInt(set.stPeriod) || 10, parseFloat(set.stMult) || 3) : null;
        const res = BT.runBacktest(ds, set, regime);
        broadcast({ type: 'btResult', data: { metrics: res.metrics, trades: res.trades.length, tf, signalStats: res.signalStats } });
        const m = res.metrics;
        const txt = `🧪 نتيجة الباك تيست\nالفريم: ${tf}\nصفقات: ${res.trades.length}\nعائد: ${m.netReturnPct}%  |  نجاح: ${m.winRate}%\nPF: ${m.profitFactor}  |  أقصى تراجع: ${m.maxDrawdownPct}%`;
        const buf = Buffer.from(JSON.stringify({ tf, settings: set, ...res }, null, 2));
        const fname = `bt_run_${tf}_${Date.now()}.json`;
        tgSend(txt, STATE.settings.cxChatBT);
        tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
      } catch (e) {
        broadcast({ type: 'btResult', data: { error: e.message } });
        const errTxt = '❌ فشل الباك تيست: ' + e.message;
        tgSend(errTxt, STATE.settings.cxChatBT);
      }
      break;
    }
    // بيانات شارت التحقق (شموع + المؤشر + كل الإشارات + الصفقات) لرمز واحد بإعدادات معيّنة
    case 'btSymChartData': {
      try {
        const sym = msg.data?.symbol;
        if (!sym) throw new Error('symbol مطلوب');
        const set = { ...STATE.settings, ...(msg.data?.settings || {}) };
        const tf = BT.ALLOWED_TF.includes(set.interval) ? set.interval : '1h';
        const ftf = BT.ALLOWED_TF.includes(set.ema200TF) ? set.ema200TF : (BT.ALLOWED_TF.includes(set.stTF) ? set.stTF : tf);
        const btc = BT.loadCandles('BTCUSDT', ftf) || BT.loadCandles('BTCUSDT', tf);
        const regime = btc ? BT.buildBtcRegime(btc, ftf, ftf, parseInt(set.stPeriod) || 10, parseFloat(set.stMult) || 3) : null;
        const data = BT.getSymbolChartData(sym, set, regime);
        if (!data) throw new Error(`لا توجد بيانات مخزّنة لـ ${sym} على فريم ${tf} — نزّل البيانات أولاً`);
        broadcast({ type: 'btSymChartDataDone', data: { symbol: sym, ...data } });
      } catch (e) { broadcast({ type: 'btSymChartDataDone', data: { error: e.message } }); }
      break;
    }
    case 'btOptimize': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      btState.busy = true; btState.cancel = false;
      const budgetMs = (parseFloat(msg.data?.budgetHours) || 24) * 3600000;
      const minTrades = parseInt(msg.data?.minTrades) || 30;
      (async () => {
        try {
          const tfs = BT.ALLOWED_TF;
          const symSet = new Set();
          for (const tf of tfs) BT.listStoredSymbols(tf).forEach(s => symSet.add(s));
          const symbols = [...symSet].filter(s => s !== 'BTCUSDT');
          if (!symbols.length) { broadcast({ type: 'btOptDone', data: { error: 'لا توجد بيانات — نزّل أولاً' } }); btState.busy = false; return; }
          const datasetByTf = BT.loadDatasetByTf(symbols, tfs);
          const btcByTf = BT.loadBtcByTf(tfs);
          const res = await BT.optimize(datasetByTf, btcByTf, {
            budgetMs, minTrades, capFrac: 0.01,
            stPeriod: parseInt(STATE.settings.stPeriod) || 10, stMult: parseFloat(STATE.settings.stMult) || 3,
            shouldStop: () => btState.cancel,
            onProgress: p => broadcast({ type: 'btProgress', data: { ...p, kind: 'optimize' } }),
          });
          broadcast({ type: 'btOptDone', data: res });
          let txt;
          if (res.best) {
            const m = res.best.metrics, c = res.best.combo;
            txt = `🏁 أفضل إعداد (Backtest)\nالفريم: ${c.interval} | ${c.mode}${c.mode !== 'RSI' ? '(' + c.maPeriod + ')' : ''}\nالإشارات: ${c.sigPreset} | فلتر: ${c.regime}\nTP1:${c.cxTP1}%  SL:${c.sl[1]}%  TP2:${c.tp2[0] === 'on' ? c.tp2[1] + '%' : '—'}\nدخول2:${c.entry2[0] === 'on' ? c.entry2[1] + '%' : '—'}  رافعة:${c.cxLev}x\n━━━━━━━━━━\n📈 عائد: ${m.netReturnPct}%  |  نجاح: ${m.winRate}%\n📊 صفقات: ${m.trades}  |  PF: ${m.profitFactor}\n📉 أقصى تراجع: ${m.maxDrawdownPct}%`;
            tgSend(txt, STATE.settings.cxChat);
          } else {
            txt = `🏁 انتهى الفحص التلقائي العام — لم يُعثر على نتيجة (تم تقييم ${res.evaluated || 0})`;
          }
          const buf = Buffer.from(JSON.stringify(res, null, 2));
          const fname = `bt_optimize_${Date.now()}.json`;
          tgSend(txt, STATE.settings.cxChatBT);
          tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btOptDone', data: { error: e.message } });
          const errTxt = '❌ فشل الفحص التلقائي العام: ' + e.message;
          tgSend(errTxt, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btOptPerSymbol': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      const recipes = BT.topRecipes(msg.data?.leaderboard, BT.ALLOWED_TF, 2);
      if (!recipes.length) { broadcast({ type: 'btOptSymDone', data: { error: 'لا توجد نتيجة فحص عام — شغّل "الفحص التلقائي" أولاً (أو استعد ملف نتيجة سابقة)' } }); break; }
      btState.busy = true; btState.cancel = false;
      const budgetMs = (parseFloat(msg.data?.budgetHours) || 3) * 3600000;
      const minTrades = parseInt(msg.data?.minTrades) || 5;
      (async () => {
        try {
          const tfs = BT.ALLOWED_TF; // الفحص لكل عملة يقتصر على هذين الفريمين فقط
          const symSet = new Set();
          for (const tf of tfs) BT.listStoredSymbols(tf).forEach(s => symSet.add(s));
          const symbols = [...symSet].filter(s => s !== 'BTCUSDT');
          if (!symbols.length) { broadcast({ type: 'btOptSymDone', data: { error: 'لا توجد بيانات — نزّل أولاً' } }); btState.busy = false; return; }
          const datasetByTf = BT.loadDatasetByTf(symbols, tfs);
          const btcByTf = BT.loadBtcByTf(tfs);
          const res = await BT.optimizePerSymbol(datasetByTf, btcByTf, recipes, {
            budgetMs, minTrades, capFrac: 0.01,
            shouldStop: () => btState.cancel,
            onProgress: p => broadcast({ type: 'btProgress', data: { ...p, kind: 'optSym' } }),
          });
          broadcast({ type: 'btOptSymDone', data: res });
          const txt = `🏁 انتهى الفحص لكل عملة\nالعملات: ${res.symbolsScanned}/${res.totalSymbols}`;
          const buf = Buffer.from(JSON.stringify(res, null, 2));
          const fname = `bt_optsym_${Date.now()}.json`;
          tgSend(txt, STATE.settings.cxChatBT);
          tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btOptSymDone', data: { error: e.message } });
          const errTxt = '❌ فشل الفحص لكل عملة: ' + e.message;
          tgSend(errTxt, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    // فحص "احترام المؤشر": أفضل توليفة مؤشر (بدون تغيير إدارة الصفقة) لكل عملة من حيث انعكاس السعر بقوة بعد إشارات الشورت واللونق
    case 'btOptRespect': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      btState.busy = true; btState.cancel = false;
      (async () => {
        try {
          const tfs = BT.ALLOWED_TF;
          const symSet = new Set();
          for (const tf of tfs) BT.listStoredSymbols(tf).forEach(s => symSet.add(s));
          const symbols = [...symSet].filter(s => s !== 'BTCUSDT');
          if (!symbols.length) { broadcast({ type: 'btOptRespectDone', data: { error: 'لا توجد بيانات — نزّل أولاً' } }); btState.busy = false; return; }
          const datasetByTf = BT.loadDatasetByTf(symbols, tfs);
          const res = await BT.optimizeIndicatorRespect(datasetByTf, {
            shouldStop: () => btState.cancel,
            onProgress: p => broadcast({ type: 'btProgress', data: { ...p, kind: 'optRespect' } }),
          });
          broadcast({ type: 'btOptRespectDone', data: res });
          const ranked = Object.entries(res.bySymbol).sort((a, b) => b[1].score - a[1].score);
          const lines = ranked.slice(0, 15).map(([sym, c], idx) =>
            `${idx + 1}. ${sym.replace('USDT', '')} — شورت ${c.short.respectRate}%(${c.short.avgReversalPct}%, ن=${c.short.total}) | لونق ${c.long.respectRate}%(${c.long.avgReversalPct}%, ن=${c.long.total})`
          );
          const txt = `🎯 احترام المؤشر — أفضل توليفة لكل عملة\nالعملات: ${res.symbolsScanned}/${res.totalSymbols}\n━━━━━━━━━━\n${lines.join('\n')}`;
          const buf = Buffer.from(JSON.stringify(res, null, 2));
          const fname = `bt_respect_${Date.now()}.json`;
          tgSend(txt, STATE.settings.cxChatBT);
          tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btOptRespectDone', data: { error: e.message } });
          const errTxt = '❌ فشل فحص احترام المؤشر: ' + e.message;
          tgSend(errTxt, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btOptTuned': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      btState.busy = true; btState.cancel = false;
      (async () => {
        try {
          const tfs = BT.ALLOWED_TF;
          const symSet = new Set();
          for (const tf of tfs) BT.listStoredSymbols(tf).forEach(s => symSet.add(s));
          const symbols = [...symSet].filter(s => s !== 'BTCUSDT');
          if (!symbols.length) { broadcast({ type: 'btOptTunedDone', data: { error: 'لا توجد بيانات — نزّل أولاً' } }); btState.busy = false; return; }
          const datasetByTf = BT.loadDatasetByTf(symbols, tfs);
          const res = await BT.optimizeIndicatorTuned(datasetByTf, {
            shouldStop: () => btState.cancel,
            onProgress: p => broadcast({ type: 'btProgress', data: { ...p, kind: 'optTuned' } }),
          });
          broadcast({ type: 'btOptTunedDone', data: res });
          const ranked = Object.entries(res.bySymbol).sort((a, b) => b[1].score - a[1].score);
          const lines = ranked.slice(0, 15).map(([sym, c], idx) =>
            `${idx + 1}. ${sym.replace('USDT', '')} — شورت ${c.short.respectRate}%(${c.short.avgReversalPct}%, ن=${c.short.total}) | لونق ${c.long.respectRate}%(${c.long.avgReversalPct}%, ن=${c.long.total}) | ${c.combo.sigPreset}`
          );
          const txt = `🧬 مؤشر العملة — أفضل توليفة كاملة (مع عتبات Trailing) لكل عملة\nالعملات: ${res.symbolsScanned}/${res.totalSymbols}\n━━━━━━━━━━\n${lines.join('\n')}`;
          const buf = Buffer.from(JSON.stringify(res, null, 2));
          const fname = `bt_tuned_${Date.now()}.json`;
          tgSend(txt, STATE.settings.cxChatBT);
          tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btOptTunedDone', data: { error: e.message } });
          const errTxt = '❌ فشل فحص مؤشر العملة: ' + e.message;
          tgSend(errTxt, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btOptManagement': {
      if (btState.busy) { broadcast({ type: 'btProgress', data: { phase: 'busy' } }); break; }
      const bySymbolRecipes = msg.data?.bySymbol;
      if (!bySymbolRecipes || !Object.keys(bySymbolRecipes).length) {
        broadcast({ type: 'btOptManagementDone', data: { error: 'لا توجد نتائج "احترام المؤشر" — شغّله أولاً من النافذة الأولى' } });
        break;
      }
      btState.busy = true; btState.cancel = false;
      (async () => {
        try {
          const tfs = BT.ALLOWED_TF;
          const symbols = Object.keys(bySymbolRecipes);
          const datasetByTf = BT.loadDatasetByTf(symbols, tfs);
          const btcByTf = BT.loadBtcByTf(tfs);
          const res = await BT.optimizeManagement(datasetByTf, btcByTf, bySymbolRecipes, {
            shouldStop: () => btState.cancel,
            onProgress: p => broadcast({ type: 'btProgress', data: { ...p, kind: 'optMgmt' } }),
          });
          broadcast({ type: 'btOptManagementDone', data: res });
          const ranked = Object.entries(res.bySymbol).filter(([, c]) => c.metrics).sort((a, b) => b[1].score - a[1].score);
          const lines = ranked.slice(0, 15).map(([sym, c], idx) =>
            `${idx + 1}. ${sym.replace('USDT', '')} — عائد ${c.metrics.netReturnPct}% | تراجع ${c.metrics.maxDrawdownPct}% | صفقات ${c.metrics.trades} | PF ${c.metrics.profitFactor} | مبلغ ${c.combo.cxAmt}`
          );
          const txt = `🚪 الدخول والخروج — أفضل إدارة صفقة لكل عملة\nالعملات: ${res.symbolsScanned}/${res.totalSymbols}\n━━━━━━━━━━\n${lines.join('\n')}`;
          const buf = Buffer.from(JSON.stringify(res, null, 2));
          const fname = `bt_management_${Date.now()}.json`;
          tgSend(txt, STATE.settings.cxChatBT);
          tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
        } catch (e) {
          broadcast({ type: 'btOptManagementDone', data: { error: e.message } });
          const errTxt = '❌ فشل فحص الدخول والخروج: ' + e.message;
          tgSend(errTxt, STATE.settings.cxChatBT);
        }
        finally { btState.busy = false; }
      })();
      break;
    }
    case 'btStop': { btState.cancel = true; broadcast({ type: 'btProgress', data: { phase: 'stopping' } }); break; }

    // إرسال ملف JSON جاهز + نص ملخص إلى قناة الباك تيست على تلغرام (لتبويب "الخلاصة")
    case 'btSendFile': {
      (async () => {
        try {
          const { text, payload, filename } = msg.data || {};
          if (!text || !payload || !filename) throw new Error('بيانات ناقصة');
          const buf = Buffer.from(JSON.stringify(payload, null, 2));
          await tgSend(text, STATE.settings.cxChatBT);
          await tgSendDocument(buf, filename, text, STATE.settings.cxChatBT);
          broadcast({ type: 'btSendFileDone', data: { ok: true } });
        } catch (e) {
          broadcast({ type: 'btSendFileDone', data: { ok: false, error: e.message } });
        }
      })();
      break;
    }

    // إرسال ملخص "النتائج" الشامل (تحليل كل ملفات الفحص المرفوعة) إلى تلغرام
    case 'resultsReport': {
      (async () => {
        try {
          const a = msg.data?.analysis;
          if (!a) throw new Error('لا توجد بيانات تحليل');
          const lines = [];
          lines.push('📊 ملخص النتائج الشامل');
          lines.push(`📁 عدد الملفات: ${a.files?.length || 0}`);
          lines.push(`🔢 عدد العملات: ${a.total || 0}  |  رابحة: ${a.profitable || 0}`);
          lines.push(`📈 متوسط العائد: ${a.avgRet}%  |  📉 متوسط التراجع: ${a.avgDD}%`);
          lines.push(`✅ نجاح: ${a.avgWin}%  |  📊 صفقات: ${a.avgTrades}`);
          lines.push('');
          lines.push(`✅ أحضر: ${a.counts?.include || 0}  |  ⚠️ راجع: ${a.counts?.watch || 0}  |  ❌ استبعد: ${a.counts?.exclude || 0}`);
          if (a.refinedCount != null) {
            lines.push(`🔧 فلاتر إشارات محسَّنة تلقائياً: ${a.refinedCount}/${a.total || 0} عملة  |  بيانات كاملة (OB/OS/CONF/TS/TL): ${a.fullBreakdownCount || 0}/${a.total || 0}`);
          }
          if (a.filters) {
            const f = a.filters;
            lines.push(`🎛️ معايير "أحضر": صفقات≥${f.minTrades} | تراجع≤${f.maxDD}% | PF≥${f.minPF} | احترام شورت≥${f.minRespectShort}% | احترام لونج≥${f.minRespectLong}% | انعكاس≥${f.minAvgReversal}%`);
          }
          if ((a.top || []).length) {
            lines.push('');
            lines.push('🏆 أفضل العملات:');
            a.top.slice(0, 10).forEach((r, i) => {
              const c = r.combo ? ` | ${r.combo.interval}/${r.combo.mode}${r.combo.sigPreset ? '/' + r.combo.sigPreset : ''}` : '';
              lines.push(`${i + 1}. ${r.sym} — عائد ${r.ret}% | نجاح ${r.win}% | صفقات ${r.trades} | PF ${r.pf} | تراجع ${r.dd}%${c}`);
            });
          }
          if ((a.worst || []).length) {
            lines.push('');
            lines.push('❌ الأضعف (مرشّحة للاستبعاد):');
            a.worst.slice(0, 10).forEach((r, i) => {
              lines.push(`${i + 1}. ${r.sym} — عائد ${r.ret}% | تراجع ${r.dd}% | صفقات ${r.trades}`);
            });
          }
          if ((a.sigTop || []).length) {
            lines.push('');
            lines.push('🎯 أفضل جودة إشارات RSI:');
            a.sigTop.forEach(s => lines.push(`${s.sym}: احترام ${s.respectRate}% (${s.total} إشارة)`));
          }
          if ((a.sigBottom || []).length) {
            lines.push('');
            lines.push('⚠️ أضعف جودة إشارات RSI:');
            a.sigBottom.forEach(s => lines.push(`${s.sym}: احترام ${s.respectRate}% (${s.total} إشارة)`));
          }
          const txt = lines.join('\n');
          const buf = Buffer.from(JSON.stringify(a, null, 2));
          const fname = `results_report_${Date.now()}.json`;
          await tgSend(txt, STATE.settings.cxChatBT);
          await tgSendDocument(buf, fname, txt, STATE.settings.cxChatBT);
          broadcast({ type: 'resultsReportDone', data: { ok: true } });
        } catch (e) {
          broadcast({ type: 'resultsReportDone', data: { ok: false, error: e.message } });
        }
      })();
      break;
    }

    // بيانات شارت لرمز واحد: شموع + إشارات + صفقات (TP/SL/الخروج) + مقاييس — للتحقق اليدوي
    case 'btChartData': {
      try {
        const symbol = String(msg.data?.symbol || '').toUpperCase().trim();
        if (!symbol) { broadcast({ type: 'btChartData', data: { error: 'حدّد رمز العملة' } }); break; }
        const set = { ...STATE.settings, ...(msg.data?.settings || {}) };
        const ftf = BT.ALLOWED_TF.includes(set.ema200TF) ? set.ema200TF : (BT.ALLOWED_TF.includes(set.stTF) ? set.stTF : (BT.ALLOWED_TF.includes(set.interval) ? set.interval : '1h'));
        const btc = BT.loadCandles('BTCUSDT', ftf) || BT.loadCandles('BTCUSDT', set.interval);
        const regime = btc ? BT.buildBtcRegime(btc, ftf, ftf, parseInt(set.stPeriod) || 10, parseFloat(set.stMult) || 3) : null;
        const res = BT.getChartData(symbol, set, regime);
        broadcast({ type: 'btChartData', data: { symbol, ...res } });
      } catch (e) { broadcast({ type: 'btChartData', data: { error: e.message } }); }
      break;
    }

    case 'forceRescan':
      rescanWithFreshCandles();
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
      db.saveWaitQueue(STATE.waitQueue);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      break;
    }

    case 'clearQueue': {
      STATE.waitQueue = [];
      db.saveWaitQueue(STATE.waitQueue);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      break;
    }

    case 'addToQueue': {
      const { sym, side, label, emoji, color, signalType, tf } = msg.data || {};
      if (!sym || !side) break;
      if (STATE.waitQueue.some(q => q.symbol === sym)) break;
      const typeKey = ['a70','b70'].includes(signalType) ? 'ob' : ['b30','a30'].includes(signalType) ? 'os' : ['cl','cs'].includes(signalType) ? 'conf' : 'trail';
      STATE.waitQueue.push({
        id: Date.now() + Math.random(), symbol: sym, side,
        signalType: typeKey, signalPrice: livePrices[sym] || 0,
        addedTs: Date.now(), addedTime: nowStr(),
        // الفريم يأتي من الإشارة المضافة؛ وإن غاب نستخدم الفريم الفعّال للعملة
        tf: tf || settingsFor(sym).interval || null,
        label: label || '', emoji: emoji || '', color: color || ''
      });
      db.saveWaitQueue(STATE.waitQueue);
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
      break;
    }

    // ── نظام القفل: أوامر يدوية ─────────────────────────
    case 'lockBreakEven': {
      const { syms, pct } = msg.data || {};
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (!master?.apiKey) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا يوجد حساب ماستر' } }); break; }
      try { master.livePositions = await getPositions(master); } catch (e) {}
      const r = await applyBreakEven(master, syms, pct ?? STATE.settings.lockBEoffsetPct, 'يدوي');
      broadcast({ type: 'lockResult', data: { ok: true, action: 'be', ...r } });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    case 'lockLossStop': {
      const { syms, pct } = msg.data || {};
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (!master?.apiKey) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا يوجد حساب ماستر' } }); break; }
      try { master.livePositions = await getPositions(master); } catch (e) {}
      const r = await applyLossStop(master, syms, pct);
      broadcast({ type: 'lockResult', data: { ok: true, action: 'sl', ...r } });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    case 'lockTrailing': {
      const { syms, pct } = msg.data || {};
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (!master?.apiKey) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا يوجد حساب ماستر' } }); break; }
      try { master.livePositions = await getPositions(master); } catch (e) {}
      const r = await applyTrailing(master, syms, pct);
      broadcast({ type: 'lockResult', data: { ok: true, action: 'trail', ...r } });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
      break;
    }

    // تسليح القفل العام — المسار الوحيد الذي يفعّله، بضغطة صريحة من المستخدم
    case 'lockArm': {
      if (masterLockActive()) { broadcast({ type: 'lockResult', data: { ok: false, error: 'القفل مفعّل بالفعل' } }); break; }
      const hrs = parseFloat(msg.data?.hours ?? STATE.settings.lockMasterHours) || 0;
      STATE.settings.lockMaster = true;
      STATE.settings.lockMasterHours = hrs;
      STATE.lockState.masterUntil = hrs > 0 ? Date.now() + hrs * HOUR_MS : 0;
      STATE.lockState.snapshot = { ...STATE.settings, lockMaster: true };
      db.saveSettings(STATE.settings);
      lockSave();
      lockNotify(hrs > 0
        ? `🔐 فُعّل القفل العام لمدّة ${fmtHours(hrs)}\nينتهي: ${fmtDateTime(STATE.lockState.masterUntil)}\n💾 حُفظت نسخة من كل الإعدادات`
        : '🔐 فُعّل القفل العام بلا مدّة — لا ينتهي تلقائياً\n💾 حُفظت نسخة من كل الإعدادات');
      broadcast({ type: 'settings', data: STATE.settings });
      broadcast({ type: 'lockState', data: lockPublic() });
      break;
    }

    case 'lockReset': {
      // إعادة فتح التداول يدوياً — ممنوع إن كان القفل العام مفعّلاً
      if (masterLockActive()) {
        broadcast({ type: 'lockResult', data: { ok: false, error: '🔒 القفل العام مفعّل — لا يمكن إعادة الضبط' } });
        break;
      }
      STATE.lockState.lockedUntil = 0;
      STATE.lockState.realizedLoss = 0;
      STATE.lockState.windowStart = Date.now();
      STATE.lockState.trades = [];
      lockSave();
      lockNotify('🔓 أُعيد ضبط النافذة يدوياً — التداول اليدوي مفتوح');
      broadcast({ type: 'lockState', data: lockPublic() });
      break;
    }

    case 'lockRefresh': {
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (master?.apiKey) {
        try { master.livePositions = await getPositions(master); master.liveBalance = await getBalance(master); } catch (e) {}
        const positions = (master.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
        try { await buildLockDiag(master, positions); } catch (e) {}
        broadcast({ type: 'accounts', data: getSafeAccounts() });
      }
      broadcast({ type: 'lockState', data: lockPublic() });
      break;
    }

    // اختبار تعديل الرسالة — يعدّل رقم الوقف في رسالة الإشارة فقط،
    // بدون لمس أي أمر على بايننس، لمعرفة هل يقرأ كورنكس التعديل
    case 'lockTestEdit': {
      const { sym, stop } = msg.data || {};
      // بلا رمز → استخدم أحدث إشارة قابلة للتعديل تلقائياً
      let s = sym ? String(sym).replace(/USDT$/i, '').toUpperCase() + 'USDT' : null;
      if (!s) {
        const cand = Object.entries(STATE.sentMsgIds)
          .filter(([, r]) => r?.id && r?.text && Date.now() - (r.ts || 0) < TG_EDIT_WINDOW_MS)
          .sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0))[0];
        if (!cand) {
          broadcast({ type: 'lockResult', data: { ok: false, error: 'لا توجد إشارة قابلة للتعديل (أحدث من ٤٨ ساعة). أرسل إشارة جديدة ثم جرّب.' } });
          break;
        }
        s = cand[0];
      }
      const rec = STATE.sentMsgIds[s];
      if (!rec?.id || !rec.text) {
        broadcast({ type: 'lockResult', data: { ok: false, error: `لا توجد رسالة إشارة محفوظة لـ ${s} — التعديل يعمل فقط على إشارات أرسلها البوت بعد آخر تحديث` } });
        break;
      }
      // سعر تجريبي محسوب من الرسالة نفسها إن لم يُحدَّد
      let px = parseFloat(stop);
      if (!isFinite(px) || px <= 0) {
        px = suggestTestStop(rec.text);
        if (!px) { broadcast({ type: 'lockResult', data: { ok: false, error: `تعذّر قراءة سعر الوقف من رسالة ${s}` } }); break; }
      }
      const before = parseSignalMsg(rec.text).stop;
      const r = await cornixEditSignal(s, { stop: px });
      const pair = s.replace('USDT', '/USDT');
      broadcast({ type: 'lockResult', data: r.ok
        ? { ok: true, action: 'testEdit', verbose: true, skipped: [], failed: [], done: [
            `✅ عُدّلت رسالة ${pair} (#${rec.id})`,
            `الوقف: ${before} → ${fmtSignalPrice(px)}`,
            `افتح القناة وتأكد أن السطر تغيّر، ثم افتح كورنكس.`,
          ] }
        : { ok: true, action: 'testEdit', verbose: true, skipped: [], failed: [], done: [
            `❌ لم تُعدَّل رسالة ${pair} (#${rec.id})`,
            `السبب: ${r.why}`,
          ] } });
      // حدّث القائمة (قد تكون رسائل انتهت صلاحيتها)
      broadcast({ type: 'lockMsgSyms', data: msgSymsList() });
      break;
    }

    // فحص وجود رسالة الإشارة: نحاول تعديلها بنفس نصها تماماً.
    // تلغرام يميّز بوضوح: "not modified" = موجودة ويملكها البوت،
    // "not found" = غير موجودة (حُذفت أو أعاد كورنكس نشرها باسمه).
    case 'lockProbeMsg': {
      const entries = Object.entries(STATE.sentMsgIds).filter(([, r]) => r?.id && r?.text)
        .sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0)).slice(0, 5);
      if (!entries.length) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا توجد رسائل محفوظة — أرسل إشارة أولاً' } }); break; }
      const lines = [];
      for (const [sym, rec] of entries) {
        const chat = rec.chat || STATE.settings.cxChat;
        const r = await tgEditDirect(chat, rec.id, rec.text);   // نفس النص = بلا تغيير فعلي
        const err = String(r.error || '');
        let verdict;
        if (r.ok) verdict = '✅ موجودة ويملكها البوت (عُدِّلت)';
        else if (/not modified/i.test(err)) verdict = '✅ موجودة ويملكها البوت';
        else if (/not found/i.test(err)) verdict = '❌ غير موجودة — حُذفت أو أعاد كورنكس نشرها';
        else if (/can't be edited|not enough rights/i.test(err)) verdict = '⛔ موجودة لكن البوت لا يملك حق تعديلها';
        else verdict = '⚠️ ' + err.slice(0, 60);
        lines.push(`${sym.replace('USDT', '/USDT')} (#${rec.id} · ${chat}): ${verdict}`);
        await new Promise(r2 => setTimeout(r2, 400));
      }
      broadcast({ type: 'lockResult', data: { ok: true, action: 'probe', done: lines, skipped: [], failed: [], verbose: true } });
      break;
    }

    // البحث عن رسالة كورنكس: أرقام الرسائل في القناة متتابعة، فرسالة كورنكس
    // البديلة تقع بعد رسالتنا مباشرة. نستخدم forwardMessage لأنه لا يغيّر شيئاً —
    // ينجح إن كانت الرسالة موجودة، ويفشل إن لم تكن.
    case 'lockFindCornix': {
      const dest = STATE.settings.lockTgChat || STATE.settings.cxChat;
      const token = STATE.settings.cxToken;
      const entries = Object.entries(STATE.sentMsgIds).filter(([, r]) => r?.id)
        .sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0));
      if (!entries.length || !token) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا توجد إشارة محفوظة' } }); break; }
      const [sym, rec] = entries[0];
      const from = rec.chat || STATE.settings.cxChat;
      const lines = [`رسالتنا لـ ${sym.replace('USDT', '/USDT')} كانت #${rec.id} في ${from}`];
      const found = [];
      for (let off = 1; off <= 6; off++) {
        const mid = rec.id + off;
        try {
          const res = await fetch(`https://api.telegram.org/bot${token}/forwardMessage`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: dest, from_chat_id: from, message_id: mid }),
          });
          const body = await res.text().catch(() => '');
          if (res.ok) { lines.push(`#${mid}: ✅ موجودة — حُوّلت لقناة القفل`); found.push(mid); }
          else {
            let d = body; try { d = JSON.parse(body).description || body; } catch (e) {}
            const s = String(d);
            // تلغرام يفحص الرسالة المصدر قبل الوجهة: فشل الوجهة يعني أن الرسالة موجودة
            if (/chat not found/i.test(s)) {
              lines.push(`#${mid}: ✅ موجودة — لكن تعذّر التحويل (قناة الوجهة ${dest} غير موجودة)`);
              found.push(mid);
            } else if (/message to forward not found/i.test(s)) {
              lines.push(`#${mid}: ❌ غير موجودة`);
            } else {
              lines.push(`#${mid}: ⚠️ ${s.slice(0, 45)}`);
            }
          }
        } catch (e) { lines.push(`#${mid}: ⚠️ ${e.message}`); }
        await new Promise(r => setTimeout(r, 400));
      }
      if (found.length) {
        lines.push('');
        lines.push(`المرشّح الأرجح لرسالة كورنكس: #${found[0]} — جرّب تعديلها في الخانة أدناه.`);
      } else {
        lines.push('لم يُعثر على رسائل بعد رسالتنا.');
      }
      STATE.lockState.cornixCandidates = found;
      lockSave();
      broadcast({ type: 'lockResult', data: { ok: true, action: 'find', done: lines, skipped: [], failed: [], verbose: true } });
      break;
    }

    // محاولة تعديل رسالة كورنكس نفسها — يُظهر رد تلغرام الحرفي
    case 'lockTryEditCornix': {
      const { msgId } = msg.data || {};
      const mid = parseInt(msgId);
      if (!mid) { broadcast({ type: 'lockResult', data: { ok: false, error: 'أدخل رقم الرسالة' } }); break; }
      const entries = Object.entries(STATE.sentMsgIds).filter(([, r]) => r?.id)
        .sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0));
      const chat = entries[0]?.[1]?.chat || STATE.settings.cxChat;
      const r = await tgEditDirect(chat, mid, '🧪 اختبار: هل يستطيع البوت تعديل رسالة كورنكس؟');
      const err = String(r.error || '');
      let verdict;
      if (r.ok) verdict = '✅✅ نجح! البوت يستطيع تعديل رسالة كورنكس — الحل ممكن';
      else if (/not modified/i.test(err)) verdict = '✅ الرسالة موجودة والبوت يملك حق تعديلها';
      else if (/not found/i.test(err)) verdict = '❌ لا توجد رسالة بهذا الرقم';
      else if (/can't be edited|MESSAGE_AUTHOR_REQUIRED|MESSAGE_ID_INVALID|not enough rights/i.test(err)) verdict = '⛔ رسالة كورنكس — تلغرام يمنع البوت من تعديل رسالة غيره حتى لو كان مشرفاً';
      else verdict = '⚠️ ' + err.slice(0, 80);
      broadcast({ type: 'lockResult', data: { ok: true, action: 'tryedit', done: [`#${mid} في ${chat}:`, verdict], skipped: [], failed: [], verbose: true } });
      break;
    }

    // اختبار الصلاحية: يرسل رسالة نصية بسيطة ثم يعدّلها فوراً —
    // يفصل "هل يقدر البوت يعدّل؟" عن "هل السجل المحفوظ صحيح؟"
    case 'lockTestPerm': {
      const token = STATE.settings.cxToken;
      const chat = STATE.settings.cxChat;
      if (!token || !chat) { broadcast({ type: 'lockResult', data: { ok: false, error: 'التوكن أو Chat ID الأساسي غير مضبوط' } }); break; }
      try {
        const res = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chat, text: '🧪 اختبار صلاحية التعديل — سطر ١' }),
        });
        const body = await res.text().catch(() => '');
        if (!res.ok) {
          let d = body; try { d = JSON.parse(body).description || body; } catch (e) {}
          broadcast({ type: 'lockResult', data: { ok: false, error: `فشل الإرسال أصلاً: ${d}` } });
          break;
        }
        const mid = JSON.parse(body)?.result?.message_id;
        if (!mid) { broadcast({ type: 'lockResult', data: { ok: false, error: 'أُرسلت الرسالة لكن تلغرام لم يُرجع message_id' } }); break; }
        await new Promise(r => setTimeout(r, 1200));
        const e = await tgEditDirect(chat, mid, '🧪 اختبار صلاحية التعديل — ✅ نجح التعديل (سطر ٢)');
        broadcast({ type: 'lockResult', data: e.ok
          ? { ok: true, action: 'perm', done: [`البوت يستطيع تعديل رسائله في القناة (رسالة#${mid})`], skipped: [], failed: [] }
          : { ok: false, error: `أرسل ✅ لكن التعديل فشل ❌ — ${e.error} (رسالة#${mid}). راجع صلاحيات البوت في القناة.` } });
      } catch (e) {
        broadcast({ type: 'lockResult', data: { ok: false, error: e.message } });
      }
      break;
    }

    // الرموز التي لدينا رسالة إشارة محفوظة لها (لاختبار التعديل)
    case 'lockMsgSyms': {
      broadcast({ type: 'lockMsgSyms', data: msgSymsList() });
      break;
    }

    // تصنيف صفقة يدوياً — الاستنتاج التلقائي يخطئ حين يكون للرمز إشارة قديمة
    // بينما الصفقة القائمة فُتحت باليد
    case 'lockSetKind': {
      const { sym, kind } = msg.data || {};
      if (!sym) break;
      const store = STATE.lockState.manualOverride || (STATE.lockState.manualOverride = {});
      if (kind === 'auto') delete store[sym];
      else store[sym] = kind === 'manual' ? 'manual' : 'bot';
      // التصنيف الجديد يعني إعادة تقييم الوقف
      if (STATE.lockState.manualSyms[sym]) delete STATE.lockState.manualSyms[sym].slPlaced;
      lockSave();
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (master?.apiKey) {
        try {
          master.livePositions = await getPositions(master);
          const positions = (master.livePositions || []).filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0);
          await buildLockDiag(master, positions);
        } catch (e) {}
      }
      broadcast({ type: 'lockState', data: lockPublic() });
      broadcast({ type: 'lockResult', data: { ok: true, action: 'kind', done: [`${sym.replace('USDT', '/USDT')} → ${kind === 'auto' ? 'تلقائي' : kind === 'manual' ? 'يدوية' : 'صفقة بوت'}`], skipped: [], failed: [] } });
      break;
    }

    // فحص فوري — يشغّل دورة المراقب الآن بدل انتظار ١٥ ثانية
    case 'lockRunNow': {
      if (!STATE.settings.lockOn) {
        broadcast({ type: 'lockResult', data: { ok: false, error: 'نظام القفل متوقف — شغّله أولاً من الزر بالأعلى' } });
        break;
      }
      const master = STATE.copyAccounts.find(a => a.isMaster);
      if (!master?.apiKey) { broadcast({ type: 'lockResult', data: { ok: false, error: 'لا يوجد حساب ماستر' } }); break; }
      try { master.livePositions = await getPositions(master); } catch (e) {}
      STATE.lockState.diagAt = 0;
      await monitorLock();
      broadcast({ type: 'lockResult', data: { ok: true, action: 'scan', done: [], skipped: [], failed: [], msg: 'اكتمل الفحص' } });
      broadcast({ type: 'accounts', data: getSafeAccounts() });
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
        // ── نظام القفل: امنع الفتح أثناء فترة الانتظار ──
        if (lockIsLocked()) {
          const mins = Math.ceil((STATE.lockState.lockedUntil - Date.now()) / 60000);
          throw new Error(`⛔ نظام القفل نشط — بلغت الحد اليومي للخسارة. يفتح بعد ${mins} دقيقة`);
        }
        const bal = await getBalance(acc);
        if (bal <= 0) throw new Error(`الرصيد صفر — تأكد من الـ API`);
        const price = livePrices[sym] || parseFloat(limitPrice) || 1;
        await ensureLotSize(sym);
        const leverage = Math.min(parseInt(lev) || 20, await getMaxLev(sym));
        const amtPct = parseFloat(pct || 5) / 100;
        // الهامش المطلوب بالدولار
        let margin = useAmt ? parseFloat(amt || 0) : bal * amtPct;
        // ── نظام القفل: اسقف الهامش عند الحد اليومي (تقليم قبل الفتح = بدون عمولة زائدة) ──
        let trimmed = 0;
        if (STATE.settings.lockOn && STATE.settings.lockDailyOn) {
          const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
          if (cap > 0 && margin > cap) { trimmed = margin - cap; margin = cap; }
        }
        const rawQty = (margin * leverage) / price;
        const qty = roundQty(rawQty, sym);
        if (qty <= 0) throw new Error(`الكمية صغيرة جداً (رصيد: $${bal.toFixed(2)}، هامش: $${margin.toFixed(2)})`);
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
          db.savePendingOrders(STATE.pendingOrders);
          broadcast({ type: 'pendingOrders', data: STATE.pendingOrders });
        }
        [acc.livePositions, acc.liveBalance] = await Promise.all([getPositions(acc), getBalance(acc)]);

        // ── نظام القفل: تسجيل الصفقة + ستوب تلقائي + إشعار ──
        if (STATE.settings.lockOn) {
          STATE.lockState.manualSyms[sym] = { ts: Date.now(), margin, slPlaced: false };
          lockSave();
          let slLine = '';
          if (STATE.settings.lockAutoSLon && orderParams.type === 'MARKET') {
            const slPct = parseFloat(STATE.settings.lockAutoSLpct) || 2;
            const fresh = (acc.livePositions || []).find(p => p.symbol === sym && Math.abs(parseFloat(p.positionAmt || 0)) > 0);
            if (fresh) {
              try {
                const isLong = parseFloat(fresh.positionAmt) > 0;
                const ep = parseFloat(fresh.entryPrice) || price;
                const px = await placeStop(acc, sym, fresh, isLong ? ep * (1 - slPct / 100) : ep * (1 + slPct / 100), 'ستوب يدوي');
                STATE.lockState.manualSyms[sym].slPlaced = true;
                lockSave();
                slLine = `\n🛡️ وقف تلقائي @ ${px} (${slPct}% ≈ ${(slPct * leverage).toFixed(0)}% من الهامش)`;
              } catch (e) {
                slLine = `\n⚠️ تعذّر وضع الوقف: ${e.message}`;
              }
            }
          }
          const cap = parseFloat(STATE.settings.lockDailyAmt) || 0;
          lockNotify(
            `📈 صفقة يدوية — #${sym.replace('USDT', '/USDT')}\n` +
            `${side === 'LONG' ? '🟢 LONG' : '🔴 SHORT'} · ${leverage}x · هامش $${margin.toFixed(2)}` +
            (trimmed > 0 ? `\n✂️ قُلّمت الزيادة: $${trimmed.toFixed(2)} (الحد $${cap})` : '') +
            slLine +
            (STATE.settings.lockDailyOn ? `\n💰 المتبقي من الحد اليومي: $${lockRemaining().toFixed(2)} من $${cap}` : '')
          );
          broadcast({ type: 'lockState', data: lockPublic() });
        }

        db.saveAccounts(STATE.copyAccounts);
        broadcast({ type: 'accounts', data: getSafeAccounts() });
        broadcast({ type: 'manualOrderResult', data: { success: true, sym, side, qty, lev: leverage, acc: acc.name, type: orderParams.type, margin, trimmed } });
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
        const master = STATE.copyAccounts.find(a => a.isMaster);
        const mPos = master?.livePositions?.find(p => p.symbol === t.symbol);
        const posAmt = mPos ? Math.abs(parseFloat(mPos.positionAmt)) : 0;
        const pnlUsd = mPos ? parseFloat(mPos.unRealizedProfit || 0) : parseFloat((posAmt * Math.abs(ep - t.entryPrice) * (t.side === 'LONG' ? (ep >= t.entryPrice ? 1 : -1) : (ep <= t.entryPrice ? 1 : -1))).toFixed(4));
        const closed = { ...t, exitPrice: ep, exitTime: nowStr(), closeTs: Date.now(), pct, pnl: pnlUsd, result: pct >= 0 ? 'win' : 'loss' };
        STATE.closedTrades = [closed, ...STATE.closedTrades].slice(0, 500);
        STATE.openTrades = STATE.openTrades.filter(x => x.id !== t.id);
        delete STATE.sentSigs[t.symbol]; saveSentSigsDebounced();
        db.saveClosedTrade(closed);
        db.saveOpenTrades(STATE.openTrades);
        const dur = Math.round((Date.now() - t.openTs) / 60000);
        tgSend(`${pct >= 0 ? '✅' : '❌'} ${hasSymOverride(t.symbol) ? '⭐ ' : ''}${t.symbol.replace('USDT', '/USDT')}\n${t.side === 'LONG' ? '🟢' : '🔴'} ${t.side}\nالنتيجة: ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%\nالمدة: ${dur}m`, STATE.settings.cxChatClose || STATE.settings.cxChat);
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
      delete STATE.sentSigs[msg.data.sym]; saveSentSigsDebounced();
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
      db.savePendingOrders(STATE.pendingOrders);
      broadcast({ type: 'pendingOrders', data: STATE.pendingOrders });
      break;
    }

    case 'sendSignalManual': {
      const { sym, side } = msg.data;
      const st = settingsFor(sym);
      if (!st.cxToken || !st.cxChat) {
        reportError('تلغرام', !st.cxToken ? 'لم يتم ضبط توكن البوت (Token) في الإعدادات' : 'لم يتم ضبط Chat ID في الإعدادات');
        break;
      }
      const { lv, note } = await resolveLeverage(sym, st.cxLev);
      const origLev = st.cxLev; st.cxLev = String(lv);
      const text = buildMsg(sym, side, st) + note;
      st.cxLev = origLev;
      await tgSend(text, st.cxChat, { trackSym: sym });
      if (STATE.settings.cxChatSettings) {
        await tgSend(buildSettingsMsg(sym, side, st, lv), STATE.settings.cxChatSettings);
      }
      break;
    }
  }
}

// ══════════════════════════════════════════════
//  REST API
// ══════════════════════════════════════════════
app.get('/api/ping', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

// صفحة تشخيص تُفتح من المتصفح مباشرة — تتجاوز الويب سوكت والواجهة،
// فتُظهر ما يحدث فعلاً حين تعذّر تتبّعه عبر الأزرار.
//   /api/lock/diag?pw=<كلمة المرور>          → عرض الحالة فقط
//   /api/lock/diag?pw=<...>&edit=1           → ينفّذ التعديل ويعرض رد تلغرام الحرفي
app.get('/api/lock/diag', async (req, res) => {
  if (!bcrypt.compareSync(String(req.query.pw || ''), ADMIN_PASS_HASH)) {
    return res.status(401).type('text/plain; charset=utf-8').send('كلمة مرور خاطئة');
  }
  const L = [];
  const P = (s) => L.push(s);
  try {
    P('═══ تشخيص تعديل الرسائل ═══');
    P(`الوقت: ${nowStr()}`);
    P(`توكن البوت: ${STATE.settings.cxToken ? 'موجود ✓' : 'مفقود ✗'}`);
    P(`قناة الإشارات: ${STATE.settings.cxChat || '(فارغة)'}`);
    P('');

    // ── فحص كل قناة: هل يستطيع البوت المراسلة فيها فعلاً؟ ──
    P('── فحص القنوات ──');
    const chats = [
      ['الإشارات', STATE.settings.cxChat],
      ['نظام القفل', STATE.settings.lockTgChat],
      ['إغلاق الصفقات', STATE.settings.cxChatClose],
      ['إعدادات الصفقات', STATE.settings.cxChatSettings],
      ['الباك تيست', STATE.settings.cxChatBT],
      ['محاكاة ST', STATE.settings.cxChatSTSim],
    ];
    for (const [name, id] of chats) {
      if (!id) { P(`  ${name}: (فارغة)`); continue; }
      try {
        const r = await fetch(`https://api.telegram.org/bot${STATE.settings.cxToken}/getChat?chat_id=${encodeURIComponent(id)}`);
        const b = await r.text().catch(() => '');
        if (r.ok) {
          let title = '';
          try { const j = JSON.parse(b); title = j?.result?.title || j?.result?.username || ''; } catch (e) {}
          P(`  ${name}: ${id} ✓ ${title ? '— ' + title : ''}`);
        } else {
          let d = b; try { d = JSON.parse(b).description || b; } catch (e) {}
          P(`  ${name}: ${id} ✗ ${String(d).slice(0, 60)}`);
          if (/chat not found/i.test(String(d))) P('      → الرقم غير صحيح، أو البوت ليس عضواً/مشرفاً في القناة');
        }
      } catch (e) { P(`  ${name}: ${id} ⚠️ ${e.message}`); }
      await new Promise(r2 => setTimeout(r2, 200));
    }
    if (deadChats.size) {
      P('');
      P('قنوات موقوفة مؤقتاً (تُعاد المحاولة بعد ١٠ دقائق):');
      for (const [c, r] of deadChats) P(`  ${c} — ${r.label}`);
    }
    P('');

    // &lock=1 → يرسل رسالة تجريبية لقناة نظام القفل ويعرض رد تلغرام الحرفي
    if (req.query.lock === '1') {
      const lc = STATE.settings.lockTgChat || STATE.settings.cxChat;
      P('── إرسال تجريبي لقناة نظام القفل ──');
      P(`الوجهة: ${lc}`);
      try {
        const r = await fetch(`https://api.telegram.org/bot${STATE.settings.cxToken}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: lc, text: '🔒 اختبار قناة نظام القفل — وصلت ✓' }),
        });
        const b = await r.text().catch(() => '');
        if (r.ok) { deadChats.delete(String(lc)); P('النتيجة: وصلت ✓ — افتح القناة وتأكد'); }
        else { let d = b; try { d = JSON.parse(b).description || b; } catch (e) {} P(`النتيجة: فشل ✗ — ${d}`); }
      } catch (e) { P(`النتيجة: استثناء — ${e.message}`); }
      return res.type('text/plain; charset=utf-8').send(L.join('\n'));
    }

    const all = Object.entries(STATE.sentMsgIds || {});
    P(`الرسائل المحفوظة: ${all.length}`);
    if (!all.length) {
      P('لا توجد رسائل محفوظة. التعديل يعمل فقط على إشارات أرسلها البوت بعد آخر تحديث.');
      return res.type('text/plain; charset=utf-8').send(L.join('\n'));
    }
    const sorted = all.sort((a, b) => (b[1].ts || 0) - (a[1].ts || 0));
    for (const [sym, r] of sorted.slice(0, 10)) {
      const ageH = ((Date.now() - (r.ts || 0)) / 3600000).toFixed(1);
      const ok = (Date.now() - (r.ts || 0)) < TG_EDIT_WINDOW_MS;
      P(`  ${sym}  #${r.id}  قناة ${r.chat}  عمر ${ageH}س  ${r.text ? 'نص✓' : 'نص✗'}  ${ok ? 'قابلة للتعديل ✓' : 'أقدم من ٤٨س ✗'}`);
    }
    P('');

    const cand = sorted.find(([, r]) => r?.id && r?.text && Date.now() - (r.ts || 0) < TG_EDIT_WINDOW_MS);
    if (!cand) { P('لا توجد رسالة صالحة للتعديل.'); return res.type('text/plain; charset=utf-8').send(L.join('\n')); }
    const [sym, rec] = cand;
    const parsed = parseSignalMsg(rec.text);
    P(`المرشّحة: ${sym} #${rec.id}`);
    P(`  الاتجاه: ${parsed.side || '؟'} · الدخول: ${parsed.entry ?? '؟'} · الوقف الحالي: ${parsed.stop ?? '؟'}`);

    if (req.query.edit !== '1') {
      P('');
      P('لتنفيذ التعديل فعلياً أضف &edit=1 إلى الرابط.');
      return res.type('text/plain; charset=utf-8').send(L.join('\n'));
    }

    const px = suggestTestStop(rec.text);
    P(`  الوقف الجديد المقترح: ${fmtSignalPrice(px)}`);
    P('');
    P('── تنفيذ التعديل ──');
    const newText = replaceStopInMsg(rec.text, fmtSignalPrice(px));
    if (!newText) { P('✗ تعذّر إيجاد سطر Stop Targets في النص المحفوظ.'); P(''); P(rec.text); return res.type('text/plain; charset=utf-8').send(L.join('\n')); }
    const r = await tgEditDirect(rec.chat || STATE.settings.cxChat, rec.id, newText);
    P(`رد تلغرام: ${r.ok ? 'نجح ✓' : 'فشل ✗'}`);
    if (!r.ok) P(`الخطأ الحرفي: ${r.error}`);
    else {
      rec.text = newText;
      saveSentMsgIdsDebounced();
      P(`عُدّلت الرسالة #${rec.id} — الوقف ${parsed.stop} ← ${fmtSignalPrice(px)}`);
      P('افتح القناة وتأكد أن السطر تغيّر.');
    }
    res.type('text/plain; charset=utf-8').send(L.join('\n'));
  } catch (e) {
    P('');
    P(`استثناء: ${e.message}`);
    res.type('text/plain; charset=utf-8').send(L.join('\n'));
  }
});

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

app.get('/backtest', (req, res) => res.sendFile(path.join(__dirname, 'backtest.html')));
app.get('/signals', (req, res) => res.sendFile(path.join(__dirname, 'signals.html')));
app.get('/signals_app.js', (req, res) => res.sendFile(path.join(__dirname, 'signals_app.js')));

app.get('/api/backtest/export', authMiddleware, (req, res) => {
  try {
    const { buffer, count } = BT.exportDataBundle();
    res.setHeader('Content-Disposition', 'attachment; filename="backtest-data.bin"');
    res.setHeader('Content-Type', 'application/octet-stream');
    res.setHeader('X-File-Count', String(count));
    res.send(buffer);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/backtest/import', authMiddleware, express.raw({ type: '*/*', limit: '500mb' }), (req, res) => {
  try {
    const { count } = BT.importDataBundle(req.body);
    res.json({ ok: true, count });
  } catch (e) { res.status(500).json({ error: e.message }); }
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

  // استعادة البيانات المحفوظة على PostgreSQL (إن وُجدت) قبل أي تحميل محلي
  await db.restoreFromPg();
  await BT.restoreCandlesFromPg();

  // تحميل الحالة من قاعدة البيانات
  STATE.settings = db.loadSettings(DEFAULT_SETTINGS);

  // ملاحظة: لا تمسح معرّفات القنوات تلقائياً هنا. القيم المحفوظة تخصّ المستخدم
  // حتى لو طابقت قيمة افتراضية قديمة. القناة التي يتعذّر الإرسال إليها تُعطَّل
  // وقتياً في drainTgQueue مع تسمية الخانة، ويعيدها تعديل الرقم من الواجهة.
  STATE.symbolSettings = db.loadSymbolSettings();
  // تحديث إعدادات التلغرام من env vars عند كل تشغيل
  if (process.env.TG_TOKEN) STATE.settings.cxToken = process.env.TG_TOKEN;
  if (process.env.TG_CHAT) STATE.settings.cxChat = process.env.TG_CHAT;
  if (process.env.TG_CHAT_CLOSE) STATE.settings.cxChatClose = process.env.TG_CHAT_CLOSE;
  if (process.env.TG_CHAT_BT) STATE.settings.cxChatBT = process.env.TG_CHAT_BT;
  if (!STATE.settings.cxChatBT) STATE.settings.cxChatBT = DEFAULT_SETTINGS.cxChatBT;
  if (process.env.TG_CHAT_SETTINGS) STATE.settings.cxChatSettings = process.env.TG_CHAT_SETTINGS;
  if (!STATE.settings.cxChatSettings) STATE.settings.cxChatSettings = DEFAULT_SETTINGS.cxChatSettings;
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
  {
    // بيانات الاحترام محفوظة على القرص مع وقت آخر فحص — نستعيدها ولا نعيد
    // فحص ٥٢٧ عملة مع كل إقلاع (كل نشر جديد يعيد تشغيل السيرفر)
    const rd = db.loadRespectData() || {};
    STATE.respectAt = rd._at || 0;
    delete rd._at;
    STATE.respectData = rd;
  }
  STATE.waitQueue = db.loadWaitQueue();
  STATE.pendingOrders = db.loadPendingOrders();
  STATE.simTrades = db.loadSimTrades();
  STATE.copyLog = db.loadCopyLog();
  STATE.sentSigs = db.loadSentSigs();
  STATE.sentMsgIds = db.loadSentMsgIds();
  const savedLock = db.loadLockState();
  if (savedLock) STATE.lockState = { ...STATE.lockState, ...savedLock, manualSyms: savedLock.manualSyms || {}, beDone: savedLock.beDone || {} };
  alertId = STATE.alerts.reduce((m, a) => Math.max(m, a.id || 0), 0);
  console.log(`📦 DB loaded: ${STATE.copyAccounts.length} accounts, ${STATE.openTrades.length} trades, ${STATE.dcaOrders.length} DCA orders`);

  // بايننس يردّ 418/429 أحياناً عند الإقلاع. محاولة واحدة كانت تترك الماسح
  // بصفر عملة بلا تعافٍ، فنعيد المحاولة بتباعد بسيط.
  let exInfo = null;
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      exInfo = await fetchBinance('/fapi/v1/exchangeInfo');
      if (attempt > 1) console.log(`✅ نجح تحميل العملات بعد ${attempt} محاولات`);
      break;
    } catch (e) {
      console.error(`⚠️ تحميل العملات فشل (${attempt}/5): ${e.message}`);
      reportError('تحميل العملات', `${e.message} — إعادة المحاولة (${attempt}/5)`);
      if (attempt < 5) await new Promise(r => setTimeout(r, 30000 * attempt));
    }
  }
  if (!exInfo) throw new Error('تعذّر تحميل العملات من بايننس بعد ٥ محاولات');

  try {
    const d = exInfo;
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
    // فلاتر الاتجاه العام تُحمَّل قبل أول فحص: كان الفحص يسبقها فتمرّ
    // إشارات معاكسة للاتجاه في أول دقائق كل إقلاع
    await Promise.all([updateEMA200(), updateSuperTrend()]);
    console.log(`📊 فلاتر الاتجاه: EMA200 ${STATE.ema200?.direction || '—'} · السوبر ${STATE.superTrend?.direction || '—'}`);
    await scanAll();
    startBinanceWS();
    setInterval(async () => {
      await scanAll();
    }, 300000);
    setInterval(updateEMA200, 10 * 60 * 1000);
    setInterval(updateSuperTrend, 10 * 60 * 1000);
    // لا نعيد الفحص إن كانت البيانات المحفوظة أحدث من ٢٤ ساعة —
    // وإلا أعاد كل نشر جديد فحص ٥٢٧ عملة بلا داعٍ
    {
      const age = Date.now() - (STATE.respectAt || 0);
      const DAY = 24 * 60 * 60 * 1000;
      const have = Object.keys(STATE.respectData || {}).length;
      if (have && age < DAY) {
        console.log(`📊 بيانات الاحترام محفوظة (${have} عملة، عمرها ${Math.floor(age / 3600000)} ساعة) — لا حاجة لإعادة الفحص`);
        setTimeout(updateRespect, DAY - age);
      } else {
        updateRespect();
      }
      setInterval(updateRespect, DAY);
    }
  } catch (e) {
    console.error('❌ Init failed:', e.message);
  }

  // heartbeat log
  setInterval(() => {
    console.log(`💓 ${new Date().toISOString()} | Symbols:${STATE.symbols.length} | Clients:${clients.size} | Accounts:${STATE.copyAccounts.length}`);
  }, 300000);

  // تحديث نسب الانعكاس في قائمة الانتظار + مراقبة صفقات المحاكاة كل 10 ثوانٍ
  setInterval(() => {
    if (STATE.waitQueue.length && clients.size)
      broadcast({ type: 'waitQueue', data: queueWithReversals() });
    checkSimTrades();
  }, 10000);

  // تحديث مراكز الماستر كل 30 ثانية حتى لو النسخ متوقف
  // يكتشف إغلاق صفقة → يرسل من القائمة تلقائياً
  let prevMasterPositions = {};
  setInterval(async () => {
    if (STATE.copyOn) return;
    const master = STATE.copyAccounts.find(a => a.isMaster);
    if (!master?.apiKey || !master?.apiSecret) return;
    try {
      master.livePositions = await getPositions(master);
      master.liveBalance = await getBalance(master);
      master.apiOk = true;
      broadcast({ type: 'accounts', data: getSafeAccounts() });

      const currPositions = {};
      master.livePositions.filter(p => Math.abs(parseFloat(p.positionAmt || 0)) > 0).forEach(p => { currPositions[p.symbol] = p; });

      // كشف الصفقات المغلقة
      for (const sym of Object.keys(prevMasterPositions)) {
        if (!currPositions[sym]) {
          const prevPos = prevMasterPositions[sym];
          const isLongPos = parseFloat(prevPos.positionAmt) > 0;
          const side = isLongPos ? 'LONG' : 'SHORT';
          const entryPrice = parseFloat(prevPos.entryPrice) || 0;
          const exitPrice = livePrices[sym] || entryPrice;
          const lev = parseFloat(prevPos.leverage) || 1;
          const rawPct = entryPrice ? ((exitPrice - entryPrice) / entryPrice) * 100 : 0;
          const pct = parseFloat(((isLongPos ? rawPct : -rawPct) * lev).toFixed(2));

          const posAmt = Math.abs(parseFloat(prevPos.positionAmt));
          const pnlUsd = parseFloat((posAmt * (exitPrice - entryPrice) * (isLongPos ? 1 : -1)).toFixed(4));
          const t = STATE.openTrades.find(x => x.symbol === sym);
          const closed = t
            ? { ...t, exitPrice, exitTime: nowStr(), closeTs: Date.now(), pct, pnl: pnlUsd, result: pct >= 0 ? 'win' : 'loss' }
            : { id: Date.now() + Math.random(), symbol: sym, side, entryPrice, exitPrice,
                pct, pnl: pnlUsd, result: pct >= 0 ? 'win' : 'loss',
                openTime: '', exitTime: nowStr(), openTs: 0, closeTs: Date.now(),
                sl: '', tp1: '', leverage: String(prevPos.leverage || 20),
                margin: prevPos.marginType || 'Cross', label: '📊 مراقبة', executed: true };

          STATE.closedTrades = [closed, ...STATE.closedTrades].slice(0, 500);
          STATE.openTrades = STATE.openTrades.filter(x => x.symbol !== sym);
          delete STATE.sentSigs[sym]; saveSentSigsDebounced();
          db.saveClosedTrade(closed);
          db.saveOpenTrades(STATE.openTrades);

          if (!master.stats) master.stats = { opens: 0, closes: 0, wins: 0, losses: 0, tot: 0 };
          master.stats.closes++;
          if (pct >= 0) master.stats.wins++; else master.stats.losses++;
          master.stats.tot = parseFloat(((master.stats.tot || 0) + pct).toFixed(2));
          if (!master.closedTrades) master.closedTrades = [];
          master.closedTrades = [{ symbol: sym, side, entryPrice, exitPrice, pnl: pnlUsd, pct, closeTs: Date.now(), closeTime: nowStr() }, ...master.closedTrades].slice(0, 200);
          if (STATE.lockState.manualSyms[sym] && !STATE.lockState.manualSyms[sym].baseline) lockRecordClose(sym, pnlUsd);

          broadcast({ type: 'trades', data: STATE.openTrades });
          broadcast({ type: 'closedTrades', data: STATE.closedTrades.slice(0, 100) });
          db.saveAccounts(STATE.copyAccounts);

          if (STATE._stslPlaced) delete STATE._stslPlaced[`stsl_${sym}`];

          addCopyLog('info', `📊 أُغلقت ${sym} ${pct >= 0 ? '✅' : '❌'} ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`);
          tgSend(`🔒 أُغلقت ${sym.replace('USDT', '/USDT')} ${pct >= 0 ? '✅' : '❌'} ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`, STATE.settings.cxChatClose || STATE.settings.cxChat);
        }
      }

      // فحص أي صفقة جديدة فُتحت
      for (const sym of Object.keys(currPositions)) {
        if (!prevMasterPositions[sym]) {
          if (STATE._stslPlaced) delete STATE._stslPlaced[`stsl_${sym}`];
        }
      }

      const openCount = Object.keys(currPositions).length;
      const prevCount = Object.keys(prevMasterPositions).length;
      if (prevCount > 0 && openCount < prevCount) {
        setTimeout(autoSendFromQueue, 1000);
      }
      prevMasterPositions = currPositions;

      if (STATE.settings.stSLon) await monitorSTSL();
    } catch {}
  }, 30000);

  // مراقب نظام القفل — كل ١٥ ثانية (يعمل سواء كان النسخ شغالاً أو لا)
  setInterval(async () => {
    if (!STATE.settings.lockOn) return;
    const master = STATE.copyAccounts.find(a => a.isMaster);
    if (!master?.apiKey) return;
    // حدّث المراكز إذا كان النسخ متوقفاً (وإلا syncCopy يحدّثها)
    if (!STATE.copyOn) {
      try { master.livePositions = await getPositions(master); } catch (e) { return; }
    }
    await monitorLock();
  }, 15000);

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
