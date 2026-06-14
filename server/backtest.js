// ══════════════════════════════════════════════════════════════════
//  backtest.js — محرّك الباك تيست (طبقة البيانات + المحاكاة + المقاييس)
//  يستخدم نفس signals.js الذي يستخدمه الماسح الحي → "نفس المؤشر بالضبط".
//  المحرّك يعمل على شموع مغلقة فقط (لا repaint) — وهو الأساس الصحيح.
// ══════════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const S = require('./signals');

// node-fetch v2 (نفس اعتماد المشروع)
let fetch;
try { fetch = require('node-fetch'); } catch (e) { fetch = global.fetch; }

const BASE = 'https://fapi.binance.com';
const DATA_DIR = process.env.BT_DATA_DIR || path.join(__dirname, '../data/backtest');
fs.mkdirSync(DATA_DIR, { recursive: true });

// الفريمات المسموحة فقط (حسب طلبك) — الساعة و4 ساعات فقط
const ALLOWED_TF = ['1h', '4h'];
const TF_MS = { '5m': 300000, '30m': 1800000, '1h': 3600000, '4h': 14400000 };

const FEE_RATE = parseFloat(process.env.BT_FEE || '0.0005'); // 0.05% تيكر لكل تعبئة/خروج

// ── طبقة البيانات ─────────────────────────────────────────────────
//  كل ملف: data/backtest/<SYMBOL>_<TF>.json.gz
//  المحتوى: مصفوفة [ openTime, open, high, low, close ] للشموع المغلقة
function fpath(sym, tf) { return path.join(DATA_DIR, `${sym}_${tf}.json.gz`); }

function saveCandles(sym, tf, rows) {
  const buf = zlib.gzipSync(JSON.stringify(rows));
  fs.writeFileSync(fpath(sym, tf), buf);
}

function loadCandles(sym, tf) {
  try {
    const buf = fs.readFileSync(fpath(sym, tf));
    return JSON.parse(zlib.gunzipSync(buf).toString('utf8'));
  } catch (e) { return null; }
}

function dataInfo(sym, tf) {
  const rows = loadCandles(sym, tf);
  if (!rows || !rows.length) return null;
  return { count: rows.length, from: rows[0][0], to: rows[rows.length - 1][0] };
}

// ── تصدير/استيراد كل بيانات الباك تيست كملف واحد (بدون اعتماديات) ──
//  لكل ملف: [طول الاسم u16][الاسم utf8][طول البيانات u32][البيانات (gzip جاهز)]
function exportDataBundle() {
  const files = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json.gz'));
  const parts = [];
  for (const f of files) {
    const data = fs.readFileSync(path.join(DATA_DIR, f));
    const nameBuf = Buffer.from(f, 'utf8');
    const head = Buffer.alloc(6);
    head.writeUInt16LE(nameBuf.length, 0);
    head.writeUInt32LE(data.length, 2);
    parts.push(head, nameBuf, data);
  }
  return { buffer: Buffer.concat(parts), count: files.length };
}

function importDataBundle(buf) {
  let off = 0, count = 0;
  while (off + 6 <= buf.length) {
    const nameLen = buf.readUInt16LE(off); off += 2;
    const dataLen = buf.readUInt32LE(off); off += 4;
    const name = buf.toString('utf8', off, off + nameLen); off += nameLen;
    const data = buf.subarray(off, off + dataLen); off += dataLen;
    if (!/^[A-Z0-9]+_(5m|30m|1h|4h)\.json\.gz$/.test(name)) continue;
    fs.writeFileSync(path.join(DATA_DIR, name), data);
    count++;
  }
  return { count };
}

// جلب klines مع ترقيم الصفحات (1500/طلب) — يُسقط الشمعة الجارية (غير المغلقة)
//  onWait(info) اختياري: يُستدعى عند تجاوز حد Binance (429/418) قبل إعادة المحاولة
const MAX_RATE_RETRIES = 6;
async function fetchKlinesRange(sym, tf, startMs, endMs, onWait) {
  const out = [];
  let cursor = startMs;
  const step = TF_MS[tf];
  while (cursor < endMs) {
    const url = `${BASE}/fapi/v1/klines?symbol=${sym}&interval=${tf}&startTime=${cursor}&limit=1500`;
    let d;
    for (let attempt = 1; ; attempt++) {
      const res = await fetch(url);
      if (res.status === 429 || res.status === 418) {
        if (attempt > MAX_RATE_RETRIES) throw new Error(`Binance ${res.status} (${sym} ${tf}) — توقف بعد عدة محاولات`);
        const ra = parseInt(res.headers.get('retry-after')) || 0;
        const waitSec = ra || (res.status === 418 ? 120 : Math.min(30, 5 * attempt));
        onWait && onWait({ sym, tf, status: res.status, waitSec, attempt });
        await sleep(waitSec * 1000);
        continue;
      }
      if (!res.ok) throw new Error(`Binance ${res.status} (${sym} ${tf})`);
      d = await res.json();
      break;
    }
    if (!Array.isArray(d) || d.length === 0) break;
    for (const k of d) {
      if (k[0] >= endMs) break;
      // [ openTime, open, high, low, close ]
      out.push([k[0], parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[7])]); // +quote volume
    }
    const last = d[d.length - 1][0];
    if (last <= cursor) break;        // لا تقدّم → اخرج
    cursor = last + step;
    await sleep(250);                 // احترام rate limit
    if (d.length < 1500) break;       // آخر صفحة
  }
  // أسقط الشمعة الأخيرة إن لم تُغلق بعد (openTime + step > now)
  const now = Date.now();
  while (out.length && out[out.length - 1][0] + step > now) out.pop();
  return out;
}

// تنزيل/تحديث مجموعة رموز × فريمات مع تقدّم
//  onProgress({ phase, doneUnits, totalUnits, sym, tf, candles })
async function downloadData(symbols, intervals, fromMs, toMs, onProgress) {
  const tfs = intervals.filter(t => ALLOWED_TF.includes(t));
  const totalUnits = symbols.length * tfs.length;
  let doneUnits = 0;
  for (const sym of symbols) {
    for (const tf of tfs) {
      try {
        const rows = await fetchKlinesRange(sym, tf, fromMs, toMs, w => {
          onProgress && onProgress({ phase: 'wait', doneUnits, totalUnits, sym: w.sym, tf: w.tf, status: w.status, waitSec: w.waitSec, attempt: w.attempt });
        });
        if (rows.length) saveCandles(sym, tf, rows);
        doneUnits++;
        onProgress && onProgress({ phase: 'download', doneUnits, totalUnits, sym, tf, candles: rows.length });
      } catch (e) {
        doneUnits++;
        onProgress && onProgress({ phase: 'download', doneUnits, totalUnits, sym, tf, error: e.message });
      }
    }
  }
  return { totalUnits, doneUnits };
}

// تحديث البيانات المخزّنة فقط (يجلب الشموع الجديدة منذ آخر شمعة محفوظة لكل رمز/فريم)
//  أسرع بكثير من التنزيل الكامل — مناسب للتشغيل اليومي
//  onProgress({ phase, doneUnits, totalUnits, sym, tf, candles })
const UPDATE_FALLBACK_DAYS = 90; // لو لا توجد بيانات مخزّنة لرمز ما، نزّل له هذا العمق
async function updateData(symbols, intervals, onProgress) {
  const tfs = intervals.filter(t => ALLOWED_TF.includes(t));
  const totalUnits = symbols.length * tfs.length;
  let doneUnits = 0;
  const toMs = Date.now();
  for (const sym of symbols) {
    for (const tf of tfs) {
      try {
        const existing = loadCandles(sym, tf) || [];
        const step = TF_MS[tf];
        const fromMs = existing.length ? existing[existing.length - 1][0] + step : toMs - UPDATE_FALLBACK_DAYS * 86400000;
        let added = 0;
        if (fromMs < toMs) {
          const rows = await fetchKlinesRange(sym, tf, fromMs, toMs, w => {
            onProgress && onProgress({ phase: 'wait', doneUnits, totalUnits, sym: w.sym, tf: w.tf, status: w.status, waitSec: w.waitSec, attempt: w.attempt });
          });
          if (rows.length) {
            const merged = existing.length ? existing.concat(rows.filter(r => r[0] > existing[existing.length - 1][0])) : rows;
            saveCandles(sym, tf, merged);
            added = merged.length - existing.length;
          }
        }
        doneUnits++;
        onProgress && onProgress({ phase: 'download', doneUnits, totalUnits, sym, tf, candles: added });
      } catch (e) {
        doneUnits++;
        onProgress && onProgress({ phase: 'download', doneUnits, totalUnits, sym, tf, error: e.message });
      }
    }
  }
  return { totalUnits, doneUnits };
}

// ── توليد الإشارات (شموع مغلقة) ────────────────────────────────────
//  candles: [ [t,o,h,l,c], ... ]  (مغلقة)
//  btcRegime: دالة (ts) → { ema200dir, stDir } للفلاتر العامة (BTC) — اختياري
//  يُرجع: [ { i, ts, side, type } ]  حيث i = فهرس شمعة الإشارة
function typeKeyOf(t){ return ['a70','b70'].includes(t)?'ob':['b30','a30'].includes(t)?'os':['cl','cs'].includes(t)?'conf':t==='ts'?'ts':t==='tl'?'tl':null; }

// كل الإشارات الخام (fSig=trail||sig + conf) موسومة بالنوع — بلا فلاتر نوع/اتجاه/سوق
//  محسوبة O(n): المؤشر (RSI/SMA/EMA) يُحسب مرة واحدة لكامل السلسلة بدل إعادة
//  حسابه من الصفر لكل شمعة (كان O(n²) ويجمّد السيرفر بالكامل أثناء التشغيل).
function generateRawSignals(candles, settings) {
  const closes = candles.map(c => c[4]);
  const out = []; const peaks = {};
  const mode = settings.mode, ma = parseInt(settings.maPeriod) || 14;
  const ed = !!settings.enableDiv; const minLen = S.RSI_P + 2;

  // المؤشر الكامل مرة واحدة (نفس قيم computeIndSeries لكل بادئة، بسبب السببية)
  const fullRsi = S.calcRSISeries(closes, S.RSI_P);
  const N = fullRsi.length;
  let fullInd;
  if (mode === 'RSI') {
    fullInd = fullRsi;
  } else if (mode === 'SMA') {
    fullInd = [];
    if (N >= ma) {
      let sum = 0;
      for (let k = 0; k < ma; k++) sum += fullRsi[k];
      fullInd.push(sum / ma);
      for (let k = ma; k < N; k++) { sum += fullRsi[k] - fullRsi[k - ma]; fullInd.push(sum / ma); }
    }
  } else { // EMA
    fullInd = [];
    if (N >= ma) {
      const k = 2 / (ma + 1);
      let e = fullRsi[0];
      fullInd.push(e);
      for (let idx = 1; idx < N; idx++) { e = fullRsi[idx] * k + e * (1 - k); if (idx >= ma - 1) fullInd.push(e); }
    }
  }

  const WIN = 40; // نافذة كافية لـ checkDiv (يحتاج ≤26) ولـ revCount
  for (let i = minLen; i < candles.length; i++) {
    const m = i + 1 - S.RSI_P, mPrev = m - 1;
    let L, Lp;
    if (mode === 'RSI') { L = m; Lp = mPrev; }
    else if (mode === 'SMA') { L = m >= ma ? m - ma + 1 : 0; Lp = mPrev >= ma ? mPrev - ma + 1 : 0; }
    else { L = m >= ma ? m - ma + 2 : 0; Lp = mPrev >= ma ? mPrev - ma + 2 : 0; }
    if (L < 1 || Lp < 1) continue;
    const cu = fullInd[L - 1], pv = fullInd[Lp - 1];

    const wlen = Math.min(L, WIN);
    const id = fullInd.slice(L - wlen, L);
    const cls = closes.slice(i + 1 - wlen, i + 1);

    const sig = S.detectSignal(pv, cu, cls, id, ed, settings);
    const conf = S.detectConf(pv, cu, cls, id, ed);
    const trail = S.detectTrail(peaks, '_', cu, cls, id, ed, settings);
    const fSig = trail || sig;
    if (fSig) out.push({ i, ts: candles[i][0], side: fSig.side, type: fSig.type, k: typeKeyOf(fSig.type) });
    if (conf) out.push({ i, ts: candles[i][0], side: conf.side, type: conf.type, k: typeKeyOf(conf.type) });
  }
  return out;
}

// فلترة رخيصة: أنواع مسموحة + اتجاه + نظام السوق (BTC)
function filterSignals(raw, opt, regimeFn) {
  return raw.filter(sg => {
    if (sg.k && opt.types && !opt.types[sg.k]) return false;
    if (opt.dirFilter && opt.dirFilter !== 'all' && sg.side) {
      if (sg.side !== (opt.dirFilter === 'long' ? 'LONG' : 'SHORT')) return false;
    }
    if (regimeFn && (opt.ema200FilterOn || opt.stFilterOn)) {
      const reg = regimeFn(sg.ts); let pass = true;
      if (opt.ema200FilterOn && reg && reg.ema200dir) pass = pass && (sg.side === 'LONG' ? reg.ema200dir === 'up' : reg.ema200dir === 'down');
      if (opt.stFilterOn && reg && reg.stDir) pass = pass && (sg.side === 'LONG' ? reg.stDir === 'up' : reg.stDir === 'down');
      if (!pass) return false;
    }
    return true;
  });
}

// توافق خلفي — نفس سلوك الباك تيست الفردي (ts/tl يحكمهما فلتر trail)
function generateSignals(candles, settings, btcRegime) {
  const sf = { ob:true, os:true, conf:true, trail:true, ...(settings.sigQueueFilters||{}) };
  const types = { ob:sf.ob, os:sf.os, conf:sf.conf, ts:sf.trail, tl:sf.trail };
  const raw = generateRawSignals(candles, settings);
  return filterSignals(raw, { types, dirFilter: settings.dirFilter, ema200FilterOn: settings.ema200FilterOn, stFilterOn: settings.stFilterOn }, btcRegime);
}

// ── محاكاة صفقة Cornix واحدة ───────────────────────────────────────
//  تبدأ من شمعة الدخول (sigIndex+1 = فتح الشمعة التالية).
//  ترجع { pnlPct (على الهامش، يشمل الرافعة والرسوم), exitReason, bars }
//  افتراضات (موثّقة): مستويات TP/SL من سعر الإشارة؛ الوقف أولاً عند تعارض داخل الشمعة.
function simulateTrade(candles, sigIndex, side, settings) {
  const isLong = side === 'LONG';
  const entryIdx = sigIndex + 1;
  if (entryIdx >= candles.length) return null;

  const sign = isLong ? 1 : -1;
  const lev = Math.max(1, parseInt(settings.cxLev) || 20);
  const refPrice = candles[sigIndex][4];                 // سعر الإشارة (مثل buildMsg)
  const entry1Px = candles[entryIdx][1];                 // فتح الشمعة التالية

  // أوزان الدخولات (تُطبَّع لتساوي 1)
  const e2on = !!settings.cxEntry2on, e3on = e2on && !!settings.cxEntry3on;
  let w1 = 100, w2 = e2on ? (parseFloat(settings.cxEntry2Amt) || 0) : 0, w3 = e3on ? (parseFloat(settings.cxEntry3Amt) || 0) : 0;
  if (e2on) w1 = Math.max(0, 100 - w2 - w3);
  const wSum = w1 + w2 + w3 || 1;
  w1 /= wSum; w2 /= wSum; w3 /= wSum;

  // أسعار الدخولات الإضافية (مسافة % عكسية عن سعر الإشارة)
  const e2Px = e2on ? refPrice * (1 - sign * (parseFloat(settings.cxEntry2Dist) || 0) / 100) : null;
  const e3Px = e3on ? refPrice * (1 - sign * (parseFloat(settings.cxEntry3Dist) || 0) / 100) : null;

  // مستويات الأهداف/الوقف من سعر الإشارة (نِسَب)
  const tp1Px = refPrice * (1 + sign * (parseFloat(settings.cxTP1) || 0) / 100);
  const tp2on = !!settings.cxTP2on;
  const tp2Px = tp2on ? refPrice * (1 + sign * (parseFloat(settings.cxTP2) || 0) / 100) : null;
  const tp1Frac = tp2on ? (parseFloat(settings.cxTP1Amt) || 50) / 100 : 1;
  const slOn = !!settings.cxSLon;
  let slPx = slOn ? refPrice * (1 - sign * (parseFloat(settings.cxSL) || 0) / 100) : null;

  // التريلينج (تبسيط: ستوب يتبع القمة/القاع بنسبة callbackRate بعد بدء الربح)
  const trailOn = settings.cxTrailTp === 'on';
  const trailPct = parseFloat(settings.cxTrailPct) || 0;
  const beOn = !!settings.cxBEon;

  // الحالة
  const fills = [{ px: entry1Px, w: w1 }];   // الدخول 1 مملوء فوراً
  let filledW = w1;
  let e2done = false, e3done = false;
  let remaining = 1;                          // نسبة الصفقة المفتوحة (من المملوء)
  let tp1hit = false;
  let realized = 0;                           // PnL تراكمي (نسبة سعرية موزونة)
  let entryNotionalFee = FEE_RATE * w1 * lev; // رسوم الدخول 1
  let peak = entry1Px;                        // لأعلى/أدنى للتريلينج
  let bars = 0, exitReason = 'open';
  let exitIdx = null, exitPx = null;          // للعرض البياني

  const avgEntry = () => fills.reduce((s, x) => s + x.px * x.w, 0) / fills.reduce((s, x) => s + x.w, 0);

  for (let j = entryIdx; j < candles.length; j++) {
    bars++;
    const [, , hi, lo] = candles[j];
    const close = candles[j][4];

    // 1) تعبئة دخولات إضافية إذا لمس السعر مستواها
    if (e2on && !e2done && ((isLong && lo <= e2Px) || (!isLong && hi >= e2Px))) {
      fills.push({ px: e2Px, w: w2 }); filledW += w2; e2done = true; entryNotionalFee += FEE_RATE * w2 * lev;
    }
    if (e3on && !e3done && ((isLong && lo <= e3Px) || (!isLong && hi >= e3Px))) {
      fills.push({ px: e3Px, w: w3 }); filledW += w3; e3done = true; entryNotionalFee += FEE_RATE * w3 * lev;
    }

    const ent = avgEntry();
    // مستوى التصفية (Liquidation): بهامش = 1/الرافعة — لا يمكن أن تخسر الصفقة أكثر من هامشها
    const liqPx = ent * (1 - sign / lev);
    // SL ديناميكي: بريك-إيفن بعد TP1
    let curSL = slPx;
    if (beOn && tp1hit) curSL = ent;
    // الوقف الفعّال = الأقرب من وقف المستخدم ومستوى التصفية (أيهما يُصاب أولاً)
    let stopPx = liqPx, stopReason = 'liquidation';
    if (curSL !== null && (isLong ? curSL > liqPx : curSL < liqPx)) {
      stopPx = curSL; stopReason = (beOn && tp1hit) ? 'breakeven' : 'sl';
    }

    // 2) تعارض داخل الشمعة → نفترض الوقف أولاً (تحفّظاً)
    const slHit = (isLong && lo <= stopPx) || (!isLong && hi >= stopPx);
    if (slHit) {
      const px = stopPx;
      realized += remaining * filledW * sign * (px / ent - 1);
      entryNotionalFee += FEE_RATE * remaining * filledW * lev; // رسوم الخروج
      remaining = 0; exitReason = stopReason;
      exitIdx = j; exitPx = px;
      break;
    }

    // 3) الأهداف
    const tp1Touch = (isLong && hi >= tp1Px) || (!isLong && lo <= tp1Px);
    if (!tp1hit && tp1Touch) {
      const part = remaining * tp1Frac;
      realized += part * filledW * sign * (tp1Px / ent - 1);
      entryNotionalFee += FEE_RATE * part * filledW * lev;
      remaining -= part; tp1hit = true;
      if (!tp2on) { exitReason = 'tp1'; remaining = 0; exitIdx = j; exitPx = tp1Px; break; }
    }
    if (tp2on && tp1hit && ((isLong && hi >= tp2Px) || (!isLong && lo <= tp2Px))) {
      realized += remaining * filledW * sign * (tp2Px / ent - 1);
      entryNotionalFee += FEE_RATE * remaining * filledW * lev;
      remaining = 0; exitReason = 'tp2'; exitIdx = j; exitPx = tp2Px; break;
    }

    // 4) تريلينج (تقريبي على أساس القمة/القاع وإغلاق الشمعة)
    if (trailOn && trailPct > 0 && tp1hit) {
      peak = isLong ? Math.max(peak, hi) : Math.min(peak, lo);
      const trigger = isLong ? peak * (1 - trailPct / 100) : peak * (1 + trailPct / 100);
      if ((isLong && close <= trigger) || (!isLong && close >= trigger)) {
        realized += remaining * filledW * sign * (trigger / ent - 1);
        entryNotionalFee += FEE_RATE * remaining * filledW * lev;
        remaining = 0; exitReason = 'trail'; exitIdx = j; exitPx = trigger; break;
      }
    }
  }

  // لم تُغلق حتى نهاية البيانات → أغلق على آخر إغلاق
  if (remaining > 0) {
    const ent = avgEntry();
    const px = candles[candles.length - 1][4];
    realized += remaining * filledW * sign * (px / ent - 1);
    entryNotionalFee += FEE_RATE * remaining * filledW * lev;
    exitReason = exitReason === 'open' ? 'expired' : exitReason;
    exitIdx = candles.length - 1; exitPx = px;
  }

  // العائد على الهامش = (الحركة السعرية الموزونة × الرافعة) − الرسوم
  const grossPct = realized * lev * 100;
  const pnlPct = grossPct - entryNotionalFee * 100;
  return {
    pnlPct: parseFloat(pnlPct.toFixed(4)), exitReason, bars, filledW: parseFloat(filledW.toFixed(3)), entryIdx,
    // تفاصيل إضافية للعرض البياني (مستويات الأسعار + نقطة الخروج)
    entryTs: candles[entryIdx][0], entryPx: parseFloat(avgEntry().toFixed(8)), refPrice,
    tp1Px: parseFloat(tp1Px.toFixed(8)), tp2Px: tp2Px !== null ? parseFloat(tp2Px.toFixed(8)) : null,
    slPx: slPx !== null ? parseFloat(slPx.toFixed(8)) : null,
    exitIdx, exitTs: exitIdx !== null ? candles[exitIdx][0] : null, exitPx: exitPx !== null ? parseFloat(exitPx.toFixed(8)) : null,
  };
}

// ── احترام المؤشر: هل انعكس السعر باتجاه الإشارة خلال REV_LOOKBACK شمعة؟ ──
//  respected = أفضل حركة باتجاه الإشارة (high لـ LONG / low لـ SHORT) >= REV_THRESHOLD%
const REV_LOOKBACK = 5, REV_THRESHOLD = 3;
function reversalStats(sigs, candles) {
  let total = 0, respected = 0, sumPct = 0, sumBarsResp = 0, maxPct = -Infinity, maxBars = 0;
  for (const sg of sigs) {
    const refPrice = candles[sg.i][4];
    let bestPct = -Infinity, bestBar = 0;
    for (let b = 1; b <= REV_LOOKBACK; b++) {
      const idx = sg.i + b;
      if (idx >= candles.length) break;
      const h = candles[idx][2], l = candles[idx][3];
      const pct = sg.side === 'LONG' ? (h - refPrice) / refPrice * 100 : (refPrice - l) / refPrice * 100;
      if (pct > bestPct) { bestPct = pct; bestBar = b; }
    }
    if (bestPct === -Infinity) continue; // لا توجد شموع بعد الإشارة (نهاية البيانات)
    total++;
    sumPct += bestPct;
    if (bestPct >= REV_THRESHOLD) { respected++; sumBarsResp += bestBar; }
    if (bestPct > maxPct) { maxPct = bestPct; maxBars = bestBar; }
  }
  return {
    total, respected, notRespected: total - respected,
    respectRate: total ? parseFloat((respected / total * 100).toFixed(1)) : 0,
    avgReversalPct: total ? parseFloat((sumPct / total).toFixed(2)) : 0,
    avgRespectedBars: respected ? parseFloat((sumBarsResp / respected).toFixed(1)) : null,
    maxReversalPct: total ? parseFloat(maxPct.toFixed(2)) : 0,
    maxReversalBars: total ? maxBars : 0,
  };
}

// ── باك تيست رمز واحد ──────────────────────────────────────────────
//  صفقة واحدة كحد أقصى لكل رمز في نفس الوقت (واقعي)
function runSymbol(candles, settings, btcRegime) {
  const sigs = generateSignals(candles, settings, btcRegime);
  const trades = [];
  let busyUntil = -1;
  for (const sg of sigs) {
    if (sg.i <= busyUntil) continue;       // صفقة مفتوحة → تجاهل الإشارة
    const tr = simulateTrade(candles, sg.i, sg.side, settings);
    if (!tr) continue;
    busyUntil = tr.entryIdx + tr.bars - 1;
    trades.push({ ...tr, side: sg.side, type: sg.type, ts: sg.ts });
  }
  return { trades, sigStats: reversalStats(sigs, candles), sigs };
}

// ── بيانات شارت التحقق: شموع + المؤشر + كل الإشارات + الصفقات لرمز واحد ──
//  تُستخدم لعرض شارت (يطابق TradingView) يبيّن أين ظهرت كل إشارة وأين
//  دخلت/خرجت كل صفقة، للتحقق اليدوي من صحة المؤشر والمحاكاة.
function getSymbolChartData(sym, settings, btcRegime) {
  const tf = ALLOWED_TF.includes(settings.interval) ? settings.interval : '1h';
  const candles = loadCandles(sym, tf);
  if (!candles || !candles.length) return null;

  const sigs = generateSignals(candles, settings, btcRegime);

  // محاكاة صفقة مستقلة لكل إشارة (بصرف النظر عن busyUntil) — تتيح للواجهة
  // إعادة حساب «صفقة واحدة بنفس الوقت» عند اختيار أنواع إشارات معيّنة فقط
  const signals = sigs.map(s => {
    const tr = simulateTrade(candles, s.i, s.side, settings);
    return {
      i: s.i, ts: s.ts, side: s.side, type: s.type,
      trade: tr ? {
        entryIdx: tr.entryIdx,
        exitIdx: Math.min(tr.entryIdx + tr.bars - 1, candles.length - 1),
        exitReason: tr.exitReason, pnlPct: tr.pnlPct,
      } : null,
    };
  });

  // صفقة واحدة كحد أقصى بنفس الوقت (لكل الأنواع معاً — الحالة الافتراضية)
  const trades = [];
  let busyUntil = -1;
  for (const sg of signals) {
    if (sg.i <= busyUntil || !sg.trade) continue;
    trades.push({ entryIdx: sg.trade.entryIdx, exitIdx: sg.trade.exitIdx, side: sg.side, type: sg.type, exitReason: sg.trade.exitReason, pnlPct: sg.trade.pnlPct });
    busyUntil = sg.trade.exitIdx;
  }

  // المؤشر (RSI/SMA/EMA) محاذٍ لفهارس الشموع — للعرض في لوحة سفلية
  const ma = parseInt(settings.maPeriod) || 14;
  const offset = settings.mode === 'RSI' ? S.RSI_P : S.RSI_P + ma - 1;
  const indSeries = S.computeIndSeries(candles.map(c => c[4]), settings.mode, ma);
  const indicator = candles.map((_, i) => {
    const k = i - offset;
    return (k >= 0 && k < indSeries.length) ? parseFloat(indSeries[k].toFixed(2)) : null;
  });

  return { tf, candles, indicator, signals, trades };
}

// ── باك تيست رمز واحد بتفاصيل كاملة (للعرض على الشارت) ──
//  يُرجع كل الإشارات المفلترة (مع موقعها وسعرها وهل تحوّلت لصفقة) + الصفقات
//  بمستويات TP1/TP2/SL ونقطة الخروج، لمطابقتها يدوياً مع المؤشر الحي
function runSymbolDetailed(candles, settings, btcRegime) {
  const raw = generateRawSignals(candles, settings);
  const sf = { ob: true, os: true, conf: true, trail: true, ...(settings.sigQueueFilters || {}) };
  const types = { ob: sf.ob, os: sf.os, conf: sf.conf, ts: sf.trail, tl: sf.trail };
  const sigs = filterSignals(raw, { types, dirFilter: settings.dirFilter, ema200FilterOn: settings.ema200FilterOn, stFilterOn: settings.stFilterOn }, btcRegime);

  const trades = [];
  let busyUntil = -1;
  const signals = sigs.map(sg => {
    let taken = false;
    if (sg.i > busyUntil) {
      const tr = simulateTrade(candles, sg.i, sg.side, settings);
      if (tr) {
        busyUntil = tr.entryIdx + tr.bars - 1;
        trades.push({ ...tr, side: sg.side, type: sg.type, ts: sg.ts });
        taken = true;
      }
    }
    return { i: sg.i, ts: sg.ts, side: sg.side, type: sg.type, k: sg.k, price: candles[sg.i][4], taken };
  });

  // إحصاءات احترام إشارات Trailing (من كامل الإشارات الخام، بصرف النظر عن فلتر trail)
  const sigStats = {
    ts: { trStart: settings.trSstart, trGap: settings.trSgap, ...reversalStats(raw.filter(s => s.k === 'ts'), candles) },
    tl: { trStart: settings.trLstart, trGap: settings.trLgap, ...reversalStats(raw.filter(s => s.k === 'tl'), candles) },
  };
  return { signals, trades, sigStats };
}

// ── بيانات شارت كاملة لرمز: شموع + إشارات + صفقات + مقاييس ──
function getChartData(symbol, settings, btcRegime) {
  const tf = ALLOWED_TF.includes(settings.interval) ? settings.interval : '1h';
  const candles = loadCandles(symbol, tf);
  if (!candles || !candles.length) return { error: `لا توجد بيانات مخزّنة لـ ${symbol} على فريم ${tf} — نزّل البيانات أولاً` };
  const { signals, trades, sigStats } = runSymbolDetailed(candles, settings, btcRegime);
  const capFrac = (parseFloat(String(settings.cxAmt).replace('%', '')) || 1) / 100;
  const metrics = computeMetrics(trades, capFrac);
  return { tf, candles, signals, trades, metrics, sigStats };
}

// ── مقاييس مجمّعة ─────────────────────────────────────────────────
//  capFrac = نسبة رأس المال لكل صفقة (cxAmt مثل "1%") — للمنحنى المركّب
function computeMetrics(trades, capFrac = 0.01) {
  const n = trades.length;
  if (!n) return { trades: 0, winRate: 0, netReturnPct: 0, profitFactor: 0, maxDrawdownPct: 0, avgPnlPct: 0, equity: [1] };
  let wins = 0, grossWin = 0, grossLoss = 0, sumPnl = 0;
  let equity = 1, peak = 1, maxDD = 0;
  const curve = [1];
  // رتّب زمنياً
  const sorted = [...trades].sort((a, b) => a.ts - b.ts);
  for (const t of sorted) {
    if (t.pnlPct >= 0) { wins++; grossWin += t.pnlPct; } else { grossLoss += -t.pnlPct; }
    sumPnl += t.pnlPct;
    equity *= (1 + (t.pnlPct / 100) * capFrac);
    curve.push(parseFloat(equity.toFixed(6)));
    if (equity > peak) peak = equity;
    const dd = (peak - equity) / peak;
    if (dd > maxDD) maxDD = dd;
  }
  return {
    trades: n,
    winRate: parseFloat((wins / n * 100).toFixed(2)),
    netReturnPct: parseFloat(((equity - 1) * 100).toFixed(2)),
    profitFactor: grossLoss === 0 ? (grossWin > 0 ? 999 : 0) : parseFloat((grossWin / grossLoss).toFixed(3)),
    maxDrawdownPct: parseFloat((maxDD * 100).toFixed(2)),
    avgPnlPct: parseFloat((sumPnl / n).toFixed(3)),
    equity: curve,
  };
}

// باك تيست محفظة كاملة (كل الرموز، إعداد واحد)
function runBacktest(dataset, settings, btcRegime) {
  // dataset: { SYM: candles[] }
  let all = [];
  const signalStats = {};
  for (const sym of Object.keys(dataset)) {
    const { trades, sigStats } = runSymbol(dataset[sym], settings, btcRegime);
    all = all.concat(trades.map(t => ({ ...t, sym })));
    if (sigStats.total) signalStats[sym] = sigStats;
  }
  const capFrac = (parseFloat(String(settings.cxAmt).replace('%', '')) || 1) / 100;
  const metrics = computeMetrics(all, capFrac);
  return { metrics, trades: all, signalStats };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = {
  ALLOWED_TF, TF_MS, DATA_DIR,
  saveCandles, loadCandles, dataInfo,
  fetchKlinesRange, downloadData, updateData,
  exportDataBundle, importDataBundle,
  generateSignals, generateRawSignals, filterSignals, simulateTrade, runSymbol, runSymbolDetailed, getChartData, computeMetrics, runBacktest,
  getSymbolChartData,
};

// ══════════════════════════════════════════════════════════════════
//  المُحسِّن التلقائي — بحث محدود بميزانية يغطّي كل بارامترات الموشر
//  (يشمل عتبات Trailing RSI، وفريم/فترة/مضاعف فلتر EMA200+SuperTrend)
// ══════════════════════════════════════════════════════════════════

function superTrendSeries(klines, period, mult) {
  const n = klines.length, dir = new Array(n).fill(null);
  if (n < period + 2) return dir;
  const H = klines.map(k => +k[2]), L = klines.map(k => +k[3]), C = klines.map(k => +k[4]);
  const atr = new Array(n).fill(0);
  let s = 0; for (let i = 1; i <= period; i++) s += Math.max(H[i]-L[i], Math.abs(H[i]-C[i-1]), Math.abs(L[i]-C[i-1]));
  atr[period] = s/period;
  for (let i = period+1; i < n; i++) { const tr = Math.max(H[i]-L[i], Math.abs(H[i]-C[i-1]), Math.abs(L[i]-C[i-1])); atr[i] = (atr[i-1]*(period-1)+tr)/period; }
  let pUB=0,pLB=0,pDir=1;
  for (let i = period; i < n; i++) {
    const hl2=(H[i]+L[i])/2, bUB=hl2+mult*atr[i], bLB=hl2-mult*atr[i];
    const ub = i===period?bUB:(bUB<pUB||C[i-1]>pUB?bUB:pUB);
    const lb = i===period?bLB:(bLB>pLB||C[i-1]<pLB?bLB:pLB);
    let d; if (i===period) d=C[i]>lb?1:-1; else if (pDir===-1&&C[i]>pUB) d=1; else if (pDir===1&&C[i]<pLB) d=-1; else d=pDir;
    dir[i]=d===1?'up':'down'; pUB=ub; pLB=lb; pDir=d;
  }
  return dir;
}
function emaDirSeries(closes, period = 200) {
  const n = closes.length, dir = new Array(n).fill(null);
  if (n < period) return dir;
  const k = 2/(period+1); let e = closes[0];
  for (let i = 1; i < n; i++) { e = closes[i]*k + e*(1-k); if (i >= period-1) dir[i] = closes[i] > e ? 'up' : 'down'; }
  return dir;
}
function buildBtcRegime(btcCandles, ema200TF, stTF, stPeriod, stMult) {
  const closes = btcCandles.map(c => c[4]);
  const emaDir = emaDirSeries(closes, 200);
  const stDir = superTrendSeries(btcCandles, stPeriod || 10, stMult || 3);
  const ts = btcCandles.map(c => c[0]);
  return (t) => {
    let lo = 0, hi = ts.length - 1, idx = -1;
    while (lo <= hi) { const m = (lo+hi)>>1; if (ts[m] <= t) { idx = m; lo = m+1; } else hi = m-1; }
    if (idx < 0) return { ema200dir: null, stDir: null };
    return { ema200dir: emaDir[idx], stDir: stDir[idx] };
  };
}

function listStoredSymbols(tf) {
  try { return fs.readdirSync(DATA_DIR).filter(f => f.endsWith(`_${tf}.json.gz`)).map(f => f.replace(`_${tf}.json.gz`, '')); }
  catch (e) { return []; }
}
function loadDatasetByTf(symbols, tfs) {
  const out = {};
  for (const tf of tfs) { out[tf] = {}; for (const sym of symbols) { const c = loadCandles(sym, tf); if (c && c.length) out[tf][sym] = c; } }
  return out;
}
function loadBtcByTf(tfs) { const out = {}; for (const tf of tfs) { const c = loadCandles('BTCUSDT', tf); if (c) out[tf] = c; } return out; }

// ── مساحة البحث الكاملة (كل بارامترات الموشر) ──
const SPACE = {
  // طبقة الإشارة (غالية)
  interval: ['1h', '4h'],
  mode: ['RSI', 'SMA', 'EMA'],
  ma: [7, 14, 21],
  rev: [['candles',1], ['candles',2], ['rsi',1]],
  div: [false, true],
  // Trailing RSI: مطفي/شورت/لونق/الاثنين — مع عتبات (start/gap)
  trail: [
    { m:'off' },
    { m:'S', ss:75, sg:3 }, { m:'S', ss:70, sg:5 },
    { m:'L', ls:25, lg:3 }, { m:'L', ls:30, lg:5 },
    { m:'both', ss:75, sg:3, ls:25, lg:3 }, { m:'both', ss:70, sg:5, ls:30, lg:5 },
  ],
  // طبقة رخيصة
  ob: [true, false], os: [true, false], conf: [true, false],
  regimeMode: ['off', 'ema', 'st', 'both'],
  filterTF: ['1h', '4h'],                            // فريم فلتر EMA200/SuperTrend (مثل ema200TF/stTF)
  stP: [7, 10, 14], stM: [2, 3],                     // فترة/مضاعف SuperTrend
  dir: ['all', 'long', 'short'],
  tp1: [2, 3, 5, 8], tp1Amt: [50, 100],
  sl: [['on',2], ['on',3], ['on',5], ['off',0]],
  tp2: [['off',0], ['on',6], ['on',10]],
  entry2: [['off',0], ['on',1.5], ['on',3]],
  trailExit: [['off',0], ['on',0.5], ['on',1]],
  be: [false, true],
  lev: [10, 20, 50],
  liqMin: [0, 10e6, 50e6],
};

function buildSettings(sl, ch) {
  const tr = sl.trail;
  return {
    interval: sl.interval, mode: sl.mode, maPeriod: sl.mode === 'RSI' ? 14 : sl.ma,
    revMode: sl.rev[0], revCount: sl.rev[1], enableDiv: sl.div,
    trSon: tr.m === 'S' || tr.m === 'both', trLon: tr.m === 'L' || tr.m === 'both',
    trSstart: tr.ss ?? 75, trSgap: tr.sg ?? 3, trLstart: tr.ls ?? 25, trLgap: tr.lg ?? 3,
    sigQueueFilters: { ob: ch.ob, os: ch.os, conf: ch.conf, trail: true },
    dirFilter: ch.dir,
    ema200FilterOn: ch.regimeMode === 'ema' || ch.regimeMode === 'both',
    stFilterOn: ch.regimeMode === 'st' || ch.regimeMode === 'both',
    ema200TF: ch.filterTF, stTF: ch.filterTF, stPeriod: ch.stP, stMult: ch.stM,
    cxTP1: ch.tp1, cxTP1Amt: ch.tp1Amt,
    cxSLon: ch.sl[0] === 'on', cxSL: ch.sl[1],
    cxTP2on: ch.tp2[0] === 'on', cxTP2: ch.tp2[1], cxTP2Amt: 50,
    cxEntry2on: ch.entry2[0] === 'on', cxEntry2Dist: ch.entry2[1], cxEntry2Amt: 50, cxEntry3on: false,
    cxTrailTp: ch.trailExit[0], cxTrailPct: ch.trailExit[1], cxBEon: ch.be,
    cxLev: ch.lev, cxAmt: '1%',
  };
}
function comboView(sl, ch) {
  const tr = sl.trail;
  const trLbl = tr.m === 'off' ? '' : tr.m === 'both' ? `+trail:both(S:${tr.ss}/${tr.sg},L:${tr.ls}/${tr.lg})` : `+trail:${tr.m}(${tr.m==='S'?tr.ss+'/'+tr.sg:tr.ls+'/'+tr.lg})`;
  const types = [ch.ob && 'ob', ch.os && 'os', ch.conf && 'conf'].filter(Boolean).join('+') + trLbl || '—';
  const regime = ch.regimeMode === 'off' ? 'off' : `${ch.regimeMode}@${ch.filterTF}${ch.regimeMode!=='ema'?'('+ch.stP+','+ch.stM+')':''}`;
  return {
    interval: sl.interval, mode: sl.mode, maPeriod: sl.mode === 'RSI' ? 14 : sl.ma,
    sigPreset: types, regime, dir: ch.dir,
    cxTP1: ch.tp1, sl: ch.sl, tp2: ch.tp2, entry2: ch.entry2, cxLev: ch.lev,
    trailExit: ch.trailExit, be: ch.be, liqMin: ch.liqMin,
  };
}

const rnd = arr => arr[(Math.random() * arr.length) | 0];

function signalCombos() {
  const out = [];
  for (const interval of SPACE.interval)
    for (const mode of SPACE.mode)
      for (const ma of (mode === 'RSI' ? [14] : SPACE.ma))
        for (const rev of SPACE.rev)
          for (const div of SPACE.div)
            for (const trail of SPACE.trail)
              out.push({ interval, mode, ma, rev, div, trail });
  return out;
}

// ── أفضل توليفات "مؤشر" (مود/فترة/انعكاس/دايفرجنس) من نتيجة الفحص العام، لكل منها على فريم 1h و4h ──
function topRecipes(leaderboard, intervals = ['1h', '4h'], maxRecipes = 2) {
  const seen = new Set(); const recipes = [];
  for (const e of (leaderboard || [])) {
    const s = e.settings || {};
    const key = JSON.stringify([s.mode, s.maPeriod, s.revMode, s.revCount, !!s.enableDiv]);
    if (seen.has(key)) continue;
    seen.add(key);
    recipes.push({ mode: s.mode, maPeriod: s.maPeriod, revMode: s.revMode, revCount: s.revCount, enableDiv: !!s.enableDiv });
    if (recipes.length >= maxRecipes) break;
  }
  const out = [];
  for (const r of recipes) for (const iv of intervals) out.push({ ...r, interval: iv });
  return out;
}

async function optimize(datasetByTf, btcByTf, opts = {}) {
  const minTrades = opts.minTrades ?? 30;
  const budgetMs = opts.budgetMs ?? (24 * 3600 * 1000);
  const onProgress = opts.onProgress || (() => {});
  const capFrac = opts.capFrac ?? 0.01;

  const combos = signalCombos();
  const totalSig = combos.length;
  const sliceMs = budgetMs / Math.max(1, totalSig);
  const t0 = Date.now();
  let sigDone = 0, evaluated = 0;
  const leaderboard = [];
  const regimeStore = {};
  const avgVolByTf = {};

  function pushLeader(e) { leaderboard.push(e); leaderboard.sort((a, b) => b.score - a.score); if (leaderboard.length > 10) leaderboard.length = 10; }
  function regimeFor(ftf, stP, stM) {
    const key = ftf + '|' + stP + '|' + stM;
    if (!regimeStore[key]) {
      const btc = btcByTf[ftf] || btcByTf['4h'] || [];
      regimeStore[key] = btc.length ? buildBtcRegime(btc, ftf, ftf, stP, stM) : (() => ({ ema200dir: null, stDir: null }));
    }
    return regimeStore[key];
  }
  function avgVol(tf, sym, candles) {
    avgVolByTf[tf] = avgVolByTf[tf] || {};
    if (avgVolByTf[tf][sym] === undefined) {
      let sum = 0, cnt = 0; for (const c of candles) { const v = c[5]; if (v != null && !isNaN(v)) { sum += v; cnt++; } }
      avgVolByTf[tf][sym] = cnt ? sum / cnt : 0;
    }
    return avgVolByTf[tf][sym];
  }

  for (const sl of combos) {
    if (opts.shouldStop && opts.shouldStop()) break;
    const dataset = datasetByTf[sl.interval] || {};
    const symbols = Object.keys(dataset);
    if (!symbols.length) { sigDone++; continue; }

    // توليد الإشارات الخام مرة واحدة لهذه التوليفة (بعتبات Trailing من sl)
    const rawSet = buildSettings(sl, { ob:true, os:true, conf:true, regimeMode:'off', filterTF:'4h', stP:10, stM:3, dir:'all', tp1:3, tp1Amt:50, sl:['on',3], tp2:['off',0], entry2:['off',0], trailExit:['off',0], be:false, lev:10, liqMin:0 });
    const raw = {};
    for (let si = 0; si < symbols.length; si++) {
      raw[symbols[si]] = generateRawSignals(dataset[symbols[si]], rawSet);
      if (si % 20 === 19) await new Promise(r => setImmediate(r));
    }
    await new Promise(r => setImmediate(r));

    const comboStart = Date.now();
    let innerN = 0;
    while (Date.now() - comboStart < sliceMs && innerN < 6000) {
      let ob = rnd(SPACE.ob), os = rnd(SPACE.os), conf = rnd(SPACE.conf);
      if (!ob && !os && !conf && sl.trail.m === 'off') ob = true;
      const regimeMode = rnd(SPACE.regimeMode);
      let ftf = '4h', stP = 10, stM = 3, regimeFn = null;
      if (regimeMode !== 'off') { ftf = rnd(SPACE.filterTF); stP = rnd(SPACE.stP); stM = rnd(SPACE.stM); regimeFn = regimeFor(ftf, stP, stM); }
      const ch = {
        ob, os, conf, regimeMode, filterTF: ftf, stP, stM, dir: rnd(SPACE.dir),
        tp1: rnd(SPACE.tp1), tp1Amt: rnd(SPACE.tp1Amt), sl: rnd(SPACE.sl), tp2: rnd(SPACE.tp2),
        entry2: rnd(SPACE.entry2), trailExit: rnd(SPACE.trailExit), be: rnd(SPACE.be), lev: rnd(SPACE.lev), liqMin: rnd(SPACE.liqMin),
      };
      const set = buildSettings(sl, ch);
      const fopt = { types: { ob: ch.ob, os: ch.os, conf: ch.conf, ts: true, tl: true }, dirFilter: ch.dir, ema200FilterOn: set.ema200FilterOn, stFilterOn: set.stFilterOn };

      let all = [];
      for (const sym of symbols) {
        if (ch.liqMin > 0 && avgVol(sl.interval, sym, dataset[sym]) < ch.liqMin) continue;
        const sigs = filterSignals(raw[sym], fopt, regimeFn);
        let busy = -1;
        for (const sg of sigs) {
          if (sg.i <= busy) continue;
          const tr = simulateTrade(dataset[sym], sg.i, sg.side, set);
          if (!tr) continue;
          busy = tr.entryIdx + tr.bars - 1;
          all.push({ ...tr, side: sg.side, ts: sg.ts });
        }
      }
      const m = computeMetrics(all, capFrac);
      let score = -1e9;
      if (m.trades >= minTrades) score = m.netReturnPct / (1 + m.maxDrawdownPct / 100);
      pushLeader({ score: parseFloat(score.toFixed(3)), settings: set, combo: comboView(sl, ch), metrics: m });
      evaluated++; innerN++;

      if (evaluated % 200 === 0) {
        const el = Date.now() - t0;
        const eta = sigDone > 0 ? Math.round(el / (sigDone + 0.001) * (totalSig - sigDone)) : Math.max(0, budgetMs - el);
        onProgress({ done: Math.min(sigDone, totalSig), total: totalSig, evaluated, elapsedMs: el, etaMs: Math.min(eta, Math.max(0, budgetMs - el)), best: leaderboard[0] || null });
        await new Promise(r => setImmediate(r));
        if (el > budgetMs) { sigDone = totalSig; break; }
      }
    }
    sigDone = Math.min(sigDone + 1, totalSig);
    if (Date.now() - t0 > budgetMs) break;
  }

  const elapsedMs = Date.now() - t0;
  onProgress({ done: Math.min(sigDone, totalSig), total: totalSig, evaluated, elapsedMs, etaMs: 0, best: leaderboard[0] || null, finished: true });
  return { best: leaderboard[0] || null, leaderboard, evaluated, elapsedMs, totalSig };
}

// ── شبكات ضبط Trailing RSI لكل عملة (إيجاد العتبة/المسافة التي تجعل الإشارة تقع عند أفضل قمة/قاع) ──
const TRAIL_S_GRID = []; for (const ss of [60, 65, 70, 75, 80, 85]) for (const sg of [2, 3, 4, 5]) TRAIL_S_GRID.push([ss, sg]);
const TRAIL_L_GRID = []; for (const ls of [40, 35, 30, 25, 20, 15]) for (const lg of [2, 3, 4, 5]) TRAIL_L_GRID.push([ls, lg]);
const REV_MIN_SIG = 5; // أقل عدد إشارات لاعتماد متوسط الانعكاس بثقة

const RAW_DEFAULTS = { ob: true, os: true, conf: true, regimeMode: 'off', filterTF: '4h', stP: 10, stM: 3, dir: 'all', tp1: 3, tp1Amt: 50, sl: ['on', 3], tp2: ['off', 0], entry2: ['off', 0], trailExit: ['off', 0], be: false, lev: 10, liqMin: 0 };

// يبحث في شبكة عتبات Trailing (شورت أو لونق) لإيجاد القيمة التي تُولِّد إشارات أقرب لقمم/قيعان السعر
async function tuneTrailSide(candles, baseSl, side) {
  const grid = side === 'S' ? TRAIL_S_GRID : TRAIL_L_GRID;
  const k = side === 'S' ? 'ts' : 'tl';
  let best = null;
  for (const [lvl, gap] of grid) {
    const trail = side === 'S' ? { m: 'S', ss: lvl, sg: gap } : { m: 'L', ls: lvl, lg: gap };
    const rawSet = buildSettings({ ...baseSl, trail }, RAW_DEFAULTS);
    const raw = generateRawSignals(candles, rawSet);
    const stats = reversalStats(raw.filter(s => s.k === k), candles);
    const score = stats.total > 0 ? stats.avgReversalPct * Math.min(1, stats.total / REV_MIN_SIG) : -Infinity;
    if (!best || score > best.score) best = { lvl, gap, stats, score };
    await new Promise(r => setImmediate(r));
  }
  return best;
}

// ── فحص لكل عملة على حدة: يضبط عتبات Trailing لتقع الإشارات عند أفضل قمة/قاع، ثم يبحث عن أفضل إدارة صفقة ──
async function optimizePerSymbol(datasetByTf, btcByTf, recipes, opts = {}) {
  const minTrades = opts.minTrades ?? 5;
  const budgetMs = opts.budgetMs ?? (3 * 3600 * 1000);
  const onProgress = opts.onProgress || (() => {});
  const capFrac = opts.capFrac ?? 0.01;

  const symSet = new Set();
  for (const r of recipes) Object.keys(datasetByTf[r.interval] || {}).forEach(s => symSet.add(s));
  const symbols = [...symSet];
  const total = Math.max(1, symbols.length * recipes.length);
  const sliceMs = budgetMs / total;
  const t0 = Date.now();
  let done = 0;
  const regimeStore = {};
  function regimeFor(ftf, stP, stM) {
    const key = ftf + '|' + stP + '|' + stM;
    if (!regimeStore[key]) {
      const btc = btcByTf[ftf] || btcByTf['4h'] || [];
      regimeStore[key] = btc.length ? buildBtcRegime(btc, ftf, ftf, stP, stM) : (() => ({ ema200dir: null, stDir: null }));
    }
    return regimeStore[key];
  }

  const bySymbol = {};
  for (const sym of symbols) {
    if (opts.shouldStop && opts.shouldStop()) break;
    let bestForSym = null;
    for (const r of recipes) {
      if (opts.shouldStop && opts.shouldStop()) break;
      const candles = (datasetByTf[r.interval] || {})[sym];
      if (candles && candles.length) {
        const baseSl = { interval: r.interval, mode: r.mode, ma: r.maPeriod, rev: [r.revMode, r.revCount], div: r.enableDiv, trail: { m: 'off' } };

        // 1) ضبط عتبات Trailing لهذه العملة (شورت ولونق كل على حدة)
        const tunedS = await tuneTrailSide(candles, baseSl, 'S');
        const tunedL = await tuneTrailSide(candles, baseSl, 'L');
        const sl = { ...baseSl, trail: { m: 'both', ss: tunedS.lvl, sg: tunedS.gap, ls: tunedL.lvl, lg: tunedL.gap } };
        const raw = generateRawSignals(candles, buildSettings(sl, RAW_DEFAULTS));

        // 2) بحث عشوائي لإعدادات الإدارة (TP/SL/دخول2/TP2/BE/رافعة/فلتر السوق)
        const comboStart = Date.now();
        let innerN = 0; let bestMgmt = null;
        while (Date.now() - comboStart < sliceMs && innerN < 1500) {
          let ob = rnd(SPACE.ob), os = rnd(SPACE.os), conf = rnd(SPACE.conf);
          if (!ob && !os && !conf) ob = true;
          const regimeMode = rnd(SPACE.regimeMode);
          let ftf = '4h', stP = 10, stM = 3, regimeFn = null;
          if (regimeMode !== 'off') { ftf = rnd(SPACE.filterTF); stP = rnd(SPACE.stP); stM = rnd(SPACE.stM); regimeFn = regimeFor(ftf, stP, stM); }
          const ch = {
            ob, os, conf, regimeMode, filterTF: ftf, stP, stM, dir: rnd(SPACE.dir),
            tp1: rnd(SPACE.tp1), tp1Amt: rnd(SPACE.tp1Amt), sl: rnd(SPACE.sl), tp2: rnd(SPACE.tp2),
            entry2: rnd(SPACE.entry2), trailExit: rnd(SPACE.trailExit), be: rnd(SPACE.be), lev: rnd(SPACE.lev), liqMin: 0,
          };
          const set = buildSettings(sl, ch);
          const fopt = { types: { ob: ch.ob, os: ch.os, conf: ch.conf, ts: true, tl: true }, dirFilter: ch.dir, ema200FilterOn: set.ema200FilterOn, stFilterOn: set.stFilterOn };

          const sigs = filterSignals(raw, fopt, regimeFn);
          let busy = -1; const all = [];
          for (const sg of sigs) {
            if (sg.i <= busy) continue;
            const tr = simulateTrade(candles, sg.i, sg.side, set);
            if (!tr) continue;
            busy = tr.entryIdx + tr.bars - 1;
            all.push({ ...tr, side: sg.side, ts: sg.ts });
          }
          const m = computeMetrics(all, capFrac);
          if (m.trades >= minTrades) {
            const score = m.netReturnPct / (1 + m.maxDrawdownPct / 100);
            if (!bestMgmt || score > bestMgmt.score) bestMgmt = { score: parseFloat(score.toFixed(3)), settings: set, combo: comboView(sl, ch), metrics: m };
          }
          innerN++;
          if (innerN % 25 === 0) {
            const elapsedMs = Date.now() - t0;
            onProgress({ done, total, sym, elapsedMs, etaMs: Math.max(0, budgetMs - elapsedMs), best: (bestMgmt ? { metrics: bestMgmt.metrics } : bestForSym) });
            await new Promise(r => setImmediate(r));
          }
        }

        const recipeScore = (tunedS.stats.total ? tunedS.stats.avgReversalPct : 0) + (tunedL.stats.total ? tunedL.stats.avgReversalPct : 0);
        const candidate = {
          recipeScore,
          sigStats: {
            ob: reversalStats(raw.filter(s => s.k === 'ob'), candles),
            os: reversalStats(raw.filter(s => s.k === 'os'), candles),
            conf: reversalStats(raw.filter(s => s.k === 'conf'), candles),
            ts: { trStart: tunedS.lvl, trGap: tunedS.gap, ...tunedS.stats },
            tl: { trStart: tunedL.lvl, trGap: tunedL.gap, ...tunedL.stats },
          },
          settings: bestMgmt ? bestMgmt.settings : buildSettings(sl, RAW_DEFAULTS),
          combo: bestMgmt ? bestMgmt.combo : comboView(sl, RAW_DEFAULTS),
          metrics: bestMgmt ? bestMgmt.metrics : null,
          score: bestMgmt ? bestMgmt.score : null,
        };
        if (!bestForSym || candidate.recipeScore > bestForSym.recipeScore) bestForSym = candidate;
      }
      done++;
      const elapsedMs = Date.now() - t0;
      onProgress({ done, total, sym, elapsedMs, etaMs: Math.max(0, budgetMs - elapsedMs), best: bestForSym });
      await new Promise(r => setImmediate(r));
    }
    if (bestForSym) bySymbol[sym] = bestForSym;
    if (Date.now() - t0 > budgetMs) break;
  }

  const elapsedMs = Date.now() - t0;
  onProgress({ done: total, total, elapsedMs, etaMs: 0, finished: true });
  return { bySymbol, symbolsScanned: Object.keys(bySymbol).length, totalSymbols: symbols.length, elapsedMs };
}

Object.assign(module.exports, {
  superTrendSeries, emaDirSeries, buildBtcRegime,
  listStoredSymbols, loadDatasetByTf, loadBtcByTf,
  SPACE, signalCombos, optimize, topRecipes, optimizePerSymbol,
});
