// ══════════════════════════════════════════════════════════════════
//  signals.js — منطق المؤشر والإشارات (نسخة واحدة مشتركة)
//  مستخرَج حرفياً من index.js. يستورده الماسح الحي والباك تيست معاً
//  لضمان أن المؤشر "نفسه بالضبط".
//
//  الفرق الوحيد عن index.js: الدوال تأخذ `settings` و`peaks` كوسائط
//  بدل القراءة من STATE العامة — حتى يقدر الباك تيست يجرّب إعدادات
//  مختلفة. المنطق الرياضي لم يتغيّر إطلاقاً.
// ══════════════════════════════════════════════════════════════════

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

// settings: { revMode, revCount, ... }
function detectSignal(pv, cu, cls, id, ed, settings) {
  if (pv === null || cu === null) return null;
  const rm = settings.revMode, rv = parseInt(settings.revCount) || 1;
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

// peaks: كائن حالة لكل رمز { sp, sf, lp, lf } — يُمرَّر بدل STATE.rsiPeaks
function detectTrail(peaks, sym, cu, cls, id, ed, settings) {
  const st = settings;
  if (!peaks[sym]) peaks[sym] = { sp: null, sf: false, lp: null, lf: false };
  const pk = peaks[sym];
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

// SuperTrend — منقول حرفياً (يأخذ klines [ [t,o,h,l,c], ... ])
function calcSuperTrend(klines, period, mult) {
  const n = klines.length;
  if (n < period + 2) return null;
  const H = klines.map(k => parseFloat(k[2]));
  const L = klines.map(k => parseFloat(k[3]));
  const C = klines.map(k => parseFloat(k[4]));
  const atrArr = new Array(n).fill(0);
  let sumTR = 0;
  for (let i = 1; i <= period; i++) sumTR += Math.max(H[i] - L[i], Math.abs(H[i] - C[i - 1]), Math.abs(L[i] - C[i - 1]));
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

module.exports = {
  RSI_P,
  calcRSI, calcRSISeries, calcSMA, calcEMA,
  computeInd, computeIndSeries, checkDiv,
  detectSignal, detectConf, detectTrail, calcSuperTrend,
};
