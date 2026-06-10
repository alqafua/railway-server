// اختبار ذاتي — يثبت أن المؤشر والمحاكاة صحيحة (بدون إنترنت)
const S = require('./signals');
const BT = require('./backtest');

let pass = 0, fail = 0;
function ok(name, cond, extra='') { if (cond) { pass++; console.log('✅', name, extra); } else { fail++; console.log('❌', name, extra); } }
function near(a, b, eps=0.01) { return Math.abs(a-b) <= eps; }

// ── 1) صحة RSI (حالات معروفة) ──
const up = Array.from({length:30}, (_,i)=>100+i);
const down = Array.from({length:30}, (_,i)=>100-i);
ok('RSI سلسلة صاعدة = 100', S.calcRSI(up,14) === 100, `(=${S.calcRSI(up,14)})`);
ok('RSI سلسلة هابطة = 0', S.calcRSI(down,14) === 0, `(=${S.calcRSI(down,14)})`);

// ── 2) كشف الإشارة (عبور 30/70) ──
const setT = { revMode:'candles', revCount:1 };
ok('a30 LONG (عبور 30 صعوداً)', S.detectSignal(28,32,up,up,false,setT)?.type==='a30');
ok('a70 SHORT (عبور 70 صعوداً)', S.detectSignal(68,72,up,up,false,setT)?.type==='a70');
ok('لا إشارة داخل النطاق', S.detectSignal(45,55,up,up,false,setT)===null);

// أداة بناء شموع: ثابتة ما لم نحدد high/low
const C = (t,o,h,l,c)=>[t,o,h,l,c];

// إعداد أساسي: رافعة 1 ليكون الربح = الحركة % بالضبط
const base = { cxLev:1, cxAmt:'1%', cxTP1:10, cxTP1Amt:100, cxTP2on:false,
  cxSLon:true, cxSL:5, cxEntry2on:false, cxEntry3on:false, cxTrailTp:'off', cxBEon:false };

// ── 3) TP1 يُصاب (LONG) — متوقع 9.9% (10% − 0.1% رسوم) ──
// إشارة عند i=0 (إغلاق 100)، الدخول فتح الشمعة 1 = 100، الهدف 110، الوقف 95
let candles = [ C(0,100,100,100,100), C(1,100,111,99,108) ]; // شمعة الدخول تلمس 111≥110
let tr = BT.simulateTrade(candles, 0, 'LONG', base);
ok('TP1 LONG سبب الخروج', tr.exitReason==='tp1', `(${tr.exitReason})`);
ok('TP1 LONG ربح ≈ 9.9%', near(tr.pnlPct, 9.9), `(=${tr.pnlPct})`);

// ── 4) SL يُصاب (LONG) — متوقع −5.1% ──
candles = [ C(0,100,100,100,100), C(1,100,101,94,96) ]; // يلمس 94 ≤ 95
tr = BT.simulateTrade(candles, 0, 'LONG', base);
ok('SL LONG سبب الخروج', tr.exitReason==='sl', `(${tr.exitReason})`);
ok('SL LONG خسارة ≈ −5.1%', near(tr.pnlPct, -5.1), `(=${tr.pnlPct})`);

// ── 5) الرافعة 10x على هدف 10% — متوقع ≈ 99% ──
let lev = { ...base, cxLev:10 };
candles = [ C(0,100,100,100,100), C(1,100,111,99,108) ];
tr = BT.simulateTrade(candles, 0, 'LONG', lev);
ok('TP1 LONG 10x ≈ 99%', near(tr.pnlPct, 99, 0.2), `(=${tr.pnlPct})`);

// ── 6) SHORT TP1 — سعر ينزل، هدف 90 ──
candles = [ C(0,100,100,100,100), C(1,100,101,89,92) ]; // ينزل إلى 89 ≤ 90
tr = BT.simulateTrade(candles, 0, 'SHORT', base);
ok('TP1 SHORT سبب الخروج', tr.exitReason==='tp1', `(${tr.exitReason})`);
ok('TP1 SHORT ربح ≈ 9.9%', near(tr.pnlPct, 9.9), `(=${tr.pnlPct})`);

// ── 7) تعارض داخل الشمعة → الوقف أولاً (تحفّظاً) ──
candles = [ C(0,100,100,100,100), C(1,100,111,94,100) ]; // يلمس الهدف 111 والوقف 94 بنفس الشمعة
tr = BT.simulateTrade(candles, 0, 'LONG', base);
ok('تعارض → وقف أولاً', tr.exitReason==='sl', `(${tr.exitReason})`);

// ── 8) دخول ثانٍ يحسّن المتوسط ──
let e2 = { ...base, cxEntry2on:true, cxEntry2Dist:2, cxEntry2Amt:50 }; // دخول2 عند 98 بنصف الحجم
candles = [ C(0,100,100,100,100), C(1,100,100,97,99), C(2,99,111,98,108) ]; // يلمس 98 ثم 111
tr = BT.simulateTrade(candles, 0, 'LONG', e2);
// متوسط دخول = (100*0.5 + 98*0.5)=99 ؛ ربح TP عند 110 = (110/99−1)=11.11% − رسوم
ok('دخول2 TP1 ربح > دخول واحد', tr.pnlPct > 10.5, `(=${tr.pnlPct})`);

// ── 9) باك تيست محفظة صغيرة شغّال end-to-end ──
function synth(n, seed){ // سلسلة شموع شبه عشوائية حول 100
  let x=100, rows=[]; let s=seed;
  for(let i=0;i<n;i++){ s=(s*1103515245+12345)&0x7fffffff; const r=(s/0x7fffffff-0.5);
    const o=x; x=Math.max(1,x*(1+r*0.04)); const h=Math.max(o,x)*1.005, l=Math.min(o,x)*0.995;
    rows.push([i, o, h, l, x]); }
  return rows;
}
const dataset = { AAAUSDT: synth(500,7), BBBUSDT: synth(500,99) };
const scan = { mode:'SMA', maPeriod:14, interval:'1h', revMode:'candles', revCount:1, enableDiv:false,
  sigQueueFilters:{ob:true,os:true,conf:true,trail:true}, dirFilter:'all' };
const res = BT.runBacktest(dataset, { ...base, ...scan });
ok('runBacktest يُنتج مقاييس', typeof res.metrics.trades === 'number' && Array.isArray(res.metrics.equity),
   `(صفقات=${res.metrics.trades}, عائد=${res.metrics.netReturnPct}%, نجاح=${res.metrics.winRate}%, PF=${res.metrics.profitFactor}, أقصى تراجع=${res.metrics.maxDrawdownPct}%)`);

console.log(`\nالنتيجة: ${pass} نجح / ${fail} فشل`);
process.exit(fail ? 1 : 0);
