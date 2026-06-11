// ══════════════════════════════════════════════════════════════════
//  signals_app.js — منطق صفحة الإشارات والشارت (/signals)
//  يعتمد على متغيرات/دوال معرّفة في signals.html: $, token, ws, send,
//  apiBase, fmtTime, fmtDate, doLogin, connect, prefill, handle
// ══════════════════════════════════════════════════════════════════

// ── أدوات عامة للحقول ──────────────────────────────────────────────
function tog(id){ const e=$(id); if(e) e.classList.toggle('on'); }
function isOn(id){ const e=$(id); return e ? e.classList.contains('on') : false; }
function setSw(id,v){ const e=$(id); if(!e) return; if(v) e.classList.add('on'); else e.classList.remove('on'); }
function fid(key){ return 'f_'+key.replace(/\./g,'_'); }
function getNested(obj,path){ return path.split('.').reduce((o,k)=>(o&&o[k]!==undefined?o[k]:undefined), obj); }
function setNested(obj,path,val){
  const keys=path.split('.'); let o=obj;
  for(let i=0;i<keys.length-1;i++){ if(typeof o[keys[i]]!=='object'||o[keys[i]]===null) o[keys[i]]={}; o=o[keys[i]]; }
  o[keys[keys.length-1]]=val;
}

// ── تعريف حقول الإعدادات (نفس مفاتيح settings في النظام) ──────────
const FIELD_GROUPS = [
  { title:'📈 المؤشر والإشارات', fields:[
    {key:'mode', label:'المؤشر', type:'select', options:['RSI','SMA','EMA']},
    {key:'maPeriod', label:'فترة MA', type:'number'},
    {key:'interval', label:'الفريم', type:'select', options:['5m','30m','1h','4h']},
    {key:'dirFilter', label:'اتجاه الإشارات', type:'select', options:[['all','الكل'],['long','LONG فقط'],['short','SHORT فقط']]},
    {key:'revMode', label:'نمط تأكيد الانعكاس', type:'select', options:[['rsi','RSI'],['candles','شموع']]},
    {key:'revCount', label:'عدد شموع التأكيد', type:'number'},
    {key:'rsiGap', label:'فجوة RSI', type:'number', step:0.1},
    {key:'enableDiv', label:'فلتر دايفرجنس', type:'toggle'},
    {key:'blockOpen', label:'منع إشارة جديدة أثناء صفقة مفتوحة', type:'toggle'},
  ]},
  { title:'🎯 فلاتر إشارات الفحص (sigQueueFilters)', fields:[
    {key:'sigQueueFilters.ob', label:'تفعيل OB (تشبع شراء)', type:'toggle'},
    {key:'sigQueueFilters.os', label:'تفعيل OS (تشبع بيع)', type:'toggle'},
    {key:'sigQueueFilters.conf', label:'تفعيل التأكيد ⚡', type:'toggle'},
    {key:'sigQueueFilters.trail', label:'تفعيل Trailing', type:'toggle'},
    {key:'ema200FilterOn', label:'فلتر EMA200', type:'toggle'},
    {key:'ema200TF', label:'فريم EMA200', type:'select', options:['1h','4h']},
    {key:'stFilterOn', label:'فلتر SuperTrend', type:'toggle'},
    {key:'stTF', label:'فريم SuperTrend', type:'select', options:['1h','4h']},
    {key:'stPeriod', label:'فترة SuperTrend', type:'number'},
    {key:'stMult', label:'مضاعف SuperTrend', type:'number', step:0.1},
  ]},
  { title:'📨 فلاتر إشعارات تلغرام (sigFilters)', fields:[
    {key:'sigFilters.ob', label:'إشعار OB (تشبع شراء)', type:'toggle'},
    {key:'sigFilters.os', label:'إشعار OS (تشبع بيع)', type:'toggle'},
    {key:'sigFilters.conf', label:'إشعار التأكيد ⚡', type:'toggle'},
    {key:'sigFilters.trail', label:'إشعار Trailing', type:'toggle'},
  ]},
  { title:'🟣 Trailing RSI', fields:[
    {key:'trSon', label:'تفعيل Trail SHORT', type:'toggle'},
    {key:'trSstart', label:'بداية Trail SHORT (RSI)', type:'number'},
    {key:'trSgap', label:'فجوة الانعكاس SHORT', type:'number', step:0.1},
    {key:'trLon', label:'تفعيل Trail LONG', type:'toggle'},
    {key:'trLstart', label:'بداية Trail LONG (RSI)', type:'number'},
    {key:'trLgap', label:'فجوة الانعكاس LONG', type:'number', step:0.1},
  ]},
  { title:'💰 إدارة الصفقة (Cornix)', fields:[
    {key:'cxMargin', label:'نوع الهامش', type:'select', options:['Cross','Isolated']},
    {key:'cxLev', label:'الرافعة', type:'number'},
    {key:'cxAmt', label:'حجم الدخول (% أو رقم)', type:'text'},
    {key:'cxSLon', label:'تفعيل وقف الخسارة', type:'toggle'},
    {key:'cxSL', label:'SL %', type:'number', step:0.1},
    {key:'cxBEon', label:'نقل SL لنقطة الدخول بعد TP1', type:'toggle'},
    {key:'cxTP1', label:'TP1 %', type:'number', step:0.1},
    {key:'cxTP1Amt', label:'كمية TP1 %', type:'text'},
    {key:'cxTP2on', label:'تفعيل TP2', type:'toggle'},
    {key:'cxTP2', label:'TP2 %', type:'number', step:0.1},
    {key:'cxTP2Amt', label:'كمية TP2 %', type:'text'},
    {key:'cxTrailTp', label:'تتبّع الهدف (Trailing TP)', type:'select', options:['on','off']},
    {key:'cxTrailPct', label:'نسبة تتبّع الهدف %', type:'number', step:0.1},
    {key:'cxEntryTrail', label:'تتبّع الدخول', type:'text'},
    {key:'cxEntry2on', label:'تفعيل دخول ثانٍ', type:'toggle'},
    {key:'cxEntry2Dist', label:'مسافة دخول2 %', type:'number', step:0.1},
    {key:'cxEntry2Amt', label:'كمية دخول2 %', type:'text'},
    {key:'cxEntry3on', label:'تفعيل دخول ثالث', type:'toggle'},
    {key:'cxEntry3Dist', label:'مسافة دخول3 %', type:'text'},
    {key:'cxEntry3Amt', label:'كمية دخول3 %', type:'text'},
  ]},
  { title:'🔔 السيولة والتنبيهات والحساب', fields:[
    {key:'liqVon', label:'تنبيه سيولة عالية', type:'toggle'},
    {key:'liqVmin', label:'أدنى سيولة (USDT)', type:'number'},
    {key:'liqOon', label:'تنبيه أوامر كبيرة', type:'toggle'},
    {key:'liqOmin', label:'أدنى قيمة أمر (USDT)', type:'number'},
    {key:'maxOpenTrades', label:'أقصى صفقات مفتوحة (0=بلا حد)', type:'number'},
    {key:'dataMode', label:'مصدر البيانات', type:'select', options:['ws','rest']},
    {key:'soundEnabled', label:'تنبيه صوتي', type:'toggle'},
    {key:'autoSend', label:'إرسال تلقائي لتلغرام', type:'toggle'},
    {key:'cxToken', label:'توكن بوت تلغرام', type:'text'},
    {key:'cxChat', label:'شات فتح الصفقة', type:'text'},
    {key:'cxChatClose', label:'شات إغلاق الصفقة', type:'text'},
  ]},
];

function fieldHtml(f){
  const id=fid(f.key);
  if(f.type==='toggle'){
    return `<div class="toggle" onclick="tog('${id}')"><span>${f.label}</span><div id="${id}" class="sw"><i></i></div></div>`;
  }
  if(f.type==='select'){
    const opts=f.options.map(o=>{ const [val,lab]=Array.isArray(o)?o:[o,o]; return `<option value="${val}">${lab}</option>`; }).join('');
    return `<div><label>${f.label}</label><select id="${id}">${opts}</select></div>`;
  }
  const step=f.step?` step="${f.step}"`:'';
  return `<div><label>${f.label}</label><input id="${id}" type="${f.type==='number'?'number':'text'}"${step}></div>`;
}

function renderSettingsForm(){
  $('fieldGroups').innerHTML = FIELD_GROUPS.map((g,gi)=>`
    <details class="fgroup" ${gi===0?'open':''}>
      <summary>${g.title}</summary>
      <div class="grid">${g.fields.map(fieldHtml).join('')}</div>
    </details>`).join('');
}

function setFormFromSettings(s){
  s = s || {};
  FIELD_GROUPS.forEach(g=>g.fields.forEach(f=>{
    const id=fid(f.key); const v=getNested(s,f.key);
    if(f.type==='toggle'){ setSw(id, !!v); }
    else if(v!==undefined && v!==null){ const el=$(id); if(el) el.value=v; }
  }));
}

function getSettingsFromForm(){
  const s=JSON.parse(JSON.stringify(window.curSettings||{}));
  FIELD_GROUPS.forEach(g=>g.fields.forEach(f=>{
    const id=fid(f.key); const el=$(id);
    if(f.type==='toggle'){ setNested(s,f.key, isOn(id)); }
    else if(el && el.value!==''){ setNested(s,f.key, f.type==='number'?(parseFloat(el.value)||0):el.value); }
  }));
  return s;
}

// ══════════════════════════════════════════════════════════════════
//  تحميل ملف نتائج الفحص (3 صيغ محتملة)
// ══════════════════════════════════════════════════════════════════
window.rows=[]; window.curRows=[]; window.curSettings={};
window.sortKey='netReturnPct'; window.sortDir=-1;

function loadScanFile(input){
  const f=input.files[0]; if(!f) return;
  const rd=new FileReader();
  rd.onload=()=>{
    let d;
    try{ d=JSON.parse(rd.result); }catch(e){ $('loadMsg').textContent='❌ ملف JSON غير صالح'; return; }
    if(d.bySymbol) loadBySymbol(d.bySymbol);
    else if(d.leaderboard) loadLeaderboard(d);
    else if(d.settings) loadSingleSettings(d);
    else $('loadMsg').textContent='❌ صيغة ملف غير معروفة';
  };
  rd.readAsText(f);
}

function loadBySymbol(bySymbol){
  window.rows = Object.entries(bySymbol).map(([symbol,v])=>({
    symbol, settings:v.settings||{}, metrics:v.metrics||{}, combo:v.combo, score:v.score, sigStats:v.sigStats,
  }));
  $('loadMsg').textContent = `✅ تم تحميل نتائج ${window.rows.length} عملة (فحص لكل عملة)`;
  $('tableCard').classList.remove('hide');
  window.sortKey='netReturnPct'; window.sortDir=-1;
  renderTable();
}

function loadLeaderboard(d){
  window.rows = (d.leaderboard||[]).map((e,i)=>({
    symbol:null, settings:e.settings||{}, metrics:e.metrics||{}, combo:e.combo, score:e.score, sigStats:null, rank:i+1,
  }));
  $('loadMsg').textContent = `✅ تم تحميل أفضل ${window.rows.length} نتيجة (فحص عام — بدون رمز محدد، اكتب رمز العملة يدوياً)`;
  $('tableCard').classList.remove('hide');
  window.sortKey='score'; window.sortDir=-1;
  renderTable();
}

function loadSingleSettings(d){
  window.rows = [];
  $('tableCard').classList.add('hide');
  window.curSettings = JSON.parse(JSON.stringify(d.settings||{}));
  setFormFromSettings(window.curSettings);
  $('loadMsg').textContent = '✅ تم تحميل ملف إعدادات واحد في النموذج أدناه';
  $('settingsCard').scrollIntoView({behavior:'smooth'});
}

// ══════════════════════════════════════════════════════════════════
//  جدول النتائج
// ══════════════════════════════════════════════════════════════════
const TABLE_COLS=[
  {key:'symbol', label:'الرمز'},
  {key:'interval', label:'الفريم'},
  {key:'mode', label:'المؤشر'},
  {key:'dirFilter', label:'الاتجاه'},
  {key:'trades', label:'صفقات'},
  {key:'winRate', label:'نجاح%'},
  {key:'netReturnPct', label:'العائد%'},
  {key:'profitFactor', label:'PF'},
  {key:'maxDrawdownPct', label:'تراجع%'},
  {key:'avgPnlPct', label:'متوسط%'},
  {key:'tsResp', label:'TS احترام%'},
  {key:'tlResp', label:'TL احترام%'},
  {key:'score', label:'Score'},
];

function sortVal(r,key){
  if(key==='symbol') return r.symbol || '';
  if(key==='interval') return (r.settings&&r.settings.interval) || '';
  if(key==='mode') return (r.settings&&r.settings.mode) || '';
  if(key==='dirFilter') return (r.settings&&r.settings.dirFilter) || '';
  if(key==='tsResp') return (r.sigStats&&r.sigStats.ts&&r.sigStats.ts.respectRate) ?? -1;
  if(key==='tlResp') return (r.sigStats&&r.sigStats.tl&&r.sigStats.tl.respectRate) ?? -1;
  if(key==='score') return r.score ?? -Infinity;
  return (r.metrics&&r.metrics[key]) ?? -Infinity;
}

function setSort(key){
  if(window.sortKey===key) window.sortDir*=-1;
  else { window.sortKey=key; window.sortDir = (key==='maxDrawdownPct')?1:-1; }
  renderTable();
}

function renderTable(){
  const q=($('search').value||'').toUpperCase().trim();
  let rows = window.rows.filter(r=>!q || (r.symbol||'').toUpperCase().includes(q));
  rows = rows.slice().sort((a,b)=>{
    const av=sortVal(a,window.sortKey), bv=sortVal(b,window.sortKey);
    if(typeof av==='string' || typeof bv==='string') return String(av).localeCompare(String(bv))*window.sortDir;
    return ((av??-Infinity)-(bv??-Infinity))*window.sortDir;
  });
  window.curRows=rows;
  $('rowCount').textContent=rows.length;
  const head = '<tr>'+TABLE_COLS.map(c=>{
    const sorted=c.key===window.sortKey;
    return `<th class="${sorted?'sorted':''}" onclick="setSort('${c.key}')">${c.label}${sorted?(window.sortDir>0?' ▲':' ▼'):''}</th>`;
  }).join('')+'<th>—</th></tr>';
  const body = rows.map((r,i)=>renderRow(r,i)).join('');
  $('resultsTbl').innerHTML = head+body;
}

function renderRow(r,i){
  const m=r.metrics||{};
  const sgn=v=>((v||0)>=0?'pos':'neg');
  const ts=(r.sigStats&&r.sigStats.ts)||{}, tl=(r.sigStats&&r.sigStats.tl)||{};
  const mode=r.settings&&r.settings.mode;
  const modeStr = mode ? (mode==='RSI'?mode:`${mode}(${r.settings.maPeriod})`) : '—';
  const dirMap={all:'الكل',long:'LONG',short:'SHORT'};
  const dir=r.settings&&r.settings.dirFilter;
  return `<tr>
    <td>${r.symbol || ('#'+(r.rank||(i+1)))}</td>
    <td>${(r.settings&&r.settings.interval)||'—'}</td>
    <td>${modeStr}</td>
    <td>${dirMap[dir]||dir||'—'}</td>
    <td>${m.trades ?? '—'}</td>
    <td>${m.winRate!=null?m.winRate+'%':'—'}</td>
    <td class="${sgn(m.netReturnPct)}">${m.netReturnPct!=null?m.netReturnPct+'%':'—'}</td>
    <td>${m.profitFactor ?? '—'}</td>
    <td class="neg">${m.maxDrawdownPct!=null?m.maxDrawdownPct+'%':'—'}</td>
    <td class="${sgn(m.avgPnlPct)}">${m.avgPnlPct!=null?m.avgPnlPct+'%':'—'}</td>
    <td>${ts.respectRate!=null?ts.respectRate+'%':'—'}</td>
    <td>${tl.respectRate!=null?tl.respectRate+'%':'—'}</td>
    <td>${r.score ?? '—'}</td>
    <td><button class="small" onclick="selectRow(${i})">فتح</button></td>
  </tr>`;
}

function selectRow(i){
  const r=window.curRows[i]; if(!r) return;
  window.curSettings = JSON.parse(JSON.stringify(r.settings||{}));
  setFormFromSettings(window.curSettings);
  if(r.symbol){ $('f_symbol').value=r.symbol; runChart(); }
  $('settingsCard').scrollIntoView({behavior:'smooth'});
}

// ══════════════════════════════════════════════════════════════════
//  تشغيل الشارت + تصدير الإعدادات
// ══════════════════════════════════════════════════════════════════
function runChart(){
  const symbol=($('f_symbol').value||'').trim().toUpperCase();
  if(!symbol){ $('settingsMsg').textContent='⚠️ حدّد رمز العملة أولاً'; return; }
  $('f_symbol').value=symbol;
  window.curSettings = getSettingsFromForm();
  $('settingsMsg').textContent='⏳ جارٍ التحميل…';
  $('runBtn').disabled=true;
  send('btChartData', {symbol, settings: window.curSettings});
}

function exportSettings(){
  const payload={settings:getSettingsFromForm(), exportedAt:new Date().toISOString()};
  const blob=new Blob([JSON.stringify(payload,null,2)],{type:'application/json'});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download=(($('f_symbol').value||'settings')+'-اعدادات.json');
  a.click();
}

// ══════════════════════════════════════════════════════════════════
//  معالجة نتيجة الشارت
// ══════════════════════════════════════════════════════════════════
const TYPE_INFO = {
  a70:{label:'تجاوز 70', emoji:'🔴', color:'#ff4757', side:'SHORT'},
  b70:{label:'نزول من 70', emoji:'🟠', color:'#ffa726', side:'SHORT'},
  b30:{label:'نزول من 30', emoji:'🔵', color:'#2196f3', side:'LONG'},
  a30:{label:'صعود من 30', emoji:'🟢', color:'#2ed573', side:'LONG'},
  cl:{label:'شراء أكيد ⚡', emoji:'⚡🟢', color:'#2ed573', side:'LONG'},
  cs:{label:'بيع أكيد ⚡', emoji:'⚡🔴', color:'#ff4757', side:'SHORT'},
  ts:{label:'Trail SHORT', emoji:'🟣📉', color:'#a259ff', side:'SHORT'},
  tl:{label:'Trail LONG', emoji:'🟣📈', color:'#a259ff', side:'LONG'},
};
const EXIT_LABEL = {tp1:'TP1 ✅', tp2:'TP2 ✅', sl:'SL ❌', breakeven:'BE ⚪', trail:'Trail TP ✅', expired:'انتهت ⏳', open:'مفتوحة 🔵'};

function onChartData(data){
  $('runBtn').disabled=false;
  $('resultCard').classList.remove('hide');
  $('r_symbol').textContent = data.symbol + (data.tf?(' · '+data.tf):'');

  if(data.error){
    $('r_error').textContent='⚠️ '+data.error;
    $('r_error').classList.remove('hide');
    $('r_body').classList.add('hide');
    $('settingsMsg').textContent='';
    return;
  }
  $('r_error').classList.add('hide');
  $('r_body').classList.remove('hide');
  $('settingsMsg').textContent='✅ تم التحميل';

  const x=data.metrics||{};
  const sgn=v=>((v||0)>=0?'pos':'neg');
  $('r_ret').innerHTML = `<span class="${sgn(x.netReturnPct)}">${x.netReturnPct ?? '—'}%</span>`;
  $('r_wr').textContent = (x.winRate ?? '—')+'%';
  $('r_tr').textContent = x.trades ?? '—';
  $('r_pf').textContent = x.profitFactor ?? '—';
  $('r_dd').innerHTML = `<span class="neg">${x.maxDrawdownPct ?? '—'}%</span>`;
  $('r_avg').innerHTML = `<span class="${sgn(x.avgPnlPct)}">${x.avgPnlPct ?? '—'}%</span>`;
  const ts=(data.sigStats&&data.sigStats.ts)||{}, tl=(data.sigStats&&data.sigStats.tl)||{};
  $('r_ts').textContent = (ts.respectRate!=null?ts.respectRate:'—')+'%';
  $('r_tl').textContent = (tl.respectRate!=null?tl.respectRate:'—')+'%';

  drawEquity(x.equity||[1]);

  window.chartData=data;
  renderChart(data);
  renderTradesTable(data.trades||[]);
}

function drawEquity(eq){
  const c=$('eq'),ctx=c.getContext('2d'),W=c.width,H=c.height,n=eq.length;
  ctx.clearRect(0,0,W,H); if(n<2) return;
  const mn=Math.min(...eq),mx=Math.max(...eq),rng=(mx-mn)||1;
  ctx.beginPath(); ctx.lineWidth=2; ctx.strokeStyle=eq[n-1]>=eq[0]?'#2ed573':'#ff4757';
  eq.forEach((v,i)=>{ const x=i/(n-1)*W, y=H-((v-mn)/rng)*(H-10)-5; i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
  ctx.stroke();
}

// ── الشارت (lightweight-charts v4) ─────────────────────────────────
let chart=null, candleSeries=null;
window.__priceLines=[];

function ensureChart(){
  if(chart) return;
  chart = LightweightCharts.createChart($('chart'), {
    layout:{background:{color:'#15263f'}, textColor:'#e8f0fb'},
    grid:{vertLines:{color:'rgba(255,255,255,.05)'}, horzLines:{color:'rgba(255,255,255,.05)'}},
    timeScale:{timeVisible:true, secondsVisible:false, borderColor:'rgba(255,255,255,.08)'},
    rightPriceScale:{borderColor:'rgba(255,255,255,.08)'},
    crosshair:{mode:LightweightCharts.CrosshairMode.Normal},
    width:$('chart').clientWidth, height:420,
  });
  candleSeries = chart.addCandlestickSeries({
    upColor:'#2ed573', downColor:'#ff4757', borderVisible:false,
    wickUpColor:'#2ed573', wickDownColor:'#ff4757',
  });
  window.addEventListener('resize', ()=>{ if(chart) chart.applyOptions({width:$('chart').clientWidth}); });
}

function pricePrecision(candles){
  if(!candles||!candles.length) return {precision:2, minMove:0.01};
  const v=Math.abs(candles[candles.length-1][4]);
  if(v>=100) return {precision:2, minMove:0.01};
  if(v>=1) return {precision:4, minMove:0.0001};
  if(v>=0.01) return {precision:6, minMove:0.000001};
  return {precision:8, minMove:0.00000001};
}

function buildMarkers(data){
  const markers=[];
  (data.signals||[]).forEach(sg=>{
    const info=TYPE_INFO[sg.type]||{};
    const isShort=info.side==='SHORT';
    markers.push({
      time: Math.floor(sg.ts/1000),
      position: isShort?'aboveBar':'belowBar',
      color: sg.taken ? (info.color||'#7d93b2') : '#7d93b2',
      shape: isShort?'arrowDown':'arrowUp',
      text: (info.emoji||sg.type) + (sg.taken?'':' (تجاهل)'),
    });
  });
  (data.trades||[]).forEach(tr=>{
    if(tr.exitTs==null) return;
    markers.push({
      time: Math.floor(tr.exitTs/1000),
      position: tr.side==='LONG'?'aboveBar':'belowBar',
      color: (tr.pnlPct>=0)?'#2ed573':'#ff4757',
      shape:'circle',
      text: (EXIT_LABEL[tr.exitReason]||tr.exitReason)+' '+(tr.pnlPct>=0?'+':'')+tr.pnlPct+'%',
    });
  });
  markers.sort((a,b)=>a.time-b.time);
  return markers;
}

function renderLegend(){
  const items=Object.entries(TYPE_INFO).map(([k,v])=>`<span>${v.emoji} ${k} — ${v.label}</span>`);
  items.push('<span style="color:var(--green)">⚪ خروج رابح</span>');
  items.push('<span style="color:var(--red)">⚪ خروج خاسر</span>');
  items.push('<span style="color:var(--text2)">رمادي = إشارة لم تُنفَّذ (صفقة مفتوحة بالفعل)</span>');
  $('legend').innerHTML=items.join('');
}

function renderChart(data){
  ensureChart();
  const {precision,minMove}=pricePrecision(data.candles||[]);
  candleSeries.applyOptions({priceFormat:{type:'price', precision, minMove}});
  const cd=(data.candles||[]).map(c=>({time:Math.floor(c[0]/1000), open:c[1], high:c[2], low:c[3], close:c[4]}));
  candleSeries.setData(cd);
  candleSeries.setMarkers(buildMarkers(data));
  (window.__priceLines||[]).forEach(pl=>{ try{candleSeries.removePriceLine(pl);}catch(e){} });
  window.__priceLines=[];
  chart.timeScale().fitContent();
  renderLegend();
}

// ── جدول الصفقات ────────────────────────────────────────────────────
function renderTradesTable(trades){
  $('tradeCount').textContent=trades.length;
  const rows=trades.map((t,i)=>{
    const sideColor=t.side==='LONG'?'pos':'neg';
    const pnlColor=(t.pnlPct>=0)?'pos':'neg';
    const info=TYPE_INFO[t.type]||{};
    return `<tr onclick="selectTrade(${i})" style="cursor:pointer">
      <td>${i+1}</td>
      <td class="${sideColor}">${t.side}</td>
      <td>${info.emoji||''} ${t.type}</td>
      <td>${fmtDate(t.entryTs)}</td>
      <td>${t.entryPx}</td>
      <td>${t.tp1Px}</td>
      <td>${t.tp2Px ?? '—'}</td>
      <td>${t.slPx ?? '—'}</td>
      <td>${EXIT_LABEL[t.exitReason]||t.exitReason}</td>
      <td>${t.exitPx ?? '—'}</td>
      <td class="${pnlColor}">${t.pnlPct>=0?'+':''}${t.pnlPct}%</td>
      <td>${t.bars}</td>
    </tr>`;
  }).join('');
  $('tradesTbl').innerHTML = `<tr><th>#</th><th>الجانب</th><th>النوع</th><th>وقت الدخول</th><th>دخول</th><th>TP1</th><th>TP2</th><th>SL</th><th>الخروج</th><th>سعر الخروج</th><th>PnL%</th><th>شموع</th></tr>`+rows;
}

function selectTrade(i){
  const t=(window.chartData&&window.chartData.trades||[])[i];
  if(!t || !candleSeries) return;

  document.querySelectorAll('#tradesTbl tr').forEach((tr,idx)=>tr.classList.toggle('sel', idx===i+1));

  (window.__priceLines||[]).forEach(pl=>{ try{candleSeries.removePriceLine(pl);}catch(e){} });
  window.__priceLines=[];
  const add=(price,color,title)=>{
    if(price==null) return;
    window.__priceLines.push(candleSeries.createPriceLine({price, color, lineWidth:1, lineStyle:LightweightCharts.LineStyle.Dashed, axisLabelVisible:true, title}));
  };
  add(t.entryPx,'#00e5ff','دخول');
  add(t.tp1Px,'#2ed573','TP1');
  if(t.tp2Px!=null) add(t.tp2Px,'#2ed573','TP2');
  if(t.slPx!=null) add(t.slPx,'#ff4757','SL');

  const tfSec = (window.chartData&&window.chartData.tf==='4h')?14400:3600;
  const from=Math.floor(t.entryTs/1000)-tfSec*8;
  const to=(t.exitTs!=null?Math.floor(t.exitTs/1000):Math.floor(t.entryTs/1000))+tfSec*8;
  chart.timeScale().setVisibleRange({from, to});
}
