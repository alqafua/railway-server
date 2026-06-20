const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const pg = require('./pgstore');

const DATA_DIR = process.env.DB_PATH
  ? path.dirname(process.env.DB_PATH)
  : process.env.RAILWAY_VOLUME_MOUNT_PATH
    ? path.join(process.env.RAILWAY_VOLUME_MOUNT_PATH, 'data')
    : path.join(__dirname, '../data');
fs.mkdirSync(DATA_DIR, { recursive: true });

function fpath(name) { return path.join(DATA_DIR, name + '.json'); }

function readJSON(name, def) {
  try { return JSON.parse(fs.readFileSync(fpath(name), 'utf8')); }
  catch (e) { return def; }
}

function writeJSON(name, data) {
  const json = JSON.stringify(data, null, 2);
  try { fs.writeFileSync(fpath(name), json); }
  catch (e) { console.error('DB write error:', name, e.message); }
  pg.putBlob('db:' + name, Buffer.from(json));
}

// استعادة ملفات البيانات من PostgreSQL (إن وُجدت) عند بدء التشغيل —
// تعالج فقدان الـ filesystem المحلي بعد كل redeploy على Railway.
const DB_FILES = ['settings', 'accounts', 'open_trades', 'closed_trades', 'dca_orders', 'alerts', 'symbol_settings', 'respect_data', 'wait_queue', 'pending_orders', 'sim_trades', 'copy_log', 'sent_sigs'];
async function restoreFromPg() {
  if (!pg.enabled) return;
  for (const name of DB_FILES) {
    if (fs.existsSync(fpath(name))) continue;
    const buf = await pg.getBlob('db:' + name);
    if (buf) {
      try { fs.writeFileSync(fpath(name), buf); console.log(`📥 DB restored from Postgres: ${name}.json`); }
      catch (e) { console.error('DB restore error:', name, e.message); }
    }
  }
}

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
    if (parts.length !== 3) return enc;
    const [ivH, tagH, dataH] = parts;
    const decipher = crypto.createDecipheriv('aes-256-gcm', getEncKey(), Buffer.from(ivH, 'hex'));
    decipher.setAuthTag(Buffer.from(tagH, 'hex'));
    return Buffer.concat([decipher.update(Buffer.from(dataH, 'hex')), decipher.final()]).toString('utf8');
  } catch (e) { return ''; }
}

function saveSettings(settings) { writeJSON('settings', settings); }
function loadSettings(defaults) {
  const s = readJSON('settings', null);
  return s ? { ...defaults, ...s } : { ...defaults };
}

function saveAccounts(accounts) {
  writeJSON('accounts', accounts.map(a => ({
    ...a,
    apiKey: encrypt(a.apiKey || ''),
    apiSecret: encrypt(a.apiSecret || ''),
    livePositions: undefined, liveBalance: undefined, apiOk: undefined,
  })));
}

function loadAccounts() {
  return readJSON('accounts', []).map(a => ({
    ...a,
    apiKey: decrypt(a.apiKey || ''),
    apiSecret: decrypt(a.apiSecret || ''),
    livePositions: [], liveBalance: null, apiOk: undefined, closedTrades: a.closedTrades || [],
  }));
}

function saveOpenTrades(trades) { writeJSON('open_trades', trades); }
function loadOpenTrades() { return readJSON('open_trades', []); }

function saveClosedTrade(trade) {
  const all = readJSON('closed_trades', []);
  all.unshift(trade);
  writeJSON('closed_trades', all.slice(0, 500));
}
function loadClosedTrades() { return readJSON('closed_trades', []); }

function saveDcaOrders(orders) { writeJSON('dca_orders', orders); }
function loadDcaOrders() { return readJSON('dca_orders', []); }

function saveAlert(alert) {
  const all = readJSON('alerts', []);
  all.unshift(alert);
  writeJSON('alerts', all.slice(0, 200));
}
function loadAlerts() { return readJSON('alerts', []); }

function saveSymbolSettings(map) { writeJSON('symbol_settings', map); }
function loadSymbolSettings() { return readJSON('symbol_settings', {}); }

function saveRespectData(data) { writeJSON('respect_data', data); }
function loadRespectData() { return readJSON('respect_data', {}); }

function saveWaitQueue(queue) { writeJSON('wait_queue', queue); }
function loadWaitQueue() { return readJSON('wait_queue', []); }

function savePendingOrders(orders) { writeJSON('pending_orders', orders); }
function loadPendingOrders() { return readJSON('pending_orders', []); }

function saveSimTrades(trades) { writeJSON('sim_trades', trades); }
function loadSimTrades() { return readJSON('sim_trades', []); }

function saveCopyLog(log) { writeJSON('copy_log', log); }
function loadCopyLog() { return readJSON('copy_log', []); }

function saveSentSigs(sigs) { writeJSON('sent_sigs', sigs); }
function loadSentSigs() { return readJSON('sent_sigs', {}); }

module.exports = {
  saveSettings, loadSettings,
  saveAccounts, loadAccounts,
  saveOpenTrades, loadOpenTrades,
  saveClosedTrade, loadClosedTrades,
  saveDcaOrders, loadDcaOrders,
  saveAlert, loadAlerts,
  saveSymbolSettings, loadSymbolSettings,
  saveRespectData, loadRespectData,
  saveWaitQueue, loadWaitQueue,
  savePendingOrders, loadPendingOrders,
  saveSimTrades, loadSimTrades,
  saveCopyLog, loadCopyLog,
  saveSentSigs, loadSentSigs,
  restoreFromPg,
};
