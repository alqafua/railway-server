const path = require('path');
const crypto = require('crypto');
const fs = require('fs');

const DATA_DIR = process.env.DB_PATH
  ? path.dirname(process.env.DB_PATH)
  : path.join(__dirname, '../data');
fs.mkdirSync(DATA_DIR, { recursive: true });

function fpath(name) { return path.join(DATA_DIR, name + '.json'); }

function readJSON(name, def) {
  try { return JSON.parse(fs.readFileSync(fpath(name), 'utf8')); }
  catch (e) { return def; }
}

function writeJSON(name, data) {
  try { fs.writeFileSync(fpath(name), JSON.stringify(data, null, 2)); }
  catch (e) { console.error('DB write error:', name, e.message); }
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
    livePositions: undefined, liveBalance: undefined, apiOk: undefined, closedTrades: undefined,
  })));
}

function loadAccounts() {
  return readJSON('accounts', []).map(a => ({
    ...a,
    apiKey: decrypt(a.apiKey || ''),
    apiSecret: decrypt(a.apiSecret || ''),
    livePositions: [], liveBalance: null, apiOk: undefined, closedTrades: [],
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

module.exports = {
  saveSettings, loadSettings,
  saveAccounts, loadAccounts,
  saveOpenTrades, loadOpenTrades,
  saveClosedTrade, loadClosedTrades,
  saveDcaOrders, loadDcaOrders,
  saveAlert, loadAlerts,
  saveSymbolSettings, loadSymbolSettings,
};
