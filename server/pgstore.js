// ══════════════════════════════════════════════════════════════════
//  pgstore.js — مرآة تخزين دائمة على PostgreSQL (Railway)
//  تُستخدم لحفظ نسخة من ملفات البيانات/الشموع، واستعادتها بعد كل
//  إعادة نشر (الـ filesystem المحلي مؤقت ويُمحى مع كل redeploy).
//  إذا لم يوجد DATABASE_URL تعمل كل الدوال كـ no-op بدون أي تأثير.
// ══════════════════════════════════════════════════════════════════
const { Pool } = require('pg');

const CONN = process.env.DATABASE_URL;
const pool = CONN ? new Pool({
  connectionString: CONN,
  ssl: process.env.PG_SSL === 'disable' ? false : { rejectUnauthorized: false },
}) : null;

if (pool) pool.on('error', e => console.error('pgstore pool error:', e.message));

let ready = null;
function ensureSchema() {
  if (!pool) return Promise.resolve(false);
  if (!ready) {
    ready = pool.query(`
      CREATE TABLE IF NOT EXISTS kv_blobs (
        key TEXT PRIMARY KEY,
        data BYTEA NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `).then(() => true).catch(e => { console.error('pgstore schema error:', e.message); ready = null; return false; });
  }
  return ready;
}

async function putBlob(key, buf) {
  if (!pool) return;
  try {
    await ensureSchema();
    await pool.query(
      `INSERT INTO kv_blobs (key, data, updated_at) VALUES ($1, $2, now())
       ON CONFLICT (key) DO UPDATE SET data = $2, updated_at = now()`,
      [key, buf]
    );
  } catch (e) { console.error('pgstore putBlob error:', key, e.message); }
}

async function getBlob(key) {
  if (!pool) return null;
  try {
    await ensureSchema();
    const r = await pool.query('SELECT data FROM kv_blobs WHERE key = $1', [key]);
    return r.rows[0] ? r.rows[0].data : null;
  } catch (e) { console.error('pgstore getBlob error:', key, e.message); return null; }
}

async function listKeys(prefix) {
  if (!pool) return [];
  try {
    await ensureSchema();
    const r = await pool.query('SELECT key FROM kv_blobs WHERE key LIKE $1', [prefix + '%']);
    return r.rows.map(row => row.key);
  } catch (e) { console.error('pgstore listKeys error:', prefix, e.message); return []; }
}

module.exports = { enabled: !!pool, putBlob, getBlob, listKeys };
