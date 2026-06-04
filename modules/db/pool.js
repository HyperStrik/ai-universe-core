const { Pool } = require('pg');
const { getEnv, stripQuotes } = require('../config/env');

let pool;

function isDatabaseConfigured() {
  const raw = getEnv().databaseUrl || process.env.DATABASE_URL;
  const url = stripQuotes(raw || '').trim();
  return Boolean(url);
}

function getPool() {
  if (!isDatabaseConfigured()) {
    const error = new Error('DATABASE_URL is not configured');
    error.code = 'DATABASE_UNAVAILABLE';
    throw error;
  }

  if (pool) return pool;

  const databaseUrl = stripQuotes(getEnv().databaseUrl || process.env.DATABASE_URL || '').trim();

  pool = new Pool({
    connectionString: databaseUrl,
    connectionTimeoutMillis: 4000,
    ssl: databaseUrl.includes('supabase.co')
      ? { rejectUnauthorized: false }
      : undefined,
  });

  pool.on('error', (err) => {
    console.warn('PostgreSQL pool idle error (non-fatal):', err.message);
  });

  return pool;
}

async function pingDatabase() {
  if (!isDatabaseConfigured()) {
    console.warn('PostgreSQL not reachable at startup: DATABASE_URL is missing.');
    return false;
  }

  try {
    await getPool().query('SELECT 1');
    console.log('PostgreSQL pool connected.');
    return true;
  } catch (err) {
    const detail = err?.message || err?.code || 'connection failed';
    console.warn('PostgreSQL not reachable at startup:', detail);
    return false;
  }
}

module.exports = { getPool, pingDatabase, isDatabaseConfigured };
