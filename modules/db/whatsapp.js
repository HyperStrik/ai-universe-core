const { getPool } = require('./pool');

async function shareBatchExists(shareBatchHash) {
  const { rows } = await getPool().query(
    `SELECT id FROM whatsapp_shares WHERE share_batch_hash = $1`,
    [shareBatchHash]
  );
  return rows.length > 0;
}

async function insertShare(userId, shareBatchHash) {
  await getPool().query(
    `INSERT INTO whatsapp_shares (user_id, share_batch_hash) VALUES ($1, $2)`,
    [userId, shareBatchHash]
  );
}

async function countUniqueSharesToday(userId, today) {
  const { rows } = await getPool().query(
    `SELECT COUNT(DISTINCT share_batch_hash)::int AS cnt
     FROM whatsapp_shares
     WHERE user_id = $1 AND shared_at::date = $2::date`,
    [userId, today]
  );
  return rows[0].cnt;
}

module.exports = {
  shareBatchExists,
  insertShare,
  countUniqueSharesToday,
};
