const { getPool } = require('./pool');

const USER_COLUMNS = `
  id, email, is_email_verified, device_fingerprint, role,
  allowed_info_scope, created_at, credits_used_today, credits_reset_date,
  whatsapp_shares_today, whatsapp_share_day, whatsapp_bonus_awarded_date
`;

async function findUserByEmail(email) {
  const { rows } = await getPool().query(
    `SELECT ${USER_COLUMNS} FROM users WHERE email = $1`,
    [email]
  );
  return rows[0] || null;
}

async function findFingerprintConflict(deviceFingerprint, email) {
  const { rows } = await getPool().query(
    `SELECT email FROM users
     WHERE device_fingerprint = $1 AND email <> $2
     LIMIT 1`,
    [deviceFingerprint, email]
  );
  return rows[0] || null;
}

async function updateDeviceFingerprint(userId, deviceFingerprint) {
  await getPool().query(
    `UPDATE users SET device_fingerprint = $1 WHERE id = $2`,
    [deviceFingerprint, userId]
  );
}

async function resetDailyCountersIfNeeded(user) {
  const today = new Date().toISOString().slice(0, 10);
  const resetDate = user.credits_reset_date
    ? String(user.credits_reset_date).slice(0, 10)
    : null;

  if (resetDate === today) return user;

  const { rows } = await getPool().query(
    `UPDATE users
     SET credits_used_today = 0,
         credits_reset_date = $2::date,
         whatsapp_shares_today = CASE
           WHEN whatsapp_share_day = $2::date THEN whatsapp_shares_today
           ELSE 0
         END,
         whatsapp_share_day = CASE
           WHEN whatsapp_share_day = $2::date THEN whatsapp_share_day
           ELSE NULL
         END
     WHERE id = $1
     RETURNING ${USER_COLUMNS}`,
    [user.id, today]
  );

  return rows[0] || user;
}

async function consumeCredit(userId) {
  await getPool().query(
    `UPDATE users SET credits_used_today = credits_used_today + 1 WHERE id = $1`,
    [userId]
  );
}

async function updateWhatsappProgress(userId, uniqueCount, today, awardBonus) {
  if (awardBonus) {
    await getPool().query(
      `UPDATE users
       SET whatsapp_shares_today = $2,
           whatsapp_share_day = $3::date,
           whatsapp_bonus_awarded_date = $3::date
       WHERE id = $1`,
      [userId, uniqueCount, today]
    );
    return;
  }

  await getPool().query(
    `UPDATE users
     SET whatsapp_shares_today = $2, whatsapp_share_day = $3::date
     WHERE id = $1`,
    [userId, uniqueCount, today]
  );
}

module.exports = {
  findUserByEmail,
  findFingerprintConflict,
  updateDeviceFingerprint,
  resetDailyCountersIfNeeded,
  consumeCredit,
  updateWhatsappProgress,
};
