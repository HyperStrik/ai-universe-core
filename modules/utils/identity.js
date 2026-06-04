function getAdminKey(req) {
  const auth = req.headers.authorization || '';
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim();

  return (
    req.headers['x-master-admin-key'] ||
    req.headers['x-admin-key'] ||
    ''
  ).trim();
}

function getClientIdentity(req) {
  const body = req.body || {};

  return {
    email: (req.headers['x-user-email'] || body.email || '').trim().toLowerCase(),
    deviceFingerprint: (
      req.headers['x-device-fingerprint'] ||
      body.device_fingerprint ||
      ''
    ).trim(),
  };
}

module.exports = { getAdminKey, getClientIdentity };
