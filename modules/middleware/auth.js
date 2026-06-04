const { getEnv } = require('../config/env');
const { getAdminKey } = require('../utils/identity');

function createAuthMiddleware() {
  const { masterAdminKey } = getEnv();

  return function authMiddleware(req, res, next) {
    const adminKey = getAdminKey(req);

    if (adminKey && masterAdminKey && adminKey === masterAdminKey) {
      req.role = 'MASTER_OWNER';
      req.user = null;
      return next();
    }

    req.role = 'CLIENT';
    next();
  };
}

module.exports = { createAuthMiddleware };
