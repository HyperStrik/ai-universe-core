const { PRICING_REDIRECT } = require('../config/constants');
const { sendError } = require('../utils/http');
const { getClientIdentity } = require('../utils/identity');
const {
  findUserByEmail,
  findFingerprintConflict,
  updateDeviceFingerprint,
  resetDailyCountersIfNeeded,
} = require('../db/users');
const {
  isTrialExpired,
  dailyCreditLimit,
  hasCreditsRemaining,
} = require('../services/credits');

function createClientRulesMiddleware() {
  return async function enforceClientRules(req, res, next) {
    if (req.role === 'MASTER_OWNER') return next();

    try {
      const { email, deviceFingerprint } = getClientIdentity(req);

      if (!email) {
        return sendError(res, 401, 'EMAIL_REQUIRED', 'Email registration is required.');
      }

      if (!deviceFingerprint) {
        return sendError(res, 401, 'DEVICE_REQUIRED', 'Device fingerprint is required.');
      }

      let user = await findUserByEmail(email);

      if (!user) {
        return sendError(res, 403, 'NOT_REGISTERED', 'Email must be registered before access.');
      }

      if (!user.is_email_verified) {
        return sendError(
          res,
          403,
          'EMAIL_NOT_VERIFIED',
          'Verify your email before using the platform.'
        );
      }

      const conflict = await findFingerprintConflict(deviceFingerprint, email);
      if (conflict) {
        return sendError(
          res,
          403,
          'DEVICE_FINGERPRINT_CONFLICT',
          'This device is already linked to another account. Access blocked.'
        );
      }

      if (user.device_fingerprint !== deviceFingerprint) {
        await updateDeviceFingerprint(user.id, deviceFingerprint);
        user.device_fingerprint = deviceFingerprint;
      }

      user = await resetDailyCountersIfNeeded(user);

      if (isTrialExpired(user)) {
        return sendError(res, 402, 'TRIAL_EXPIRED', 'Free trial ended. Upgrade to continue.', {
          redirect: PRICING_REDIRECT,
        });
      }

      if (user.role === 'CLIENT' && !hasCreditsRemaining(user)) {
        const limit = dailyCreditLimit(user);
        return sendError(res, 429, 'DAILY_CREDIT_LIMIT', 'Daily credit limit reached.', {
          limit,
          used: user.credits_used_today,
          redirect: PRICING_REDIRECT,
        });
      }

      req.user = user;
      req.effectiveRole =
        user.role === 'OVERRIDE_UNLIMITED' ? 'OVERRIDE_UNLIMITED' : 'CLIENT';
      next();
    } catch (err) {
      console.error('clientRules middleware error:', err);
      sendError(res, 500, 'CLIENT_GATE_FAILED', 'Failed to validate client access.');
    }
  };
}

module.exports = { createClientRulesMiddleware };
