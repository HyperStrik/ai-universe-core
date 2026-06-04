const express = require('express');
const {
  WHATSAPP_SHARES_REQUIRED,
  MIN_SHARE_HASH_LENGTH,
} = require('../config/constants');
const { sendError } = require('../utils/http');
const {
  shareBatchExists,
  insertShare,
  countUniqueSharesToday,
} = require('../db/whatsapp');
const { updateWhatsappProgress } = require('../db/users');
const { bonusAlreadyAwardedToday } = require('../services/credits');

function createWhatsappRouter(authMiddleware, clientRulesMiddleware) {
  const router = express.Router();

  router.post(
    '/api/user/whatsapp-share-track',
    authMiddleware,
    clientRulesMiddleware,
    async (req, res) => {
      try {
        const { share_batch_hash: shareBatchHash } = req.body || {};

        if (!shareBatchHash || typeof shareBatchHash !== 'string') {
          return sendError(res, 400, 'BATCH_HASH_REQUIRED', 'share_batch_hash is required.');
        }

        const hash = shareBatchHash.trim();
        if (hash.length < MIN_SHARE_HASH_LENGTH) {
          return sendError(
            res,
            400,
            'INVALID_BATCH_HASH',
            `share_batch_hash must be at least ${MIN_SHARE_HASH_LENGTH} characters.`
          );
        }

        if (await shareBatchExists(hash)) {
          return sendError(
            res,
            409,
            'DUPLICATE_BATCH_HASH',
            'This share batch was already recorded.'
          );
        }

        await insertShare(req.user.id, hash);

        const today = new Date().toISOString().slice(0, 10);
        const uniqueCount = await countUniqueSharesToday(req.user.id, today);
        let bonusAwarded = false;

        if (uniqueCount >= WHATSAPP_SHARES_REQUIRED) {
          const alreadyAwarded = bonusAlreadyAwardedToday(req.user);

          if (!alreadyAwarded) {
            await updateWhatsappProgress(req.user.id, uniqueCount, today, true);
            bonusAwarded = true;
          } else {
            await updateWhatsappProgress(req.user.id, uniqueCount, today, false);
          }
        } else {
          await updateWhatsappProgress(req.user.id, uniqueCount, today, false);
        }

        res.json({
          success: true,
          share_batch_hash: hash,
          unique_shares_today: uniqueCount,
          bonus_credit_awarded: bonusAwarded,
          shares_required: WHATSAPP_SHARES_REQUIRED,
        });
      } catch (err) {
        console.error('whatsapp-share-track error:', err);
        sendError(res, 500, 'SHARE_TRACK_FAILED', 'Failed to record WhatsApp share event.');
      }
    }
  );

  return router;
}

module.exports = { createWhatsappRouter };
