const express = require('express');
const { MAX_RESPONSE_WORDS } = require('../config/constants');
const { sendError } = require('../utils/http');
const { truncateToWordLimit } = require('../utils/text');
const { consumeCredit } = require('../db/users');
const { executeModel, executeModelStream } = require('../services/ai');
const { buildScopedPrompt, detectScopeViolation } = require('../services/scope');

function createChatRouter(authMiddleware, clientRulesMiddleware) {
  const router = express.Router();

  router.post('/api/chat', authMiddleware, clientRulesMiddleware, async (req, res) => {
    try {
      const { prompt } = req.body || {};

      if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
        return sendError(res, 400, 'PROMPT_REQUIRED', 'A non-empty prompt is required.');
      }

      let finalPrompt = prompt.trim();

      if (req.role === 'MASTER_OWNER') {
        const wantsStream =
          req.query.stream === 'true' ||
          req.headers.accept?.includes('text/event-stream') ||
          req.body?.stream === true;

        if (wantsStream) {
          await executeModelStream(finalPrompt, res);
          return;
        }

        const outputText = await executeModel(finalPrompt, { uncensored: true });
        return res.json({
          role: 'MASTER_OWNER',
          output: outputText,
          censored: false,
          word_limit_applied: false,
          streaming: false,
        });
      }

      const scopeViolation = detectScopeViolation(finalPrompt, req.user.allowed_info_scope);
      if (scopeViolation) {
        return sendError(res, 403, 'SCOPE_VIOLATION', scopeViolation);
      }

      finalPrompt = buildScopedPrompt(finalPrompt, req.user.allowed_info_scope);
      const outputText = await executeModel(finalPrompt, { uncensored: false });

      if (req.effectiveRole === 'CLIENT') {
        await consumeCredit(req.user.id);
      }

      const limitedOutput = truncateToWordLimit(outputText, MAX_RESPONSE_WORDS);

      res.json({
        role: req.effectiveRole,
        output: limitedOutput,
        censored: true,
        word_limit_applied: true,
        max_words: MAX_RESPONSE_WORDS,
        credits_bypassed: req.effectiveRole === 'OVERRIDE_UNLIMITED',
      });
    } catch (err) {
      console.error('/api/chat error:', err);
      if (!res.headersSent) {
        sendError(res, 500, 'CHAT_FAILED', err.message || 'Chat execution failed.');
      } else {
        res.end();
      }
    }
  });

  return router;
}

module.exports = { createChatRouter };
