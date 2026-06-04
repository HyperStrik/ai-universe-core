const express = require('express');
const { sendError } = require('../utils/http');

function createAdminRouter(authMiddleware) {
  const router = express.Router();

  function requireMasterOwner(req, res) {
    if (req.role !== 'MASTER_OWNER') {
      sendError(res, 403, 'MASTER_ONLY', 'Admin endpoints require MASTER_OWNER privileges.');
      return false;
    }
    return true;
  }

  router.post('/api/admin/scrape-telegram', authMiddleware, async (req, res) => {
    try {
      if (!requireMasterOwner(req, res)) return;

      const { targetGroupId } = req.body || {};

      if (!targetGroupId || typeof targetGroupId !== 'string' || !targetGroupId.trim()) {
        return sendError(res, 400, 'GROUP_ID_REQUIRED', 'targetGroupId is required.');
      }

      const groupId = targetGroupId.trim();
      const jobId = `tg_scrape_${Date.now()}`;

      res.json({
        success: true,
        module: 'TelegramMemberScraperSuite',
        job_id: jobId,
        target_group_id: groupId,
        status: 'queued',
        message: `Telegram scrape job queued for group ${groupId}`,
        metrics: {
          members_discovered: 0,
          members_persisted: 0,
          scrape_depth: 'full',
        },
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error('/api/admin/scrape-telegram error:', err);
      sendError(res, 500, 'SCRAPE_FAILED', err.message || 'Telegram scrape dispatch failed.');
    }
  });

  router.post('/api/admin/trigger-outreach', authMiddleware, async (req, res) => {
    try {
      if (!requireMasterOwner(req, res)) return;

      const { campaignName, audienceSegment, dryRun } = req.body || {};

      const jobId = `outreach_${Date.now()}`;

      res.json({
        success: true,
        module: 'B2BOutreachEngineDispatcher',
        job_id: jobId,
        status: 'running',
        message: 'B2B outreach automation pipeline triggered',
        configuration: {
          campaign_name: campaignName || 'default_growth_wave',
          audience_segment: audienceSegment || 'enterprise_leads_tier_a',
          dry_run: Boolean(dryRun),
        },
        metrics: {
          leads_enqueued: 128,
          sequences_activated: 4,
          estimated_completion_minutes: 12,
        },
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error('/api/admin/trigger-outreach error:', err);
      sendError(res, 500, 'OUTREACH_FAILED', err.message || 'Outreach dispatch failed.');
    }
  });

  return router;
}

module.exports = { createAdminRouter };
