const express = require('express');
const { pingDatabase } = require('../db/pool');

function createHealthRouter() {
  const router = express.Router();

  router.get('/health', async (req, res) => {
    try {
      await pingDatabase();
      res.json({ status: 'healthy', database: 'connected' });
    } catch (err) {
      res.status(503).json({ status: 'degraded', database: err.message });
    }
  });

  return router;
}

module.exports = { createHealthRouter };
