const express = require('express');
const { API_VERSION } = require('../config/constants');
const { createHealthRouter } = require('./health');
const { createWhatsappRouter } = require('./whatsapp');
const { createChatRouter } = require('./chat');
const { createFactoryRouter } = require('./factory');
const { createAdminRouter } = require('./admin');

function registerRoutes(app, deps) {
  const { authMiddleware, clientRulesMiddleware, pathResolver } = deps;

  app.get('/api/status', (req, res) => {
    res.json({
      status: 'online',
      message: 'AI Universe Core API is running',
      version: API_VERSION,
    });
  });

  app.use(createHealthRouter());
  app.use(createWhatsappRouter(authMiddleware, clientRulesMiddleware));
  app.use(createChatRouter(authMiddleware, clientRulesMiddleware));
  app.use(createFactoryRouter(authMiddleware, pathResolver));
  app.use(createAdminRouter(authMiddleware));
}

module.exports = { registerRoutes };
