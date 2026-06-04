const express = require('express');
const cors = require('cors');
const path = require('path');
const { JSON_BODY_LIMIT } = require('./config/constants');
const { createAuthMiddleware } = require('./middleware/auth');
const { createClientRulesMiddleware } = require('./middleware/clientRules');
const { createPathResolver } = require('./utils/paths');
const { sendError } = require('./utils/http');
const { registerRoutes } = require('./routes');

function createApp(projectRoot) {
  const app = express();

  const root = projectRoot || path.resolve(__dirname, '..');

  app.use(cors());
  app.use(express.json({ limit: JSON_BODY_LIMIT }));

  const authMiddleware = createAuthMiddleware();
  const clientRulesMiddleware = createClientRulesMiddleware();
  const pathResolver = createPathResolver(projectRoot || path.resolve(__dirname, '..'));

  registerRoutes(app, {
    authMiddleware,
    clientRulesMiddleware,
    pathResolver,
  });

  return app;
}

function attachErrorHandlers(app) {
  app.use((req, res) => {
    sendError(res, 404, 'NOT_FOUND', `Route ${req.method} ${req.path} not found.`);
  });

  app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    sendError(res, 500, 'INTERNAL_ERROR', 'An unexpected server error occurred.');
  });

}

async function bootstrap(projectRoot, onRoutesRegistered) {
  const app = createApp(projectRoot);

  if (typeof onRoutesRegistered === 'function') {
    onRoutesRegistered(app, projectRoot);
  }

  attachErrorHandlers(app);

  return app;
}

module.exports = { createApp, attachErrorHandlers, bootstrap };
