const express = require('express');
const path = require('path');
const { sendError } = require('../utils/http');
const { writeProjectFile } = require('../services/factory');

function createFactoryRouter(authMiddleware, pathResolver) {
  const router = express.Router();
  const { root, resolveSafePath } = pathResolver;

  router.post('/api/factory/write', authMiddleware, async (req, res) => {
    try {
      if (req.role !== 'MASTER_OWNER') {
        return sendError(
          res,
          403,
          'MASTER_ONLY',
          'Factory write requires MASTER_OWNER privileges.'
        );
      }

      const { relativePath, content } = req.body || {};

      if (!relativePath || typeof relativePath !== 'string') {
        return sendError(res, 400, 'PATH_REQUIRED', 'relativePath is required.');
      }

      if (content === undefined || content === null) {
        return sendError(res, 400, 'CONTENT_REQUIRED', 'content is required.');
      }

      const result = await writeProjectFile(resolveSafePath, relativePath, content);

      res.json({
        success: true,
        path: path.relative(root, result.absolutePath),
        bytes_written: result.bytesWritten,
      });
    } catch (err) {
      console.error('/api/factory/write error:', err);
      sendError(res, 500, 'FACTORY_WRITE_FAILED', err.message || 'Failed to write file.');
    }
  });

  return router;
}

module.exports = { createFactoryRouter };
