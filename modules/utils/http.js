function sendError(res, status, code, message, extra = {}) {
  return res.status(status).json({ error: true, code, message, ...extra });
}

module.exports = { sendError };
