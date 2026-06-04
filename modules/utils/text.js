const { MAX_RESPONSE_WORDS } = require('../config/constants');

function truncateToWordLimit(text, maxWords = MAX_RESPONSE_WORDS) {
  if (!text || typeof text !== 'string') return '';
  const words = text.trim().split(/\s+/);
  if (words.length <= maxWords) return text.trim();
  return `${words.slice(0, maxWords).join(' ')}…`;
}

module.exports = { truncateToWordLimit };
