const path = require('path');

function createPathResolver(projectRoot) {
  const root = path.resolve(projectRoot);

  function resolveSafePath(relativePath) {
    const normalized = path.normalize(relativePath).replace(/^(\.\.(\/|\\|$))+/, '');
    const absolute = path.resolve(root, normalized);

    if (!absolute.startsWith(root)) {
      throw new Error('Path escapes project root.');
    }

    return absolute;
  }

  return { root, resolveSafePath };
}

module.exports = { createPathResolver };
