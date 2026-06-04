const path = require('path');
const fs = require('fs-extra');

async function writeProjectFile(resolveSafePath, relativePath, content) {
  const targetPath = resolveSafePath(relativePath);
  await fs.ensureDir(path.dirname(targetPath));
  await fs.writeFile(targetPath, String(content), 'utf8');

  return {
    absolutePath: targetPath,
    bytesWritten: Buffer.byteLength(String(content), 'utf8'),
  };
}

module.exports = { writeProjectFile };
