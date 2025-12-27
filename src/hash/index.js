/**
 * SHA256 Hash Utility
 *
 * Computes SHA256 hashes for files and generates HASHES.txt
 */

import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { readFile } from 'node:fs/promises';

/**
 * Computes SHA256 hash of a file
 * @param {string} filePath - Path to file
 * @returns {Promise<string>} Hex-encoded SHA256 hash
 */
export async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(filePath);

    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

/**
 * Computes SHA256 hash of a string
 * @param {string} content - String content
 * @returns {string} Hex-encoded SHA256 hash
 */
export function sha256String(content) {
  return createHash('sha256').update(content, 'utf8').digest('hex');
}

/**
 * Generates HASHES.txt content
 * @param {object} hashes - Object with hash entries
 * @returns {string} HASHES.txt content
 */
export function generateHashesContent(hashes) {
  const lines = [];
  for (const [key, value] of Object.entries(hashes)) {
    lines.push(`${key}=${value}`);
  }
  return lines.join('\n') + '\n';
}

/**
 * Writes HASHES.txt file
 * @param {string} outputPath - Output file path
 * @param {object} hashes - Object with hash entries
 */
export async function writeHashesFile(outputPath, hashes) {
  const content = generateHashesContent(hashes);
  const ws = createWriteStream(outputPath);
  ws.write(content);
  ws.end();
  return new Promise((resolve, reject) => {
    ws.on('finish', resolve);
    ws.on('error', reject);
  });
}

/**
 * Computes hashes for GENESIS packet
 * @param {object} paths - Object with file paths
 * @returns {Promise<object>} Object with computed hashes
 */
export async function computeGenesisHashes(paths) {
  const hashes = {};

  if (paths.schema) {
    hashes.schema_sha256 = await sha256File(paths.schema);
  }

  if (paths.leaderboard) {
    hashes.leaderboard_real_sha256 = await sha256File(paths.leaderboard);
  }

  if (paths.replaySummary) {
    hashes.replay_summary_real_sha256 = await sha256File(paths.replaySummary);
  }

  return hashes;
}
