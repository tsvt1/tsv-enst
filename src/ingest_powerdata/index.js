/**
 * Power Data Ingestion Module
 *
 * Reads power utilization traces from various formats:
 * - CSV (timestamp, power_w)
 * - NDJSON (ts, power_w, site_id)
 * - json.gz shards
 *
 * Emits normalized power records: {ts, site_id, power_w, energy_j_window}
 */

import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';

/**
 * Reads CSV power traces
 * @param {string} filePath - Path to CSV file
 * @yields {object} Parsed power records
 */
export async function* readPowerCsv(filePath) {
  const stream = createReadStream(filePath, { encoding: 'utf8' });

  const rl = createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  let headerParsed = false;
  let headers = [];

  for await (const line of rl) {
    if (!line.trim()) continue;

    const parts = line.split(',').map(p => p.trim());

    if (!headerParsed) {
      headers = parts.map(h => h.toLowerCase().replace(/[^a-z0-9_]/g, '_'));
      headerParsed = true;
      continue;
    }

    const record = {};
    for (let i = 0; i < headers.length && i < parts.length; i++) {
      const val = parts[i];
      // Try to parse as number
      const num = parseFloat(val);
      record[headers[i]] = isNaN(num) ? val : num;
    }

    yield record;
  }
}

/**
 * Reads NDJSON power traces
 * @param {string} filePath - Path to NDJSON file
 * @yields {object} Parsed power records
 */
export async function* readPowerNdjson(filePath) {
  const stream = createReadStream(filePath, { encoding: 'utf8' });

  const rl = createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      try {
        yield JSON.parse(line);
      } catch (e) {
        continue;
      }
    }
  }
}

/**
 * Reads gzipped NDJSON power traces
 * @param {string} filePath - Path to .json.gz file
 * @yields {object} Parsed power records
 */
export async function* readPowerGzip(filePath) {
  const gunzip = createGunzip();
  const stream = createReadStream(filePath).pipe(gunzip);

  const rl = createInterface({
    input: stream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      try {
        yield JSON.parse(line);
      } catch (e) {
        continue;
      }
    }
  }
}

/**
 * Discovers power trace files in a directory
 * @param {string} dir - Directory to scan
 * @returns {Promise<string[]>} List of file paths
 */
export async function discoverPowerFiles(dir) {
  const files = [];

  try {
    const entries = await readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = join(dir, entry.name);

      if (entry.isDirectory()) {
        const subFiles = await discoverPowerFiles(fullPath);
        files.push(...subFiles);
      } else if (entry.isFile()) {
        const name = entry.name.toLowerCase();
        if (
          name.includes('power') ||
          name.includes('energy') ||
          name.includes('pdu') ||
          name.includes('watt')
        ) {
          if (
            name.endsWith('.csv') ||
            name.endsWith('.json') ||
            name.endsWith('.json.gz') ||
            name.endsWith('.ndjson')
          ) {
            files.push(fullPath);
          }
        }
      }
    }
  } catch (e) {
    // Directory doesn't exist or not accessible
  }

  return files.sort();
}

/**
 * Power record aggregator into time windows
 */
export class PowerAggregator {
  constructor(windowSizeUs = 300_000_000) { // 5 minute default
    this.windowSizeUs = windowSizeUs;
    this.windows = new Map();
  }

  getWindowKey(ts, siteId) {
    const windowStart = Math.floor(ts / this.windowSizeUs) * this.windowSizeUs;
    return `${siteId}:${windowStart}`;
  }

  /**
   * Normalizes timestamp to microseconds
   * @param {number} ts - Timestamp in unknown unit
   * @returns {number} Timestamp in microseconds
   */
  normalizeTimestamp(ts) {
    if (ts < 1e10) {
      // Likely seconds
      return ts * 1_000_000;
    } else if (ts < 1e13) {
      // Likely milliseconds
      return ts * 1_000;
    }
    // Already microseconds
    return ts;
  }

  addPowerRecord(record) {
    // Extract timestamp (various field names)
    let ts = record.ts ?? record.timestamp ?? record.time ?? record.t ?? 0;
    ts = this.normalizeTimestamp(ts);

    const siteId = record.site_id ?? record.machine_id ?? record.node_id ?? 'unknown';
    const key = this.getWindowKey(ts, siteId);

    if (!this.windows.has(key)) {
      const windowStart = Math.floor(ts / this.windowSizeUs) * this.windowSizeUs;
      this.windows.set(key, {
        ts_start: windowStart,
        ts_end: windowStart + this.windowSizeUs,
        site_id: siteId,
        power_samples: [],
        last_ts: windowStart
      });
    }

    const window = this.windows.get(key);

    // Extract power value (various field names)
    const powerW =
      record.power_w ??
      record.power ??
      record.watts ??
      record.watt ??
      record.power_watts ??
      record.pdu_power ??
      null;

    if (powerW !== null && !isNaN(powerW)) {
      window.power_samples.push({
        ts,
        power: Number(powerW)
      });
      window.last_ts = Math.max(window.last_ts, ts);
    }
  }

  /**
   * Computes energy in joules from power samples
   * Uses trapezoidal integration
   * @param {Array} samples - Power samples with timestamps
   * @returns {number} Energy in joules
   */
  computeEnergy(samples) {
    if (samples.length < 2) {
      // If only one sample, assume constant power over window
      if (samples.length === 1) {
        const windowDurationS = this.windowSizeUs / 1_000_000;
        return samples[0].power * windowDurationS;
      }
      return 0;
    }

    // Sort by timestamp
    samples.sort((a, b) => a.ts - b.ts);

    let energyJ = 0;

    for (let i = 1; i < samples.length; i++) {
      const dt = (samples[i].ts - samples[i - 1].ts) / 1_000_000; // Convert to seconds
      const avgPower = (samples[i].power + samples[i - 1].power) / 2;
      energyJ += avgPower * dt;
    }

    return energyJ;
  }

  /**
   * Emits finalized normalized power records
   * @yields {object} Normalized power record
   */
  *emit() {
    for (const window of this.windows.values()) {
      const meanPower = window.power_samples.length > 0
        ? window.power_samples.reduce((sum, s) => sum + s.power, 0) / window.power_samples.length
        : 0;

      const energyJ = this.computeEnergy(window.power_samples);

      yield {
        ts: window.ts_start,
        ts_start: window.ts_start,
        ts_end: window.ts_end,
        site_id: window.site_id,
        power_w: meanPower,
        energy_j_window: energyJ,
        sample_count: window.power_samples.length
      };
    }
  }

  clear() {
    this.windows.clear();
  }
}

/**
 * Main ingestion function for power data
 * @param {string} inputDir - Directory containing power trace data
 * @param {object} options - Configuration options
 * @yields {object} Normalized power records
 */
export async function* ingestPowerdata(inputDir, options = {}) {
  const { windowSizeUs = 300_000_000, siteId = null } = options;
  const aggregator = new PowerAggregator(windowSizeUs);

  const files = await discoverPowerFiles(inputDir);

  for (const file of files) {
    let reader;

    if (file.endsWith('.json.gz')) {
      reader = readPowerGzip(file);
    } else if (file.endsWith('.csv')) {
      reader = readPowerCsv(file);
    } else {
      reader = readPowerNdjson(file);
    }

    for await (const record of reader) {
      if (siteId) {
        record.site_id = siteId;
      }
      aggregator.addPowerRecord(record);
    }
  }

  for (const record of aggregator.emit()) {
    yield record;
  }
}

/**
 * Generate synthetic power data for testing
 * @param {object} options - Generation options
 * @yields {object} Synthetic power records
 */
export async function* generateSyntheticPower(options = {}) {
  const {
    siteId = 'synthetic-site',
    startTs = Date.now() * 1000, // microseconds
    windowCount = 100,
    windowSizeUs = 300_000_000,
    basePowerW = 1000,
    varianceW = 200
  } = options;

  for (let i = 0; i < windowCount; i++) {
    const ts = startTs + (i * windowSizeUs);
    const power = basePowerW + (Math.random() - 0.5) * 2 * varianceW;
    const windowDurationS = windowSizeUs / 1_000_000;

    yield {
      ts,
      ts_start: ts,
      ts_end: ts + windowSizeUs,
      site_id: siteId,
      power_w: Math.max(0, power),
      energy_j_window: Math.max(0, power) * windowDurationS,
      sample_count: 1
    };
  }
}
