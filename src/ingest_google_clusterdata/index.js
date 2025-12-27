/**
 * Google Clusterdata Ingestion Module
 *
 * Reads sharded json.gz tables from Google cluster trace format:
 * - collection_events
 * - instance_events
 * - instance_usage
 * - machine_events
 * - machine_attributes
 *
 * Emits normalized usage records: {ts, site_id, cpu_util, gpu_util, job_queue_depth, resource_seconds_window}
 */

import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { readdir, stat } from 'node:fs/promises';
import { join, basename } from 'node:path';

const TABLE_TYPES = [
  'collection_events',
  'instance_events',
  'instance_usage',
  'machine_events',
  'machine_attributes'
];

/**
 * Reads a single json.gz shard file as async generator
 * @param {string} filePath - Path to .json.gz file
 * @yields {object} Parsed JSON records
 */
export async function* readShardedGzip(filePath) {
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
        // Skip malformed lines
        continue;
      }
    }
  }
}

/**
 * Reads plain JSON file (newline-delimited)
 * @param {string} filePath - Path to .json file
 * @yields {object} Parsed JSON records
 */
export async function* readNdjson(filePath) {
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
 * Discovers all shard files for a given table type in a directory
 * @param {string} dir - Base directory
 * @param {string} tableType - One of TABLE_TYPES
 * @returns {Promise<string[]>} List of file paths
 */
export async function discoverShards(dir, tableType) {
  const files = [];

  try {
    const entries = await readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = join(dir, entry.name);

      if (entry.isDirectory()) {
        // Recurse into subdirectories
        const subFiles = await discoverShards(fullPath, tableType);
        files.push(...subFiles);
      } else if (entry.isFile()) {
        const name = entry.name.toLowerCase();
        if (name.includes(tableType) && (name.endsWith('.json.gz') || name.endsWith('.json'))) {
          files.push(fullPath);
        }
      }
    }
  } catch (e) {
    // Directory doesn't exist or not accessible
  }

  return files.sort();
}

/**
 * Ingests instance_usage records and aggregates to windows
 */
export class UsageAggregator {
  constructor(windowSizeUs = 300_000_000) { // 5 minute default
    this.windowSizeUs = windowSizeUs;
    this.windows = new Map(); // windowKey -> aggregated data
  }

  getWindowKey(ts, siteId) {
    const windowStart = Math.floor(ts / this.windowSizeUs) * this.windowSizeUs;
    return `${siteId}:${windowStart}`;
  }

  addUsageRecord(record) {
    const ts = record.start_time || record.time || record.ts || 0;
    const siteId = record.machine_id || record.site_id || 'unknown';
    const key = this.getWindowKey(ts, siteId);

    if (!this.windows.has(key)) {
      const windowStart = Math.floor(ts / this.windowSizeUs) * this.windowSizeUs;
      this.windows.set(key, {
        ts_start: windowStart,
        ts_end: windowStart + this.windowSizeUs,
        site_id: siteId,
        cpu_samples: [],
        gpu_samples: [],
        mem_samples: [],
        job_count: 0,
        resource_seconds: 0
      });
    }

    const window = this.windows.get(key);

    // Extract CPU utilization (Google format uses average_usage.cpus)
    const cpuUtil = record.average_usage?.cpus ?? record.cpu_util ?? record.assigned_memory ?? null;
    if (cpuUtil !== null) {
      window.cpu_samples.push(Number(cpuUtil));
    }

    // Extract memory utilization
    const memUtil = record.average_usage?.memory ?? record.mem_util ?? null;
    if (memUtil !== null) {
      window.mem_samples.push(Number(memUtil));
    }

    // GPU if present
    const gpuUtil = record.gpu_util ?? null;
    if (gpuUtil !== null) {
      window.gpu_samples.push(Number(gpuUtil));
    }

    // Count job/task events
    if (record.type !== undefined || record.event_type !== undefined) {
      window.job_count++;
    }

    // Accumulate resource seconds
    const duration = (record.end_time || ts + 1_000_000) - ts;
    const durationSec = duration / 1_000_000;
    window.resource_seconds += (cpuUtil || 0) * durationSec;
  }

  /**
   * Emits finalized normalized usage records
   * @yields {object} Normalized usage record
   */
  *emit() {
    for (const window of this.windows.values()) {
      const cpuUtil = window.cpu_samples.length > 0
        ? window.cpu_samples.reduce((a, b) => a + b, 0) / window.cpu_samples.length
        : 0;

      const gpuUtil = window.gpu_samples.length > 0
        ? window.gpu_samples.reduce((a, b) => a + b, 0) / window.gpu_samples.length
        : null;

      const memUtil = window.mem_samples.length > 0
        ? window.mem_samples.reduce((a, b) => a + b, 0) / window.mem_samples.length
        : null;

      yield {
        ts: window.ts_start,
        ts_start: window.ts_start,
        ts_end: window.ts_end,
        site_id: window.site_id,
        cpu_util: Math.min(1, Math.max(0, cpuUtil)),
        gpu_util: gpuUtil !== null ? Math.min(1, Math.max(0, gpuUtil)) : null,
        mem_util: memUtil !== null ? Math.min(1, Math.max(0, memUtil)) : null,
        job_queue_depth: window.job_count,
        resource_seconds_window: window.resource_seconds
      };
    }
  }

  clear() {
    this.windows.clear();
  }
}

/**
 * Main ingestion function for Google clusterdata
 * @param {string} inputDir - Directory containing cluster trace data
 * @param {object} options - Configuration options
 * @yields {object} Normalized usage records
 */
export async function* ingestGoogleClusterdata(inputDir, options = {}) {
  const { windowSizeUs = 300_000_000, siteId = null } = options;
  const aggregator = new UsageAggregator(windowSizeUs);

  // Discover and process instance_usage files (primary usage data)
  const usageFiles = await discoverShards(inputDir, 'instance_usage');

  for (const file of usageFiles) {
    const reader = file.endsWith('.gz') ? readShardedGzip(file) : readNdjson(file);

    for await (const record of reader) {
      if (siteId) {
        record.site_id = siteId;
      }
      aggregator.addUsageRecord(record);
    }
  }

  // Also process collection_events for job queue depth
  const collectionFiles = await discoverShards(inputDir, 'collection_events');
  for (const file of collectionFiles) {
    const reader = file.endsWith('.gz') ? readShardedGzip(file) : readNdjson(file);

    for await (const record of reader) {
      if (siteId) {
        record.site_id = siteId;
      }
      aggregator.addUsageRecord(record);
    }
  }

  // Emit aggregated windows
  for (const record of aggregator.emit()) {
    yield record;
  }
}

export { TABLE_TYPES };
