/**
 * TSV Normalization Module
 *
 * Merges usage records and power records into unified TSV NDJSON windows.
 */

/**
 * Merges usage and power streams into TSV records
 */
export class TsvNormalizer {
  constructor(options = {}) {
    this.windowSizeUs = options.windowSizeUs || 300_000_000;
    this.usageByKey = new Map();
    this.powerByKey = new Map();
  }

  getWindowKey(ts, siteId) {
    const windowStart = Math.floor(ts / this.windowSizeUs) * this.windowSizeUs;
    return `${siteId}:${windowStart}`;
  }

  addUsageRecord(record) {
    const key = this.getWindowKey(record.ts_start || record.ts, record.site_id);
    this.usageByKey.set(key, record);
  }

  addPowerRecord(record) {
    const key = this.getWindowKey(record.ts_start || record.ts, record.site_id);
    this.powerByKey.set(key, record);
  }

  *emit() {
    const allKeys = new Set([...this.usageByKey.keys(), ...this.powerByKey.keys()]);

    for (const key of allKeys) {
      const usage = this.usageByKey.get(key) || {};
      const power = this.powerByKey.get(key) || {};

      const [siteId, tsStr] = key.split(':');
      const tsStart = parseInt(tsStr, 10);
      const windowDurationS = this.windowSizeUs / 1_000_000;

      yield {
        ts_start: usage.ts_start || power.ts_start || tsStart,
        ts_end: usage.ts_end || power.ts_end || tsStart + this.windowSizeUs,
        site_id: siteId,
        cpu_util: usage.cpu_util ?? 0,
        gpu_util: usage.gpu_util ?? null,
        mem_util: usage.mem_util ?? null,
        job_queue_depth: usage.job_queue_depth ?? 0,
        resource_seconds: usage.resource_seconds_window ?? 0,
        power_w: power.power_w ?? null,
        energy_j: power.energy_j_window ?? null,
        validated_work_units: null,
        throughput_ops: null,
        enst: null,
        window_duration_s: windowDurationS
      };
    }
  }

  clear() {
    this.usageByKey.clear();
    this.powerByKey.clear();
  }
}

export async function normalizeTsv(usageRecords, powerRecords, options = {}) {
  const normalizer = new TsvNormalizer(options);

  for await (const record of usageRecords) {
    normalizer.addUsageRecord(record);
  }

  for await (const record of powerRecords) {
    normalizer.addPowerRecord(record);
  }

  return Array.from(normalizer.emit());
}
