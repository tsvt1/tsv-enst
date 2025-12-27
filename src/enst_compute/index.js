/**
 * ENST Computation Module
 *
 * Computes Energy-Normalized System Throughput:
 *   ENST = work_units / energy_j
 *
 * Supports dual-mode work unit computation:
 *   - infra: cpu_core_seconds + gpu_seconds * gpu_weight
 *   - domain: validated_steps or timesteps
 */

import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import {
  computeCostUsd,
  resolvePrice,
  buildNotes,
  DEFAULT_PRICE_USD_PER_MWH
} from '../cost/index.js';

/**
 * Computes work units in infrastructure mode
 * @param {object} tsv - TSV record
 * @param {number} gpuWeight - Weight for GPU seconds (default: 1)
 * @returns {number} Work units
 */
export function computeWorkUnitsInfra(tsv, gpuWeight = 1) {
  const cpuCoreSeconds = tsv.cpu_core_seconds ??
    ((tsv.cpu_util || 0) * (tsv.window_duration_s || 300) * 100);
  const gpuSeconds = tsv.gpu_seconds ??
    ((tsv.gpu_util || 0) * (tsv.window_duration_s || 300) * 8);
  return cpuCoreSeconds + (gpuSeconds * gpuWeight);
}

/**
 * Computes work units in domain mode
 * @param {object} tsv - TSV record
 * @returns {{value: number, mode: string}} Work units and actual mode used
 */
export function computeWorkUnitsDomain(tsv) {
  if (tsv.validated_steps !== null && tsv.validated_steps !== undefined) {
    return { value: tsv.validated_steps, mode: 'domain' };
  }
  if (tsv.timesteps !== null && tsv.timesteps !== undefined) {
    return { value: tsv.timesteps, mode: 'domain' };
  }
  // Fallback to infra
  return { value: computeWorkUnitsInfra(tsv), mode: 'infra_fallback' };
}

/**
 * Computes work units based on mode
 * @param {object} tsv - TSV record
 * @param {string} mode - 'infra' or 'domain'
 * @param {number} gpuWeight - GPU weight for infra mode
 * @returns {{value: number, mode: string}} Work units and mode
 */
export function computeWorkUnits(tsv, mode = 'infra', gpuWeight = 1) {
  if (mode === 'domain') {
    return computeWorkUnitsDomain(tsv);
  }
  return { value: computeWorkUnitsInfra(tsv, gpuWeight), mode: 'infra' };
}

/**
 * Computes ENST for a single TSV record
 * @param {object} tsv - TSV record with energy_j and work_units
 * @returns {number|null} ENST value or null if not computable
 */
export function computeEnst(tsv) {
  const energy = tsv.energy_j;
  const work = tsv.work_units;

  if (energy === null || energy === undefined || energy <= 0) {
    return null;
  }

  if (work === null || work === undefined) {
    return null;
  }

  return work / energy;
}

/**
 * Legacy: Estimates work units from resource utilization
 * @deprecated Use computeWorkUnits instead
 */
export function estimateWorkUnits(tsv) {
  const cpuWork = (tsv.cpu_util || 0) * (tsv.resource_seconds || 0);
  const gpuWork = (tsv.gpu_util || 0) * (tsv.resource_seconds || 0) * 10;
  return cpuWork + gpuWork;
}

/**
 * Processes TSV records and computes ENST
 * @param {AsyncIterable|Iterable} tsvRecords - TSV records
 * @param {object} options - Computation options
 * @yields {object} TSV records with ENST computed
 */
export async function* computeEnstStream(tsvRecords, options = {}) {
  const { workUnitsMode = 'infra', gpuWeight = 1 } = options;

  for await (const tsv of tsvRecords) {
    const record = { ...tsv };

    // Compute work units
    const { value: workUnits, mode: actualMode } = computeWorkUnits(
      record,
      workUnitsMode,
      gpuWeight
    );
    record.work_units = workUnits;
    record.work_units_mode = actualMode;

    // Legacy field for compatibility
    if (record.validated_work_units === null || record.validated_work_units === undefined) {
      record.validated_work_units = workUnits;
    }

    // Compute ENST
    record.enst = computeEnst(record);

    yield record;
  }
}

/**
 * Aggregates ENST by site for leaderboard with extended schema including cost
 */
export class EnstLeaderboard {
  constructor(options = {}) {
    this.sites = new Map();
    this.workUnitsMode = options.workUnitsMode || 'infra';
    this.defaultPriceUsdPerMwh = options.defaultPriceUsdPerMwh || DEFAULT_PRICE_USD_PER_MWH;
  }

  addRecord(record) {
    const siteId = record.site_id;
    const clusterId = record.cluster_id || 'default';
    const key = `${siteId}:${clusterId}`;

    if (!this.sites.has(key)) {
      this.sites.set(key, {
        site_id: siteId,
        cluster_id: clusterId,
        total_energy_j: 0,
        total_work_units: 0,
        window_count: 0,
        windows: [],
        pue_samples: [],
        thermal_headroom_samples: [],
        grid_stress_samples: [],
        price_samples: [],
        price_defaulted_count: 0,
        data_sources: new Set(),
        work_units_mode: record.work_units_mode || this.workUnitsMode
      });
    }

    const site = this.sites.get(key);
    site.window_count++;

    if (record.energy_j !== null && record.energy_j !== undefined) {
      site.total_energy_j += record.energy_j;
    }

    if (record.work_units !== null && record.work_units !== undefined) {
      site.total_work_units += record.work_units;
    }

    // Track window data for output
    site.windows.push({
      ts_start: record.ts_start,
      ts_end: record.ts_end,
      energy_j: record.energy_j,
      work_units: record.work_units,
      enst: record.enst,
      power_w: record.power_w
    });

    // Collect optional metrics
    if (record.pue !== null && record.pue !== undefined) {
      site.pue_samples.push(record.pue);
    }
    if (record.thermal_headroom_w !== null && record.thermal_headroom_w !== undefined) {
      site.thermal_headroom_samples.push(record.thermal_headroom_w);
    }
    if (record.grid_stress_index !== null && record.grid_stress_index !== undefined) {
      site.grid_stress_samples.push(record.grid_stress_index);
    }

    // Track price (weighted by energy for averaging)
    const { price, defaulted } = resolvePrice(record, this.defaultPriceUsdPerMwh);
    site.price_samples.push({ price, energy: record.energy_j || 0 });
    if (defaulted) {
      site.price_defaulted_count++;
    }

    // Track data source
    if (record.data_source) {
      site.data_sources.add(record.data_source);
    }
  }

  getLeaderboard() {
    const entries = [];

    for (const site of this.sites.values()) {
      const enst = site.total_energy_j > 0
        ? site.total_work_units / site.total_energy_j
        : 0;

      const meanPue = site.pue_samples.length > 0
        ? site.pue_samples.reduce((a, b) => a + b, 0) / site.pue_samples.length
        : null;

      const meanThermalHeadroom = site.thermal_headroom_samples.length > 0
        ? site.thermal_headroom_samples.reduce((a, b) => a + b, 0) / site.thermal_headroom_samples.length
        : null;

      const meanGridStress = site.grid_stress_samples.length > 0
        ? site.grid_stress_samples.reduce((a, b) => a + b, 0) / site.grid_stress_samples.length
        : null;

      // Compute weighted average price
      let totalPriceEnergy = 0;
      let weightedPriceSum = 0;
      for (const { price, energy } of site.price_samples) {
        totalPriceEnergy += energy;
        weightedPriceSum += price * energy;
      }
      const avgPrice = totalPriceEnergy > 0
        ? weightedPriceSum / totalPriceEnergy
        : this.defaultPriceUsdPerMwh;

      // Compute cost
      const costUsd = computeCostUsd(site.total_energy_j, avgPrice);

      // Build notes
      const dataSourceStr = site.data_sources.size > 0
        ? Array.from(site.data_sources).join(',')
        : null;
      const notes = buildNotes({
        priceDefaulted: site.price_defaulted_count > 0,
        dataSource: dataSourceStr
      });

      // Get time range from windows
      const sortedWindows = site.windows.sort((a, b) => a.ts_start - b.ts_start);
      const windowStart = sortedWindows.length > 0 ? sortedWindows[0].ts_start : 0;
      const windowEnd = sortedWindows.length > 0 ? sortedWindows[sortedWindows.length - 1].ts_end : 0;

      entries.push({
        window_start: windowStart,
        window_end: windowEnd,
        site_id: site.site_id,
        cluster_id: site.cluster_id,
        energy_j: site.total_energy_j,
        work_units: site.total_work_units,
        work_units_mode: site.work_units_mode,
        enst_units_per_j: enst,
        pue: meanPue,
        thermal_headroom_w: meanThermalHeadroom,
        grid_stress_index: meanGridStress,
        price_usd_per_mwh: avgPrice,
        cost_usd: costUsd,
        notes: notes
      });
    }

    return entries.sort((a, b) => b.enst_units_per_j - a.enst_units_per_j);
  }

  toCsv() {
    const leaderboard = this.getLeaderboard();
    const headers = [
      'window_start', 'window_end', 'site_id', 'cluster_id',
      'energy_j', 'work_units', 'work_units_mode', 'enst_units_per_j',
      'pue', 'thermal_headroom_w', 'grid_stress_index',
      'price_usd_per_mwh', 'cost_usd', 'notes'
    ];

    const lines = [headers.join(',')];

    for (const entry of leaderboard) {
      lines.push([
        entry.window_start,
        entry.window_end,
        entry.site_id,
        entry.cluster_id,
        entry.energy_j.toFixed(2),
        entry.work_units.toFixed(2),
        entry.work_units_mode,
        entry.enst_units_per_j.toFixed(6),
        entry.pue !== null ? entry.pue.toFixed(3) : '',
        entry.thermal_headroom_w !== null ? entry.thermal_headroom_w.toFixed(2) : '',
        entry.grid_stress_index !== null ? entry.grid_stress_index.toFixed(4) : '',
        entry.price_usd_per_mwh.toFixed(2),
        entry.cost_usd.toFixed(2),
        entry.notes
      ].join(','));
    }

    return lines.join('\n');
  }
}

/**
 * Writes leaderboard to CSV file
 */
export async function writeLeaderboardCsv(leaderboard, outputPath) {
  await mkdir(dirname(outputPath), { recursive: true });
  const ws = createWriteStream(outputPath);
  ws.write(leaderboard.toCsv());
  ws.end();
  return new Promise((resolve, reject) => {
    ws.on('finish', resolve);
    ws.on('error', reject);
  });
}

/**
 * Computes summary statistics
 */
export function computeSummaryStats(leaderboard) {
  const entries = leaderboard.getLeaderboard();

  if (entries.length === 0) {
    return { site_count: 0, total_energy_j: 0, total_work_units: 0, global_enst: 0 };
  }

  const totalEnergy = entries.reduce((sum, e) => sum + e.energy_j, 0);
  const totalWork = entries.reduce((sum, e) => sum + e.work_units, 0);
  const globalEnst = totalEnergy > 0 ? totalWork / totalEnergy : 0;
  const totalCostUsd = entries.reduce((sum, e) => sum + e.cost_usd, 0);

  const enstValues = entries.map(e => e.enst_units_per_j).filter(v => v > 0);
  enstValues.sort((a, b) => a - b);

  return {
    site_count: entries.length,
    total_energy_j: totalEnergy,
    total_work_units: totalWork,
    global_enst: globalEnst,
    total_cost_usd: totalCostUsd,
    min_enst: enstValues.length > 0 ? enstValues[0] : 0,
    max_enst: enstValues.length > 0 ? enstValues[enstValues.length - 1] : 0,
    median_enst: enstValues.length > 0 ? enstValues[Math.floor(enstValues.length / 2)] : 0,
    p90_enst: enstValues.length > 0 ? enstValues[Math.floor(enstValues.length * 0.9)] : 0
  };
}
