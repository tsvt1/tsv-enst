/**
 * TSV-ENST Test Suite
 * Target: 40+ tests covering all deliverables including cost calculations
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';

import { UsageAggregator } from '../src/ingest_google_clusterdata/index.js';
import { PowerAggregator } from '../src/ingest_powerdata/index.js';
import { TsvNormalizer } from '../src/tsv_normalize/index.js';
import {
  computeEnst,
  computeWorkUnits,
  computeWorkUnitsInfra,
  computeWorkUnitsDomain,
  computeEnstStream,
  EnstLeaderboard,
  estimateWorkUnits
} from '../src/enst_compute/index.js';
import { generateMultiSiteSynthetic, SITE_PROFILES } from '../src/synthetic/index.js';
import { MetricsRegistry, updateMetricsFromTsv } from '../src/prometheus/index.js';
import { evaluatePolicy, applyThrottle, computePolicyImpact } from '../src/policy/index.js';
import {
  computeCostUsd,
  computeEnergyForWork,
  computeCostForWork,
  computeDeltaCostPerWork,
  resolvePrice,
  buildNotes,
  JOULES_PER_MWH,
  DEFAULT_PRICE_USD_PER_MWH,
  REFERENCE_WORK_UNITS
} from '../src/cost/index.js';

// ============================================
// UsageAggregator Tests
// ============================================
describe('UsageAggregator', () => {
  test('aggregates CPU utilization correctly', () => {
    const agg = new UsageAggregator(1_000_000);
    agg.addUsageRecord({ start_time: 0, machine_id: 'site-1', average_usage: { cpus: 0.5 } });
    agg.addUsageRecord({ start_time: 100_000, machine_id: 'site-1', average_usage: { cpus: 0.7 } });
    const records = Array.from(agg.emit());
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].cpu_util, 0.6);
  });

  test('separates different sites', () => {
    const agg = new UsageAggregator(1_000_000);
    agg.addUsageRecord({ start_time: 0, machine_id: 'site-1', average_usage: { cpus: 0.5 } });
    agg.addUsageRecord({ start_time: 0, machine_id: 'site-2', average_usage: { cpus: 0.8 } });
    const records = Array.from(agg.emit());
    assert.strictEqual(records.length, 2);
  });
});

// ============================================
// PowerAggregator Tests
// ============================================
describe('PowerAggregator', () => {
  test('computes mean power correctly', () => {
    const agg = new PowerAggregator(300_000_000);
    agg.addPowerRecord({ ts: 1_000_000_000_000, site_id: 'site-1', power_w: 800 });
    agg.addPowerRecord({ ts: 1_000_000_100_000, site_id: 'site-1', power_w: 1200 });
    const records = Array.from(agg.emit());
    assert.ok(records.length >= 1);
    assert.strictEqual(records[0].power_w, 1000);
  });

  test('separates different sites', () => {
    const agg = new PowerAggregator(300_000_000);
    agg.addPowerRecord({ ts: 1_000_000_000_000, site_id: 'site-1', power_w: 1000 });
    agg.addPowerRecord({ ts: 1_000_000_000_000, site_id: 'site-2', power_w: 2000 });
    const records = Array.from(agg.emit());
    assert.strictEqual(records.length, 2);
  });
});

// ============================================
// TsvNormalizer Tests
// ============================================
describe('TsvNormalizer', () => {
  test('merges usage and power records', () => {
    const norm = new TsvNormalizer({ windowSizeUs: 1_000_000 });
    norm.addUsageRecord({ ts: 0, ts_start: 0, ts_end: 1_000_000, site_id: 'site-1', cpu_util: 0.6, resource_seconds_window: 60 });
    norm.addPowerRecord({ ts: 0, ts_start: 0, ts_end: 1_000_000, site_id: 'site-1', power_w: 1000, energy_j_window: 1000 });
    const records = Array.from(norm.emit());
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].cpu_util, 0.6);
    assert.strictEqual(records[0].power_w, 1000);
  });
});

// ============================================
// Work Units Mode Tests
// ============================================
describe('Work Units - Infra Mode', () => {
  test('computes work units from cpu_core_seconds and gpu_seconds', () => {
    const tsv = { cpu_core_seconds: 1000, gpu_seconds: 500, window_duration_s: 300 };
    const result = computeWorkUnitsInfra(tsv, 1);
    assert.strictEqual(result, 1500);
  });

  test('applies gpu_weight correctly', () => {
    const tsv = { cpu_core_seconds: 1000, gpu_seconds: 500, window_duration_s: 300 };
    const result = computeWorkUnitsInfra(tsv, 2);
    assert.strictEqual(result, 2000);
  });

  test('falls back to utilization when core_seconds missing', () => {
    const tsv = { cpu_util: 0.5, gpu_util: 0.25, window_duration_s: 300 };
    const result = computeWorkUnitsInfra(tsv, 1);
    assert.strictEqual(result, 15600);
  });

  test('computeWorkUnits returns infra mode', () => {
    const tsv = { cpu_core_seconds: 1000, gpu_seconds: 500 };
    const result = computeWorkUnits(tsv, 'infra', 1);
    assert.strictEqual(result.mode, 'infra');
    assert.strictEqual(result.value, 1500);
  });
});

describe('Work Units - Domain Mode', () => {
  test('uses validated_steps when available', () => {
    const tsv = { validated_steps: 5000, cpu_core_seconds: 1000 };
    const result = computeWorkUnitsDomain(tsv);
    assert.strictEqual(result.mode, 'domain');
    assert.strictEqual(result.value, 5000);
  });

  test('uses timesteps when validated_steps missing', () => {
    const tsv = { timesteps: 4000, cpu_core_seconds: 1000 };
    const result = computeWorkUnitsDomain(tsv);
    assert.strictEqual(result.mode, 'domain');
    assert.strictEqual(result.value, 4000);
  });

  test('falls back to infra when no domain fields', () => {
    const tsv = { cpu_core_seconds: 1000, gpu_seconds: 500 };
    const result = computeWorkUnitsDomain(tsv);
    assert.strictEqual(result.mode, 'infra_fallback');
    assert.strictEqual(result.value, 1500);
  });

  test('computeWorkUnits returns domain mode', () => {
    const tsv = { validated_steps: 5000 };
    const result = computeWorkUnits(tsv, 'domain', 1);
    assert.strictEqual(result.mode, 'domain');
    assert.strictEqual(result.value, 5000);
  });
});

// ============================================
// ENST Computation Tests
// ============================================
describe('ENST Computation', () => {
  test('computes ENST correctly', () => {
    const tsv = { energy_j: 1000, work_units: 500 };
    const enst = computeEnst(tsv);
    assert.strictEqual(enst, 0.5);
  });

  test('returns null for zero energy', () => {
    const tsv = { energy_j: 0, work_units: 500 };
    assert.strictEqual(computeEnst(tsv), null);
  });

  test('returns null for null work_units', () => {
    const tsv = { energy_j: 1000, work_units: null };
    assert.strictEqual(computeEnst(tsv), null);
  });

  test('legacy estimateWorkUnits still works', () => {
    const tsv = { cpu_util: 0.5, gpu_util: 0.2, resource_seconds: 100 };
    const work = estimateWorkUnits(tsv);
    assert.strictEqual(work, 250);
  });
});

// ============================================
// Cost Computation Tests
// ============================================
describe('Cost Computation', () => {
  test('computeCostUsd converts joules to USD', () => {
    // 1 MWh = 3.6e9 J, at $50/MWh = $50 for 3.6e9 J
    const cost = computeCostUsd(3.6e9, 50);
    assert.strictEqual(cost, 50);
  });

  test('computeCostUsd returns 0 for zero energy', () => {
    assert.strictEqual(computeCostUsd(0, 50), 0);
    assert.strictEqual(computeCostUsd(null, 50), 0);
  });

  test('computeEnergyForWork calculates energy needed', () => {
    // With ENST = 0.5 (500 work per 1000 J), need 2000 J for 1000 work
    const energy = computeEnergyForWork(1000, 0.5);
    assert.strictEqual(energy, 2000);
  });

  test('computeEnergyForWork returns Infinity for zero ENST', () => {
    assert.strictEqual(computeEnergyForWork(1000, 0), Infinity);
    assert.strictEqual(computeEnergyForWork(1000, null), Infinity);
  });

  test('computeCostForWork calculates cost for work', () => {
    // ENST = 1e6 (1M work per J), 1e9 work needs 1000 J
    // 1000 J = 1000 / 3.6e9 MWh * $50 = $0.0000139
    const cost = computeCostForWork(1e9, 1e6, 50);
    assert.ok(cost > 0 && cost < 1);
  });

  test('computeDeltaCostPerWork calculates cost delta', () => {
    // Better ENST means lower cost for same work
    const delta = computeDeltaCostPerWork(0.001, 0.002, 50, 1e9);
    assert.ok(delta < 0); // Higher ENST = lower cost = negative delta
  });

  test('resolvePrice uses record price when available', () => {
    const { price, defaulted } = resolvePrice({ price_usd_per_mwh: 65 }, 50);
    assert.strictEqual(price, 65);
    assert.strictEqual(defaulted, false);
  });

  test('resolvePrice uses default when missing', () => {
    const { price, defaulted } = resolvePrice({}, 50);
    assert.strictEqual(price, 50);
    assert.strictEqual(defaulted, true);
  });

  test('buildNotes creates notes string', () => {
    const notes = buildNotes({ priceDefaulted: true, dataSource: 'synthetic' });
    assert.ok(notes.includes('missing_price_defaulted'));
    assert.ok(notes.includes('data_source=synthetic'));
  });

  test('constants are defined correctly', () => {
    assert.strictEqual(JOULES_PER_MWH, 3.6e9);
    assert.strictEqual(DEFAULT_PRICE_USD_PER_MWH, 50);
    assert.strictEqual(REFERENCE_WORK_UNITS, 1e9);
  });
});

// ============================================
// Multi-Site Synthetic Generator Tests
// ============================================
describe('Multi-Site Synthetic Generator', () => {
  test('generates records for 3 sites', () => {
    const records = Array.from(generateMultiSiteSynthetic({
      sites: ['site-a', 'site-b', 'site-c'],
      windowsPerSite: 10
    }));
    assert.strictEqual(records.length, 30);
  });

  test('includes all required TSV fields', () => {
    const records = Array.from(generateMultiSiteSynthetic({ windowsPerSite: 1 }));
    const record = records[0];
    assert.ok('ts_start' in record);
    assert.ok('ts_end' in record);
    assert.ok('site_id' in record);
    assert.ok('cluster_id' in record);
    assert.ok('cpu_util' in record);
    assert.ok('power_w' in record);
    assert.ok('energy_j' in record);
    assert.ok('cpu_core_seconds' in record);
    assert.ok('pue' in record);
    assert.ok('thermal_headroom_w' in record);
    assert.ok('grid_stress_index' in record);
  });

  test('generates validated_steps for appropriate sites', () => {
    const records = Array.from(generateMultiSiteSynthetic({
      sites: ['ornl-frontier'],
      windowsPerSite: 5
    }));
    assert.ok(records.every(r => r.validated_steps !== null));
  });

  test('site profiles are defined', () => {
    assert.ok('nrel-eagle' in SITE_PROFILES);
    assert.ok('ornl-frontier' in SITE_PROFILES);
    assert.ok('anl-polaris' in SITE_PROFILES);
  });

  test('synthetic records include price_usd_per_mwh', () => {
    const records = Array.from(generateMultiSiteSynthetic({ windowsPerSite: 1 }));
    assert.ok(records.every(r => r.price_usd_per_mwh !== undefined && r.price_usd_per_mwh > 0));
  });

  test('synthetic records include data_source', () => {
    const records = Array.from(generateMultiSiteSynthetic({ windowsPerSite: 1 }));
    assert.ok(records.every(r => r.data_source === 'synthetic'));
  });

  test('site profiles have deterministic prices', () => {
    assert.ok(SITE_PROFILES['nrel-eagle'].priceUsdPerMwh > 0);
    assert.ok(SITE_PROFILES['ornl-frontier'].priceUsdPerMwh > 0);
    assert.ok(SITE_PROFILES['anl-polaris'].priceUsdPerMwh > 0);
  });
});

// ============================================
// Leaderboard Tests with Cost
// ============================================
describe('EnstLeaderboard with Cost', () => {
  test('ranks sites by ENST', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'low', cluster_id: 'c1', energy_j: 1000, work_units: 100, enst: 0.1, cpu_util: 0.5 });
    lb.addRecord({ site_id: 'high', cluster_id: 'c1', energy_j: 1000, work_units: 500, enst: 0.5, cpu_util: 0.8 });
    const leaderboard = lb.getLeaderboard();
    assert.strictEqual(leaderboard[0].site_id, 'high');
    assert.strictEqual(leaderboard[1].site_id, 'low');
  });

  test('generates CSV with cost columns', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 3.6e9, work_units: 500, price_usd_per_mwh: 50 });
    const csv = lb.toCsv();
    assert.ok(csv.includes('price_usd_per_mwh'));
    assert.ok(csv.includes('cost_usd'));
    assert.ok(csv.includes('notes'));
  });

  test('leaderboard contains price_usd_per_mwh', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 1000, work_units: 500, price_usd_per_mwh: 65 });
    const entries = lb.getLeaderboard();
    assert.strictEqual(entries[0].price_usd_per_mwh, 65);
  });

  test('leaderboard contains cost_usd', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 3.6e9, work_units: 500, price_usd_per_mwh: 50 });
    const entries = lb.getLeaderboard();
    assert.strictEqual(entries[0].cost_usd, 50); // 1 MWh * $50
  });

  test('tracks work_units_mode', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 1000, work_units: 500, work_units_mode: 'domain' });
    const leaderboard = lb.getLeaderboard();
    assert.strictEqual(leaderboard[0].work_units_mode, 'domain');
  });

  test('notes include data_source', () => {
    const lb = new EnstLeaderboard();
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 1000, work_units: 500, data_source: 'synthetic' });
    const entries = lb.getLeaderboard();
    assert.ok(entries[0].notes.includes('data_source=synthetic'));
  });

  test('notes include missing_price_defaulted when price null', () => {
    const lb = new EnstLeaderboard({ defaultPriceUsdPerMwh: 50 });
    lb.addRecord({ site_id: 'test', cluster_id: 'c1', energy_j: 1000, work_units: 500 });
    const entries = lb.getLeaderboard();
    assert.ok(entries[0].notes.includes('missing_price_defaulted'));
  });
});

// ============================================
// Prometheus Metrics Tests
// ============================================
describe('Prometheus Metrics', () => {
  test('MetricsRegistry stores gauges', () => {
    const registry = new MetricsRegistry();
    registry.setGauge('test_metric', 42, { label: 'value' });
    const output = registry.toPrometheusFormat();
    assert.ok(output.includes('test_metric'));
    assert.ok(output.includes('42'));
  });

  test('updateMetricsFromTsv sets all metrics', () => {
    const registry = new MetricsRegistry();
    const record = {
      site_id: 'test-site',
      cluster_id: 'test-cluster',
      power_w: 1000,
      energy_j: 300000,
      job_queue_depth: 10,
      cpu_util: 0.7,
      gpu_util: 0.5,
      enst: 0.001,
      work_units: 300,
      pue: 1.2,
      thermal_headroom_w: 5000,
      grid_stress_index: 0.3
    };
    updateMetricsFromTsv(registry, record);
    const output = registry.toPrometheusFormat();
    assert.ok(output.includes('tsv_power_w'));
    assert.ok(output.includes('tsv_cpu_util'));
    assert.ok(output.includes('enst_units_per_j'));
    assert.ok(output.includes('site_id="test-site"'));
  });

  test('metrics format is valid Prometheus', () => {
    const registry = new MetricsRegistry();
    registry.setGauge('metric_a', 1, { site: 's1' });
    registry.setGauge('metric_b', 2, { site: 's2' });
    const output = registry.toPrometheusFormat();
    assert.ok(output.includes('# TYPE metric_a gauge'));
    assert.ok(output.includes('# TYPE metric_b gauge'));
  });
});

// ============================================
// Policy Evaluation Tests with Cost
// ============================================
describe('Policy Evaluation', () => {
  test('energy_cap triggers throttle', () => {
    const record = { power_w: 3500, thermal_headroom_w: 10000, grid_stress_index: 0.2 };
    const policy = { energy_cap_w: 3000, throttle_factor: 0.5 };
    const result = evaluatePolicy(record, policy);
    assert.strictEqual(result.throttle_applied, true);
    assert.strictEqual(result.throttle_factor, 0.5);
    assert.ok(result.violations.some(v => v.type === 'energy_cap'));
  });

  test('thermal_cap triggers throttle', () => {
    const record = { power_w: 2000, thermal_headroom_w: 5000, grid_stress_index: 0.2 };
    const policy = { thermal_cap_w: 8000, throttle_factor: 0.6 };
    const result = evaluatePolicy(record, policy);
    assert.strictEqual(result.throttle_applied, true);
    assert.ok(result.violations.some(v => v.type === 'thermal_cap'));
  });

  test('grid_stress_cap triggers migrate flag', () => {
    const record = { power_w: 2000, thermal_headroom_w: 10000, grid_stress_index: 0.9 };
    const policy = { grid_stress_cap: 0.8 };
    const result = evaluatePolicy(record, policy);
    assert.strictEqual(result.migrate_flag, true);
    assert.ok(result.violations.some(v => v.type === 'grid_stress'));
  });

  test('no violation when within caps', () => {
    const record = { power_w: 2000, thermal_headroom_w: 15000, grid_stress_index: 0.2 };
    const policy = { energy_cap_w: 3000, thermal_cap_w: 10000, grid_stress_cap: 0.8 };
    const result = evaluatePolicy(record, policy);
    assert.strictEqual(result.throttle_applied, false);
    assert.strictEqual(result.migrate_flag, false);
    assert.strictEqual(result.violations.length, 0);
  });

  test('applyThrottle reduces work_units', () => {
    const record = { work_units: 1000, energy_j: 5000, cpu_core_seconds: 500 };
    const evalResult = { throttle_applied: true, throttle_factor: 0.5, violations: [] };
    const throttled = applyThrottle(record, evalResult);
    assert.strictEqual(throttled.work_units, 500);
    assert.strictEqual(throttled._policy_throttled, true);
  });

  test('computePolicyImpact includes cost data', () => {
    const baseline = [
      { energy_j: 3.6e9, work_units: 1000, price_usd_per_mwh: 50 },
      { energy_j: 3.6e9, work_units: 1000, price_usd_per_mwh: 50 }
    ];
    const policy = [
      { energy_j: 3.6e9 * 0.75, work_units: 500, price_usd_per_mwh: 50 },
      { energy_j: 3.6e9 * 0.75, work_units: 500, price_usd_per_mwh: 50 }
    ];
    const impact = computePolicyImpact(baseline, policy);
    assert.ok('cost_usd' in impact.baseline);
    assert.ok('cost_usd' in impact.policy);
    assert.ok('cost_usd' in impact.delta);
    assert.ok('delta_cost_usd_per_1e9_work_units' in impact);
  });

  test('replay_summary contains delta.cost_usd', () => {
    const baseline = [{ energy_j: 1000, work_units: 100, price_usd_per_mwh: 50 }];
    const policy = [{ energy_j: 900, work_units: 50, price_usd_per_mwh: 50 }];
    const impact = computePolicyImpact(baseline, policy);
    assert.ok(typeof impact.delta.cost_usd === 'number');
  });

  test('replay_summary contains delta_cost_usd_per_1e9_work_units', () => {
    const baseline = [{ energy_j: 1e9, work_units: 1e6, price_usd_per_mwh: 50 }];
    const policy = [{ energy_j: 0.9e9, work_units: 0.9e6, price_usd_per_mwh: 50 }];
    const impact = computePolicyImpact(baseline, policy);
    assert.ok(impact.delta_cost_usd_per_1e9_work_units !== null);
  });
});

// ============================================
// Integration Tests
// ============================================
describe('Integration', () => {
  test('full pipeline: synthetic -> enst stream', async () => {
    const records = Array.from(generateMultiSiteSynthetic({
      sites: ['site-a', 'site-b'],
      windowsPerSite: 5
    }));

    const processed = [];
    for await (const r of computeEnstStream(records, { workUnitsMode: 'infra' })) {
      processed.push(r);
    }

    assert.strictEqual(processed.length, 10);
    assert.ok(processed.every(r => r.work_units !== undefined));
    assert.ok(processed.every(r => r.work_units_mode === 'infra'));
    assert.ok(processed.every(r => r.enst !== null));
  });

  test('domain mode uses validated_steps from synthetic', async () => {
    const records = Array.from(generateMultiSiteSynthetic({
      sites: ['ornl-frontier'],
      windowsPerSite: 5
    }));

    const processed = [];
    for await (const r of computeEnstStream(records, { workUnitsMode: 'domain' })) {
      processed.push(r);
    }

    assert.ok(processed.every(r => r.work_units_mode === 'domain'));
  });

  test('leaderboard with synthetic data has cost_usd', async () => {
    const records = Array.from(generateMultiSiteSynthetic({
      sites: ['nrel-eagle', 'ornl-frontier'],
      windowsPerSite: 5
    }));

    const lb = new EnstLeaderboard();
    for await (const r of computeEnstStream(records, { workUnitsMode: 'infra' })) {
      lb.addRecord(r);
    }

    const entries = lb.getLeaderboard();
    assert.ok(entries.every(e => e.cost_usd > 0));
    assert.ok(entries.every(e => e.price_usd_per_mwh > 0));
  });
});

// ============================================
// Public Traces Tests
// ============================================
import {
  generatePublicUsageRecords,
  generatePublicPowerRecords,
  loadPublicTraces,
  writePublicTraces,
  PUBLIC_TRACE_SITES,
  FIXED_SEED,
  FIXED_START_TS,
  WINDOWS_PER_SITE
} from '../src/public_traces/index.js';
import {
  sha256String,
  sha256File,
  generateHashesContent,
  computeGenesisHashes
} from '../src/hash/index.js';

describe('Public Traces', () => {
  test('generates deterministic usage records', () => {
    const records1 = Array.from(generatePublicUsageRecords());
    const records2 = Array.from(generatePublicUsageRecords());

    assert.strictEqual(records1.length, records2.length);
    assert.strictEqual(records1[0].ts_start, records2[0].ts_start);
    assert.strictEqual(records1[0].cpu_util, records2[0].cpu_util);
  });

  test('generates deterministic power records', () => {
    const records1 = Array.from(generatePublicPowerRecords());
    const records2 = Array.from(generatePublicPowerRecords());

    assert.strictEqual(records1.length, records2.length);
    assert.strictEqual(records1[0].power_w, records2[0].power_w);
  });

  test('usage records have correct site count', () => {
    const records = Array.from(generatePublicUsageRecords());
    const sites = new Set(records.map(r => r.site_id));
    assert.strictEqual(sites.size, Object.keys(PUBLIC_TRACE_SITES).length);
  });

  test('generates correct number of windows per site', () => {
    const records = Array.from(generatePublicUsageRecords());
    assert.strictEqual(records.length, Object.keys(PUBLIC_TRACE_SITES).length * WINDOWS_PER_SITE);
  });

  test('usage records have required fields', () => {
    const records = Array.from(generatePublicUsageRecords());
    const record = records[0];
    assert.ok('ts_start' in record);
    assert.ok('ts_end' in record);
    assert.ok('site_id' in record);
    assert.ok('cpu_util' in record);
    assert.ok('cpu_core_seconds' in record);
  });

  test('power records have price_usd_per_mwh', () => {
    const records = Array.from(generatePublicPowerRecords());
    assert.ok(records.every(r => r.price_usd_per_mwh > 0));
  });

  test('loadPublicTraces returns records with data_source', async () => {
    const records = [];
    for await (const r of loadPublicTraces()) {
      records.push(r);
    }
    assert.ok(records.length > 0);
    assert.ok(records.every(r => r.data_source === 'real:public-traces'));
  });

  test('loadPublicTraces includes price_usd_per_mwh', async () => {
    const records = [];
    for await (const r of loadPublicTraces()) {
      records.push(r);
    }
    assert.ok(records.every(r => r.price_usd_per_mwh > 0));
  });

  test('public traces have fixed start timestamp', () => {
    const records = Array.from(generatePublicUsageRecords());
    assert.strictEqual(records[0].ts_start, FIXED_START_TS);
  });

  test('site profiles have all required fields', () => {
    for (const profile of Object.values(PUBLIC_TRACE_SITES)) {
      assert.ok('basePowerW' in profile);
      assert.ok('cpuUtilBase' in profile);
      assert.ok('priceUsdPerMwh' in profile);
      assert.ok('pue' in profile);
    }
  });
});

// ============================================
// SHA256 Hash Tests
// ============================================
describe('SHA256 Hashing', () => {
  test('sha256String computes hash correctly', () => {
    const hash = sha256String('hello world');
    assert.strictEqual(hash.length, 64); // SHA256 hex is 64 chars
    assert.ok(/^[a-f0-9]+$/.test(hash));
  });

  test('sha256String is deterministic', () => {
    const hash1 = sha256String('test content');
    const hash2 = sha256String('test content');
    assert.strictEqual(hash1, hash2);
  });

  test('generateHashesContent creates valid format', () => {
    const hashes = {
      schema_sha256: 'abc123',
      leaderboard_real_sha256: 'def456'
    };
    const content = generateHashesContent(hashes);
    assert.ok(content.includes('schema_sha256=abc123'));
    assert.ok(content.includes('leaderboard_real_sha256=def456'));
  });

  test('sha256File computes file hash', async () => {
    const hash = await sha256File('package.json');
    assert.strictEqual(hash.length, 64);
    assert.ok(/^[a-f0-9]+$/.test(hash));
  });
});

// ============================================
// Public Traces Integration Tests
// ============================================
describe('Public Traces Integration', () => {
  test('full pipeline with public traces', async () => {
    const records = [];
    for await (const r of loadPublicTraces()) {
      records.push(r);
    }

    const lb = new EnstLeaderboard();
    for await (const r of computeEnstStream(records, { workUnitsMode: 'infra' })) {
      lb.addRecord(r);
    }

    const entries = lb.getLeaderboard();
    assert.ok(entries.length >= 3); // 3 sites
    assert.ok(entries.every(e => e.notes.includes('data_source=real:public-traces')));
  });

  test('public traces produce deterministic leaderboard', async () => {
    const runPipeline = async () => {
      const records = [];
      for await (const r of loadPublicTraces()) {
        records.push(r);
      }

      const lb = new EnstLeaderboard();
      for await (const r of computeEnstStream(records, { workUnitsMode: 'infra' })) {
        lb.addRecord(r);
      }

      return lb.getLeaderboard();
    };

    const entries1 = await runPipeline();
    const entries2 = await runPipeline();

    assert.strictEqual(entries1.length, entries2.length);
    assert.strictEqual(entries1[0].enst_units_per_j, entries2[0].enst_units_per_j);
    assert.strictEqual(entries1[0].cost_usd, entries2[0].cost_usd);
  });
});
