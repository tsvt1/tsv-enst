#!/usr/bin/env node

/**
 * replay CLI
 *
 * Replays TSV records with policy evaluation for ENST and cost impact analysis.
 */

import { parseArgs } from 'node:util';
import { createReadStream, createWriteStream } from 'node:fs';
import { readFile, mkdir } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { dirname } from 'node:path';

import { computeEnstStream } from '../src/enst_compute/index.js';
import { applyPolicyStream, computePolicyImpact } from '../src/policy/index.js';
import { DEFAULT_PRICE_USD_PER_MWH } from '../src/cost/index.js';

const options = {
  'input': { type: 'string', short: 'i', default: 'outputs/tsv.ndjson' },
  'output': { type: 'string', short: 'o', default: 'outputs/replay_summary.json' },
  'speed': { type: 'string', short: 's', default: '0' },
  'format': { type: 'string', short: 'f', default: 'json' },
  'filter-site': { type: 'string' },
  'start-ts': { type: 'string' },
  'end-ts': { type: 'string' },
  'fields': { type: 'string' },
  'stats': { type: 'boolean', default: false },
  'policy': { type: 'string' },
  'work-units-mode': { type: 'string', short: 'm', default: 'infra' },
  'default-price-usd-per-mwh': { type: 'string', default: '50' },
  'help': { type: 'boolean', short: 'h' }
};

function printUsage() {
  console.log(`
replay - Replay TSV records with policy evaluation

Usage:
  replay [options]

Options:
  -i, --input <path>                Input TSV NDJSON file (default: outputs/tsv.ndjson)
  -o, --output <path>               Output summary file (default: outputs/replay_summary.json)
  -s, --speed <factor>              Replay speed multiplier (default: 0 = instant)
  -f, --format <type>               Output format: json, csv, table (default: json)
  --filter-site <id>                Filter to specific site_id
  --start-ts <ts>                   Start timestamp (microseconds)
  --end-ts <ts>                     End timestamp (microseconds)
  --fields <list>                   Comma-separated fields to output
  --stats                           Print statistics instead of records
  --policy <json|file>              Policy JSON or path to policy file
  -m, --work-units-mode             Work units mode: infra, domain (default: infra)
  --default-price-usd-per-mwh <n>   Default electricity price (default: 50)
  -h, --help                        Show this help message

Policy Schema:
  {
    "energy_cap_w": 3000,        // Max power before throttling
    "thermal_cap_w": 10000,      // Min thermal headroom before throttling
    "grid_stress_cap": 0.8,      // Max grid stress before migration flag
    "throttle_factor": 0.5       // Work reduction factor when capped
  }

Outputs (with --policy):
  replay_summary.json includes:
    - baseline: { enst_units_per_j, energy_j, work_units, price_usd_per_mwh, cost_usd }
    - policy: { enst_units_per_j, energy_j, work_units, price_usd_per_mwh, cost_usd }
    - delta: { enst_units_per_j, energy_j, cost_usd }
    - delta_cost_usd_per_1e9_work_units

Examples:
  replay -i ./outputs/tsv.ndjson --stats
  replay --policy '{"energy_cap_w": 2500}' -o ./outputs/policy_impact.json
  replay --policy ./policy.json --default-price-usd-per-mwh 65
`);
}

async function* readTsvNdjson(filePath) {
  const stream = createReadStream(filePath, { encoding: 'utf8' });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

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

async function loadPolicy(policyArg) {
  if (!policyArg) return null;

  // Try to parse as JSON first
  try {
    return JSON.parse(policyArg);
  } catch (e) {
    // Try to read as file
    try {
      const content = await readFile(policyArg, 'utf8');
      return JSON.parse(content);
    } catch (e2) {
      throw new Error(`Failed to parse policy: ${e2.message}`);
    }
  }
}

function formatRecord(record, format, fields) {
  let data = record;

  if (fields && fields.length > 0) {
    data = {};
    for (const f of fields) {
      if (record[f] !== undefined) {
        data[f] = record[f];
      }
    }
  }

  switch (format) {
    case 'csv':
      return Object.values(data).map(v => v === null ? '' : v).join(',');

    case 'table':
      const parts = [];
      for (const [k, v] of Object.entries(data)) {
        if (v !== null && v !== undefined && !k.startsWith('_')) {
          const val = typeof v === 'number' ? v.toFixed(4) : v;
          parts.push(`${k}=${val}`);
        }
      }
      return parts.join(' | ');

    case 'json':
    default:
      // Filter internal fields
      const filtered = {};
      for (const [k, v] of Object.entries(data)) {
        if (!k.startsWith('_')) {
          filtered[k] = v;
        }
      }
      return JSON.stringify(filtered);
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  let args;
  try {
    args = parseArgs({ options, allowPositionals: true });
  } catch (e) {
    console.error(`Error: ${e.message}`);
    printUsage();
    process.exit(1);
  }

  if (args.values.help) {
    printUsage();
    process.exit(0);
  }

  const inputPath = args.values.input || 'outputs/tsv.ndjson';
  const outputPath = args.values.output || 'outputs/replay_summary.json';
  const speed = parseFloat(args.values.speed || '0');
  const format = args.values.format || 'json';
  const filterSite = args.values['filter-site'] || null;
  const startTs = args.values['start-ts'] ? parseInt(args.values['start-ts'], 10) : null;
  const endTs = args.values['end-ts'] ? parseInt(args.values['end-ts'], 10) : null;
  const fields = args.values.fields ? args.values.fields.split(',').map(f => f.trim()) : null;
  const showStats = args.values.stats;
  const workUnitsMode = args.values['work-units-mode'] || 'infra';
  const defaultPriceUsdPerMwh = parseFloat(
    args.values['default-price-usd-per-mwh'] || String(DEFAULT_PRICE_USD_PER_MWH)
  );

  // Load policy if specified
  const policy = await loadPolicy(args.values.policy);
  const hasPolicy = policy !== null;

  // Statistics tracking
  const stats = {
    record_count: 0,
    filtered_count: 0,
    sites: new Set(),
    min_ts: Infinity,
    max_ts: -Infinity,
    cpu_util_sum: 0,
    enst_sum: 0,
    enst_count: 0
  };

  const baselineRecords = [];
  const policyRecords = [];

  // Print CSV header if needed
  if (format === 'csv' && fields && !showStats && !hasPolicy) {
    console.log(fields.join(','));
  }

  let lastTs = null;

  // First pass: collect all records
  for await (const record of readTsvNdjson(inputPath)) {
    stats.record_count++;

    // Apply filters
    if (filterSite && record.site_id !== filterSite) {
      continue;
    }

    const recordTs = record.ts_start || record.ts;
    if (startTs !== null && recordTs < startTs) {
      continue;
    }
    if (endTs !== null && recordTs > endTs) {
      continue;
    }

    baselineRecords.push(record);
  }

  // Process with ENST computation
  const processedBaseline = [];
  for await (const record of computeEnstStream(baselineRecords, { workUnitsMode })) {
    processedBaseline.push(record);

    stats.filtered_count++;
    stats.sites.add(record.site_id);
    const recordTs = record.ts_start || record.ts;
    stats.min_ts = Math.min(stats.min_ts, recordTs);
    stats.max_ts = Math.max(stats.max_ts, recordTs);
    stats.cpu_util_sum += record.cpu_util || 0;

    if (record.enst !== null && record.enst !== undefined) {
      stats.enst_sum += record.enst;
      stats.enst_count++;
    }
  }

  // Apply policy if specified
  if (hasPolicy) {
    console.error(`Applying policy: ${JSON.stringify(policy)}`);
    console.error(`Default price: $${defaultPriceUsdPerMwh}/MWh`);

    for await (const record of applyPolicyStream(processedBaseline, policy)) {
      policyRecords.push(record);
    }

    // Compute and write impact summary
    const impact = computePolicyImpact(processedBaseline, policyRecords, {
      defaultPriceUsdPerMwh
    });
    impact.policy_config = policy;
    impact.work_units_mode = workUnitsMode;

    await mkdir(dirname(outputPath), { recursive: true });
    const ws = createWriteStream(outputPath);
    ws.write(JSON.stringify(impact, null, 2));
    ws.end();
    await new Promise((resolve, reject) => {
      ws.on('finish', resolve);
      ws.on('error', reject);
    });

    console.error(`Wrote policy impact summary: ${outputPath}`);

    // Print impact summary
    console.log('\n=== Policy Impact Summary ===');
    console.log(`Baseline ENST:     ${impact.baseline.enst_units_per_j.toFixed(6)} units/J`);
    console.log(`Policy ENST:       ${impact.policy.enst_units_per_j.toFixed(6)} units/J`);
    console.log(`Delta ENST:        ${impact.delta.enst_units_per_j.toFixed(6)} (${impact.delta.enst_pct.toFixed(2)}%)`);
    console.log(`\nEnergy Change:     ${impact.delta.energy_j_pct.toFixed(2)}%`);
    console.log(`Work Change:       ${impact.delta.work_units_pct.toFixed(2)}%`);
    console.log(`\nBaseline Cost:     $${impact.baseline.cost_usd.toFixed(2)} USD`);
    console.log(`Policy Cost:       $${impact.policy.cost_usd.toFixed(2)} USD`);
    console.log(`Delta Cost:        $${impact.delta.cost_usd.toFixed(2)} (${impact.delta.cost_usd_pct.toFixed(2)}%)`);

    if (impact.delta_cost_usd_per_1e9_work_units !== null) {
      const sign = impact.delta_cost_usd_per_1e9_work_units >= 0 ? '+' : '';
      console.log(`\nDelta Cost/1e9 Work Units: ${sign}$${impact.delta_cost_usd_per_1e9_work_units.toFixed(2)}`);
    }

    console.log(`\nThrottled Windows: ${impact.throttled_windows}`);
    console.log(`Migrate Flagged:   ${impact.migrate_flagged_windows}`);
    console.log('\nViolations:');
    console.log(`  Energy Cap:      ${impact.violations.energy_cap}`);
    console.log(`  Thermal Cap:     ${impact.violations.thermal_cap}`);
    console.log(`  Grid Stress:     ${impact.violations.grid_stress}`);

  } else if (!showStats) {
    // Output records without policy
    for (const record of processedBaseline) {
      const recordTs = record.ts_start || record.ts;

      if (speed > 0 && lastTs !== null) {
        const deltaUs = recordTs - lastTs;
        const deltaMs = deltaUs / 1000 / speed;
        if (deltaMs > 0 && deltaMs < 10000) {
          await sleep(deltaMs);
        }
      }
      lastTs = recordTs;

      console.log(formatRecord(record, format, fields));
    }
  }

  if (showStats && !hasPolicy) {
    const duration = stats.max_ts - stats.min_ts;
    const durationSec = duration / 1_000_000;

    console.log('\n=== Replay Statistics ===');
    console.log(`Total Records:    ${stats.record_count}`);
    console.log(`Filtered Records: ${stats.filtered_count}`);
    console.log(`Unique Sites:     ${stats.sites.size}`);
    console.log(`Time Range:       ${durationSec.toFixed(2)} seconds`);
    console.log(`Mean CPU Util:    ${(stats.cpu_util_sum / stats.filtered_count).toFixed(4)}`);

    if (stats.enst_count > 0) {
      console.log(`Mean ENST:        ${(stats.enst_sum / stats.enst_count).toFixed(6)}`);
    }

    console.log('\nSites:');
    for (const site of stats.sites) {
      console.log(`  - ${site}`);
    }
  }
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
