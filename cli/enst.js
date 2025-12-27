#!/usr/bin/env node

/**
 * enst CLI
 *
 * Computes ENST (Energy-Normalized System Throughput) from TSV records.
 * Supports dual-mode work units (infra/domain) and cost calculation.
 */

import { parseArgs } from 'node:util';
import { createReadStream, createWriteStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';

import {
  computeEnstStream,
  EnstLeaderboard,
  writeLeaderboardCsv,
  computeSummaryStats
} from '../src/enst_compute/index.js';
import { DEFAULT_PRICE_USD_PER_MWH } from '../src/cost/index.js';

const options = {
  'input': { type: 'string', short: 'i', default: 'outputs/tsv.ndjson' },
  'output-dir': { type: 'string', short: 'o', default: 'outputs' },
  'format': { type: 'string', short: 'f', default: 'all' },
  'work-units-mode': { type: 'string', short: 'm', default: 'infra' },
  'gpu-weight': { type: 'string', default: '1' },
  'default-price-usd-per-mwh': { type: 'string', default: '50' },
  'help': { type: 'boolean', short: 'h' }
};

function printUsage() {
  console.log(`
enst - Compute Energy-Normalized System Throughput

Usage:
  enst [options]

Options:
  -i, --input <path>                Input TSV NDJSON file (default: outputs/tsv.ndjson)
  -o, --output-dir <path>           Output directory (default: outputs)
  -f, --format <type>               Output format: csv, json, all (default: all)
  -m, --work-units-mode             Work units mode: infra, domain (default: infra)
  --gpu-weight <n>                  GPU weight for infra mode (default: 1)
  --default-price-usd-per-mwh <n>   Default electricity price (default: 50)
  -h, --help                        Show this help message

Work Units Modes:
  infra:   work_units = cpu_core_seconds + gpu_seconds * gpu_weight
  domain:  work_units = validated_steps or timesteps (fallback to infra)

Outputs:
  leaderboard.csv    Site rankings with cost columns
  summary.json       Aggregate statistics with total_cost_usd
  enst.ndjson        TSV records with ENST values

Examples:
  enst -i ./data/tsv.ndjson --work-units-mode infra
  enst -m domain --default-price-usd-per-mwh 65
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
  const outputDir = args.values['output-dir'] || 'outputs';
  const format = args.values.format || 'all';
  const workUnitsMode = args.values['work-units-mode'] || 'infra';
  const gpuWeight = parseFloat(args.values['gpu-weight'] || '1');
  const defaultPriceUsdPerMwh = parseFloat(
    args.values['default-price-usd-per-mwh'] || String(DEFAULT_PRICE_USD_PER_MWH)
  );

  if (!['infra', 'domain'].includes(workUnitsMode)) {
    console.error(`Error: Invalid work-units-mode. Must be 'infra' or 'domain'.`);
    process.exit(1);
  }

  await mkdir(outputDir, { recursive: true });

  const leaderboard = new EnstLeaderboard({
    workUnitsMode,
    defaultPriceUsdPerMwh
  });
  const outputRecords = [];

  console.error(`Computing ENST from: ${inputPath}`);
  console.error(`Work units mode: ${workUnitsMode}, GPU weight: ${gpuWeight}`);
  console.error(`Default price: $${defaultPriceUsdPerMwh}/MWh`);

  const tsvStream = readTsvNdjson(inputPath);
  const enstStream = computeEnstStream(tsvStream, { workUnitsMode, gpuWeight });

  for await (const record of enstStream) {
    leaderboard.addRecord(record);
    outputRecords.push(record);
  }

  // Write outputs based on format
  if (format === 'all' || format === 'csv') {
    const csvPath = join(outputDir, 'leaderboard.csv');
    await writeLeaderboardCsv(leaderboard, csvPath);
    console.error(`Wrote leaderboard: ${csvPath}`);
  }

  if (format === 'all' || format === 'json') {
    const summaryPath = join(outputDir, 'summary.json');
    const summary = computeSummaryStats(leaderboard);
    summary.work_units_mode = workUnitsMode;
    summary.gpu_weight = gpuWeight;
    summary.default_price_usd_per_mwh = defaultPriceUsdPerMwh;

    const ws = createWriteStream(summaryPath);
    ws.write(JSON.stringify(summary, null, 2));
    ws.end();
    await new Promise((resolve, reject) => {
      ws.on('finish', resolve);
      ws.on('error', reject);
    });
    console.error(`Wrote summary: ${summaryPath}`);

    const enstPath = join(outputDir, 'enst.ndjson');
    const enstWs = createWriteStream(enstPath);
    for (const record of outputRecords) {
      enstWs.write(JSON.stringify(record) + '\n');
    }
    enstWs.end();
    await new Promise((resolve, reject) => {
      enstWs.on('finish', resolve);
      enstWs.on('error', reject);
    });
    console.error(`Wrote ENST records: ${enstPath}`);
  }

  // Print summary to stdout
  const summary = computeSummaryStats(leaderboard);
  console.log('\n=== ENST Summary ===');
  console.log(`Mode:            ${workUnitsMode}`);
  console.log(`Sites:           ${summary.site_count}`);
  console.log(`Total Energy:    ${summary.total_energy_j.toFixed(2)} J`);
  console.log(`Total Work:      ${summary.total_work_units.toFixed(2)} units`);
  console.log(`Global ENST:     ${summary.global_enst.toFixed(6)} units/J`);
  console.log(`Total Cost:      $${summary.total_cost_usd.toFixed(2)} USD`);
  console.log(`Min ENST:        ${summary.min_enst.toFixed(6)}`);
  console.log(`Median ENST:     ${summary.median_enst.toFixed(6)}`);
  console.log(`Max ENST:        ${summary.max_enst.toFixed(6)}`);

  // Print leaderboard
  const entries = leaderboard.getLeaderboard().slice(0, 10);
  if (entries.length > 0) {
    console.log('\n=== Top Sites by ENST ===');
    entries.forEach((e, i) => {
      console.log(`${i + 1}. ${e.site_id}/${e.cluster_id}: ${e.enst_units_per_j.toFixed(6)} units/J | $${e.cost_usd.toFixed(2)} (${e.work_units_mode})`);
    });
  }
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
