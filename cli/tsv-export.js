#!/usr/bin/env node

/**
 * tsv-export CLI
 *
 * Exports normalized TSV records from cluster and power data sources.
 * Supports multi-site synthetic generation, public traces, and Prometheus metrics export.
 */

import { parseArgs } from 'node:util';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';

import { ingestGoogleClusterdata } from '../src/ingest_google_clusterdata/index.js';
import { ingestPowerdata } from '../src/ingest_powerdata/index.js';
import { TsvNormalizer } from '../src/tsv_normalize/index.js';
import { generateMultiSiteSynthetic } from '../src/synthetic/index.js';
import { loadPublicTraces, writePublicTraces, DATA_DIR } from '../src/public_traces/index.js';
import { PrometheusExporter } from '../src/prometheus/index.js';

const options = {
  'cluster-dir': { type: 'string', short: 'c' },
  'power-dir': { type: 'string', short: 'p' },
  'output': { type: 'string', short: 'o', default: 'outputs/tsv.ndjson' },
  'site-id': { type: 'string', short: 's' },
  'window-size': { type: 'string', short: 'w', default: '300' },
  'synthetic': { type: 'boolean', default: false },
  'synthetic-sites': { type: 'string', default: '3' },
  'synthetic-windows': { type: 'string', default: '100' },
  'public-traces': { type: 'boolean', default: false },
  'prometheus-port': { type: 'string' },
  'help': { type: 'boolean', short: 'h' }
};

function printUsage() {
  console.log(`
tsv-export - Export normalized TSV records

Usage:
  tsv-export [options]

Options:
  -c, --cluster-dir <path>     Directory containing Google clusterdata traces
  -p, --power-dir <path>       Directory containing power trace data
  -o, --output <path>          Output NDJSON file (default: outputs/tsv.ndjson)
  -s, --site-id <id>           Override site identifier
  -w, --window-size <sec>      Window size in seconds (default: 300)
  --synthetic                  Generate synthetic test data
  --synthetic-sites <n>        Number of synthetic sites (default: 3)
  --synthetic-windows <n>      Windows per site (default: 100)
  --public-traces              Use bundled public traces (real:public-traces)
  --prometheus-port <port>     Enable Prometheus metrics on port
  -h, --help                   Show this help message

Examples:
  tsv-export --synthetic --synthetic-sites 3 -o ./outputs/tsv.ndjson
  tsv-export --public-traces -o ./outputs/tsv_real.ndjson
  tsv-export -c ./data/cluster -p ./data/power --prometheus-port 9090
`);
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

  const windowSizeS = parseInt(args.values['window-size'] || '300', 10);
  const windowSizeUs = windowSizeS * 1_000_000;
  const siteId = args.values['site-id'] || null;
  const prometheusPort = args.values['prometheus-port']
    ? parseInt(args.values['prometheus-port'], 10)
    : null;

  // Determine output path based on mode
  let outputPath = args.values.output || 'outputs/tsv.ndjson';
  if (args.values['public-traces'] && outputPath === 'outputs/tsv.ndjson') {
    outputPath = 'outputs/tsv_real.ndjson';
  }

  // Start Prometheus exporter if requested
  let prometheus = null;
  if (prometheusPort) {
    prometheus = new PrometheusExporter(prometheusPort);
    await prometheus.start();
    console.error(`Prometheus metrics available at http://localhost:${prometheusPort}/metrics`);
  }

  const records = [];

  if (args.values['public-traces']) {
    // Load from bundled public traces
    console.error('Loading public traces from: data/public_traces/');

    // Ensure public traces exist
    try {
      await writePublicTraces();
    } catch (e) {
      // Files may already exist, continue
    }

    for await (const record of loadPublicTraces()) {
      records.push(record);
      if (prometheus) {
        prometheus.updateFromTsv(record);
      }
    }
    console.error(`Loaded ${records.length} records with data_source=real:public-traces`);

  } else if (args.values.synthetic) {
    // Generate multi-site synthetic data
    const siteCount = parseInt(args.values['synthetic-sites'] || '3', 10);
    const windowsPerSite = parseInt(args.values['synthetic-windows'] || '100', 10);

    const defaultSites = ['nrel-eagle', 'ornl-frontier', 'anl-polaris'];
    const sites = siteId
      ? [siteId]
      : defaultSites.slice(0, Math.min(siteCount, defaultSites.length));

    // Add generic sites if needed
    while (sites.length < siteCount) {
      sites.push(`site-${sites.length + 1}`);
    }

    console.error(`Generating ${windowsPerSite} windows for ${sites.length} sites...`);

    for (const record of generateMultiSiteSynthetic({
      sites,
      windowsPerSite,
      windowSizeS
    })) {
      records.push(record);
      if (prometheus) {
        prometheus.updateFromTsv(record);
      }
    }
  } else {
    // Ingest real data
    const clusterDir = args.values['cluster-dir'];
    const powerDir = args.values['power-dir'];
    const normalizer = new TsvNormalizer({ windowSizeUs });

    if (clusterDir) {
      console.error(`Ingesting cluster data from: ${clusterDir}`);
      for await (const record of ingestGoogleClusterdata(clusterDir, { windowSizeUs, siteId })) {
        normalizer.addUsageRecord(record);
      }
    }

    if (powerDir) {
      console.error(`Ingesting power data from: ${powerDir}`);
      for await (const record of ingestPowerdata(powerDir, { windowSizeUs, siteId })) {
        normalizer.addPowerRecord(record);
      }
    }

    if (!clusterDir && !powerDir) {
      console.error('Error: Specify --cluster-dir, --power-dir, --synthetic, or --public-traces');
      printUsage();
      process.exit(1);
    }

    for (const record of normalizer.emit()) {
      records.push(record);
      if (prometheus) {
        prometheus.updateFromTsv(record);
      }
    }
  }

  // Write output
  await mkdir(dirname(outputPath), { recursive: true });
  const ws = createWriteStream(outputPath);

  for (const record of records) {
    ws.write(JSON.stringify(record) + '\n');
  }

  ws.end();

  await new Promise((resolve, reject) => {
    ws.on('finish', resolve);
    ws.on('error', reject);
  });

  console.error(`Exported ${records.length} TSV records to: ${outputPath}`);

  // Keep process alive if Prometheus is running
  if (prometheus) {
    console.error('Prometheus server running. Press Ctrl+C to stop.');
    process.on('SIGINT', async () => {
      await prometheus.stop();
      process.exit(0);
    });
  }
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
