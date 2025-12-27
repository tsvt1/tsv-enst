#!/usr/bin/env node

/**
 * Pack GENESIS Script
 *
 * Creates GENESIS_PACKET.txt and HASHES.txt with:
 * - Repo tag placeholder
 * - Exact repro commands
 * - Key metric delta_cost_usd_per_1e9_work_units
 * - SHA256 hashes for schema + real outputs
 */

import { readFile, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sha256File, writeHashesFile, computeGenesisHashes } from '../src/hash/index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');

async function main() {
  console.error('=== PACK: GENESIS INTEGRATION ===\n');

  const paths = {
    schema: join(ROOT, 'spec/tsv.schema.json'),
    leaderboard: join(ROOT, 'outputs/leaderboard_real.csv'),
    replaySummary: join(ROOT, 'outputs/replay_summary_real.json')
  };

  // Compute hashes
  console.error('Computing SHA256 hashes...');
  const hashes = await computeGenesisHashes(paths);

  // Write HASHES.txt
  const hashesPath = join(ROOT, 'outputs/HASHES.txt');
  await writeHashesFile(hashesPath, hashes);
  console.error(`Wrote: ${hashesPath}`);

  // Read replay summary for key metric
  let deltaMetric = 'N/A';
  try {
    const summaryContent = await readFile(paths.replaySummary, 'utf8');
    const summary = JSON.parse(summaryContent);
    if (summary.delta_cost_usd_per_1e9_work_units !== null) {
      deltaMetric = summary.delta_cost_usd_per_1e9_work_units.toFixed(2);
    }
  } catch (e) {
    console.error(`Warning: Could not read replay summary: ${e.message}`);
  }

  // Generate GENESIS_PACKET.txt
  const packetContent = `# GENESIS Integration Packet
# TSV-ENST v0.2 (tag placeholder)
# Generated: ${new Date().toISOString()}

## Reproduction Commands

\`\`\`bash
# Clone and install
git clone <repo-url>
cd tsv-enst
npm install

# Run public traces demo
npm run demo:public

# Verify outputs
cat outputs/leaderboard_real.csv | head -5
cat outputs/replay_summary_real.json | head -20
cat outputs/HASHES.txt
\`\`\`

## Key Metrics

delta_cost_usd_per_1e9_work_units: $${deltaMetric}

This metric represents the cost change per 1 billion work units
when applying an energy_cap_w=2500 policy vs baseline.
Negative values indicate savings from improved ENST.

## SHA256 Hashes

schema_sha256=${hashes.schema_sha256}
leaderboard_real_sha256=${hashes.leaderboard_real_sha256}
replay_summary_real_sha256=${hashes.replay_summary_real_sha256}

## Files

- spec/tsv.schema.json: TSV schema definition
- outputs/tsv_real.ndjson: Raw TSV records (real:public-traces)
- outputs/leaderboard_real.csv: Site rankings with cost
- outputs/replay_summary_real.json: Policy impact analysis
- outputs/HASHES.txt: Verification hashes

## Data Source

data_source=real:public-traces
Deterministic traces from fixed seed (20241215) for reproducibility.
No external downloads required.
`;

  const packetPath = join(ROOT, 'outputs/GENESIS_PACKET.txt');
  await writeFile(packetPath, packetContent);
  console.error(`Wrote: ${packetPath}`);

  console.error('\n=== PACK COMPLETE ===');
  console.error('Files:');
  console.error('  - outputs/GENESIS_PACKET.txt');
  console.error('  - outputs/HASHES.txt');
  console.error(`\nKey metric: delta_cost_usd_per_1e9_work_units = $${deltaMetric}`);
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
