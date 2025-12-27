#!/usr/bin/env node

/**
 * Demo Public Script
 *
 * Runs the complete public-traces demo pipeline:
 * 1. Generate TSV from public traces
 * 2. Compute ENST
 * 3. Run replay with policy
 * 4. Rename outputs to _real.csv / _real.json
 */

import { spawn } from 'node:child_process';
import { copyFile, rename, mkdir, writeFile } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const POLICY_FILE = join(ROOT, 'outputs', 'demo_policy.json');

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    console.error(`> ${cmd} ${args.join(' ')}`);
    const proc = spawn(cmd, args, {
      cwd: ROOT,
      stdio: 'inherit',
      shell: true
    });
    proc.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Command failed with code ${code}`));
    });
    proc.on('error', reject);
  });
}

async function main() {
  console.error('=== DEMO: PUBLIC TRACES ===\n');

  // Ensure outputs directory exists
  await mkdir(join(ROOT, 'outputs'), { recursive: true });

  // Step 1: Export TSV from public traces
  console.error('Step 1: Exporting TSV from public traces...');
  await run('node', [
    'cli/tsv-export.js',
    '--public-traces',
    '-o', 'outputs/tsv_real.ndjson'
  ]);

  // Step 2: Compute ENST
  console.error('\nStep 2: Computing ENST...');
  await run('node', [
    'cli/enst.js',
    '-i', 'outputs/tsv_real.ndjson',
    '-o', 'outputs',
    '-m', 'infra',
    '--default-price-usd-per-mwh', '50'
  ]);

  // Step 3: Run replay with policy
  console.error('\nStep 3: Running replay with policy...');
  const policy = { energy_cap_w: 2500 };
  await writeFile(POLICY_FILE, JSON.stringify(policy, null, 2));
  await run('node', [
    'cli/replay.js',
    '-i', 'outputs/tsv_real.ndjson',
    '--policy', POLICY_FILE,
    '--default-price-usd-per-mwh', '50',
    '-o', 'outputs/replay_summary_real.json'
  ]);

  // Step 4: Copy/rename outputs
  console.error('\nStep 4: Finalizing outputs...');
  await copyFile(
    join(ROOT, 'outputs/leaderboard.csv'),
    join(ROOT, 'outputs/leaderboard_real.csv')
  );

  console.error('\n=== DEMO COMPLETE ===');
  console.error('Outputs:');
  console.error('  - outputs/tsv_real.ndjson');
  console.error('  - outputs/leaderboard_real.csv');
  console.error('  - outputs/replay_summary_real.json');
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
