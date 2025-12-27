#!/usr/bin/env node

/**
 * Generate Public Traces Script
 *
 * Generates the bundled public traces files in data/public_traces/
 * Run once to create the deterministic dataset.
 */

import { writePublicTraces } from '../src/public_traces/index.js';

async function main() {
  console.error('Generating public traces...');
  const { usagePath, powerPath } = await writePublicTraces();
  console.error(`Wrote: ${usagePath}`);
  console.error(`Wrote: ${powerPath}`);
  console.error('Done.');
}

main().catch(e => {
  console.error(`Fatal: ${e.message}`);
  process.exit(1);
});
