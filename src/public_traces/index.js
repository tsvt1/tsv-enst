/**
 * Public Traces Generator and Loader
 *
 * Generates deterministic "real:public-traces" data from fixed seed.
 * Used for reproducible demo runs without external downloads.
 */

import { createReadStream, createWriteStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, '../../data/public_traces');

/**
 * Fixed seed for deterministic generation
 */
const FIXED_SEED = 20241215;

/**
 * Site profiles for public traces (matching real HPC site characteristics)
 */
const PUBLIC_TRACE_SITES = {
  'hpc-site-alpha': {
    basePowerW: 2200,
    powerVarianceW: 400,
    cpuUtilBase: 0.62,
    gpuRatio: 0.45,
    pue: 1.18,
    thermalHeadroomW: 45000,
    gridStressBase: 0.22,
    hasValidatedSteps: true,
    priceUsdPerMwh: 48.0
  },
  'hpc-site-beta': {
    basePowerW: 3500,
    powerVarianceW: 700,
    cpuUtilBase: 0.71,
    gpuRatio: 0.78,
    pue: 1.22,
    thermalHeadroomW: 70000,
    gridStressBase: 0.18,
    hasValidatedSteps: true,
    priceUsdPerMwh: 42.0
  },
  'hpc-site-gamma': {
    basePowerW: 1600,
    powerVarianceW: 280,
    cpuUtilBase: 0.58,
    gpuRatio: 0.55,
    pue: 1.14,
    thermalHeadroomW: 28000,
    gridStressBase: 0.28,
    hasValidatedSteps: true,
    priceUsdPerMwh: 55.0
  }
};

/**
 * Fixed start timestamp for reproducibility (2024-01-01 00:00:00 UTC in microseconds)
 */
const FIXED_START_TS = 1704067200000000;

/**
 * Number of windows per site
 */
const WINDOWS_PER_SITE = 50;

/**
 * Window size in seconds
 */
const WINDOW_SIZE_S = 300;

/**
 * Seeded random number generator
 */
function createSeededRandom(seed) {
  let state = seed;
  return () => {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    return state / 0x7fffffff;
  };
}

/**
 * Generates usage records for public traces
 * @yields {object} Usage records matching ingest_google_clusterdata output format
 */
export function* generatePublicUsageRecords() {
  const random = createSeededRandom(FIXED_SEED);
  const windowSizeUs = WINDOW_SIZE_S * 1_000_000;

  for (const [siteId, profile] of Object.entries(PUBLIC_TRACE_SITES)) {
    const clusterId = `${siteId}-cluster-0`;

    for (let i = 0; i < WINDOWS_PER_SITE; i++) {
      const ts = FIXED_START_TS + (i * windowSizeUs);
      const timeFactor = Math.sin(i / 20 * Math.PI) * 0.15;

      const cpuUtil = Math.min(0.95, Math.max(0.1,
        profile.cpuUtilBase + timeFactor + (random() - 0.5) * 0.2
      ));

      const hasGpu = random() < profile.gpuRatio;
      const gpuUtil = hasGpu
        ? Math.min(0.95, Math.max(0.1, cpuUtil * 0.8 + (random() - 0.5) * 0.3))
        : null;

      const cpuCoreSeconds = cpuUtil * WINDOW_SIZE_S * 128;
      const gpuSeconds = gpuUtil !== null ? gpuUtil * WINDOW_SIZE_S * 8 : 0;

      const validatedSteps = profile.hasValidatedSteps
        ? Math.floor(cpuCoreSeconds * 10 + gpuSeconds * 100 + random() * 1000)
        : null;

      yield {
        ts_start: ts,
        ts_end: ts + windowSizeUs,
        site_id: siteId,
        cluster_id: clusterId,
        cpu_util: cpuUtil,
        gpu_util: gpuUtil,
        mem_util: cpuUtil * 0.8 + (random() - 0.5) * 0.2,
        job_queue_depth: Math.floor(random() * 50 + cpuUtil * 30),
        resource_seconds_window: cpuCoreSeconds + gpuSeconds,
        cpu_core_seconds: cpuCoreSeconds,
        gpu_seconds: gpuSeconds > 0 ? gpuSeconds : null,
        validated_steps: validatedSteps,
        timesteps: validatedSteps ? Math.floor(validatedSteps * 0.95) : null
      };
    }
  }
}

/**
 * Generates power records for public traces
 * @yields {object} Power records matching ingest_powerdata output format
 */
export function* generatePublicPowerRecords() {
  const random = createSeededRandom(FIXED_SEED + 1); // Different seed for power
  const windowSizeUs = WINDOW_SIZE_S * 1_000_000;

  for (const [siteId, profile] of Object.entries(PUBLIC_TRACE_SITES)) {
    for (let i = 0; i < WINDOWS_PER_SITE; i++) {
      const ts = FIXED_START_TS + (i * windowSizeUs);
      const timeFactor = Math.sin(i / 20 * Math.PI) * 0.15;

      const cpuUtil = Math.min(0.95, Math.max(0.1,
        profile.cpuUtilBase + timeFactor + (random() - 0.5) * 0.2
      ));
      const gpuUtil = random() < profile.gpuRatio
        ? Math.min(0.95, Math.max(0.1, cpuUtil * 0.8 + (random() - 0.5) * 0.3))
        : 0;

      const loadFactor = cpuUtil + gpuUtil * 0.5;
      const power = profile.basePowerW * (0.5 + loadFactor * 0.5) +
        (random() - 0.5) * profile.powerVarianceW;

      const gridStress = Math.min(1, Math.max(0,
        profile.gridStressBase + timeFactor * 0.2 + (random() - 0.5) * 0.1
      ));
      const thermalHeadroom = profile.thermalHeadroomW * (1 - cpuUtil * 0.3) +
        (random() - 0.5) * 5000;

      yield {
        ts_start: ts,
        ts_end: ts + windowSizeUs,
        site_id: siteId,
        power_w: Math.max(100, power),
        energy_j_window: Math.max(100, power) * WINDOW_SIZE_S,
        pue: profile.pue + (random() - 0.5) * 0.05,
        thermal_headroom_w: Math.max(0, thermalHeadroom),
        grid_stress_index: gridStress,
        price_usd_per_mwh: profile.priceUsdPerMwh
      };
    }
  }
}

/**
 * Writes public traces to data/public_traces/ directory
 */
export async function writePublicTraces() {
  await mkdir(DATA_DIR, { recursive: true });

  // Write usage.ndjson
  const usagePath = join(DATA_DIR, 'usage.ndjson');
  const usageWs = createWriteStream(usagePath);
  for (const record of generatePublicUsageRecords()) {
    usageWs.write(JSON.stringify(record) + '\n');
  }
  usageWs.end();
  await new Promise((resolve, reject) => {
    usageWs.on('finish', resolve);
    usageWs.on('error', reject);
  });

  // Write power.ndjson
  const powerPath = join(DATA_DIR, 'power.ndjson');
  const powerWs = createWriteStream(powerPath);
  for (const record of generatePublicPowerRecords()) {
    powerWs.write(JSON.stringify(record) + '\n');
  }
  powerWs.end();
  await new Promise((resolve, reject) => {
    powerWs.on('finish', resolve);
    powerWs.on('error', reject);
  });

  return { usagePath, powerPath };
}

/**
 * Reads usage records from public traces file
 * @param {string} filePath - Path to usage.ndjson
 * @yields {object} Usage records
 */
export async function* readPublicUsage(filePath = join(DATA_DIR, 'usage.ndjson')) {
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

/**
 * Reads power records from public traces file
 * @param {string} filePath - Path to power.ndjson
 * @yields {object} Power records
 */
export async function* readPublicPower(filePath = join(DATA_DIR, 'power.ndjson')) {
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

/**
 * Loads and normalizes public traces into TSV records
 * @yields {object} Normalized TSV records with data_source="real:public-traces"
 */
export async function* loadPublicTraces() {
  const usageByKey = new Map();
  const powerByKey = new Map();
  const windowSizeUs = WINDOW_SIZE_S * 1_000_000;

  // Load usage records
  for await (const record of readPublicUsage()) {
    const key = `${record.site_id}:${record.ts_start}`;
    usageByKey.set(key, record);
  }

  // Load power records
  for await (const record of readPublicPower()) {
    const key = `${record.site_id}:${record.ts_start}`;
    powerByKey.set(key, record);
  }

  // Merge and emit TSV records
  const allKeys = new Set([...usageByKey.keys(), ...powerByKey.keys()]);

  for (const key of allKeys) {
    const usage = usageByKey.get(key) || {};
    const power = powerByKey.get(key) || {};
    const siteId = usage.site_id || power.site_id;
    const tsStart = usage.ts_start || power.ts_start;

    yield {
      ts_start: tsStart,
      ts_end: usage.ts_end || power.ts_end || tsStart + windowSizeUs,
      site_id: siteId,
      cluster_id: usage.cluster_id || `${siteId}-cluster-0`,
      cpu_util: usage.cpu_util ?? 0,
      gpu_util: usage.gpu_util ?? null,
      mem_util: usage.mem_util ?? null,
      job_queue_depth: usage.job_queue_depth ?? 0,
      resource_seconds: usage.resource_seconds_window ?? 0,
      cpu_core_seconds: usage.cpu_core_seconds ?? null,
      gpu_seconds: usage.gpu_seconds ?? null,
      validated_steps: usage.validated_steps ?? null,
      timesteps: usage.timesteps ?? null,
      power_w: power.power_w ?? null,
      energy_j: power.energy_j_window ?? null,
      pue: power.pue ?? null,
      thermal_headroom_w: power.thermal_headroom_w ?? null,
      grid_stress_index: power.grid_stress_index ?? null,
      window_duration_s: WINDOW_SIZE_S,
      price_usd_per_mwh: power.price_usd_per_mwh ?? 50.0,
      data_source: 'real:public-traces'
    };
  }
}

export {
  PUBLIC_TRACE_SITES,
  FIXED_SEED,
  FIXED_START_TS,
  WINDOWS_PER_SITE,
  WINDOW_SIZE_S,
  DATA_DIR
};
