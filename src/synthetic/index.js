/**
 * Synthetic Data Generator
 *
 * Generates multi-site synthetic TSV data for testing and validation.
 */

/**
 * Site configuration profiles with deterministic price_usd_per_mwh
 */
const SITE_PROFILES = {
  'nrel-eagle': {
    basePowerW: 2500,
    powerVarianceW: 500,
    cpuUtilBase: 0.65,
    gpuRatio: 0.4,
    pue: 1.15,
    thermalHeadroomW: 50000,
    gridStressBase: 0.2,
    hasValidatedSteps: false,
    priceUsdPerMwh: 45.0  // Colorado wholesale avg
  },
  'ornl-frontier': {
    basePowerW: 4000,
    powerVarianceW: 800,
    cpuUtilBase: 0.75,
    gpuRatio: 0.85,
    pue: 1.20,
    thermalHeadroomW: 80000,
    gridStressBase: 0.15,
    hasValidatedSteps: true,
    priceUsdPerMwh: 38.0  // Tennessee Valley Authority
  },
  'anl-polaris': {
    basePowerW: 1800,
    powerVarianceW: 300,
    cpuUtilBase: 0.55,
    gpuRatio: 0.6,
    pue: 1.12,
    thermalHeadroomW: 30000,
    gridStressBase: 0.25,
    hasValidatedSteps: true,
    priceUsdPerMwh: 52.0  // Illinois ComEd
  }
};

/**
 * Default price when site profile not found
 */
const DEFAULT_PRICE_USD_PER_MWH = 50.0;

/**
 * Generates synthetic TSV records for multiple sites
 * @param {object} options - Generation options
 * @yields {object} TSV records
 */
export function* generateMultiSiteSynthetic(options = {}) {
  const {
    sites = ['nrel-eagle', 'ornl-frontier', 'anl-polaris'],
    windowsPerSite = 100,
    windowSizeS = 300,
    startTs = Date.now() * 1000,
    seed = null
  } = options;

  // Simple seeded random for reproducibility
  let rngState = seed !== null ? seed : Math.floor(Math.random() * 1000000);
  const random = () => {
    rngState = (rngState * 1103515245 + 12345) & 0x7fffffff;
    return rngState / 0x7fffffff;
  };

  const windowSizeUs = windowSizeS * 1_000_000;

  for (const siteId of sites) {
    const profile = SITE_PROFILES[siteId] || SITE_PROFILES['nrel-eagle'];
    const clusterId = `${siteId}-cluster-0`;
    const priceUsdPerMwh = profile.priceUsdPerMwh ?? DEFAULT_PRICE_USD_PER_MWH;

    for (let i = 0; i < windowsPerSite; i++) {
      const ts = startTs + (i * windowSizeUs);

      // Generate utilization with temporal correlation
      const timeFactor = Math.sin(i / 20 * Math.PI) * 0.15;
      const cpuUtil = Math.min(0.95, Math.max(0.1,
        profile.cpuUtilBase + timeFactor + (random() - 0.5) * 0.2
      ));

      const hasGpu = random() < profile.gpuRatio;
      const gpuUtil = hasGpu
        ? Math.min(0.95, Math.max(0.1, cpuUtil * 0.8 + (random() - 0.5) * 0.3))
        : null;

      // Power with load correlation
      const loadFactor = cpuUtil + (gpuUtil || 0) * 0.5;
      const power = profile.basePowerW * (0.5 + loadFactor * 0.5) +
        (random() - 0.5) * profile.powerVarianceW;

      // Compute resource metrics
      const cpuCoreSeconds = cpuUtil * windowSizeS * 128; // Assume 128 cores
      const gpuSeconds = gpuUtil !== null ? gpuUtil * windowSizeS * 8 : 0; // Assume 8 GPUs

      // Domain metrics (only for sites with validated steps)
      const validatedSteps = profile.hasValidatedSteps
        ? Math.floor(cpuCoreSeconds * 10 + gpuSeconds * 100 + random() * 1000)
        : null;
      const timesteps = profile.hasValidatedSteps
        ? Math.floor(validatedSteps * 0.95)
        : null;

      // Grid and thermal metrics
      const gridStress = Math.min(1, Math.max(0,
        profile.gridStressBase + timeFactor * 0.2 + (random() - 0.5) * 0.1
      ));
      const thermalHeadroom = profile.thermalHeadroomW * (1 - cpuUtil * 0.3) +
        (random() - 0.5) * 5000;

      yield {
        ts_start: ts,
        ts_end: ts + windowSizeUs,
        site_id: siteId,
        cluster_id: clusterId,
        cpu_util: cpuUtil,
        gpu_util: gpuUtil,
        mem_util: cpuUtil * 0.8 + (random() - 0.5) * 0.2,
        job_queue_depth: Math.floor(random() * 50 + cpuUtil * 30),
        resource_seconds: cpuCoreSeconds + gpuSeconds,
        cpu_core_seconds: cpuCoreSeconds,
        gpu_seconds: gpuSeconds > 0 ? gpuSeconds : null,
        validated_steps: validatedSteps,
        timesteps: timesteps,
        power_w: Math.max(100, power),
        energy_j: Math.max(100, power) * windowSizeS,
        pue: profile.pue + (random() - 0.5) * 0.05,
        thermal_headroom_w: Math.max(0, thermalHeadroom),
        grid_stress_index: gridStress,
        window_duration_s: windowSizeS,
        price_usd_per_mwh: priceUsdPerMwh,
        data_source: 'synthetic'
      };
    }
  }
}

/**
 * Generates synthetic data for a single site
 * @param {object} options - Generation options
 * @yields {object} TSV records
 */
export function* generateSingleSiteSynthetic(options = {}) {
  const {
    siteId = 'synthetic-site',
    windowCount = 100,
    windowSizeS = 300,
    startTs = Date.now() * 1000
  } = options;

  yield* generateMultiSiteSynthetic({
    sites: [siteId],
    windowsPerSite: windowCount,
    windowSizeS,
    startTs
  });
}

export { SITE_PROFILES, DEFAULT_PRICE_USD_PER_MWH };
