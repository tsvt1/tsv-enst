/**
 * Cost Computation Module
 *
 * Computes USD costs from energy consumption and electricity prices.
 * Provides delta calculations for ENST improvements.
 */

/**
 * Joules per MWh conversion factor
 * 1 MWh = 3.6e9 J
 */
export const JOULES_PER_MWH = 3.6e9;

/**
 * Default electricity price in USD per MWh
 */
export const DEFAULT_PRICE_USD_PER_MWH = 50.0;

/**
 * Reference work units for delta cost calculation
 */
export const REFERENCE_WORK_UNITS = 1e9;

/**
 * Computes cost in USD from energy in joules
 * @param {number} energyJ - Energy in joules
 * @param {number} priceUsdPerMwh - Price in USD per MWh
 * @returns {number} Cost in USD
 */
export function computeCostUsd(energyJ, priceUsdPerMwh) {
  if (energyJ === null || energyJ === undefined || energyJ <= 0) {
    return 0;
  }
  const mwh = energyJ / JOULES_PER_MWH;
  return mwh * priceUsdPerMwh;
}

/**
 * Computes energy needed to produce a fixed amount of work
 * @param {number} workUnits - Target work units
 * @param {number} enst - ENST (work_units / energy_j)
 * @returns {number} Energy in joules needed
 */
export function computeEnergyForWork(workUnits, enst) {
  if (enst === null || enst === undefined || enst <= 0) {
    return Infinity;
  }
  return workUnits / enst;
}

/**
 * Computes cost to produce a fixed amount of work
 * @param {number} workUnits - Target work units
 * @param {number} enst - ENST (work_units / energy_j)
 * @param {number} priceUsdPerMwh - Price in USD per MWh
 * @returns {number} Cost in USD
 */
export function computeCostForWork(workUnits, enst, priceUsdPerMwh) {
  const energyJ = computeEnergyForWork(workUnits, enst);
  if (!isFinite(energyJ)) {
    return Infinity;
  }
  return computeCostUsd(energyJ, priceUsdPerMwh);
}

/**
 * Computes delta cost per reference work units between two ENST values
 * @param {number} enstBaseline - Baseline ENST
 * @param {number} enstPolicy - Policy ENST
 * @param {number} priceUsdPerMwh - Price in USD per MWh
 * @param {number} referenceWorkUnits - Reference work units (default 1e9)
 * @returns {number} Delta cost in USD (negative = savings)
 */
export function computeDeltaCostPerWork(
  enstBaseline,
  enstPolicy,
  priceUsdPerMwh,
  referenceWorkUnits = REFERENCE_WORK_UNITS
) {
  const costBaseline = computeCostForWork(referenceWorkUnits, enstBaseline, priceUsdPerMwh);
  const costPolicy = computeCostForWork(referenceWorkUnits, enstPolicy, priceUsdPerMwh);

  if (!isFinite(costBaseline) || !isFinite(costPolicy)) {
    return null;
  }

  return costPolicy - costBaseline;
}

/**
 * Resolves price from record or default
 * @param {object} record - TSV record
 * @param {number} defaultPrice - Default price if missing
 * @returns {{price: number, defaulted: boolean}} Price and whether default was used
 */
export function resolvePrice(record, defaultPrice = DEFAULT_PRICE_USD_PER_MWH) {
  if (record.price_usd_per_mwh !== null && record.price_usd_per_mwh !== undefined) {
    return { price: record.price_usd_per_mwh, defaulted: false };
  }
  return { price: defaultPrice, defaulted: true };
}

/**
 * Computes weighted average price from records
 * @param {object[]} records - Array of TSV records
 * @param {number} defaultPrice - Default price if missing
 * @returns {{avgPrice: number, defaultedCount: number}} Average price and count of defaulted
 */
export function computeWeightedAveragePrice(records, defaultPrice = DEFAULT_PRICE_USD_PER_MWH) {
  let totalEnergy = 0;
  let weightedPriceSum = 0;
  let defaultedCount = 0;

  for (const record of records) {
    const energyJ = record.energy_j || 0;
    const { price, defaulted } = resolvePrice(record, defaultPrice);

    if (defaulted) {
      defaultedCount++;
    }

    totalEnergy += energyJ;
    weightedPriceSum += energyJ * price;
  }

  const avgPrice = totalEnergy > 0 ? weightedPriceSum / totalEnergy : defaultPrice;

  return { avgPrice, defaultedCount };
}

/**
 * Builds notes string for leaderboard
 * @param {object} options - Note options
 * @returns {string} Notes string
 */
export function buildNotes(options) {
  const parts = [];

  if (options.priceDefaulted) {
    parts.push('missing_price_defaulted');
  }

  if (options.dataSource) {
    parts.push(`data_source=${options.dataSource}`);
  }

  return parts.join(';') || '';
}
