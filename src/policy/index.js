/**
 * Policy Evaluation Module
 *
 * Evaluates power/thermal/grid policies against TSV records.
 * Computes throttling factors, migration flags, and cost impacts.
 */

import {
  computeCostUsd,
  computeCostForWork,
  computeWeightedAveragePrice,
  resolvePrice,
  DEFAULT_PRICE_USD_PER_MWH,
  REFERENCE_WORK_UNITS
} from '../cost/index.js';

/**
 * Default policy configuration
 */
export const DEFAULT_POLICY = {
  energy_cap_w: null,      // Max power before throttling
  thermal_cap_w: null,     // Min thermal headroom before throttling
  grid_stress_cap: null,   // Max grid stress before migration flag
  throttle_factor: 0.5     // Work reduction when capped
};

/**
 * Evaluates a single TSV record against a policy
 * @param {object} record - TSV record
 * @param {object} policy - Policy configuration
 * @returns {object} Evaluation result with throttle and flags
 */
export function evaluatePolicy(record, policy) {
  const result = {
    throttle_applied: false,
    throttle_factor: 1.0,
    migrate_flag: false,
    violations: []
  };

  // Energy/power cap check
  if (policy.energy_cap_w !== null && record.power_w !== null) {
    if (record.power_w > policy.energy_cap_w) {
      result.throttle_applied = true;
      result.throttle_factor = Math.min(result.throttle_factor, policy.throttle_factor);
      result.violations.push({
        type: 'energy_cap',
        actual: record.power_w,
        cap: policy.energy_cap_w
      });
    }
  }

  // Thermal headroom check
  if (policy.thermal_cap_w !== null && record.thermal_headroom_w !== null) {
    if (record.thermal_headroom_w < policy.thermal_cap_w) {
      result.throttle_applied = true;
      result.throttle_factor = Math.min(result.throttle_factor, policy.throttle_factor);
      result.violations.push({
        type: 'thermal_cap',
        actual: record.thermal_headroom_w,
        cap: policy.thermal_cap_w
      });
    }
  }

  // Grid stress check
  if (policy.grid_stress_cap !== null && record.grid_stress_index !== null) {
    if (record.grid_stress_index > policy.grid_stress_cap) {
      result.migrate_flag = true;
      result.violations.push({
        type: 'grid_stress',
        actual: record.grid_stress_index,
        cap: policy.grid_stress_cap
      });
    }
  }

  return result;
}

/**
 * Applies policy throttling to a TSV record
 * @param {object} record - TSV record
 * @param {object} evalResult - Policy evaluation result
 * @returns {object} Modified record with throttled values
 */
export function applyThrottle(record, evalResult) {
  if (!evalResult.throttle_applied) {
    return { ...record };
  }

  const throttled = { ...record };
  const factor = evalResult.throttle_factor;

  // Reduce work units by throttle factor
  if (throttled.work_units !== null && throttled.work_units !== undefined) {
    throttled.work_units = throttled.work_units * factor;
  }

  if (throttled.validated_work_units !== null && throttled.validated_work_units !== undefined) {
    throttled.validated_work_units = throttled.validated_work_units * factor;
  }

  // Reduce resource consumption proportionally
  if (throttled.cpu_core_seconds !== null && throttled.cpu_core_seconds !== undefined) {
    throttled.cpu_core_seconds = throttled.cpu_core_seconds * factor;
  }

  if (throttled.gpu_seconds !== null && throttled.gpu_seconds !== undefined) {
    throttled.gpu_seconds = throttled.gpu_seconds * factor;
  }

  // Energy reduced proportionally (simplified model)
  if (throttled.energy_j !== null && throttled.energy_j !== undefined) {
    throttled.energy_j = throttled.energy_j * (0.5 + factor * 0.5); // Partial reduction
  }

  // Recompute ENST
  if (throttled.energy_j > 0 && throttled.work_units !== null) {
    throttled.enst = throttled.work_units / throttled.energy_j;
  }

  // Add policy metadata
  throttled._policy_throttled = true;
  throttled._throttle_factor = factor;
  throttled._migrate_flag = evalResult.migrate_flag;

  return throttled;
}

/**
 * Processes TSV stream with policy evaluation
 * @param {AsyncIterable|Iterable} records - TSV records
 * @param {object} policy - Policy configuration
 * @yields {object} Records with policy applied
 */
export async function* applyPolicyStream(records, policy) {
  const mergedPolicy = { ...DEFAULT_POLICY, ...policy };

  for await (const record of records) {
    const evalResult = evaluatePolicy(record, mergedPolicy);
    const processed = applyThrottle(record, evalResult);
    processed._policy_evaluation = evalResult;
    yield processed;
  }
}

/**
 * Computes policy impact summary with cost calculations
 * @param {object[]} baselineRecords - Original records
 * @param {object[]} policyRecords - Records after policy applied
 * @param {object} options - Options including defaultPriceUsdPerMwh
 * @returns {object} Impact summary with cost data
 */
export function computePolicyImpact(baselineRecords, policyRecords, options = {}) {
  const { defaultPriceUsdPerMwh = DEFAULT_PRICE_USD_PER_MWH } = options;

  // Baseline metrics
  const baselineEnergy = baselineRecords.reduce((sum, r) => sum + (r.energy_j || 0), 0);
  const baselineWork = baselineRecords.reduce((sum, r) => sum + (r.work_units || 0), 0);
  const baselineEnst = baselineEnergy > 0 ? baselineWork / baselineEnergy : 0;

  // Policy metrics
  const policyEnergy = policyRecords.reduce((sum, r) => sum + (r.energy_j || 0), 0);
  const policyWork = policyRecords.reduce((sum, r) => sum + (r.work_units || 0), 0);
  const policyEnst = policyEnergy > 0 ? policyWork / policyEnergy : 0;

  // Compute weighted average prices
  const { avgPrice: baselinePrice } = computeWeightedAveragePrice(baselineRecords, defaultPriceUsdPerMwh);
  const { avgPrice: policyPrice } = computeWeightedAveragePrice(policyRecords, defaultPriceUsdPerMwh);

  // Compute costs
  const baselineCostUsd = computeCostUsd(baselineEnergy, baselinePrice);
  const policyCostUsd = computeCostUsd(policyEnergy, policyPrice);

  // Compute delta cost per 1e9 work units
  const baselineCostPer1e9 = computeCostForWork(REFERENCE_WORK_UNITS, baselineEnst, baselinePrice);
  const policyCostPer1e9 = computeCostForWork(REFERENCE_WORK_UNITS, policyEnst, policyPrice);
  const deltaCostPer1e9 = isFinite(baselineCostPer1e9) && isFinite(policyCostPer1e9)
    ? policyCostPer1e9 - baselineCostPer1e9
    : null;

  const throttledCount = policyRecords.filter(r => r._policy_throttled).length;
  const migrateCount = policyRecords.filter(r => r._migrate_flag).length;

  const violations = {
    energy_cap: 0,
    thermal_cap: 0,
    grid_stress: 0
  };

  for (const record of policyRecords) {
    if (record._policy_evaluation) {
      for (const v of record._policy_evaluation.violations) {
        violations[v.type] = (violations[v.type] || 0) + 1;
      }
    }
  }

  return {
    baseline: {
      enst_units_per_j: baselineEnst,
      energy_j: baselineEnergy,
      work_units: baselineWork,
      price_usd_per_mwh: baselinePrice,
      cost_usd: baselineCostUsd,
      record_count: baselineRecords.length
    },
    policy: {
      enst_units_per_j: policyEnst,
      energy_j: policyEnergy,
      work_units: policyWork,
      price_usd_per_mwh: policyPrice,
      cost_usd: policyCostUsd,
      record_count: policyRecords.length
    },
    delta: {
      enst_units_per_j: policyEnst - baselineEnst,
      enst_pct: baselineEnst > 0 ? ((policyEnst - baselineEnst) / baselineEnst) * 100 : 0,
      energy_j: policyEnergy - baselineEnergy,
      energy_j_pct: baselineEnergy > 0 ? ((policyEnergy - baselineEnergy) / baselineEnergy) * 100 : 0,
      work_units: policyWork - baselineWork,
      work_units_pct: baselineWork > 0 ? ((policyWork - baselineWork) / baselineWork) * 100 : 0,
      cost_usd: policyCostUsd - baselineCostUsd,
      cost_usd_pct: baselineCostUsd > 0 ? ((policyCostUsd - baselineCostUsd) / baselineCostUsd) * 100 : 0
    },
    delta_cost_usd_per_1e9_work_units: deltaCostPer1e9,
    violations,
    throttled_windows: throttledCount,
    migrate_flagged_windows: migrateCount
  };
}
