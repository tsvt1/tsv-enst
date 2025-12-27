/**
 * Prometheus Metrics Export Module
 *
 * Exposes TSV/ENST metrics via HTTP endpoint.
 * Minimal implementation with no external dependencies.
 */

import { createServer } from 'node:http';

/**
 * Metrics registry for Prometheus format
 */
export class MetricsRegistry {
  constructor() {
    this.gauges = new Map();
  }

  setGauge(name, value, labels = {}) {
    const labelStr = Object.entries(labels)
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');
    const key = labelStr ? `${name}{${labelStr}}` : name;
    this.gauges.set(key, { name, value, labels, timestamp: Date.now() });
  }

  toPrometheusFormat() {
    const lines = [];
    const seenNames = new Set();

    for (const [key, metric] of this.gauges) {
      if (!seenNames.has(metric.name)) {
        lines.push(`# TYPE ${metric.name} gauge`);
        seenNames.add(metric.name);
      }
      lines.push(`${key} ${metric.value}`);
    }

    return lines.join('\n') + '\n';
  }

  clear() {
    this.gauges.clear();
  }
}

/**
 * Updates metrics from a TSV record
 * @param {MetricsRegistry} registry - Metrics registry
 * @param {object} record - TSV record
 */
export function updateMetricsFromTsv(registry, record) {
  const labels = {
    site_id: record.site_id || 'unknown',
    cluster_id: record.cluster_id || 'default'
  };

  if (record.power_w !== null && record.power_w !== undefined) {
    registry.setGauge('tsv_power_w', record.power_w, labels);
  }

  if (record.energy_j !== null && record.energy_j !== undefined) {
    registry.setGauge('tsv_energy_j_window', record.energy_j, labels);
  }

  if (record.job_queue_depth !== null && record.job_queue_depth !== undefined) {
    registry.setGauge('tsv_job_queue_depth', record.job_queue_depth, labels);
  }

  if (record.gpu_util !== null && record.gpu_util !== undefined) {
    registry.setGauge('tsv_gpu_util', record.gpu_util, labels);
  }

  if (record.cpu_util !== null && record.cpu_util !== undefined) {
    registry.setGauge('tsv_cpu_util', record.cpu_util, labels);
  }

  if (record.enst !== null && record.enst !== undefined) {
    registry.setGauge('enst_units_per_j', record.enst, labels);
  }

  if (record.work_units !== null && record.work_units !== undefined) {
    registry.setGauge('tsv_work_units', record.work_units, labels);
  }

  if (record.pue !== null && record.pue !== undefined) {
    registry.setGauge('tsv_pue', record.pue, labels);
  }

  if (record.thermal_headroom_w !== null && record.thermal_headroom_w !== undefined) {
    registry.setGauge('tsv_thermal_headroom_w', record.thermal_headroom_w, labels);
  }

  if (record.grid_stress_index !== null && record.grid_stress_index !== undefined) {
    registry.setGauge('tsv_grid_stress_index', record.grid_stress_index, labels);
  }
}

/**
 * Creates a Prometheus metrics HTTP server
 * @param {MetricsRegistry} registry - Metrics registry
 * @param {number} port - Port to listen on
 * @returns {Promise<http.Server>} HTTP server instance
 */
export function createMetricsServer(registry, port) {
  return new Promise((resolve, reject) => {
    const server = createServer((req, res) => {
      if (req.url === '/metrics' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(registry.toPrometheusFormat());
      } else if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok' }));
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });

    server.on('error', reject);
    server.listen(port, () => {
      resolve(server);
    });
  });
}

/**
 * Prometheus metrics exporter
 */
export class PrometheusExporter {
  constructor(port) {
    this.port = port;
    this.registry = new MetricsRegistry();
    this.server = null;
  }

  async start() {
    this.server = await createMetricsServer(this.registry, this.port);
    return this;
  }

  updateFromTsv(record) {
    updateMetricsFromTsv(this.registry, record);
  }

  async stop() {
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(resolve);
      });
    }
  }

  getMetrics() {
    return this.registry.toPrometheusFormat();
  }
}
