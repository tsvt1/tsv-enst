# TSV-ENST

Temporal State Vector with Energy-Normalized System Throughput computation.

## Install

```bash
npm install
```

## Generate Synthetic TSV (3 sites)

```bash
node cli/tsv-export.js --synthetic --synthetic-sites 3 --synthetic-windows 100 -o outputs/tsv.ndjson
```

## Compute ENST (infra mode)

```bash
node cli/enst.js -i outputs/tsv.ndjson -m infra -o outputs
```

## Compute ENST (domain mode)

```bash
node cli/enst.js -i outputs/tsv.ndjson -m domain -o outputs
```

## Start Prometheus Export

```bash
node cli/tsv-export.js --synthetic --prometheus-port 9090
# Metrics at http://localhost:9090/metrics
```

## Replay with Policy

```bash
node cli/replay.js -i outputs/tsv.ndjson --policy '{"energy_cap_w": 2500, "grid_stress_cap": 0.7}'
```

## Sample leaderboard.csv

```csv
window_start,window_end,site_id,cluster_id,energy_j,work_units,work_units_mode,enst_units_per_j,pue,thermal_headroom_w,grid_stress_index
1703980800000000,1704010800000000,ornl-frontier,ornl-frontier-cluster-0,120450000.00,48234567.00,infra,0.400454,1.198,78234.56,0.1523
1703980800000000,1704010800000000,anl-polaris,anl-polaris-cluster-0,54230000.00,19876543.00,infra,0.366512,1.118,28976.34,0.2467
1703980800000000,1704010800000000,nrel-eagle,nrel-eagle-cluster-0,75120000.00,24567890.00,infra,0.327037,1.152,48123.78,0.2034
```

## Sample TSV NDJSON

```json
{"ts_start":1703980800000000,"ts_end":1703981100000000,"site_id":"ornl-frontier","cluster_id":"ornl-frontier-cluster-0","cpu_util":0.78,"gpu_util":0.65,"cpu_core_seconds":24960,"gpu_seconds":1560,"power_w":3842,"energy_j":1152600,"pue":1.19,"thermal_headroom_w":76543,"grid_stress_index":0.15,"window_duration_s":300}
{"ts_start":1703981100000000,"ts_end":1703981400000000,"site_id":"anl-polaris","cluster_id":"anl-polaris-cluster-0","cpu_util":0.52,"gpu_util":0.48,"cpu_core_seconds":16640,"gpu_seconds":1152,"power_w":1654,"energy_j":496200,"pue":1.12,"thermal_headroom_w":29876,"grid_stress_index":0.24,"window_duration_s":300}
{"ts_start":1703981400000000,"ts_end":1703981700000000,"site_id":"nrel-eagle","cluster_id":"nrel-eagle-cluster-0","cpu_util":0.61,"gpu_util":0.35,"cpu_core_seconds":19520,"gpu_seconds":840,"power_w":2234,"energy_j":670200,"pue":1.15,"thermal_headroom_w":48765,"grid_stress_index":0.21,"window_duration_s":300}
```

## GENESIS Integration Packet (no PDF)

Generate reproducible outputs from bundled public traces:

```bash
npm run demo:public
```

Create GENESIS_PACKET.txt and HASHES.txt:

```bash
npm run pack:genesis
```

### Sample outputs/tsv_real.ndjson (first 8 lines)

```json
{"ts_start":1704067200000000,"ts_end":1704067500000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.72,"gpu_util":0.58,"memory_util":0.65,"power_w":2840,"energy_j":852000,"pue":1.15,"thermal_headroom_w":45000,"grid_stress_index":0.18,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704067500000000,"ts_end":1704067800000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.71,"gpu_util":0.56,"memory_util":0.64,"power_w":2820,"energy_j":846000,"pue":1.15,"thermal_headroom_w":44500,"grid_stress_index":0.19,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704067800000000,"ts_end":1704068100000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.74,"gpu_util":0.61,"memory_util":0.67,"power_w":2890,"energy_j":867000,"pue":1.15,"thermal_headroom_w":46000,"grid_stress_index":0.17,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704068100000000,"ts_end":1704068400000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.73,"gpu_util":0.59,"memory_util":0.66,"power_w":2860,"energy_j":858000,"pue":1.15,"thermal_headroom_w":45500,"grid_stress_index":0.18,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704068400000000,"ts_end":1704068700000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.70,"gpu_util":0.55,"memory_util":0.63,"power_w":2780,"energy_j":834000,"pue":1.15,"thermal_headroom_w":44000,"grid_stress_index":0.20,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704068700000000,"ts_end":1704069000000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.75,"gpu_util":0.62,"memory_util":0.68,"power_w":2920,"energy_j":876000,"pue":1.15,"thermal_headroom_w":46500,"grid_stress_index":0.16,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704069000000000,"ts_end":1704069300000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.72,"gpu_util":0.58,"memory_util":0.65,"power_w":2840,"energy_j":852000,"pue":1.15,"thermal_headroom_w":45000,"grid_stress_index":0.18,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
{"ts_start":1704069300000000,"ts_end":1704069600000000,"site_id":"hpc-site-alpha","cluster_id":"hpc-site-alpha-cluster-0","cpu_util":0.69,"gpu_util":0.54,"memory_util":0.62,"power_w":2750,"energy_j":825000,"pue":1.15,"thermal_headroom_w":43500,"grid_stress_index":0.21,"price_usd_per_mwh":45,"data_source":"real:public-traces","window_duration_s":300}
```

### Sample outputs/leaderboard_real.csv (first 4 lines)

```csv
window_start,window_end,site_id,cluster_id,energy_j,work_units,work_units_mode,enst_units_per_j,cost_usd,price_usd_per_mwh,pue,thermal_headroom_w,grid_stress_index,notes
1704067200000000,1704082200000000,hpc-site-beta,hpc-site-beta-cluster-0,32910000,6024321,infra,0.183063,0.82,90,1.08,32000,0.35,data_source=real:public-traces
1704067200000000,1704082200000000,hpc-site-gamma,hpc-site-gamma-cluster-0,25560000,4287456,infra,0.167773,0.43,60,1.22,52000,0.12,data_source=real:public-traces
1704067200000000,1704082200000000,hpc-site-alpha,hpc-site-alpha-cluster-0,42600000,6543210,infra,0.153597,0.53,45,1.15,45000,0.18,data_source=real:public-traces
```

## Run Tests

```bash
npm test
```
