# Public Traces Pack

Public-like traces pack for reproducibility.

## Provenance

Deterministic traces generated from fixed seed (20241215).
No external downloads required.

## Files

- `usage.ndjson` - Usage/compute metrics (CPU, GPU, memory)
- `power.ndjson` - Power/energy metrics (watts, joules, PUE)

## Regenerate

```bash
npm run generate:public-traces
```

## Use

```bash
node cli/tsv-export.js --public-traces -o outputs/tsv_real.ndjson
```

## Schema

### usage.ndjson
```json
{"ts_start":..., "ts_end":..., "site_id":"hpc-site-alpha", "cpu_util":0.65, ...}
```

### power.ndjson
```json
{"ts_start":..., "ts_end":..., "site_id":"hpc-site-alpha", "power_w":2200, ...}
```
