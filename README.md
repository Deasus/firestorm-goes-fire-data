# firestorm-goes-fire-data

National, continuous wildfire detection for [FIRESTORM](https://github.com/Deasus/Firestorm) from the
**GOES-R ABI Level-2 Fire / Hot Spot Characterization (FDC)** product.

## Why

FIRESTORM already shows NASA FIRMS / VIIRS thermal hotspots — but those come from
**polar-orbiting** satellites with only ~2-4 overpasses/day, leaving multi-hour
blind windows where a fire can blow up unseen. GOES ABI is **geostationary**: it
stares at the hemisphere continuously and the FDC product refreshes every ~5 min
(CONUS sector). This pipeline turns that into a slim JSON the single-file
FIRESTORM frontend reads — giving operators **near-continuous Fire-Radiative-Power
(FRP) growth nationwide**, the biggest temporal-coverage gap in the detection
stack and a direct blow-up / spot-fire early-warning signal.

- **GOES-19 (East)** covers the eastern + central US.
- **GOES-18 (West)** covers the western US + Alaska + Hawaii.
- Together: **national** coverage, every few minutes.

## What it is / isn't

| | |
|---|---|
| Resolution | 2 km at nadir (coarser than VIIRS 375 m / Landsat 30 m) |
| Cadence | ~5 min (FDCC CONUS sector); our cron loop ~90 s |
| Latency | strike → JSON ≈ 5-12 min |
| Role | the **early/continuous** detector. VIIRS/Landsat stay the **precise locator**. Keep all layers. |
| Caveat | heavy cloud blocks 3.9/11.2 µm detection (flagged); disk-edge block-out zones simply absent |

FDC is **situational awareness**, not a replacement for aircraft-IR perimeter mapping. The
FIRESTORM frontend badges it `GOES ABI FDC · geostationary · 2km` and never presents it as ground truth.

## Output — `data/goes_fire.json`

```jsonc
{
  "generated_at": "2026-05-31T21:37:56Z",
  "newest_granule": "2026-05-31T21:31:18Z",
  "product": "GOES-R ABI L2 FDC ... CONUS sector, 2km",
  "counts": { "total": 15, "g19": 15, "g18": 0 },
  "detections": [
    { "lat": 28.79, "lng": -109.06, "tier": "high", "sat": "G19", "age_sec": 399, "frp": 181.9, "tempK": 570.1 }
    // tier ∈ {good, saturated, high, medium}  (low-prob + cloud-contaminated dropped)
    // frp = Fire Radiative Power (MW); tempK = fire brightness temp (K)
  ]
}
```

Frontend reads it from `raw.githubusercontent.com/Deasus/firestorm-goes-fire-data/main/data/goes_fire.json`.

## Source — public, anonymous, $0

```
s3://noaa-goes19/ABI-L2-FDCC/<YYYY>/<DDD>/<HH>/*.nc   (East/CONUS)
s3://noaa-goes18/ABI-L2-FDCC/<YYYY>/<DDD>/<HH>/*.nc   (West/CONUS)
```
NOAA Open Data Dissemination — same anonymous `boto3` UNSIGNED pattern as the GLM lightning pipeline,
same buckets, different prefix. No auth, no egress charge, public domain.

## Run locally

```bash
pip install boto3 botocore netCDF4 numpy
python fetch_fdc.py        # writes data/goes_fire.json
```

## Cadence

`.github/workflows/update-goes-fire.yml` runs an internal fetch loop (~90 s × 4 ≈ 5 min) and
re-dispatches itself, with a `*/5` schedule as a dead-man restart — the pattern proven on
`firestorm-lightning-data` to beat GitHub Actions' schedule throttling. Public repo = unlimited free minutes.

See `ARCHITECTURE.md` for the projection math, Mask-tier mapping, and dedup logic.
