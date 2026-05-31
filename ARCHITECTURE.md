# Architecture — firestorm-goes-fire-data

Companion to the GLM lightning pipeline; same bridge pattern (GHA cron → slim JSON
in repo → frontend `fetch()` from `raw.githubusercontent.com`). This doc records the
non-obvious bits so a future session doesn't have to re-derive them.

## Pipeline

```
NOAA S3 (noaa-goes19 + noaa-goes18, ABI-L2-FDCC)   [public, anonymous]
        │  newest FDCC granule per satellite
        ▼
fetch_fdc.py  (GHA runner, every ~90s in a 5-min loop)
        │  netCDF4 decode → Mask-tier filter → geostationary→lat/lon → FRP/Temp
        │  → merge G19+G18 → dedup overlap → slim JSON
        ▼
data/goes_fire.json   (committed to repo)
        ▼
FIRESTORM index.html  fetch() → new "GOES FIRE" GraphicsLayer (FRP color-ramp)
```

## The data product

`ABI-L2-FDCC` = ABI Level-2 **F**ire/Hot Spot **C**haracterization, **C**ONUS sector.
- Sectors: **FDCC** CONUS (~5 min, chosen) · FDCF full-disk (~10 min) · FDCM mesoscale.
- Grid: 1500×2500 (y,x) of 2 km pixels in the GOES fixed-grid (geostationary) projection.
- Operational production since 2025-04-07. NOAA data is public domain, no use restrictions.

Per-pixel arrays we use: `Mask` (fire category), `Power` (FRP, MW, fill `-9.0`,
valid 0–200000), `Temp` (fire brightness temp, K), `Area` (m², not currently emitted).

## Mask → tier (verified against the live granule's `flag_meanings`)

The `Mask` variable has 45 `flag_values`. We map the fire-bearing ones to a coarse
confidence `tier` and DROP the rest:

| flag_values | tier | kept? |
|---|---|---|
| 10, 30 | good | ✅ |
| 11, 31 | saturated | ✅ |
| 13, 33 | high (probability) | ✅ |
| 14, 34 | medium | ✅ |
| 15, 35 | low | ❌ dropped (too speculative for an ops COP) |
| 12, 32 | cloud-contaminated | ❌ dropped |
| everything else | (no-fire / block-out / QA) | ❌ ignored |

`30s` = temporally-filtered variants (GOES smooths across recent frames). `KEEP_TIERS`
in `fetch_fdc.py` is the single place to change this policy.

## Geostationary scan-angle → lat/lon

The `x`/`y` coords are **scan angles in radians**, not lat/lon. Conversion uses the
GOES-R PUG Vol.3 algorithm with params from the `goes_imager_projection` variable
(`perspective_point_height`, `semi_major/minor_axis`, `longitude_of_projection_origin`).
Implemented vectorized in `_geo_latlon()` — solve the satellite-to-ground intersection
quadratic, then geodetic lat/lon. Verified: G19 fire pixels decode to plausible North
American coords (lat 16–48°N, lng −114 to −82°W). `longitude_of_projection_origin` is
−75° for G19 (East) and −137° for G18 (West); the code reads it per-granule so the same
function works for both.

## Merge + dedup

G19 (East) and G18 (West) overlap over the central US, so a fire there can appear in
both granules. After merging, we grid-bucket dedup at ~0.03° (~3 km ≈ 1.5 FDC pixels),
keeping the higher-FRP hit. Cheap and order-independent (sort by FRP desc first).

## Cadence — beating GHA schedule throttling

GitHub Actions `schedule` fires ~hourly on free runners regardless of `*/5`. So the
workflow runs an internal loop (fetch → 90s → fetch, ×4 ≈ 5 min, committing each pass)
and re-dispatches itself via `gh workflow run` at the end, keeping `*/5` only as a
dead-man restart. `concurrency: cancel-in-progress:false` prevents pile-up if the
schedule and a self-dispatch overlap. Public repo → unlimited free minutes.

This is the same fix applied to `firestorm-lightning-data` on 2026-05-31.

## Frontend integration (FIRESTORM index.html)

- New `gfxLayers.goes_fire` GraphicsLayer + `activeLayers.goes_fire` (default OFF — it's
  a context/early-warning layer; FIRMS/VIIRS/incidents are the always-on detection set).
- `fetchGoesFire()` on the standard poller; markers color-ramped by FRP, sized by tier.
- Badge: `GOES ABI FDC · geostationary · 2km · <age>`; staleness gate on `newest_granule`.
- AI context: feed FRP + per-granule deltas to the briefing as an "intensifying detections"
  / blow-up signal. (Pairs with the future agentic "Sentinel" monitor — FRP growth is the
  ideal proactive trigger.)
- Honesty rule: 2 km coarse, geostationary — never imply it's a precise perimeter; it's the
  continuous early detector between VIIRS passes.

## Caveats / gotchas

- **Cloud blocks detection** (flagged `cloud_contaminated`, which we drop). Absence under
  cloud ≠ "no fire."
- **Disk-edge block-out zones** (high local-zenith / sun-glint) → pixels simply absent.
- **2 km resolution** means one "pixel" is ~4 km²; a hot detection is an area, not a point.
- **G18 may be sparse in US daytime-West / nighttime** depending on active fires in its
  sector — `counts.g18:0` is normal, not a wiring bug (cross-check against the lightning
  pipeline's same dual-satellite behavior).
- GOES bucket rotation gotcha (see global CLAUDE.md): G19=East, G18=West **today**. If NOAA
  rotates G19→G20 (early 2030s), re-probe the bucket before trusting the prefix.
