#!/usr/bin/env python3
"""
FIRESTORM GOES fire pipeline — pulls the GOES-R ABI Level-2 Fire / Hot Spot
Characterization (FDC) product from NOAA's Open Data S3 buckets, decodes the
netCDF, merges GOES-East (G19) + GOES-West (G18) CONUS sectors, and writes a
slim JSON the frontend reads via raw.githubusercontent.com.

WHY THIS EXISTS — national, continuous fire detection between VIIRS passes.
FIRESTORM already shows NASA FIRMS/VIIRS thermal hotspots, but those come from
POLAR-orbiting satellites: only ~2-4 overpasses/day, leaving multi-hour blind
windows during which a fire can blow up unseen. GOES ABI is GEOSTATIONARY — it
stares at the same hemisphere continuously and the FDC product refreshes every
~5 minutes (CONUS sector). Adding it gives operators near-continuous
Fire-Radiative-Power (FRP) growth curves nationwide — the single biggest
temporal-coverage gap in the current detection stack, and a direct blow-up /
spot-fire early-warning signal. This is a NATIONAL capability: G19 covers the
eastern + central US, G18 covers the western US + Alaska + Hawaii.

WHAT FDC IS (and isn't):
  • 2 km spatial resolution at nadir (coarser than VIIRS 375 m / Landsat 30 m).
    So FDC is the EARLY/CONTINUOUS detector; VIIRS/Landsat remain the precise
    locator. They complement — keep both layers.
  • Each granule carries per-pixel Mask (fire category), Power (FRP in MW),
    Temp (K), Area (m^2). We emit fire pixels + FRP/Temp.
  • Detection is by 3.9 µm / 11.2 µm brightness-temperature; heavy cloud blocks
    it (flagged cloud_contaminated). Sun-glint / high-zenith block-out zones
    exist near the disk edge — those pixels are simply absent, not wrong.
  • End-to-end latency strike→JSON ≈ 5-12 min (ABI scan + ground processing +
    S3 publish + our cron). Situational awareness, not a replacement for
    aircraft IR perimeter mapping.

OUTPUT: data/goes_fire.json
Shape: { "generated_at": ISO8601,
         "window_minutes": N,           # how far back we scanned
         "counts": {"total": N, "g19": N19, "g18": N18},
         "detections": [ {lat,lng,frp,tempK,tier,sat,age_sec}, ... ] }

SOURCE (public, anonymous, no auth, no egress charge — same NODD program as GLM):
  s3://noaa-goes19/ABI-L2-FDCC/<YYYY>/<DDD>/<HH>/*.nc   (East/CONUS)
  s3://noaa-goes18/ABI-L2-FDCC/<YYYY>/<DDD>/<HH>/*.nc   (West/CONUS)
  FDCC = CONUS sector (~5 min). (FDCF = full disk ~10 min; FDCM = mesoscale.)

Requires: boto3, botocore, netCDF4, numpy. No API key.
"""
from __future__ import annotations
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import netCDF4
import numpy as np

# ── Config ───────────────────────────────────────────────────────────
# Scan window: how many recent granules to consider. FDCC publishes ~1 granule
# every 5 min. We take the SINGLE most-recent granule per satellite (a full
# snapshot of all current fire pixels) rather than accumulating — FDC is a
# current-state product, not an event stream like GLM. The window is only used
# to find the newest file across a possible hour boundary.
LOOKBACK_HOURS = 2          # search this many hours back to find the newest granule
SATS = [("noaa-goes19", "G19", "g19"), ("noaa-goes18", "G18", "g18")]
PRODUCT_PREFIX = "ABI-L2-FDCC"     # CONUS sector
OUT_PATH = os.path.join(os.path.dirname(__file__), "data", "goes_fire.json")

# GOES FDC Mask flag_values → a coarse confidence tier we surface to the
# frontend. (Full meanings verified from the live granule's flag_meanings attr.)
#   10/30 good · 11/31 saturated · 13/33 high-prob · 14/34 med-prob ·
#   15/35 low-prob · 12/32 cloud-contaminated.  (30s = temporally-filtered.)
FIRE_TIERS = {
    10: "good", 30: "good",
    11: "saturated", 31: "saturated",
    13: "high", 33: "high",
    14: "medium", 34: "medium",
    15: "low", 35: "low",
    12: "cloud", 32: "cloud",
}
# Tiers we DROP from the published layer (too speculative for an ops COP). Low-
# probability + cloud-contaminated stay OUT of the headline detections to avoid
# false positives; good/saturated/high/medium are kept. The frontend can still
# style by tier. Adjust here, not in the frontend.
KEEP_TIERS = {"good", "saturated", "high", "medium"}

S3 = boto3.client(
    "s3",
    config=Config(signature_version=UNSIGNED, read_timeout=30, retries={"max_attempts": 3}),
)


def _newest_granule_key(bucket: str) -> str | None:
    """Find the most recent FDCC .nc key across the lookback window."""
    now = datetime.now(timezone.utc)
    best = None
    for h in range(LOOKBACK_HOURS + 1):
        t = now - timedelta(hours=h)
        prefix = f"{PRODUCT_PREFIX}/{t.year}/{t.timetuple().tm_yday:03d}/{t.hour:02d}/"
        token = None
        keys = []
        while True:
            kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            r = S3.list_objects_v2(**kw)
            keys.extend(o["Key"] for o in r.get("Contents", []))
            if r.get("IsTruncated"):
                token = r.get("NextContinuationToken")
            else:
                break
        if keys:
            # filenames sort lexically == chronologically (start-time encoded)
            best = max(keys)
            break  # newest hour with data wins
    return best


def _geo_latlon(proj, x_rad: np.ndarray, y_rad: np.ndarray):
    """GOES-R fixed-grid scan angles → geodetic lat/lon (PUG vol3 algorithm)."""
    H = proj.perspective_point_height + proj.semi_major_axis
    req = proj.semi_major_axis
    rpol = proj.semi_minor_axis
    lon0 = np.radians(proj.longitude_of_projection_origin)
    sinx, cosx = np.sin(x_rad), np.cos(x_rad)
    siny, cosy = np.sin(y_rad), np.cos(y_rad)
    a = sinx ** 2 + (cosx ** 2) * (cosy ** 2 + (req ** 2 / rpol ** 2) * siny ** 2)
    b = -2 * H * cosx * cosy
    c = H ** 2 - req ** 2
    disc = b * b - 4 * a * c
    good = disc >= 0
    rs = np.full_like(a, np.nan)
    rs[good] = (-b[good] - np.sqrt(disc[good])) / (2 * a[good])
    sx = rs * cosx * cosy
    sy = -rs * sinx
    sz = rs * cosx * siny
    lat = np.degrees(np.arctan((req ** 2 / rpol ** 2) * (sz / np.sqrt((H - sx) ** 2 + sy ** 2))))
    lon = np.degrees(lon0 - np.arctan(sy / (H - sx)))
    return lat, lon, good


def _decode(raw: bytes, sat_label: str, now: datetime):
    ds = netCDF4.Dataset("inmem", memory=raw)
    try:
        proj = ds.variables["goes_imager_projection"]
        x = ds.variables["x"][:]
        y = ds.variables["y"][:]
        mask = ds.variables["Mask"][:]
        power = ds.variables["Power"][:]
        temp = ds.variables["Temp"][:]
        tstart = ds.time_coverage_start
        gen = datetime.strptime(tstart[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        age = int((now - gen).total_seconds())

        fire_codes = np.fromiter(FIRE_TIERS.keys(), dtype=int)
        fy, fx = np.where(np.isin(np.asarray(mask), fire_codes))
        if fy.size == 0:
            return [], gen
        # vectorized lat/lon for just the fire pixels
        xr = np.asarray(x)[fx]
        yr = np.asarray(y)[fy]
        lat, lon, good = _geo_latlon(proj, xr, yr)
        out = []
        for n in range(fy.size):
            if not good[n]:
                continue
            j, i = int(fy[n]), int(fx[n])
            tier = FIRE_TIERS[int(mask[j, i])]
            if tier not in KEEP_TIERS:
                continue
            rec = {
                "lat": round(float(lat[n]), 4),
                "lng": round(float(lon[n]), 4),
                "tier": tier,
                "sat": sat_label,
                "age_sec": age,
            }
            pv = power[j, i]
            if pv is not np.ma.masked and float(pv) >= 0:
                rec["frp"] = round(float(pv), 1)
            tv = temp[j, i]
            if tv is not np.ma.masked:
                rec["tempK"] = round(float(tv), 1)
            out.append(rec)
        return out, gen
    finally:
        ds.close()


def main() -> int:
    now = datetime.now(timezone.utc)
    detections = []
    counts = {}
    newest_gen = None
    for bucket, sat_label, count_key in SATS:
        try:
            key = _newest_granule_key(bucket)
            if not key:
                print(f"[{sat_label}] no recent FDCC granule found", file=sys.stderr)
                counts[count_key] = 0
                continue
            obj = S3.get_object(Bucket=bucket, Key=key)
            raw = obj["Body"].read()
            recs, gen = _decode(raw, sat_label, now)
            detections.extend(recs)
            counts[count_key] = len(recs)
            if newest_gen is None or gen > newest_gen:
                newest_gen = gen
            print(f"[{sat_label}] {key.split('/')[-1]} -> {len(recs)} kept detections")
        except Exception as e:  # one satellite failing must not kill the other
            print(f"[{sat_label}] ERROR: {e}", file=sys.stderr)
            counts[count_key] = 0

    # De-dup G19/G18 overlap (central US sees both): keep the higher-FRP hit
    # within ~0.03 deg (~3 km, ~1.5 FDC pixels). Cheap grid-bucket dedup.
    seen = {}
    for d in sorted(detections, key=lambda r: -(r.get("frp", 0))):
        k = (round(d["lat"] / 0.03), round(d["lng"] / 0.03))
        if k not in seen:
            seen[k] = d
    deduped = list(seen.values())

    payload = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "newest_granule": (newest_gen.strftime("%Y-%m-%dT%H:%M:%SZ") if newest_gen else None),
        "product": "GOES-R ABI L2 FDC (Fire/Hot Spot Characterization), CONUS sector, 2km",
        "counts": {"total": len(deduped), **counts},
        "detections": deduped,
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"wrote {OUT_PATH}: {len(deduped)} detections "
          f"(G19={counts.get('g19',0)}, G18={counts.get('g18',0)}), "
          f"newest granule {payload['newest_granule']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
