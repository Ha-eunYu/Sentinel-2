from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timezone

import numpy as np
import rasterio
import matplotlib.pyplot as plt

import boto3
from botocore.config import Config
from dotenv import load_dotenv


EODATA_ENDPOINT = "https://eodata.dataspace.copernicus.eu"


def parse_s3_href(href: str) -> tuple[str, str]:
    u = urlparse(href)
    if u.scheme != "s3":
        raise ValueError(f"Not an s3:// href: {href}")
    return u.netloc, u.path.lstrip("/")


def dt_from_isoz(s: str) -> datetime:
    # "2024-08-19T02:15:39.024Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def normalize_rgb_uint8(r: np.ndarray, g: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Sentinel-2 L2A reflectance is typically scaled ~0..10000 (int16/uint16).
    We do a robust stretch using percentiles to get nice visualization.
    Output: HxWx3 uint8
    """
    rgb = np.stack([r, g, b], axis=-1).astype(np.float32)

    # Handle nodata/zeros robustly
    valid = np.isfinite(rgb) & (rgb > 0)
    if not np.any(valid):
        # fallback
        rgb = np.clip(rgb, 0, 10000)
        rgb = (rgb / 10000.0) * 255.0
        return np.clip(rgb, 0, 255).astype(np.uint8)

    # Percentile stretch per-channel
    out = np.zeros_like(rgb, dtype=np.float32)
    for c in range(3):
        chan = rgb[..., c]
        v = chan[np.isfinite(chan) & (chan > 0)]
        if v.size < 100:
            lo, hi = np.percentile(chan[np.isfinite(chan)], [2, 98])
        else:
            lo, hi = np.percentile(v, [2, 98])

        if hi <= lo:
            lo, hi = float(np.min(v)), float(np.max(v) if v.size else 1.0)
            if hi <= lo:
                hi = lo + 1.0

        chan = (chan - lo) / (hi - lo)
        out[..., c] = np.clip(chan, 0, 1)

    # mild gamma to improve contrast
    gamma = 1.15
    out = np.power(out, 1.0 / gamma)

    return (out * 255.0).astype(np.uint8)


def read_jp2_band(path: Path, out_shape: tuple[int, int] | None = None) -> np.ndarray:
    """
    Read single-band JP2. Optionally resample to out_shape (H, W) using rasterio.
    """
    with rasterio.open(path) as ds:
        if out_shape is None:
            arr = ds.read(1)
            return arr
        else:
            h, w = out_shape
            arr = ds.read(
                1,
                out_shape=(h, w),
                resampling=rasterio.enums.Resampling.bilinear,
            )
            return arr


def ensure_download(s3, href: str, out_path: Path) -> None:
    bucket, key = parse_s3_href(href)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return

    s3.download_file(bucket, key, str(out_path))


def make_triplet_compare_png(
    triplet: list[dict],
    out_png: Path,
    title: str,
    max_side: int = 1400,
) -> None:
    """
    triplet: list of 3 dicts, each includes:
      - id
      - datetime
      - eo:cloud_cover
      - paths: {"B02": Path, "B03": Path, "B04": Path}
    """
    panels = []
    subtitles = []

    for cand in triplet:
        p = cand["paths"]

        # Read bands; resample all to same shape (use B04 as reference)
        r_path = p["B04"]
        g_path = p["B03"]
        b_path = p["B02"]

        with rasterio.open(r_path) as rds:
            H, W = rds.height, rds.width

        # Downscale if too large (keep aspect)
        scale = 1.0
        if max(H, W) > max_side:
            scale = max_side / float(max(H, W))
        out_shape = (int(H * scale), int(W * scale))

        r = read_jp2_band(r_path, out_shape=out_shape)
        g = read_jp2_band(g_path, out_shape=out_shape)
        b = read_jp2_band(b_path, out_shape=out_shape)

        rgb8 = normalize_rgb_uint8(r, g, b)
        panels.append(rgb8)

        subtitles.append(
            f"{cand['id']}\n{cand['datetime']} | cloud={cand['eo:cloud_cover']}"
        )

    # Plot 1x3
    plt.figure(figsize=(18, 6))
    for i in range(3):
        ax = plt.subplot(1, 3, i + 1)
        ax.imshow(panels[i])
        ax.set_title(subtitles[i], fontsize=9)
        ax.axis("off")

    plt.suptitle(title, fontsize=14)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(out_png, dpi=200)
    plt.close()


def main():
    load_dotenv()

    # S3 credentials (CDSE S3)
    access_key = os.environ["CDSE_S3_ACCESS_KEY"]
    secret_key = os.environ["CDSE_S3_SECRET_KEY"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=EODATA_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )

    # Manifest must include top-3 assets per target
    manifest_path = Path("downloads") / "s2_stac_picks_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path.resolve()}")

    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    out_root = Path(r".\downloads")
    dl_root = out_root / "S2_TOP3_JP2"
    fig_root = out_root / "S2_TOP3_COMPARE"
    dl_root.mkdir(parents=True, exist_ok=True)
    fig_root.mkdir(parents=True, exist_ok=True)

    for t in data["targets"]:
        sensor = t.get("sensor", "UNKNOWN")
        if t.get("status") != "ok":
            print(f"[{sensor}] skip: {t.get('status')} ({t.get('reason')})")
            continue

        target_date = t.get("target_date", "unknown")
        search_used = t.get("search_used", {})
        candidates = t.get("candidates_topk", [])
        candidates_assets = t.get("candidates_topk_rgb_assets", [])

        if len(candidates) < 3 or len(candidates_assets) < 3:
            raise RuntimeError(
                f"[{sensor}] Manifest does not contain top-3 candidates. "
                f"Need candidates_topk and candidates_topk_rgb_assets with 3 items."
            )

        # Build triplet
        triplet = []
        for i in range(3):
            cand = candidates[i]
            assets = candidates_assets[i]["rgb_assets"]

            item_id = cand["id"]
            out_dir = dl_root / sensor / item_id
            paths = {}

            for band in ["B02", "B03", "B04"]:
                a = assets.get(band)
                if not a or not a.get("href"):
                    raise RuntimeError(f"[{sensor}] Missing href for {item_id} {band} in manifest.")
                href = a["href"]

                out_path = out_dir / f"{item_id}_{band}.jp2"
                ensure_download(s3, href, out_path)
                paths[band] = out_path

            triplet.append({
                "id": item_id,
                "datetime": cand.get("datetime"),
                "eo:cloud_cover": cand.get("eo:cloud_cover"),
                "paths": paths
            })

        # Create compare figure
        title = f"{sensor} | target={target_date} | STAC window=±{search_used.get('window_days')}d | cloud<{search_used.get('cloud_lt')}"
        out_png = fig_root / f"{sensor}_{target_date}_top3_compare.png"
        make_triplet_compare_png(triplet, out_png, title=title)

        print(f"✅ [{sensor}] saved compare PNG -> {out_png}")

    print("\n✅ All done.")


if __name__ == "__main__":
    main()