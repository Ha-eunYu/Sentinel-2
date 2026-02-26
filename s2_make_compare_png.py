from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import rasterio
import matplotlib.pyplot as plt


def normalize_rgb_uint8(r: np.ndarray, g: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Robust percentile stretch + mild gamma for visualization.
    Input: arrays ~0..10000 (S2 L2A reflectance scaled)
    Output: HxWx3 uint8
    """
    rgb = np.stack([r, g, b], axis=-1).astype(np.float32)

    valid = np.isfinite(rgb) & (rgb > 0)
    if not np.any(valid):
        rgb = np.clip(rgb, 0, 10000)
        rgb = (rgb / 10000.0) * 255.0
        return np.clip(rgb, 0, 255).astype(np.uint8)

    out = np.zeros_like(rgb, dtype=np.float32)
    for c in range(3):
        chan = rgb[..., c]
        v = chan[np.isfinite(chan) & (chan > 0)]
        if v.size < 100:
            vv = chan[np.isfinite(chan)]
            lo, hi = np.percentile(vv, [2, 98]) if vv.size else (0.0, 1.0)
        else:
            lo, hi = np.percentile(v, [2, 98])

        if hi <= lo:
            lo = float(np.min(v)) if v.size else 0.0
            hi = float(np.max(v)) if v.size else 1.0
            if hi <= lo:
                hi = lo + 1.0

        out[..., c] = np.clip((chan - lo) / (hi - lo), 0, 1)

    gamma = 1.15
    out = np.power(out, 1.0 / gamma)
    return (out * 255.0).astype(np.uint8)


def read_band_resampled(path: Path, out_shape: tuple[int, int] | None = None) -> np.ndarray:
    with rasterio.open(path) as ds:
        if out_shape is None:
            return ds.read(1)
        h, w = out_shape
        return ds.read(
            1,
            out_shape=(h, w),
            resampling=rasterio.enums.Resampling.bilinear,
        )


def make_triplet_compare_png(
    triplet: list[dict],
    out_png: Path,
    title: str,
    max_side: int = 1400,
) -> None:
    panels = []
    subtitles = []

    for cand in triplet:
        item_id = cand["id"]
        dt = cand.get("datetime")
        cloud = cand.get("eo:cloud_cover")
        paths = cand["paths"]

        r_path = paths["B04"]
        g_path = paths["B03"]
        b_path = paths["B02"]

        with rasterio.open(r_path) as rds:
            H, W = rds.height, rds.width

        scale = 1.0
        if max(H, W) > max_side:
            scale = max_side / float(max(H, W))
        out_shape = (max(1, int(H * scale)), max(1, int(W * scale)))

        r = read_band_resampled(r_path, out_shape=out_shape)
        g = read_band_resampled(g_path, out_shape=out_shape)
        b = read_band_resampled(b_path, out_shape=out_shape)

        rgb8 = normalize_rgb_uint8(r, g, b)
        panels.append(rgb8)
        subtitles.append(f"{item_id}\n{dt} | cloud={cloud}")

    n = len(panels)
    plt.figure(figsize=(6 * n, 6))
    for i in range(n):
        ax = plt.subplot(1, n, i + 1)
        ax.imshow(panels[i])
        ax.set_title(subtitles[i], fontsize=9)
        ax.axis("off")

    plt.suptitle(title, fontsize=14)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(out_png, dpi=200)
    plt.close()

def main() -> None:
    manifest_path = Path("downloads") / "s2_stac_picks_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path.resolve()}")

    out_root = Path(r".\downloads")
    dl_root = out_root / "S2_TOPK_JP2"        # ✅ 다운로드 루트와 맞추기
    fig_root = out_root / "S2_TOPK_COMPARE"
    fig_root.mkdir(parents=True, exist_ok=True)

    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    for t in data.get("targets", []):
        sensor = t.get("sensor", "UNKNOWN")
        status = t.get("status")
        if status != "ok":
            print(f"[{sensor}] skip: {status} ({t.get('reason')})")
            continue

        target_date = t.get("target_date", "unknown")
        search_used = t.get("search_used", {})
        candidates = t.get("candidates_topk", [])

        n = min(3, len(candidates))
        if n == 0:
            print(f"[{sensor}] skip: no candidates")
            continue

        triplet = []
        for i in range(n):
            cand = candidates[i]
            item_id = cand["id"]

            base = dl_root / sensor / item_id
            paths = {
                "B02": base / f"{item_id}_B02_10m.jp2",   # ✅ 파일명 일치
                "B03": base / f"{item_id}_B03_10m.jp2",
                "B04": base / f"{item_id}_B04_10m.jp2",
            }
            for band, p in paths.items():
                if not p.exists():
                    raise FileNotFoundError(
                        f"[{sensor}] Missing file for {item_id} {band}: {p}\n"
                        f"Run the download script first."
                    )

            triplet.append({
                "id": item_id,
                "datetime": cand.get("datetime"),
                "eo:cloud_cover": cand.get("eo:cloud_cover"),
                "paths": paths,
            })

        title = (
            f"{sensor} | target={target_date} | "
            f"window=±{search_used.get('window_days')}d | cloud<{search_used.get('cloud_lt')}"
        )
        out_png = fig_root / f"{sensor}_{target_date}_top{n}_compare.png"

        make_triplet_compare_png(triplet, out_png, title=title)
        print(f"✅ [{sensor}] saved -> {out_png}")

    print("\n✅ All compare images created.")
    print(f"📁 PNG root: {fig_root}")


if __name__ == "__main__":
    main()