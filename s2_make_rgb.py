from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.windows import from_bounds
from rasterio.transform import Affine


def percentile_stretch(x: np.ndarray, p_low=2.0, p_high=98.0) -> np.ndarray:
    """Scale to [0,1] using percentile stretch (robust to outliers)."""
    x = x.astype(np.float32)
    lo = np.nanpercentile(x, p_low)
    hi = np.nanpercentile(x, p_high)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        # fallback
        lo = float(np.nanmin(x))
        hi = float(np.nanmax(x))
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
            return np.zeros_like(x, dtype=np.float32)
    y = (x - lo) / (hi - lo)
    return np.clip(y, 0.0, 1.0)


def apply_gamma(x01: np.ndarray, gamma: float = 1.0) -> np.ndarray:
    """Gamma correction on [0,1]. gamma<1 brighter, gamma>1 darker."""
    if gamma is None or gamma == 1.0:
        return x01
    x01 = np.clip(x01, 0.0, 1.0)
    return np.power(x01, 1.0 / float(gamma))


def read_one_band(
    path: Path,
    aoi_bounds: Optional[Tuple[float, float, float, float]] = None,
    aoi_bounds_crs: str = "EPSG:4326",
) -> tuple[np.ndarray, Affine, rasterio.crs.CRS]:
    """
    Read single-band JP2 as array.
    Optionally crop by aoi_bounds (minx, miny, maxx, maxy) in aoi_bounds_crs.
    """
    with rasterio.open(path) as src:
        if aoi_bounds is None:
            arr = src.read(1)
            return arr, src.transform, src.crs

        # transform AOI bounds CRS -> src CRS
        from rasterio.warp import transform_bounds

        b = transform_bounds(aoi_bounds_crs, src.crs, *aoi_bounds, densify_pts=21)
        win = from_bounds(*b, transform=src.transform)
        win = win.round_offsets().round_lengths()

        # clamp window to dataset
        win = win.intersection(rasterio.windows.Window(0, 0, src.width, src.height))

        arr = src.read(1, window=win)
        transform = rasterio.windows.transform(win, src.transform)
        return arr, transform, src.crs


def save_rgb_geotiff(
    out_tif: Path,
    rgb01: np.ndarray,
    transform: Affine,
    crs,
) -> None:
    """
    rgb01: float32 [3,H,W] in [0,1]
    Save as uint8 GeoTIFF (3 bands).
    """
    out_tif.parent.mkdir(parents=True, exist_ok=True)
    rgb8 = (np.clip(rgb01, 0, 1) * 255.0 + 0.5).astype(np.uint8)

    profile = {
        "driver": "GTiff",
        "height": rgb8.shape[1],
        "width": rgb8.shape[2],
        "count": 3,
        "dtype": "uint8",
        "crs": crs,
        "transform": transform,
        "compress": "deflate",
        "predictor": 2,
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
    }

    with rasterio.open(out_tif, "w", **profile) as dst:
        dst.write(rgb8[0], 1)  # R
        dst.write(rgb8[1], 2)  # G
        dst.write(rgb8[2], 3)  # B


def save_rgb_png(out_png: Path, rgb01: np.ndarray) -> None:
    """Save RGB PNG using rasterio (no PIL dependency)."""
    out_png.parent.mkdir(parents=True, exist_ok=True)
    rgb8 = (np.clip(rgb01, 0, 1) * 255.0 + 0.5).astype(np.uint8)
    profile = {
        "driver": "PNG",
        "height": rgb8.shape[1],
        "width": rgb8.shape[2],
        "count": 3,
        "dtype": "uint8",
    }
    with rasterio.open(out_png, "w", **profile) as dst:
        dst.write(rgb8[0], 1)
        dst.write(rgb8[1], 2)
        dst.write(rgb8[2], 3)


def save_tci_png(out_png: Path, tci_jp2: Path, aoi_bounds=None, aoi_bounds_crs="EPSG:4326") -> None:
    """
    TCI JP2는 보통 3밴드(또는 4밴드)로 들어옵니다.
    여기서는 앞의 3밴드를 RGB로 저장합니다.
    """
    out_png.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(tci_jp2) as src:
        if aoi_bounds is None:
            arr = src.read()  # [count,H,W]
        else:
            from rasterio.warp import transform_bounds
            b = transform_bounds(aoi_bounds_crs, src.crs, *aoi_bounds, densify_pts=21)
            win = from_bounds(*b, transform=src.transform)
            win = win.round_offsets().round_lengths()
            win = win.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
            arr = src.read(window=win)

    if arr.shape[0] < 3:
        raise RuntimeError(f"TCI has <3 bands: {tci_jp2}")

    rgb = arr[:3].astype(np.float32)

    # TCI는 이미 시각화용이어서 보통 0-255/0-10000 등 케이스가 있음 -> 퍼센타일 스트레치로 안전하게
    r = percentile_stretch(rgb[0])
    g = percentile_stretch(rgb[1])
    b = percentile_stretch(rgb[2])
    rgb01 = np.stack([r, g, b], axis=0)

    save_rgb_png(out_png, rgb01)


def make_rgb_from_bands(
    b04_path: Path, b03_path: Path, b02_path: Path,
    aoi_bounds=None, aoi_bounds_crs="EPSG:4326",
    p_low=2.0, p_high=98.0, gamma=1.0
) -> tuple[np.ndarray, Affine, rasterio.crs.CRS]:
    """
    Sentinel-2 TrueColor 합성 규칙:
      R = B04, G = B03, B = B02
    """
    b04, transform, crs = read_one_band(b04_path, aoi_bounds, aoi_bounds_crs)
    b03, transform2, crs2 = read_one_band(b03_path, aoi_bounds, aoi_bounds_crs)
    b02, transform3, crs3 = read_one_band(b02_path, aoi_bounds, aoi_bounds_crs)

    # sanity (대부분 동일해야 함)
    if crs != crs2 or crs != crs3:
        raise RuntimeError("CRS mismatch among bands.")
    if transform != transform2 or transform != transform3 or b04.shape != b03.shape or b04.shape != b02.shape:
        raise RuntimeError("Band grids mismatch (shape/transform). Crop bounds must be consistent.")

    r = apply_gamma(percentile_stretch(b04, p_low, p_high), gamma)
    g = apply_gamma(percentile_stretch(b03, p_low, p_high), gamma)
    b = apply_gamma(percentile_stretch(b02, p_low, p_high), gamma)

    rgb01 = np.stack([r, g, b], axis=0).astype(np.float32)
    return rgb01, transform, crs


def main():
    # ✅ 다운로드 루트 (s2api 다운로드 스크립트에서 만든 폴더)
    dl_root = Path(r".\downloads") / "S2_TOPK_JP2"

    # ✅ 결과 저장 폴더
    out_root = Path(r".\downloads") / "S2_RGB_OUT"
    out_root.mkdir(parents=True, exist_ok=True)

    # ✅ AOI crop (원하면 활성화)
    # WGS84 lon/lat bounds: (minLon, minLat, maxLon, maxLat)
    # aoi_bounds = (127.2, 36.2, 127.6, 36.5)
    aoi_bounds = None

    # 시각화 파라미터
    p_low, p_high = 2.0, 98.0
    gamma = 1.0  # 0.8 정도면 더 밝아짐

    # dl_root 구조: S2_TOPK_JP2/<sensor>/<item_id>/<files...>
    for sensor_dir in sorted(dl_root.glob("*")):
        if not sensor_dir.is_dir():
            continue
        sensor = sensor_dir.name

        for item_dir in sorted(sensor_dir.glob("*")):
            if not item_dir.is_dir():
                continue
            item_id = item_dir.name

            # 기대 파일명(다운로더가 item_id_key.jp2로 저장했음)
            tci = item_dir / f"{item_id}_TCI_10m.jp2"
            b02 = item_dir / f"{item_id}_B02_10m.jp2"
            b03 = item_dir / f"{item_id}_B03_10m.jp2"
            b04 = item_dir / f"{item_id}_B04_10m.jp2"

            out_dir = out_root / sensor / item_id
            out_dir.mkdir(parents=True, exist_ok=True)

            print(f"\n[{sensor}] {item_id}")

            # (1) TCI → PNG
            if tci.exists():
                out_png = out_dir / f"{item_id}_TCI_10m.png"
                if out_png.exists():
                    print("  - TCI png: exists")
                else:
                    print("  - TCI png: writing...")
                    save_tci_png(out_png, tci, aoi_bounds=aoi_bounds)
                    print("    saved ->", out_png)
            else:
                print("  - TCI jp2 missing")

            # (2) Bands → RGB 합성 → GeoTIFF + PNG
            if b02.exists() and b03.exists() and b04.exists():
                out_tif = out_dir / f"{item_id}_RGB_from_B02B03B04_10m.tif"
                out_png = out_dir / f"{item_id}_RGB_from_B02B03B04_10m.png"

                if out_tif.exists() and out_png.exists():
                    print("  - RGB(tif/png): exists")
                else:
                    print("  - RGB 합성: writing...")
                    rgb01, transform, crs = make_rgb_from_bands(
                        b04, b03, b02,
                        aoi_bounds=aoi_bounds,
                        p_low=p_low, p_high=p_high, gamma=gamma
                    )
                    save_rgb_geotiff(out_tif, rgb01, transform, crs)
                    save_rgb_png(out_png, rgb01)
                    print("    saved ->", out_tif)
                    print("    saved ->", out_png)
            else:
                print("  - B02/B03/B04 jp2 missing (skip RGB compositing)")


if __name__ == "__main__":
    main()