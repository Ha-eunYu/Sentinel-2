from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import rasterio
from rasterio.enums import Resampling
import matplotlib.pyplot as plt


# --- SCL 마스크: 보통 제외 권장 클래스 ---
# (ESA SCL legend 기준이지만, 필요시 조정하세요)
# 0 No data, 1 Saturated/defective, 2 Dark area pixels,
# 3 Cloud shadow, 7/8/9 Clouds, 10 Cirrus, 11 Snow/ice
SCL_EXCLUDE = {0, 1, 2, 3, 7, 8, 9, 10, 11}


def read_resampled(path: Path, ref_profile: dict, resampling: Resampling) -> np.ndarray:
    """ref_profile의 height/width/transform에 맞춰 리샘플해서 1밴드 읽기"""
    with rasterio.open(path) as ds:
        arr = ds.read(
            1,
            out_shape=(ref_profile["height"], ref_profile["width"]),
            resampling=resampling,
        )
    return arr


def safe_index(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    out = np.full(num.shape, np.nan, dtype=np.float32)
    m = np.isfinite(num) & np.isfinite(den) & (den != 0)
    out[m] = (num[m] / den[m]).astype(np.float32)
    return out

def write_geotiff(out_path: Path, ref_profile: dict, arr: np.ndarray) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    profile = ref_profile.copy()

    profile.update(
        driver="GTiff",
        dtype="float32",
        count=1,
        nodata=np.nan,
        compress="DEFLATE",
        predictor=2,
        tiled=True,
    )

    profile.pop("photometric", None)
    profile.pop("interleave", None)

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(np.float32), 1)


def save_quicklook_png(out_png: Path, arr: np.ndarray, title: str, vmin: float = -1, vmax: float = 1) -> None:
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7, 6))
    im = plt.imshow(arr, vmin=vmin, vmax=vmax)
    plt.title(title, fontsize=10)
    plt.colorbar(im, fraction=0.046, pad=0.04)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def main() -> None:
    manifest_path = Path("downloads") / "s2_stac_picks_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path.resolve()}")

    dl_root = Path("downloads") / "S2_TOPK_JP2"
    out_root = Path("downloads") / "S2_INDICES"
    out_root.mkdir(parents=True, exist_ok=True)

    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    for t in data.get("targets", []):
        sensor = t.get("sensor", "UNKNOWN")
        if t.get("status") != "ok":
            continue

        cands = t.get("candidates_topk", [])
        assets = t.get("candidates_topk_rgb_assets", [])
        n = min(len(cands), len(assets))
        if n == 0:
            continue

        for i in range(n):
            item_id = cands[i]["id"]
            entry = assets[i]  # candidates_topk_rgb_assets[i]
            print("DEBUG entry keys:", entry.keys())
            bands = entry.get("bands", {}) or {}
            extra = entry.get("index") or entry.get("index_extra") or {}

            # 필수 키 확인
            if not (bands.get("B03") and bands.get("B04") and extra.get("B08") and extra.get("B11") and extra.get("SCL")):
                print(f"[{sensor}] {item_id}: required bands missing (need B03, B04, B08, B11, SCL). skip")
                continue

            base = dl_root / sensor / item_id

            # --- 경로 구성 ---
            p_b03 = None
            if bands.get("B03"):
                p_b03 = base / f"{item_id}_{bands['B03']['key']}.jp2"
            p_b04 = None
            if bands.get("B04"):
                p_b04 = base / f"{item_id}_{bands['B04']['key']}.jp2"
            p_b08 = None
            if extra.get("B08"):
                p_b08 = base / f"{item_id}_{extra['B08']['key']}.jp2"
            p_b11 = None
            if extra.get("B11"):
                p_b11 = base / f"{item_id}_{extra['B11']['key']}.jp2"
            p_scl = None
            if extra.get("SCL"):
                p_scl = base / f"{item_id}_{extra['SCL']['key']}.jp2"
            # 필수 밴드 확인
            for p in [p_b03, p_b04, p_b08]:
                if not p.exists():
                    raise FileNotFoundError(f"Missing required band: {p}\n(먼저 B03/B04/B08 다운로드 필요)")

            # B03_10m을 기준 그리드로 사용
            with rasterio.open(p_b03) as ref:
                ref_profile = ref.profile

            b03 = read_resampled(p_b03, ref_profile, Resampling.bilinear).astype(np.float32)
            b04 = read_resampled(p_b04, ref_profile, Resampling.bilinear).astype(np.float32)
            b08 = read_resampled(p_b08, ref_profile, Resampling.bilinear).astype(np.float32)

            # SCL(20m)은 nearest가 정석
            scl = None
            if p_scl and p_scl.exists():
                scl = read_resampled(p_scl, ref_profile, Resampling.nearest).astype(np.int16)

            # B11(20m)은 bilinear로 10m로 맞춤
            b11 = None
            if p_b11 and p_b11.exists():
                b11 = read_resampled(p_b11, ref_profile, Resampling.bilinear).astype(np.float32)

            # --- indices ---
            ndvi = safe_index((b08 - b04), (b08 + b04))
            ndwi = safe_index((b03 - b08), (b03 + b08))  # McFeeters NDWI

            mndwi = None
            if b11 is not None:
                mndwi = safe_index((b03 - b11), (b03 + b11))

            # --- SCL mask 적용 ---
            if scl is not None:
                mask_bad = np.isin(scl, list(SCL_EXCLUDE))
                ndvi[mask_bad] = np.nan
                ndwi[mask_bad] = np.nan
                if mndwi is not None:
                    mndwi[mask_bad] = np.nan

            out_dir = out_root / sensor / item_id
            out_dir.mkdir(parents=True, exist_ok=True)

            # save tifs
            write_geotiff(out_dir / f"{item_id}_NDVI.tif", ref_profile, ndvi)
            write_geotiff(out_dir / f"{item_id}_NDWI.tif", ref_profile, ndwi)
            if mndwi is not None:
                write_geotiff(out_dir / f"{item_id}_MNDWI.tif", ref_profile, mndwi)

            # quicklook png
            save_quicklook_png(out_dir / f"{item_id}_NDVI.png", ndvi, f"{sensor} NDVI | {item_id}", vmin=-0.2, vmax=0.9)
            save_quicklook_png(out_dir / f"{item_id}_NDWI.png", ndwi, f"{sensor} NDWI | {item_id}", vmin=-1, vmax=1)
            if mndwi is not None:
                save_quicklook_png(out_dir / f"{item_id}_MNDWI.png", mndwi, f"{sensor} MNDWI | {item_id}", vmin=-1, vmax=1)

            print(f"✅ [{sensor}] {item_id}: NDVI/NDWI" + ("/MNDWI" if mndwi is not None else "") + " saved")

    print("\n✅ Done. outputs -> downloads/S2_INDICES")


if __name__ == "__main__":
    main()