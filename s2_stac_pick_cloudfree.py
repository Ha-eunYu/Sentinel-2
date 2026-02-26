from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import json
import pystac_client

import rasterio

STAC_URL = "https://stac.dataspace.copernicus.eu/v1"


@dataclass
class PickConfig:
    bbox: List[float]  # [minLon, minLat, maxLon, maxLat]
    collection: str = "sentinel-2-l2a"
    window_days: int = 3          # ±N days
    cloud_lt: float = 20.0        # eo:cloud_cover < cloud_lt
    max_items: int = 200          # search result cap (server-side 'max_items' not always honored; we cap locally)
    prefer_same_orbit: bool = False  # placeholder (S2 orbit constraints usually not needed for figure background)


def _to_dt_utc(s: str) -> datetime:
    # STAC datetime is ISO8601 like "2021-01-27T02:19:49.024Z"
    # Convert 'Z' to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _safe_get_cloud(item) -> float:
    # eo:cloud_cover is usually present in properties for S2
    v = item.properties.get("eo:cloud_cover", None)
    if v is None:
        return float("inf")
    try:
        return float(v)
    except Exception:
        return float("inf")


def _score_item(item, target_dt: datetime) -> Tuple[float, float, datetime]:
    """
    Sort key:
      1) cloud cover (ascending)
      2) |time difference| in hours (ascending)
      3) acquisition datetime (ascending) as stable tie-breaker
    """
    cloud = _safe_get_cloud(item)
    dt = _to_dt_utc(item.properties["datetime"])
    dt_diff_hours = abs((dt - target_dt).total_seconds()) / 3600.0
    return (cloud, dt_diff_hours, dt)

def pick_s2_tci_asset(item):
    a = (item.assets or {}).get("TCI_10m")
    if a and getattr(a, "href", None):
        return {"key": "TCI_10m", "href": a.href, "type": getattr(a, "media_type", None)}
    return None

def pick_s2_rgb_bands(item):

    assets = item.assets or {}

    def get(key):
        a = assets.get(key)
        if a and getattr(a, "href", None):
            return {"key": key, "href": a.href, "type": getattr(a, "media_type", None)}
        return None

    out = {
        "B02": get("B02_10m"),
        "B03": get("B03_10m"),
        "B04": get("B04_10m"),
    }
    # 필수(10m) 없으면 None 처리
    if not (out["B02"] and out["B03"] and out["B04"]):
        return None
    return out


def pick_s2_index_assets(item):
    assets = item.assets or {}

    def get(key):
        a = assets.get(key)
        if a and getattr(a, "href", None):
            return {"key": key, "href": a.href, "type": getattr(a, "media_type", None)}
        return None

    out = {
        "B08": get("B08_10m"),
        "B11": get("B11_20m"),   # 20m
        "SCL": get("SCL_20m"),   # 20m
    }
    # 필수(10m) 없으면 None 처리
    if not out["B08"]:
        return None
    return out

def pick_topk_items(client, target_date: str, cfg: PickConfig, k: int = 3):
    target_dt = datetime.fromisoformat(target_date).replace(tzinfo=timezone.utc)

    plan = [
        (cfg.window_days, cfg.cloud_lt),
        (max(cfg.window_days, 5), max(cfg.cloud_lt, 40.0)),
        (max(cfg.window_days, 10), 100.0),
]

    last_reason = None

    for window_days, cloud_lt in plan:
        start = (target_dt - timedelta(days=window_days)).strftime("%Y-%m-%dT00:00:00Z")
        end   = (target_dt + timedelta(days=window_days)).strftime("%Y-%m-%dT23:59:59Z")
        datetime_range = f"{start}/{end}"

        search = client.search(
            collections=[cfg.collection],
            bbox=cfg.bbox,
            datetime=datetime_range,
            query={"eo:cloud_cover": {"lt": cloud_lt}},
            limit=100,
        )

        items = []
        for it in search.items():
            items.append(it)
            if len(items) >= cfg.max_items:
                break

        if not items:
            last_reason = f"No items in ±{window_days} days with eo:cloud_cover < {cloud_lt}"
            continue

        items_sorted = sorted(items, key=lambda x: _score_item(x, target_dt))

        print("ASSET KEYS:", sorted((items_sorted[0].assets or {}).keys()))

        # build top-k summary
        topk = []
        for it in items_sorted[:k]:
            topk.append({
                "id": it.id,
                "datetime": it.properties.get("datetime"),
                "eo:cloud_cover": it.properties.get("eo:cloud_cover"),
                "proj:epsg": it.properties.get("proj:epsg"),
            })

        # store BOTH TCI and RGB bands for each top-k item
        topk_rgb_assets = []
        for it in items_sorted[:k]:
            topk_rgb_assets.append({
                "id": it.id,
                "tci": pick_s2_tci_asset(it),
                "bands": pick_s2_rgb_bands(it),
                "index": pick_s2_index_assets(it),   
            })

        return {
            "target_date": target_date,
            "status": "ok",
            "search_used": {"datetime": datetime_range, "cloud_lt": cloud_lt, "window_days": window_days},
            "candidates_topk": topk,
            "candidates_topk_rgb_assets": topk_rgb_assets,
        }

    return {
        "target_date": target_date,
        "status": "no_items",
        "reason": last_reason or "No items found",
    }

def main() -> None:
    # WGS84 lon/lat
    cfg = PickConfig(
        bbox=[127.2, 36.2, 127.6, 36.5],  # 127.463, 36.465 127.475, 36.475
        window_days=10,  
        cloud_lt=100.0,  
        max_items=200,
    )

    targets = [
        ("ICEYE", "2021-01-21"),
        ("UMBRA", "2024-07-17"),
        ("Capella", "2024-08-19"),
    ]

    client = pystac_client.Client.open(STAC_URL)
    results = {"stac_url": STAC_URL, "config": cfg.__dict__, "targets": []}

    for sensor, date_str in targets:
        print(f"\n=== {sensor} | target={date_str} | window=±{cfg.window_days}d | cloud<{cfg.cloud_lt}% ===")
        # res = pick_best_item(client, date_str, cfg)
        res = pick_topk_items(client, date_str, cfg, k=3)
        results["targets"].append({"sensor": sensor, **res})

        if res["status"] != "ok":
            print("-> NO ITEMS:", res.get("reason"))
            continue
        print("-> search used:", res["search_used"])
        
        for i, cand in enumerate(res["candidates_topk"], start=1):
            print(f"   [{i}] id={cand['id']}")
            print(f"       datetime={cand['datetime']}, cloud={cand['eo:cloud_cover']}, proj:epsg={cand['proj:epsg']}")

        print(res["candidates_topk_rgb_assets"][0])

        # ✅ CRS 확인: 첫 번째 후보의 B04 href로 테스트 (없으면 스킵)
        entry0 = res["candidates_topk_rgb_assets"][0]
        tci0 = entry0.get("tci")      # {"key": "TCI_10m", "href": "..."} or None
        bands0 = entry0.get("bands")  # {"B02": {...}, "B03": {...}, "B04": {...}} or None

        if tci0:
            print("TCI href:", tci0["href"])
        if bands0:
            print("B04 href:", bands0["B04"]["href"])

        if tci0 and tci0.get("href"):
            href = tci0["href"]
        elif bands0 and bands0.get("B04") and bands0["B04"].get("href"):
            href = bands0["B04"]["href"]
        else:
            href = None

        if href:
            try:
                with rasterio.open(href) as src:
                    print(src.crs, src.crs.to_epsg())
            except Exception as e:
                print("⚠️ rasterio로 원격 href를 바로 열지 못함:", repr(e))
                print("   -> 해결: 먼저 다운로드 후 local file로 rasterio.open()")

    # 결과를 manifest로 저장 (다음 단계: RGB 다운로드/합성에서 그대로 씀)
    out = Path(r".\downloads") / "s2_stac_picks_manifest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    
    print(f"\n✅ Saved manifest: {out}")
    data = json.loads(out.read_text(encoding="utf-8"))
    assert len(data["targets"]) > 0, "Manifest targets is empty! Append failed?"
    print(f"Manifest targets count = {len(data['targets'])}")


if __name__ == "__main__":
    main()

# Get-Content .\downloads\s2_stac_picks_manifest.json | ConvertFrom-Json | % {$_.targets | Select sensor, status}
