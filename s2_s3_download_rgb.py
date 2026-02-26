from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from dotenv import load_dotenv


EODATA_ENDPOINT = "https://eodata.dataspace.copernicus.eu"


def parse_s3_href(href: str) -> tuple[str, str]:
    """
    Parse STAC asset href like:
      s3://eodata/...
      s3://EODATA/...
    Returns (bucket, key)
    """
    u = urlparse(href)
    if u.scheme != "s3":
        raise ValueError(f"Not an s3:// href: {href}")
    return u.netloc, u.path.lstrip("/")


def ensure_download(s3, href: str, out_path: Path) -> None:
    bucket, key = parse_s3_href(href)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return

    s3.download_file(bucket, key, str(out_path))


def main() -> None:
    load_dotenv()

    access_key = os.environ["CDSE_S3_ACCESS_KEY"]
    secret_key = os.environ["CDSE_S3_SECRET_KEY"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=EODATA_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )

    manifest_path = Path(r".\downloads") / "s2_stac_picks_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path.resolve()}")

    out_root = Path(r".\downloads")
    dl_root = out_root / "S2_TOPK_JP2"
    dl_root.mkdir(parents=True, exist_ok=True)

    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    for t in data.get("targets", []):
        sensor = t.get("sensor", "UNKNOWN")
        status = t.get("status")

        if status != "ok":
            print(f"[{sensor}] skip: {status} ({t.get('reason')})")
            continue

        candidates = t.get("candidates_topk", [])
        candidates_assets = t.get("candidates_topk_rgb_assets", [])

        if not candidates or not candidates_assets:
            print(f"[{sensor}] skip: no candidates in manifest")
            continue

        # top-k 실제 길이에 맞춰서 진행 (UMBRA처럼 1개만 있을 수 있음)
        n = min(len(candidates), len(candidates_assets))
        target_date = t.get("target_date", "unknown")
        print(f"\n=== {sensor} | target={target_date} | downloading top-{n} RGB ===")

        for i in range(n):
            cand = candidates[i]
            item_id = cand["id"]
            dt = cand.get("datetime")
            cloud = cand.get("eo:cloud_cover")

            entry = candidates_assets[i]  # ✅ {"id":..., "tci":..., "bands":...}
            if entry.get("id") and entry["id"] != item_id:
                print(f"      - ⚠️ manifest order mismatch: cand={item_id} vs entry={entry['id']}")
                # 안전하게 entry id를 우선 적용
                item_id = entry["id"]
                  
            tci = entry.get("tci")
            bands = entry.get("bands", {}) or {}
            extra = entry.get("index", {}) or {}

            out_dir = dl_root / sensor / item_id
            out_dir.mkdir(parents=True, exist_ok=True)

            print(f"  [{i+1}] {item_id} | datetime={dt} | cloud={cloud}")

            # (1) TCI
            if tci and tci.get("href"):
                href = tci["href"]
                keyname = tci.get("key", "TCI_10m")
                out_path = out_dir / f"{item_id}_{keyname}.jp2"

                if out_path.exists() and out_path.stat().st_size > 0:
                    print(f"      - {keyname}: exists")
                else:
                    print(f"      - {keyname}: downloading...")
                    ensure_download(s3, href, out_path)
                    print(f"        saved -> {out_path}")
            else:
                print("      - TCI: 없음(스킵)")

            # (2) RGB(B02/B03/B04)
            if bands:
                for band in ["B02", "B03", "B04"]:
                    a = bands.get(band)
                    if not a or not a.get("href"):
                        raise RuntimeError(f"[{sensor}] Missing href for {item_id} {band} in manifest.")

                    href = a["href"]
                    band_key = a.get("key", band)
                    out_path = out_dir / f"{item_id}_{band_key}.jp2"

                    if out_path.exists() and out_path.stat().st_size > 0:
                        print(f"      - {band_key}: exists")
                    else:
                        print(f"      - {band_key}: downloading...")
                        ensure_download(s3, href, out_path)
                        print(f"        saved -> {out_path}")
            else:
                print("      - BANDS: 없음(스킵)")
        
        # (3) index_extra (B08/B11/SCL)
            if extra:
                if not (extra.get("B08") and extra["B08"].get("href")):
                    print("      - ⚠️ extra: B08 없음 (NDVI/NDWI 계산 불가)")
                for key in ["B08","B11","SCL"]:
                    a = extra.get(key)
                    if not a or not a.get("href"):
                        print(f"      - {key}: 없음(스킵)")
                        continue
                    href = a["href"]
                    keyname = a.get("key", key)
                    out_path = out_dir / f"{item_id}_{keyname}.jp2"

                    if out_path.exists() and out_path.stat().st_size > 0:
                        print(f"      - {keyname}: exists")
                    else:
                        print(f"      - {keyname}: downloading...")
                        ensure_download(s3, href, out_path)
                        print(f"        saved -> {out_path}")
            else:
                print("      - index_extra(B08/B11/SCL): 없음(스킵)")

    print("\n✅ Download done.")
    print(f"📁 JP2 root: {dl_root}")


if __name__ == "__main__":
    main()