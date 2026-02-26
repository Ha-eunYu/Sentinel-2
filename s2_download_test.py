import os
import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import requests
from dotenv import load_dotenv


# ----------------------------
# Config
# ----------------------------
STAC_SEARCH_URL = "https://stac.dataspace.copernicus.eu/v1/search"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_ZIPPER_TEMPLATE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
ODATA_SEARCH_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

def odata_get_uuid_by_name(product_name_no_safe: str, token: str) -> str:
    # OData의 Name은 보통 ".SAFE" 포함 형태로 조회하는 게 안정적입니다.
    name = product_name_no_safe if product_name_no_safe.endswith(".SAFE") else product_name_no_safe + ".SAFE"

    # filter에서 작은따옴표 이스케이프
    name_escaped = name.replace("'", "''")

    params = {
        "$filter": f"Name eq '{name_escaped}'",
        "$select": "Id,Name",
        "$top": "1",
    }

    # 검색은 토큰 없이 되는 경우도 있지만, 토큰을 붙이면 권한/정책 변화에도 안전합니다.
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(ODATA_SEARCH_URL, params=params, headers=headers, timeout=60)
    if not r.ok:
        raise RuntimeError(f"OData lookup failed {r.status_code}: {r.text}")

    js = r.json()
    values = js.get("value", [])
    if not values:
        raise RuntimeError(f"OData returned 0 items for Name='{name}'. (Name이 다르거나 제품이 비활성/미러링 상태일 수 있음)")

    return values[0]["Id"]  # UUID

def get_access_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Token request failed {r.status_code}: {r.text}")
    js = r.json()
    return js["access_token"]


def stac_search(bbox: list, datetime_range: str, collection: str, limit: int = 5) -> Dict[str, Any]:
    query = {
        "collections": [collection],
        "bbox": bbox,
        "datetime": datetime_range,
        "limit": limit,
    }
    r = requests.post(STAC_SEARCH_URL, json=query, timeout=60)
    if not r.ok:
        # print error body for diagnostics
        raise RuntimeError(f"STAC search failed {r.status_code}: {r.text}")
    return r.json()


def pick_first_item(fc: Dict[str, Any]) -> Tuple[str, str]:
    """Return (item_id, datetime_str)."""
    feats = fc.get("features", [])
    if not feats:
        raise RuntimeError("STAC returned 0 features. Relax bbox/datetime or check collection.")
    item = feats[0]
    item_id = item["id"]
    # STAC Item datetime is usually in properties.datetime
    dt = (item.get("properties", {}) or {}).get("datetime", "unknown")
    return item_id, dt


def download_with_token(
    product_id: str,
    token: str,
    out_zip: Path,
    chunk_size: int = 1024 * 1024,
    max_retries: int = 3,
) -> None:
    url = ODATA_ZIPPER_TEMPLATE.format(product_id=product_id)
    headers = {"Authorization": f"Bearer {token}"}

    out_zip.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                if not r.ok:
                    raise RuntimeError(f"Download failed {r.status_code}: {r.text[:500]}")

                total = int(r.headers.get("Content-Length", "0") or "0")
                written = 0

                tmp_path = out_zip.with_suffix(out_zip.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        written += len(chunk)
                        if total > 0:
                            pct = (written / total) * 100
                            sys.stdout.write(f"\rDownloading... {pct:6.2f}% ({written}/{total} bytes)")
                            sys.stdout.flush()

                # move into place
                tmp_path.replace(out_zip)
                sys.stdout.write("\n")
                print(f"✅ Saved: {out_zip}")
                return

        except Exception as e:
            print(f"\n⚠️ Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
            else:
                raise


def main() -> None:
    load_dotenv()

    # ---- user inputs (edit here or pass as env/args) ----
    # Example bbox around Daejeon-ish. Replace with your AOI.
    bbox = [127.2, 36.2, 127.6, 36.5]  # [minLon, minLat, maxLon, maxLat]
    datetime_range = "2021-01-10T00:00:00Z/2021-01-30T23:59:59Z"
    collection = "sentinel-2-l2a"  # or sentinel-2-l1c
    limit = 3

    # out_dir = Path("./downloads")
    out_dir = Path(r"D:\21_SENTINEL2")

    user = os.environ["CDSE_USERNAME"]
    pw = os.environ["CDSE_PASSWORD"]

    # 1) token
    token = get_access_token(user, pw)
    print("✅ Token OK")

    # 2) stac search
    fc = stac_search(bbox=bbox, datetime_range=datetime_range, collection=collection, limit=limit)
    item_id, dt = pick_first_item(fc)
    print(f"✅ STAC OK: first item id = {item_id}")
    print(f"   datetime = {dt}")

    # 3) download
    product_uuid = odata_get_uuid_by_name(item_id, token)
    print(f"✅ OData UUID = {product_uuid}")
    out_zip = out_dir / f"{item_id}.zip"
    download_with_token(product_id=item_id, token=token, out_zip=out_zip)


if __name__ == "__main__":
    main()