import pystac_client
from datetime import datetime, timedelta

URL = "https://stac.dataspace.copernicus.eu/v1"
cat = pystac_client.Client.open(URL)

center_date = datetime(2024, 7, 17)
start = (center_date - timedelta(days=3)).strftime("%Y-%m-%d")
end   = (center_date + timedelta(days=3)).strftime("%Y-%m-%d")

search = cat.search(
    collections=["sentinel-2-l2a"],
    bbox=[127.2, 36.2, 127.6, 36.5],
    datetime=f"{start}/{end}",
    query={"eo:cloud_cover": {"lt": 20}},
)

items = list(search.items())
print(len(items))

for item in items:
    print(item.id, item.properties["eo:cloud_cover"])