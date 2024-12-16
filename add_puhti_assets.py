import pystac_client
import requests
import re
import argparse
import getpass
from urllib.parse import urljoin

from utils.json_convert import convert_json_to_geoserver


if __name__ == "__main__":

    online_data_prefix = "https://www.nic.funet.fi/index/geodata/"
    puhti_data_prefix = "/appl/data/geo/"

    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", help="Specific collection", required=True)
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)

    args = parser.parse_args()
    pwd = getpass.getpass()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})

    collection = catalog.get_child(args.collection)
    items = collection.get_items()
    for item in items:
        assets = item.assets
        # If Puhti assets added for some Items, skip them
        if len(assets) > 1:
            continue
        cloned_assets = []
        for asset in assets:
            cloned_asset = assets[asset].clone()
            cloned_asset.href = re.sub(online_data_prefix, puhti_data_prefix, cloned_asset.href)
            cloned_asset.title = re.sub("paituli", "puhti", cloned_asset.title)
            cloned_assets.append(cloned_asset)
        
        for clone in cloned_assets:
            item.add_asset(
                key = clone.title,
                asset = clone
            )

        item_dict = item.to_dict()
        converted_item = convert_json_to_geoserver(item_dict)
        request_point = f"collections/{collection.id}/products/{item.id}"
        r = requests.put(urljoin(app_host, request_point), json=converted_item, auth=("admin", pwd))
        r.raise_for_status()
        print(f" + Added Puhti links to {item.id}")

    print("Added all Puhti links")