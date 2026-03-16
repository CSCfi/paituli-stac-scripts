import pystac_client
import requests
import re
import argparse
import getpass
import pandas as pd
from urllib.parse import urljoin

from utils.json_convert import convert_json_to_geoserver

if __name__ == "__main__":

    online_data_prefix = "https://www.nic.funet.fi/index/geodata/"
    puhti_data_prefix = "/appl/data/geo/"
    puhti_pattern = "at_puhti"
    pw_filename = '../passwords.txt'

    parser = argparse.ArgumentParser()
    parser.add_argument("--collections", nargs="+", help="Specific collection", required=True)
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)

    args = parser.parse_args()
    try:
        pw_file = pd.read_csv(pw_filename, header=None)
        pwd = pw_file.at[0,0]
    except FileNotFoundError:
        print("No password given.")
        pwd = getpass.getpass("GeoServer Password: ")

    app_host = f"{args.host}/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    collections = args.collections

    for collection in collections:
        stac_col = catalog.get_child(collection)
        items = list(stac_col.get_items())
        number_of_items = len(items)
        added_puhti_link_count = 0
        
        for item in items:
            assets = item.assets
            # If Puhti assets added for some Items, skip them
            if any(puhti_pattern in asset_id for asset_id in assets):
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
            request_point = f"collections/{stac_col.id}/products/{item.id}"
            r = requests.put(urljoin(app_host, request_point), json=converted_item, auth=("admin", pwd), headers={"User-Agent":"update-script"})
            r.raise_for_status()
            added_puhti_link_count += 1

        if added_puhti_link_count == 0:
            print(f"All Puhti links are already added for {stac_col.id}.")
        else:
            print(f"+ Added all Puhti links for {stac_col.id}. Number of additions: {added_puhti_link_count}/{number_of_items}")