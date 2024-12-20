import pystac
import rasterio
import requests
import datetime
import pandas as pd
import re
import time
import getpass
import argparse
import pystac_client
from bs4 import BeautifulSoup
from rio_stac.stac import create_stac_item
from urllib.parse import urljoin

from utils.json_convert import convert_json_to_geoserver
from utils.geocubes_api import get_datasets

def update_catalog(app_host, csc_catalog_client):

    """
    The main updating function of the script. Checks the collection items in the Geocubes and compares the to the ones in CSC catalog.

    app_host - The REST API path for updating the collections
    csc_catalog_client - The STAC API path for checking which items are already in the collections
    """
    title_regex_pattern = r" \(GeoCubes\)"
    session = requests.Session()
    session.auth = ("admin", pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering

    # Get all Geocubes collections from the app_host
    csc_collections = [col for col in csc_catalog_client.get_collections() if col.id.endswith("at_geocubes")]

    csc_title_id_map = {c.title: c.id for c in csc_collections}
    collection_csv = pd.read_csv('files/karttatasot.csv', index_col='Nimi').to_dict('index')

    # Get the titles and IDs from CSC STAC and make the title correspond them to the ones in the CSV
    titles_and_ids = {}
    for title in csc_title_id_map:
        fixed_title = re.sub(title_regex_pattern, '', title)
        titles_and_ids[fixed_title] = csc_title_id_map[title]

    geocubes_datasets = get_datasets()
    for dataset in geocubes_datasets:
        try: # If there's more datasets in GeoCubes than in CSC STAC, skip them in this update script
            translated_name = collection_csv[dataset]["Name"]
        except KeyError:
            continue

        collection_id = titles_and_ids[translated_name]
        csc_collection = csc_catalog_client.get_child(collection_id)
        csc_collection_item_ids = [item.id for item in csc_collection.get_items()]

        paths = geocubes_datasets[dataset]['paths']
        print(f"Checking new items for {csc_collection.id}: ", end="")

        number_of_items_in_geocubes = 0
        number_of_items_added = 0
        for year_path in paths:

            #TIFs through BeautifulSoup
            page = requests.get(year_path)
            data = page.text
            soup = BeautifulSoup(data, features="html.parser")

            links = [link for link in soup.find_all("a")]

            item_links = [link.get("href") for link in links if link.get("href").endswith("tif")]
            item_sets = [item.split(".")[0] for item in item_links]
                
            grouped_dict = {}
            for item in item_sets:
                prefix = "_".join(item.split("_")[:4])
                if prefix not in grouped_dict:
                    grouped_dict[prefix] = []
                    grouped_dict[prefix].append(item)
            
            number_of_items_in_geocubes = number_of_items_in_geocubes + len(grouped_dict.keys())
            for key in grouped_dict.keys():
                
                # Takes the year from the path
                item_starttime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-01-01", "%Y-%m-%d")
                item_endtime = datetime.datetime.strptime(f"{year_path.split('/')[-2]}-12-31", "%Y-%m-%d")
                    
                # The sentinel and NDVI items are named a bit differently from the rest
                item_year = year_path.split("/")[-1]
                if "sentinel" in key:
                    name = key.split("_")[0].replace('-', '_')
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{name.lower().replace(' ', '_').replace(',', '')}_{item_info}"
                elif "ndvi" in key:
                    name = key.split("_")[0]
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{name.lower()}_{item_info}"
                else:
                    item_info = "_".join(key.split(".")[0].split("_")[1:])
                    item_id = f"{translated_name.lower().replace(' ', '_').replace(',', '')}_{item_info}"

                if item_id in csc_collection_item_ids:
                    continue
                else:
                    number_of_items_added = number_of_items_added + 1
                    with rasterio.open(year_path+grouped_dict[key][0]+".tif") as src:
                        assets = {
                            "COG": pystac.Asset(
                                href=year_path+grouped_dict[key][0]+".tif", 
                                media_type="image/tiff; application=geotiff; profile=cloud-optimized", 
                                title="COG",
                                roles=["data"],
                                extra_fields={
                                    "gsd": int(src.res[0]),
                                    "proj:shape": src.shape,
                                    "proj:transform": [
                                        src.transform.a,
                                        src.transform.b,
                                        src.transform.c,
                                        src.transform.d,
                                        src.transform.e,
                                        src.transform.f,
                                        src.transform.g,
                                        src.transform.h,
                                        src.transform.i
                                    ]
                                }
                            )
                        }
                    min_gsd = assets["COG"].extra_fields["gsd"]
                    for asset in grouped_dict[key][1:]:
                        with rasterio.open(year_path+asset+".tif") as src:
                            asset_id = asset.split("_")[-1]
                            assets[asset_id] = pystac.Asset(
                                href=year_path+asset+".tif",
                                media_type="image/tiff; application=geotiff", 
                                title=asset.split('_')[-1],
                                roles=["data"],
                                extra_fields={
                                    "gsd": int(src.res[0]),
                                    "proj:shape": src.shape,
                                    "proj:transform": [
                                        src.transform.a,
                                        src.transform.b,
                                        src.transform.c,
                                        src.transform.d,
                                        src.transform.e,
                                        src.transform.f,
                                        src.transform.g,
                                        src.transform.h,
                                        src.transform.i
                                    ]
                                }
                            )
                        
                        # Add the GSD into the Collection Summaries if not in it
                        if assets[asset_id].extra_fields["gsd"] not in csc_collection.summaries.lists["gsd"]:
                            csc_collection.summaries.lists["gsd"].append(assets[asset_id].extra_fields["gsd"])
                        min_gsd = min(min_gsd, assets[asset_id].extra_fields["gsd"])

                    item = create_stac_item(
                        source=year_path+key+".tif",
                        id=item_id,
                        assets=assets, 
                        asset_media_type=pystac.MediaType.TIFF, 
                        with_proj=True,
                    )
                    item.common_metadata.start_datetime = item_starttime
                    item.common_metadata.end_datetime = item_endtime
                    item.extra_fields["gsd"] = min_gsd
                    item.properties["proj:epsg"] = 3067
                    csc_collection.add_item(item)

                    item_dict = item.to_dict()
                    converted_item = convert_json_to_geoserver(item_dict)
                    request_point = f"collections/{csc_collection.id}/products"
                    r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                    r.raise_for_status()

        print(f"{len(csc_collection_item_ids)}/{number_of_items_in_geocubes}")
        if number_of_items_added:
            # Update the extents from the GeoCubes Items
            csc_collection.update_extent_from_items()
            collection_dict = csc_collection.to_dict()
            converted_collection = convert_json_to_geoserver(collection_dict)
            request_point = f"collections/{csc_collection.id}/"

            r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
            r.raise_for_status()
            print(f" + Number of items added: {number_of_items_added}")
            print(" + Updated Collection Extents.")
        else:
            print(" * All items present.")


if __name__ == "__main__":

    """
    The first check for REST API password is from a password file. 
    If a password file is not found, the script prompts the user to give a password through CLI
    """
    pw_filename = 'passwords.txt'
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    
    args = parser.parse_args()

    try:
        pw_file = pd.read_csv(pw_filename, header=None)
        pwd = pw_file.at[0,0]
    except FileNotFoundError:
        print("Password not given as an argument and no password file found")
        pwd = getpass.getpass()
        
    start = time.time()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog_client = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})

    print(f"Updating STAC Catalog at {args.host}")
    update_catalog(app_host, csc_catalog_client)

    end = time.time()
    print(f"Script took {end-start:.2f} seconds")