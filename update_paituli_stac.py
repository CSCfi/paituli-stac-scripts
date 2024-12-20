import pystac
import psycopg2
import rasterio
import requests
import datetime
import getpass
import argparse
import re
import os
import json
import time
import pystac_client
import pandas as pd
from rio_stac.stac import create_stac_item
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils.json_convert import convert_json_to_geoserver
from utils.paituli import recursive_filecheck, get_new_local_files, generate_timestamps, generate_item_id

def create_item(path: str, data_dict: dict, item_media_type: str, label: str | None) -> pystac.Item:

    """
        path - String of the URL where the file is located
        data_dict - Dictionary of the dataset from the Postgresql DB
        item_media_type - String of the media type the file is in
        label - String of the label given in the index_wgs84

        -> pystac.Item
    """

    # If create_item is called, flip flag to True
    global added_items_flag
    added_items_flag = True

    item_timestamps = generate_timestamps(path, data_dict, label)

    item_id = generate_item_id(path, data_dict, item_timestamps["item_date"], label)

    # There are files which have case-sensitive file-extensions
    # If the default extension returns 404, switch it to uppercase
    r = requests.head(path)
    if r.status_code == 404:
        address = os.path.dirname(path)
        filename = os.path.basename(path)
        current_extension = os.path.splitext(filename)[1]
        new_filename = os.path.splitext(filename)[0] + current_extension.upper()
        path = os.path.join(address, new_filename)

    asset_id = f"{data_dict['stac_id']}_{item_media_type.lower()}"

    with rasterio.open(path) as src:
        asset = pystac.Asset(
            href = path, 
            media_type = media_types[item_media_type]["mime"], 
            title = asset_id,
            roles = ["data"],
            extra_fields = {
                "gsd": float(src.res[0]),
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
        if item_media_type != "NetCDF":
            if src.crs:
                item_epsg = src.crs.to_epsg(confidence_threshold=50)
            else:
                if data_dict["coord_sys"] == "ETRS-TM35FIN" or data_dict["coord_sys"] == "WGS84/ETRS-TM35FIN":
                    item_epsg = 3067
                else:
                    kkj_codes = {
                            "kkj": 4123,
                            "kkj0": 3386,
                            "kkj1": 2391,
                            "kkj2": 2392,
                            "kkj3": 2393,
                            "kkj4": 2394,
                            "kkj5": 3387
                        }
                    if "compress95" in path:
                        # Take the KKJ Zone from the path
                        path_kkj = path.split("/")[-7]
                        if path_kkj in kkj_codes:
                            item_epsg = kkj_codes[path_kkj]
                    elif "thematic_rasters" in data_dict["stac_id"]:
                        item_epsg = kkj_codes["kkj3"]

        else: # NetCDF datasets are in 3067
            item_epsg = 3067

    item = create_stac_item(
        source = path,
        id = item_id,
        assets = {
            asset_id : asset
        },
        asset_media_type = media_types[item_media_type]["mime"], 
        with_proj = True
    )
    # If add_puhti argument given, add puhti asset
    if args.add_puhti:
        puhti_asset = asset.clone()
        puhti_asset.href = re.sub(online_data_prefix, puhti_data_prefix, puhti_asset.href)
        puhti_asset.title = re.sub("paituli", "puhti", puhti_asset.title)
        item.add_asset(key=puhti_asset.title, asset=puhti_asset)

    item.extra_fields["gsd"] = item.assets[asset_id].extra_fields["gsd"]
    item.common_metadata.start_datetime = item_timestamps["item_start_time"]
    item.common_metadata.end_datetime = item_timestamps["item_end_time"]
    if item.properties["proj:epsg"] == None: item.properties["proj:epsg"] = item_epsg
    if item.properties["proj:epsg"] == 9391 or item.properties["proj:epsg"] == "EPSG:9391": item.properties["proj:epsg"] = 3067

    return item

def get_datasets(collections: list) -> dict:

    """
        Retrieves all the datasets associated with the given STAC Collection IDs.
        Returns a dictionary of the datasets with the associated STAC Collection ID as the key.
    """

    conn = psycopg2.connect(f"host={args.db_host} port={paituli_port} user=paituli-ro password={paituli_pwd} dbname=paituli")

    with conn.cursor() as curs:
            
        data = (collections,)
        query = "select data_id, stac_id, org_eng, name_eng, scale, year, format_eng, coord_sys, license_url, meta from dataset where access=1 and stac_id=ANY(%s)"
        curs.execute(query, data)
        datasets = {}
        for result in curs:
            new_dict = dict(zip(["data_id", "stac_id", "org_eng", "name_eng", "scale", "year", "format_eng", "coord_sys", "license_url", "metadata"], result))
            if new_dict["stac_id"] not in datasets.keys(): 
                datasets[new_dict["stac_id"]] = [{key: value for key, value in new_dict.items()}]
            else:
                datasets[new_dict["stac_id"]].append({key: value for key, value in new_dict.items()})

    for collection in collections:
        if collection not in datasets:
            print(f"! Collection \"{collection}\" not found, make sure the ID is correct.")
    
    return datasets

def update_catalog_collection(app_host: str, csc_catalog_client: pystac_client.Client, datasets: dict) -> None:

    global added_items_flag

    conn = psycopg2.connect(
        host=args.db_host, 
        port=paituli_port, 
        user="paituli-ro", 
        password=paituli_pwd, 
        dbname="paituli",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    session = requests.Session()
    session.auth = ("admin", geoserver_pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering

    if args.local:
        local_files = get_new_local_files()

    for stac_id in datasets:

        print(f"Checking {stac_id}:")

        csc_collection = csc_catalog_client.get_collection(stac_id)
        collection_item_ids = set(item.id for item in csc_collection.get_items())

        # Check if the Collection contains NetCDF files and create a list for storing the added IDs
        netcdf_present = False
        for data_dict in datasets[stac_id]:
            if data_dict["format_eng"] == "NetCDF":
                netcdf_present = True
                
        for data_dict in datasets[stac_id]:
            data_id = data_dict["data_id"]

            with conn.cursor() as curs:

                data = (data_id,)
                query = "select gid, data_id, label, path, geom , ST_AsGeoJSON(geom) from index_wgs84 where index_wgs84.data_id=(%s)"
                curs.execute(query, data)
                items = []
                for result in curs:
                    items.append(dict(zip(["gid", "data_id", "label", "path", "geom", "geojson"], result)))
            
            item_media_type = data_dict["format_eng"].split(",")[0]
            
            # If local flag given, get only the files that have been modified/downloaded recently
            if args.local:
                items = [x for x in items if x["path"].split(".")[0] in local_files]

            for item in items:
                
                item_path = item["path"]
                
                if len(item["label"].split("_")) > 1 or len(item["label"].split("(")) > 1 or len(item["label"].split(" ")) > 1:
                    label = None
                else:
                    label = item["label"].lower()

                #If path does not include a file, the filelinks are taken via BeautifulSoup
                if not item_path.endswith(media_types[item_media_type]['ext']) and not item_path.endswith(".*") and not item_path.endswith("*"):
                    # Check folder contents with BeautifulSoup
                    page_url = online_data_prefix+item_path
                    page = requests.get(page_url)
                    data = page.text
                    soup = BeautifulSoup(data, features="html.parser")
                    if not item_path.endswith("/"):
                        item_path = item_path + "/"

                    links = [link for link in soup.find_all("a")]

                    recursive_links = [] # Empty the recursive links if multiple Collections updated
                    recursive_links = recursive_filecheck(page_url, links, recursive_links)
                    
                    item_links = [link.get("href") for link in recursive_links if link.get("href").endswith(media_types[item_media_type]['ext'])]
                    if len(item_links) > 0:
                        for link in item_links:
                            data_path = online_data_prefix + item_path + link
                            item_timestamps = generate_timestamps(data_path, data_dict, label)
                            stac_item_id = generate_item_id(data_path, data_dict, item_timestamps["item_date"], label)
                            if not netcdf_present and stac_item_id in collection_item_ids:
                                continue
                            if netcdf_present and stac_item_id in [item.id for item in csc_collection.get_items()]:
                                item_to_add_asset = csc_collection.get_item(stac_item_id)
                                item_asset_extensions = [asset.split("_")[-1] for asset in item_to_add_asset.assets]
                                if item_media_type.lower() in item_asset_extensions: #If asset already in item, skip
                                    continue
                                else:
                                    asset_id = f"{data_dict['stac_id']}_{item_media_type.lower()}"
                                    with rasterio.open(data_path) as src:
                                        asset = pystac.Asset(
                                            href = data_path, 
                                            media_type = media_types[item_media_type]["mime"], 
                                            title = asset_id,
                                            roles = ["data"],
                                            extra_fields = {
                                                "gsd": float(src.res[0]),
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
                                    item_to_add_asset.add_asset(key=asset_id, asset=asset)
                                    # If add_puhti argument given, add puhti assets
                                    if args.add_puhti:
                                        puhti_asset = asset.clone()
                                        puhti_asset.href = re.sub(online_data_prefix, puhti_data_prefix, puhti_asset.href)
                                        puhti_asset.title = re.sub("paituli", "puhti", puhti_asset.title)
                                        item_to_add_asset.add_asset(key=puhti_asset.title, asset=puhti_asset)

                                    item_dict = item_to_add_asset.to_dict()
                                    converted_item = convert_json_to_geoserver(item_dict)
                                    request_point = f"collections/{csc_collection.id}/products/{item_to_add_asset.id}"
                                    r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                                    r.raise_for_status()
                            else:
                                stac_item = create_item(data_path, data_dict, item_media_type, label)
                                print(f" + Added {stac_item_id}")

                                # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
                                if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                                    geojson = json.loads(item["geojson"])
                                    stac_item.geometry = geojson
                                    stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)
                                    
                                csc_collection.add_item(stac_item)
                                item_dict = stac_item.to_dict()
                                converted_item = convert_json_to_geoserver(item_dict)
                                request_point = f"collections/{csc_collection.id}/products"
                                r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                                r.raise_for_status()
                else:
                    # Check if file path ends in a file or is the path marked with "*".
                    if item_path.endswith(media_types[item_media_type]['ext']):
                        data_path = online_data_prefix+item_path
                    elif item_path.endswith(".*"):
                        data_path = online_data_prefix+item_path.replace("*", media_types[item_media_type]["ext"])
                    elif item_path.endswith("*"):
                        data_path = online_data_prefix+item_path.replace("*", f".{media_types[item_media_type]['ext']}")
                    item_timestamps = generate_timestamps(data_path, data_dict, label)
                    stac_item_id = generate_item_id(data_path, data_dict, item_timestamps["item_date"], label)
                    if not netcdf_present and stac_item_id in collection_item_ids:
                        continue
                    elif netcdf_present and stac_item_id in [item.id for item in csc_collection.get_items()]:
                        item_to_add_asset = csc_collection.get_item(stac_item_id)
                        item_asset_extensions = [asset.split("_")[-1] for asset in item_to_add_asset.assets]
                        if item_media_type.lower() in item_asset_extensions: #If asset already in item, skip
                            continue
                        else:
                            asset_id = f"{data_dict['stac_id']}_{item_media_type.lower()}"
                            with rasterio.open(data_path) as src:
                                asset = pystac.Asset(
                                    href = data_path, 
                                    media_type = media_types[item_media_type]["mime"], 
                                    title = asset_id,
                                    roles = ["data"],
                                    extra_fields = {
                                        "gsd": float(src.res[0]),
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
                            item_to_add_asset.add_asset(key=asset_id, asset=asset)

                            # If add_puhti argument given, add puhti assets
                            if args.add_puhti:
                                puhti_asset = asset.clone()
                                puhti_asset.href = re.sub(online_data_prefix, puhti_data_prefix, puhti_asset.href)
                                puhti_asset.title = re.sub("paituli", "puhti", puhti_asset.title)
                                item_to_add_asset.add_asset(key=puhti_asset.title, asset=puhti_asset)

                            item_dict = item_to_add_asset.to_dict()
                            converted_item = convert_json_to_geoserver(item_dict)
                            request_point = f"collections/{csc_collection.id}/products/{item_to_add_asset.id}"
                            r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                            r.raise_for_status()
                    else:
                        stac_item = create_item(data_path, data_dict, item_media_type, label)
                        csc_collection.add_item(stac_item)
                        print(f" + Added {stac_item_id}")

                        # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
                        if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                            geojson = json.loads(item["geojson"])
                            stac_item.geometry = geojson
                            stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)
                            
                        item_dict = stac_item.to_dict()
                        converted_item = convert_json_to_geoserver(item_dict)
                        request_point = f"collections/{csc_collection.id}/products"
                        r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                        r.raise_for_status()

        if added_items_flag or args.update_extents:
            csc_collection.update_extent_from_items()
            collection_dict = csc_collection.to_dict()
            converted_collection = convert_json_to_geoserver(collection_dict)
            request_point = f"collections/{csc_collection.id}/"

            r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
            r.raise_for_status()
            print(" + Updated collection extents.")
        else:
            print(f" - No new items for {csc_collection.id}")
        
if __name__ == "__main__":

    start = time.time()
    config_filename = 'passwords.txt'
    online_data_prefix = "https://www.nic.funet.fi/index/geodata/"
    puhti_data_prefix = "/appl/data/geo/"
    media_types = {
        "TIFF": {
            "mime": "image/tiff; application=geotiff",
            "ext": "tif"
        },
        "PNG": {
            "mime": "image/png",
            "ext": "png"
        },
        "JPEG2000": {
            "mime": "image/jp2",
            "ext": "jp2"
        },
        "NetCDF": {
            "mime": "application/x-netcdf",
            "ext": "nc"
        }
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action='store_true')
    parser.add_argument("--add_puhti", action='store_true')
    parser.add_argument("--update_extents", action="store_true")
    parser.add_argument("--port", type=str, help="Port for the paituli database")
    parser.add_argument("--collections", nargs="+", help="Specific collections to be made", required=True)
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    parser.add_argument("--db_host", type=str, help="Hostname of the Paituli DB", required=True)

    args = parser.parse_args()

    if args.port:
        paituli_port = args.port
    else:
        try:
            config_file = pd.read_csv(config_filename, header=None)
            paituli_port = config_file.at[5,0]
        except FileNotFoundError:
            paituli_port = input("Please provide port: ")

    try:
        config_file = pd.read_csv(config_filename, header=None)
        paituli_pwd = config_file.at[6,0]
        geoserver_pwd = config_file.at[0,0]
    except FileNotFoundError:
        print("Password not given as an argument and no password file found")
        paituli_pwd = getpass.getpass(prompt="Paituli password: ")
        geoserver_pwd = getpass.getpass(prompt="GeoServer password: ")

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog_client = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    
    datasets = get_datasets(args.collections)        

    # Skip updating and sending collection if no items were added
    # Using a global flag for this might be janky, because if the functions are imported they are still calling the `global added_items_flag`
    # This is needed to make the update script to run faster if there's nothing to update
    added_items_flag = False

    # Run the script if there's datasets
    if datasets:
        print(f"Updating STAC Catalog at {args.host}")
        update_catalog_collection(app_host, csc_catalog_client, datasets)

    end = time.time()
    print(f"Script took {end-start:.2f} seconds")