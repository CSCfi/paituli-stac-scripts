import json
import getpass
import requests
import pystac_client
import argparse
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
from pathlib import Path

from utils.json_convert import convert_json_to_geoserver

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    parser.add_argument("--pwd", type=str, help="Password for GeoServer")
    parser.add_argument("--catalog", type=str, help="Name of the local Catalog")
    parser.add_argument("--collections", nargs="+", help="Specific collections to upload to GeoServer", required=True)

    args = parser.parse_args()

    if args.pwd:
        geoserver_pwd = args.pwd
    else:
        print("Password not given as an argument and no password file found")
        geoserver_pwd = getpass.getpass(prompt="GeoServer password: ")

    app_host = f"{args.host}/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})

    collections = args.collections

    # Use given Catalog folder if provided, else get the name from user
    stac_catalog = args.catalog if args.catalog else input("Provide STAC Catalog folder name: ")

    for collection in collections:

        working_dir = Path(__file__).parent
        # Change Catalog directory name here if different
        catalog_dir = Path(working_dir / stac_catalog)
        
        # Check if Catalog directory exists
        if not catalog_dir.exists():
            print(f"Catalog folder named {stac_catalog} does not exist.")
            break
        collection_folder = Path(catalog_dir / collection)

        # Check if the collection with the given ID exists and convert the STAC collection json into json that GeoServer can handle
        try:
            converted = convert_json_to_geoserver(collection_folder / "collection.json")
        except FileNotFoundError:
            print(f"Collection {collection} not found.")
            continue

        #Additional code for changing collection data if the collection already exists
        collections = catalog.get_collections()
        col_ids = [col.id for col in collections]
        if collection in col_ids:
            r = requests.put(urljoin(app_host + "collections/", collection), json=converted, auth=HTTPBasicAuth("admin", geoserver_pwd))
            r.raise_for_status()
            print(f"Updated {collection}")
        else:
            r = requests.post(urljoin(app_host, "collections/"), json=converted, auth=HTTPBasicAuth("admin", geoserver_pwd))
            r.raise_for_status()
            print(f"Added new collection: {collection}")

        # Get the posted items from the specific collection
        posted = catalog.search(collections=[collection]).item_collection()
        posted_ids = [x.id for x in posted]
        print(f"Uploaded Items: {len(posted_ids)}")

        with open(collection_folder / "collection.json") as f:
            rootcollection = json.load(f)

        items = [x['href'] for x in rootcollection["links"] if x["rel"] == "item"]

        print("Uploading Items:")
        number_of_items = len(items)
        for i, item in enumerate(items):
            with open(collection_folder / item) as f:
                payload = json.load(f)
            # Convert the STAC item json into json that GeoServer can handle
            converted = convert_json_to_geoserver(collection_folder / item)
            request_point = f"collections/{rootcollection['id']}/products"
            if payload["id"] in posted_ids:
                request_point = f"collections/{rootcollection['id']}/products/{payload['id']}"
                r = requests.put(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", geoserver_pwd))
                r.raise_for_status()
            else:
                r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", geoserver_pwd))
                r.raise_for_status()
            if number_of_items >= 5: # Just to keep track that the script is still running
                if i == int(number_of_items / 5):
                    print("~20% of Items added")
                elif i == int(number_of_items / 5) * 2:
                    print("~40% of Items added")
                elif i == int(number_of_items / 5) * 3:
                    print("~60% of Items added")
                elif i == int(number_of_items / 5) * 4:
                    print("~80% of Items added")
        print("All Items uploaded")