import argparse
import urllib
import getpass
import requests
import pystac_client
import pandas as pd
import rasterio
import time
import json
from urllib.parse import urljoin
from pystac import Collection, Item

def retry_errors(list_of_items, list_of_errors):
    """
    Function to retry retrieving the items that were timed out during the process.
    
    list_of_items - List containing the STAC items from the source. The errored items will be appended to this list when successfully retrieved
    list_of_errors - List of links to the items that timed out during the retrieving process. Function will run until this list is empty
    """

    print(" * Trying to add items that timedout")
    while len(list_of_errors) > 0:
        for item in list_of_errors:
            try:
                list_of_items.append(Item.from_file(item))
                print(f" * Listed {item}")
                list_of_errors.remove(item)
            except Exception as e:
                print(f" ! {e} on {item}")

def json_convert(content):

    """ 
    A function to map the STAC dictionaries into the GeoServer database layout.
    There are different json layouts for Collections and Items. The function checks if the dictionary is of type "Collection",
    or of type "Feature" (=Item).

    content - STAC dictionary from where the modified JSON will be made
    """
    
    if content["type"] == "Collection":

        new_json = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ]

                    ]
                ]
            },
            "properties": {
                "name": content["id"],
                "title": content["title"],
                "eo:identifier": content["id"],
                "description": content["description"],
                "timeStart": content["extent"]["temporal"]["interval"][0][0],
                "timeEnd": content["extent"]["temporal"]["interval"][0][1],
                "primary": True,
                "license": content["license"],
                "providers": content["providers"], # Providers added
                "licenseLink": None,
                "summaries": content["summaries"],
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = { #New License URL link
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                }
            elif link["rel"] == "derived_from":
                derived_href = link["href"]
                new_json["properties"]["derivedFrom"] = {
                    "href": derived_href,
                    "rel": "derived_from",
                    "type": "application/json"
                }

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["start_datetime"],
                "timeEnd": content["properties"]["end_datetime"],
                "eop:resolution": content["gsd"],
                # "opt:cloudCover": int(content["properties"]["eo:cloud_cover"]),
                "crs": content["proj:epsg"],
                "projTransform": content["proj:transform"],
                # "thumbnailURL": content["links"]["thumbnail"]["href"],
                "assets": content["assets"]
            }
        }

        if content["properties"]["start_datetime"] is None and content["properties"]["end_datetime"] is None and content["properties"]["datetime"] is not None:
            new_json["properties"]["timeStart"] = content["properties"]["datetime"]
            new_json["properties"]["timeEnd"] = content["properties"]["datetime"]

    return new_json

def update_catalog(app_host, csc_catalog_client):

    """
    The main updating function of the script. Checks the collection items in the FMI catalog and compares the to the ones in CSC catalog.

    app_host - The REST API path for updating the collections
    csc_catalog_client - The STAC API path for checking which items are already in the collections
    """
    
    session = requests.Session()
    session.auth = ("admin", pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering

    # Get all FMI collections from the app_host
    csc_collections = [col for col in csc_catalog_client.get_collections() if col.id.endswith("at_fmi")]

    for collection in csc_collections:

        derived_from = [link.target for link in collection.links if link.rel == "derived_from"]

        # Some collections have wrongly configured Temporal Extents
        try:
            fmi_collection = Collection.from_file(derived_from[0])
        except ValueError:
            with urllib.request.urlopen(derived_from[0]) as url:
                data = json.load(url)
                data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                fmi_collection = Collection.from_dict(data)

        fmi_collection.id = collection.id
        print(f"# Checking collection {collection.id}:")
        fmi_collection_links = fmi_collection.get_child_links()

        sub_collections = []
        for link in fmi_collection_links:

            # Some collections have wrongly configured Temporal Extents
            try:
                sub_collections.append(Collection.from_file(link.target))
            except ValueError:
                with urllib.request.urlopen(link.target) as url:
                    data = json.load(url)
                    data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                    sub_collections.append(Collection.from_dict(data))

        item_links = list(set([link.target for sub in sub_collections for link in sub.get_item_links()]))
        csc_item_ids = [x.id for x in collection.get_items()]

        items = []
        errors = []
        for item in item_links:
            try:
                items.append(Item.from_file(item))
            except Exception as e:
                print(f" ! {e} on {item}")
                errors.append(item)

        # If there were connection errors during the item making process, the item generation for errors is retried
        if len(errors) > 0:
            retry_errors(items, errors)
            print(" * All errors fixed")
        
        print(f" * Number of items in CSC STAC and FMI: {len(csc_item_ids)}/{len(items)}")

        for item in items:
            if item.id not in csc_item_ids:
                fmi_collection.add_item(item)

                with rasterio.open(next(iter(item.assets.values())).href) as src:
                    item.extra_fields["gsd"] = src.res[0]
                    # 9391 EPSG code is false, replace by the standard 3067
                    if src.crs.to_epsg() == 9391:
                        item.extra_fields["proj:epsg"] = 3067
                    else:
                        item.extra_fields["proj:epsg"] = src.crs.to_epsg()
                    item.extra_fields["proj:transform"] = [
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

                for asset in item.assets:
                    if item.assets[asset].roles is not list:
                        item.assets[asset].roles = [item.assets[asset].roles]
    
                del item.extra_fields["license"]
                item.remove_links("license")

                item_dict = item.to_dict()
                converted_item = json_convert(item_dict)
                request_point = f"collections/{collection.id}/products"
                r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
                r.raise_for_status()

                print(f" + Added item {item.id}")

        print(f" * All items present")

        # Update the extents from the FMI collection
        collection.extent = fmi_collection.extent
        collection_dict = collection.to_dict()
        converted_collection = json_convert(collection_dict)
        request_point = f"collections/{collection.id}/"

        r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
        r.raise_for_status()
        print(f" * Updated collection")
    
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