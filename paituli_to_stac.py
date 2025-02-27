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
from rio_stac.stac import create_stac_item
from shapely.geometry import GeometryCollection, shape
from bs4 import BeautifulSoup

from utils.paituli import recursive_filecheck, generate_item_id, generate_timestamps

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

def create_collection(catalog, data_dict) -> pystac.Collection:

    if "-" in data_dict["year"]:
        years = data_dict["year"].split("-")
        if years[1] == ">": # If the dataset is updated regularly, set the year as the first instance and update from items afterwards
            temporal_extent = pystac.TemporalExtent([(
                datetime.datetime.strptime(f"{years[0]}-01-01", "%Y-%m-%d"), 
                datetime.datetime.strptime(f"{years[0]}-12-31", "%Y-%m-%d")
            )])
        else:
            # Remove the years in parenthesis
            if len(years[1]) > 4:
                temporal_extent = pystac.TemporalExtent([(
                    datetime.datetime.strptime(f"{years[0]}-01-01", "%Y-%m-%d"), 
                    datetime.datetime.strptime(f"{years[1].split('(')[0].rstrip()}-12-31", "%Y-%m-%d")
                )])
            else:
                temporal_extent = pystac.TemporalExtent([(
                        datetime.datetime.strptime(f"{years[0]}-01-01", "%Y-%m-%d"), 
                        datetime.datetime.strptime(f"{years[1]}-12-31", "%Y-%m-%d")
                )])
    else: 
        temporal_extent = pystac.TemporalExtent([(
                datetime.datetime.strptime(f"{data_dict['year']}-01-01", "%Y-%m-%d"),
                datetime.datetime.strptime(f"{data_dict['year']}-12-31", "%Y-%m-%d")
        )])
    
    collection_scale = data_dict['scale'].rstrip()

    # Include newest in the Title and Description if the collection is has the newest keyword
    if "newest" in data_dict["stac_id"]:
        collection_description = f"{data_dict['name_eng']} newest. Provided by {data_dict['org_eng']}. Scale: {collection_scale}. Coordinate systems: {data_dict['coord_sys']}."
        collection_title = f"{data_dict['name_eng']} newest, {collection_scale} (Paituli)"
    else:
        if data_dict["stac_id"] == "nls_topographic_map_100k_at_paituli" and data_dict["name_eng"] == "Sverige-Finland map": #Dirty fix for the newest dataset having a different name
            collection_description = f"Basic or topographic map. Provided by {data_dict['org_eng']}. Scale: {collection_scale}. Coordinate systems: {data_dict['coord_sys']}."
            collection_title = f"Basic or topographic map, {collection_scale} (Paituli)"
        else:
            collection_description = f"{data_dict['name_eng']}. Provided by {data_dict['org_eng']}. Scale: {collection_scale}. Coordinate systems: {data_dict['coord_sys']}."
            collection_title = f"{data_dict['name_eng']}, {collection_scale} (Paituli)"

    collection = pystac.Collection(
        id = data_dict["stac_id"],
        license = "CC-BY-4.0",
        title = collection_title,
        description = collection_description,
        extent = pystac.Extent(
            spatial = pystac.SpatialExtent([[0,0,0,0]]), #Placeholder extent, updated from items later   
            temporal = temporal_extent
        ),
        providers = [
            pystac.Provider(
                name = data_dict["org_eng"],
                roles = ["licensor", "producer"]
            ),
            pystac.Provider(
                name = "CSC Finland",
                url = "https://www.csc.fi/",
                roles = ["host"]
            ),
        ],
        assets={
            "meta": pystac.Asset(
                href = "https://urn.fi/"+data_dict["metadata"],
                title = "Metadata",
                roles = ["metadata"]
            )
        },
        extra_fields={
            "scale": [data_dict["scale"]],
            "coord_sys": [data_dict["coord_sys"]]
        }
    )

    #Other datasets have the same license, Landsat has a different one
    if data_dict['name_eng'] == "Landsat":
        collection.license = "PDDL-1.0"

    collection.add_link(
        pystac.Link(
            rel = "license",
            target = data_dict["license_url"],
            title = "License"
    ))
    print(f"Collection made: {collection.id}")
    catalog.add_child(collection)
    return collection

def create_item(path, collection, data_dict, item_media_type, label) -> pystac.Item:
    """
    A bunch of different scenarios for timestamps:
     - If dataset has only the latest data, get the Last-Modified header from the file
     - If the database year has a span of multiple years, set the timespan accordingly
     - If the dataset contains monthly data, get the year and month from filename
     - Else, try to get the year from the full path of the file
     - Lastly, if the path does not contain the year, take the last year from the database year-field
    """
    
    item_timestamps = generate_timestamps(path, data_dict, label)

    item_id = generate_item_id(path, data_dict, item_timestamps["item_date"], label)
    
    collection_item_ids = {item.id for item in collection.get_items()}

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

    if item_id not in collection_item_ids:
        item = create_stac_item(
            path,
            id = item_id,
            assets = {
                asset_id : asset
            },
            asset_media_type = media_types[item_media_type]["mime"], 
            with_proj = True
        )
        item.extra_fields["gsd"] = item.assets[asset_id].extra_fields["gsd"]
        item.common_metadata.start_datetime = item_timestamps["item_start_time"]
        item.common_metadata.end_datetime = item_timestamps["item_end_time"]
        if item.properties["proj:epsg"] == None: item.properties["proj:epsg"] = item_epsg
        if item.properties["proj:epsg"] == 9391 or item.properties["proj:epsg"] == "EPSG:9391": item.properties["proj:epsg"] = 3067
        collection.add_item(item)
        print(f"* Item made: {item.id}")
    else:
        item = collection.get_item(item_id)
        item.add_asset(
            key = asset_id,
            asset = asset
        )
        print(f"** Asset made for {item.id}")

    return item

if __name__ == "__main__":

    dir_path = os.path.dirname(os.path.realpath(__file__))

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=str, help="Port for the paituli database", required=True)
    parser.add_argument("--pwd", type=str, help="Password for paituli database")
    parser.add_argument("--collections", nargs="+", help="Specific collections to be made")
    parser.add_argument("--db_host", type=str, help="Hostname of the Paituli DB", required=True)

    args = parser.parse_args()
    paituli_port = args.port
    selected_collections = None

    if args.pwd:
        paituli_pwd = args.pwd
    else:
        paituli_pwd = getpass.getpass("Paituli password: ")
    
    if args.collections:
        selected_collections = args.collections

    try:
        catalog = pystac.Catalog.from_file(f"{dir_path}/Paituli/catalog.json")
    except:
        catalog = pystac.Catalog("Paituli", "Paituli Catalog", catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)

    conn = psycopg2.connect(f"host={args.db_host} port={paituli_port} user=paituli-ro password={paituli_pwd} dbname=paituli")

    with conn.cursor() as curs:

        query = "select data_id, stac_id, org_eng, name_eng, scale, year, format_eng, coord_sys, license_url, meta from dataset where access=1 and (format_eng like 'JP%' or format_eng like 'PNG%' or format_eng like 'Net%' or format_eng like 'TIFF%') order by 1, 2, 3, 5, 6;"
        curs.execute(query)
        datasets = {}
        for result in curs:
            new_dict = dict(zip(["data_id", "stac_id", "org_eng", "name_eng", "scale", "year", "format_eng", "coord_sys", "license_url", "metadata"], result))
            if new_dict["stac_id"]:
                datasets[new_dict["data_id"]] = {key: value for key, value in new_dict.items() if key != 'data_id'}
    
    # Group the datasets to NetCDF and The Others
    netcdf_datasets = {x : datasets[x] for x in datasets if datasets[x]['format_eng'] == "NetCDF"}
    regular_datasets = {y : datasets[y] for y in datasets if y not in netcdf_datasets.keys()}

    for dataset in regular_datasets:

        data_dict = datasets[dataset]
        stac_id = data_dict["stac_id"]

        if selected_collections: 
        # Run with selected datasets
            if stac_id not in selected_collections:
                continue

        catalog_collection_ids = {collection.id for collection in catalog.get_collections()}

        if stac_id in catalog_collection_ids:
            collection = catalog.get_child(stac_id)
            # If multiple scales or coordinate systems, add them to the description
            coord_pattern = r"Coordinate systems:\s*(.*?)\."
            scale_pattern = r"Scale:\s*(.*?)\."

            if data_dict["scale"] not in collection.extra_fields["scale"]:
                match = re.search(scale_pattern, collection.description)
                collection.description = collection.description.replace(match.group(1), f"{match.group(1)}, {data_dict['scale']}")
                collection.extra_fields["scale"].append(data_dict["scale"])
                # If multiple scales, remove scale from Title
                collection.title = f"{data_dict['name_eng']} (Paituli)"

            if data_dict["coord_sys"] not in collection.extra_fields["coord_sys"]:
                match = re.search(coord_pattern, collection.description)
                collection.description = collection.description.replace(match.group(1), f"{match.group(1)}, {data_dict['coord_sys']}")
                collection.extra_fields["coord_sys"].append(data_dict["coord_sys"])
        else:
            collection = create_collection(catalog, data_dict)

        with conn.cursor() as curs:

            data = (dataset,)
            query = "select gid, data_id, label, path, geom , ST_AsGeoJSON(geom) from index_wgs84 where index_wgs84.data_id=(%s)"
            curs.execute(query, data)
            items = {}
            for i,result in enumerate(curs):
                items[i] = dict(zip(["gid", "data_id", "label", "path", "geom", "geojson"], result))

        item_media_type = data_dict["format_eng"].split(",")[0]

        for i in items:

            # Check if file path ends in a file or is the path marked with "*". Lastly if none match, the filelinks are taken via BeautifulSoup
            if items[i]["path"].endswith(media_types[item_media_type]['ext']):
                data_path = online_data_prefix+items[i]["path"]
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            elif items[i]["path"].endswith(".*"):
                data_path = online_data_prefix+items[i]["path"].replace("*", media_types[item_media_type]["ext"])
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            elif items[i]["path"].endswith("*"):
                data_path = online_data_prefix+items[i]["path"].replace("*", f".{media_types[item_media_type]['ext']}")
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            else:
                # Check folder contents with BeautifulSoup
                page_url = online_data_prefix+items[i]["path"]
                page = requests.get(page_url)
                data = page.text
                soup = BeautifulSoup(data, features="html.parser")
                if not items[i]["path"].endswith("/"):
                    item_path = items[i]["path"] + "/"
                else:
                    item_path = items[i]["path"]

                links = [link for link in soup.find_all("a")]
                
                recursive_links = recursive_filecheck(page_url, links)
                
                item_links = [link.get("href") for link in recursive_links if link.get("href").endswith(media_types[item_media_type]['ext'])]
                if len(item_links) > 0:
                    for link in item_links:
                        data_path = online_data_prefix + item_path + link
                        stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
                        # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
                        if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                            geojson = json.loads(items[i]["geojson"])
                            stac_item.geometry = geojson
                            stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)

            # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
            if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                geojson = json.loads(items[i]["geojson"])
                stac_item.geometry = geojson
                stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)

        bounds = [GeometryCollection([shape(s.geometry) for s in collection.get_all_items()]).bounds]
        start_times = [st.common_metadata.start_datetime for st in collection.get_all_items()]
        end_times = [et.common_metadata.end_datetime for et in collection.get_all_items()]
        temporal = [[min(start_times), max(end_times)]]
        collection.extent.spatial = pystac.SpatialExtent(bounds)
        collection.extent.temporal = pystac.TemporalExtent(temporal)

    for dataset in netcdf_datasets:

        data_dict = datasets[dataset]
        stac_id = data_dict["stac_id"]

        if selected_collections: 
        # Run with selected datasets
            if stac_id not in selected_collections:
                continue

        catalog_collection_ids = [collection.id for collection in catalog.get_collections()]

        if stac_id in catalog_collection_ids:
            collection = catalog.get_child(stac_id)
            # If multiple coordinate systems, add them to the description
            coord_pattern = r"Coordinate systems:\s*(.*?)\."
            scale_pattern = r"Scale:\s*(.*?)\."

            if data_dict["scale"] not in collection.extra_fields["scale"]:
                match = re.search(scale_pattern, collection.description)
                collection.description = collection.description.replace(match.group(1), f"{match.group(1)}, {data_dict['scale']}")
                collection.extra_fields["scale"].append(data_dict["scale"])
                # If multiple scales, remove scale from Title
                collection.title = f"{data_dict['name_eng']} (Paituli)"

            if data_dict["coord_sys"] not in collection.extra_fields["coord_sys"]:
                match = re.search(coord_pattern, collection.description)
                collection.description = collection.description.replace(match.group(1), f"{match.group(1)}, {data_dict['coord_sys']}")
                collection.extra_fields["coord_sys"].append(data_dict["coord_sys"])
        else:
            collection = create_collection(catalog, data_dict)

        with conn.cursor() as curs:

            data = (dataset,)
            query = "select data_id, label, path, geom , ST_AsGeoJSON(geom) from index_wgs84 where index_wgs84.data_id=(%s)"
            curs.execute(query, data)
            items = {}
            for i,result in enumerate(curs):
                items[i] = dict(zip(["data_id", "label", "path", "geom", "geojson"], result))

        item_media_type = data_dict["format_eng"].split(",")[0]

        for i in items:
            
            # Check if file path ends in a file or is the path marked with "*". Lastly if none match the filelinks are taken via Beautiful Soup
            if items[i]["path"].endswith(media_types[item_media_type]['ext']):
                data_path = online_data_prefix+items[i]["path"]
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            elif items[i]["path"].endswith(".*"):
                data_path = online_data_prefix+items[i]["path"].replace("*", media_types[item_media_type]["ext"])
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            elif items[i]["path"].endswith("*"):
                data_path = online_data_prefix+items[i]["path"].replace("*", f".{media_types[item_media_type]['ext']}")
                stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
            else:
                # Check folder contents with BeautifulSoup
                page_url = online_data_prefix+items[i]["path"]
                page = requests.get(page_url)
                data = page.text
                soup = BeautifulSoup(data, features="html.parser")
                if not items[i]["path"].endswith("/"):
                    item_path = items[i]["path"] + "/"
                else:
                    item_path = items[i]["path"]

                links = [link for link in soup.find_all("a")]
                
                recursive_links = recursive_filecheck(page_url, links)
                
                item_links = [link.get("href") for link in recursive_links if link.get("href").endswith(media_types[item_media_type]['ext'])]
                if len(item_links) > 0:
                    for link in item_links:
                        data_path = online_data_prefix + item_path +link
                        stac_item = create_item(data_path, collection, data_dict, item_media_type, items[i]["label"])
                        # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
                        if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                            geojson = json.loads(items[i]["geojson"])
                            stac_item.geometry = geojson
                            stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)

            # If rio-stac does not get the geometry from the file, insert it from the database using geom transformed to a GeoJSON
            if stac_item.bbox == [-180.0,-90.0,180.0,90.0]:
                geojson = json.loads(items[i]["geojson"])
                stac_item.geometry = geojson
                stac_item.bbox = pystac.utils.geometry_to_bbox(geojson)
        
        bounds = [GeometryCollection([shape(s.geometry) for s in collection.get_all_items()]).bounds]
        start_times = [st.common_metadata.start_datetime for st in collection.get_all_items()]
        end_times = [et.common_metadata.end_datetime for et in collection.get_all_items()]
        temporal = [[min(start_times), max(end_times)]]
        collection.extent.spatial = pystac.SpatialExtent(bounds)
        collection.extent.temporal = pystac.TemporalExtent(temporal)
    
    catalog.normalize_and_save("Paituli", skip_unresolved=True)