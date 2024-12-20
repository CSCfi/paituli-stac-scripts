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

from utils.paituli import recursive_filecheck

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

    year_pattern = r'(19\d{2}(?![\d_])|20\d{2}(?![\d_])|21\d{2}(?![\d_]))(-(?=\d{4}))?'
    number_pattern = r'^\d+$'
    label_pattern = r'(?<=\()18\d{2}(?=\))|(?<=\()19\d{2}(?=\))|(?<=\()20\d{2}(?=\))'
    
    # National Land Survey of Finland old maps has the year in the label
    if label:
        check_label = re.search(label_pattern, label)
    else:
        check_label = None

    if "nls_digital_elevation_model_2m" in data_dict['stac_id']: # This gets the time for 2m DEM, there's no better alternative atm
        resp = requests.head(path)
        modified = resp.headers["Last-Modified"]
        item_starttime = datetime.datetime.strptime(modified, "%a, %d %b %Y %H:%M:%S %Z")
        item_endtime = datetime.datetime.strptime(modified, "%a, %d %b %Y %H:%M:%S %Z")
        item_date = item_starttime.year
    elif "nls_topographic_map_42k" in data_dict['stac_id'] and "x" in data_dict['year']: #Some datasets have years as 192x
        item_starttime = datetime.datetime.strptime(f"1920-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"1930-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{item_starttime.year}_{item_endtime.year}"
    elif check_label: # If year in label, use that
        label_year = check_label.group(0)
        item_starttime = datetime.datetime.strptime(f"{label_year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{label_year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = label_year
    elif label and "(-)" in label and data_dict["org_eng"] == "National Land Survey of Finland": # If label year is blank, the year is unknown
        split_year = data_dict["year"].split("-")
        item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{split_year[0]}_{split_year[1]}"
    elif "-" not in data_dict["year"]: # If only one year in dataset, use that
        item_starttime = datetime.datetime.strptime(f"{data_dict['year']}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{data_dict['year']}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = data_dict["year"]
    elif "snow_load_on_trees" in data_dict["stac_id"]: # filename is type rcp**{startyear}{endyear}
        split_file = path.split("/")[-1].split(".")[0]
        start_year = split_file[-8:-4]
        end_year = split_file[-4:]
        item_starttime = datetime.datetime.strptime(f"{start_year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{end_year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{start_year}_{end_year}"
    elif "predictions" in data_dict["stac_id"]:
        # There are some datasets with parantheses in the years column
        if len(data_dict['year'].split('(')) > 1: # Monthly mean precipitation and temperature predictions
            split_fix = data_dict['year'].split('(')[0].strip()
            split_years = split_fix.split('-')
        else:
            split_years = data_dict['year'].split('-')
        item_starttime = datetime.datetime.strptime(f"{split_years[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{split_years[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{split_years[0]}_{split_years[1]}"
    elif "monthly_avg" in data_dict["stac_id"] or "monthly_precipitation_1km" in data_dict["stac_id"]:
        split_file = path.split("/")[-1].split(".")[0].split("_")
        for split in split_file:
            match = re.search(number_pattern, split)
            if match:
                numbers = match.group(0)
                item_starttime = datetime.datetime.strptime(f"{numbers}-01", "%Y%m-%d")
                # Calculate the last day of the corresponding month
                first_day_of_next_month = item_starttime + datetime.timedelta(days=32)
                lastday = first_day_of_next_month - datetime.timedelta(days=first_day_of_next_month.day)
                item_endtime = datetime.datetime.strptime(f"{numbers}-{lastday.day}", "%Y%m-%d")
                item_date = numbers
    else:
        match = re.search(year_pattern, path)
        # Some HY SPECTRE data items have the publication date in the path and not the data date
        if match and not data_dict['stac_id'].startswith("hy_spectre"):
            if match.group(1) not in data_dict['stac_id']:
                year = match.group(1)
                item_starttime = datetime.datetime.strptime(f"{year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_endtime = datetime.datetime.strptime(f"{year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_date = year
            else:
                split_year = data_dict["year"].split("-")
                item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_date = f"{split_year[0]}_{split_year[1]}"
        else:
            split_year = data_dict["year"].split("-")
            item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
            item_date = f"{split_year[0]}_{split_year[1]}"

    # This specific dataset has an incomplete filepath in the dataset
    if "ei_kkayria" in path and item_date == "2006":
        split_path = path.split(".")
        path = ".".join(split_path[:-1]) + "_RK2_2.tif"

    # Check if label is usable in item ID generation
    if len(label.split("_")) > 1 or len(label.split("(")) > 1 or len(label.split(" ")) > 1:
        label_info = None
    else:
        label_info = label.lower()

    if label_info:
        if label_info == item_date: # If label and date is the same, just use one
            item_id = f"{data_dict['stac_id']}_{label_info}"
        else:
            item_id = f"{data_dict['stac_id']}_{label_info}_{item_date}"
    else:
        item_name = path.split("/")[-1].split(".")[0].split("_")[0].lower()
        item_name = item_name.replace("-", "_")
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"

    # For orthoimages, construct a different ID, which includes the dataset, elevation model, and the version number
    if "orthoimage" in data_dict['stac_id']:
        if label:
            item_id = f"{data_dict['stac_id']}_{label_info}_{item_date}_{path.split('/')[-6]}_{path.split('/')[-3]}_{path.split('/')[-2]}"
        else:
            leaf = path.split("/")[-1].split(".")[0]
            item_id = f"{data_dict['stac_id']}_{leaf}_{item_date}_{path.split('/')[-6]}_{path.split('/')[-3]}_{path.split('/')[-2]}"
    elif "general_map" in data_dict['stac_id']:
        split = path.split(".")[-2].split("_")
        item_name = "".join(split)
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"
    elif "predictions" in data_dict["stac_id"] and "monthly" in data_dict["stac_id"]:
        split = path.split("/")[-1].split(".")[-2].split("_")
        item_name = "_".join(split[0:3])
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"
    elif data_dict['stac_id'].startswith("hy"):
        item_id = f"{data_dict['stac_id']}_{item_date}"
    elif "nls_topographic_map_42k" in data_dict['stac_id'] and "x" in data_dict['year']:
        item_name = path.split("/")[-1].split(".")[0].split("_")[0].lower()
        item_name = item_name.replace("-", "_")
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"

    # Some IDs have periods in them, remove them
    if "." in item_id:
        item_id = item_id.replace(".", "")
    # Something fishy going with label having / in it even though it doesn't show up in database
    elif "/" in item_id:
        split = item_id.split("_")
        label_split = split[-2]
        fix = label_split.split("/")[-1]
        item_id = item_id.replace(label_split, fix)
    
    collection_item_ids = [item.id for item in collection.get_items()]

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
        item.common_metadata.start_datetime = item_starttime
        item.common_metadata.end_datetime = item_endtime
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

        catalog_collection_ids = [collection.id for collection in catalog.get_collections()]

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