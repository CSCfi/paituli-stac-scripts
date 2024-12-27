import boto3
import pystac
import rasterio
import re
import pandas as pd
import getpass
import argparse
import requests
import pystac_client
import time
from urllib.parse import urljoin
from datetime import datetime
from shapely.geometry import box, mapping
from pystac.extensions.eo import EOExtension
from pystac.extensions.projection import ProjectionExtension

from utils.json_convert import convert_json_to_geoserver
from utils.allas_sentinel import get_sentinel2_bands, init_client, get_buckets, transform_crs, get_crs, get_metadata_content, get_metadata_from_xml

def make_item(uri, metadatacontent, crs_metadata):
    """
        uri: The SAFE ID of the item (currently URL of the image, could be changes to SAFE later)
        metadatacontent: Metadata dict got from get_metadata_content()
        crs_metadata: CRS metadata dict containing CRS string and shapes for different resolutions from get_crs()
    """
    params = {}

    if re.match(r".+?\d{4}/S2(A|B)", uri):
        params['id'] = uri.split("/")[5].split('.')[0]
    else:
        params['id'] = uri.split('/')[4].split('.')[0]
    
    with rasterio.open(uri) as src:
        item_transform = src.transform
        # as lat,lon
        params['bbox'] = transform_crs(list([src.bounds]),crs_metadata['CRS'])
        params['geometry'] = mapping(box(*params['bbox']))
            
    mtddict = get_metadata_from_xml(metadatacontent)

    # Datetime from filename
    params['datetime'] = datetime.strptime(uri.split('_')[2][0:8], '%Y%m%d')

    params['properties'] = {}
    params['properties']['eo:cloud_cover'] = mtddict['cc_perc']
    #following are not part of eo extension
    params['properties']['data_cover'] = mtddict['data_cover']
    params['properties']['orbit'] = mtddict['orbit']
    params['properties']['baseline'] = mtddict['baseline']
    # following are part of general metadata hardcoded for Sentinel-2
    params['properties']['platform'] = 'sentinel-2'
    params['properties']['instrument'] = 'msi'
    params['properties']['constellation'] = 'sentinel-2'
    params['properties']['mission'] = 'copernicus'
    params['properties']['proj:epsg'] = int(crs_metadata['CRS'])
    params['properties']['gsd'] = 10

    stacItem = pystac.Item(**params)

    # Adding the EO and Projecting Extensions to the item
    eo_ext = EOExtension.ext(stacItem, add_if_missing=True)
    eo_ext.bands = [s2_bands[band]['band'] for band in s2_bands]
    proj_ext = ProjectionExtension.ext(stacItem, add_if_missing=True)
    proj_ext.apply(epsg = int(crs_metadata['CRS']), transform = item_transform)

    return stacItem

def add_asset(stacItem, uri, crsmetadata=None, thumbnail=False):

    """ 
        Adds an asset to the STAC Item based on whether the asset is a thumbnail or an image. 
        stacItem: stac.Item object
        uri: Image URL
        crsmetadata: CRS metadata dict containing CRS string and shapes for different resolutions from get_crs()
        thumbnail: Boolean value indicating if the asset is a thumbnail or not
    """

    if uri.endswith('geo.jp2'): # A few special cases where there were differently named image files that contained different metadata
        splitter = uri.split('/')[-1].split('.')[0].split('_')
        full_bandname = '_'.join(splitter[-3:-1])
        band = splitter[-3]
        resolution = splitter[-2].split('m')[0]
        asset = pystac.Asset(
            href=uri,
            title=full_bandname,
            media_type=pystac.MediaType.JPEG2000,
            roles=["data"],
            extra_fields= {
                'gsd': int(resolution),
                'proj:shape': crsmetadata['shapes'][resolution],
            }
        )
        if band in s2_bands:
            asset_eo_ext = EOExtension.ext(asset)
            asset_eo_ext.bands = [s2_bands[band]["band"]]
        stacItem.add_asset(
            key=full_bandname,
            asset=asset
        )
        
        return stacItem

    if not thumbnail: # If the asset is a standard image
        splitter = uri.split('/')[-1].split('.')[0].split('_')
        full_bandname = '_'.join(splitter[-2:])
        band = splitter[-2]
        resolution = splitter[-1].split('m')[0]
        asset = pystac.Asset(
                href=uri,
                title=full_bandname,
                media_type=pystac.MediaType.JPEG2000,
                roles=["data"],
                extra_fields= {
                    'gsd': int(resolution),
                    'proj:shape': crsmetadata['shapes'][resolution],
                }
        )
        if band in s2_bands:
            asset_eo_ext = EOExtension.ext(asset)
            asset_eo_ext.bands = [s2_bands[band]["band"]]
        stacItem.add_asset(
            key=full_bandname, 
            asset=asset
        )

    else: # If the asset is a thumbnail image
        with rasterio.open(uri) as src:
            shape = src.shape

        full_bandname = uri.split('/')[-1].split('_')[-1].split('.')[0]
        asset = pystac.Asset(
                href=uri,
                title="Thumbnail image",
                media_type=pystac.MediaType.JPEG2000,
                roles=["thumbnail"],
                extra_fields= {
                    'proj:shape': shape,
                }
        )
        stacItem.add_asset(
            key="thumbnail", 
            asset=asset
        )

    return stacItem

def update_catalog(app_host, csc_collection):

    s3_client = init_client()
    buckets = get_buckets(s3_client)
    session = requests.Session()
    session.auth = ("admin", pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering
    original_csc_collection_ids = {item.id for item in csc_collection.get_all_items()}
    print(" * CSC Items collected.")
    items_to_add = {}

    for bucket in buckets:

        # Usual list_objects_v2 function only lists up to 1000 objects so pagination is needed when using a client
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)
        bucketcontents = [x['Key'] for page in pages for x in page['Contents']]

        # Gather needed contents into different lists containing filenames
        bucketcontent_jp2 = [x for x in bucketcontents if x.endswith('jp2')]
        bucketcontent_mtd = [x for x in bucketcontents if x.endswith('MTD_MSIL2A.xml')]
        bucketcontent_crs = [x for x in bucketcontents if x.endswith('MTD_TL.xml')]

        exclude = {'index.html'}
        listofsafes = list(set(list(map(lambda x: x.split('/')[0], bucketcontents))) - exclude)
        # One project includes pseudofolders in the path representing the years, with this check, get the actual SAFEs instead
        if any(re.match(r"\d{4}", safe) for safe in listofsafes):
            listofsafes.clear()
            listofsafes = list(set(list(map(lambda x: x.split('/')[1], bucketcontents))) - exclude)

        for safe in listofsafes:
            
            # SAFE-filename without the subfix
            safename = str(safe.split('.')[0])

            # IF safename is in Collection, the items are already added
            if safename in original_csc_collection_ids:
                continue

            metadatafile = ''.join((x for x in bucketcontent_mtd if safename in x))
            crsmetadatafile = ''.join((x for x in bucketcontent_crs if safename in x))
            if not metadatafile or not crsmetadatafile:
                # If there is no metadatafile or CRS-metadatafile, the SAFE does not include data relevant to the script
                continue
            # THIS FAILS WITH FOLDER BUCKETS
            safecrs_metadata = get_crs(get_metadata_content(bucket, crsmetadatafile, s3_client))
            
            # only jp2 that are image bands
            jp2images = [x for x in bucketcontent_jp2 if safename in x and 'IMG_DATA' in x]
            # if there are no jp2 imagefiles in the bucket, continue to the next bucket
            if not jp2images:
                continue

            # jp2 that are preview images
            previewimage = next(x for x in bucketcontent_jp2 if safename in x and 'PVI' in x)
            metadatacontent = get_metadata_content(bucket, metadatafile, s3_client)
            
            for image in jp2images:

                uri = 'https://a3s.fi/' + bucket + '/' + image

                # Get the item if it's added during the update, if None, the item is made and preview image added
                if safename not in items_to_add:
                    item = make_item(uri, metadatacontent, safecrs_metadata)
                    items_to_add[safename] = item
                    csc_collection.add_item(item)
                    add_asset(item, 'https://a3s.fi/' + bucket + '/' + previewimage, None, True)
                else:
                    item = items_to_add[safename]
                    add_asset(item, uri, safecrs_metadata)

    for item in items_to_add:
        item_dict = items_to_add[item].to_dict()
        converted_item = convert_json_to_geoserver(item_dict)
        request_point = f"collections/{csc_collection.id}/products"
        r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
        r.raise_for_status()
    
    if items_to_add:
        print(f" + Number of items added: {len(items_to_add)}")
        # Update the extents from the Allas Items
        csc_collection.update_extent_from_items()
        collection_dict = csc_collection.to_dict()
        converted_collection = convert_json_to_geoserver(collection_dict)
        request_point = f"collections/{csc_collection.id}/"

        r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
        r.raise_for_status()
        print(" + Updated Collection Extents.")
    else:
        print(" * All items present.")

if __name__ == "__main__":

    """
    The first check for REST API password is from a password file. 
    If a password file is not found, the script prompts the user to give a password through CLI
    """

    s2_bands = get_sentinel2_bands()

    pw_filename = '../passwords.txt'
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
    csc_catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    csc_collection = csc_catalog.get_collection("sentinel2-l2a")
    print(f"Updating STAC Catalog at {args.host}")
    update_catalog(app_host, csc_collection)

    end = time.time()
    print(f"Script took {end-start:.2f} seconds")