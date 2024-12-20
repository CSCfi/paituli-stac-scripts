import boto3
import re
import pandas as pd
from itertools import chain
from xml.dom import minidom
from pystac.extensions.eo import Band

from rasterio.crs import CRS
from rasterio.warp import transform_bounds

def get_sentinel2_bands():
    """
        Get the Sentinel 2 Bands
    """
    # Sentinel 2 Band information
    bands = {
        "B01": {
            "band": Band.create(name='B01', description='Coastal: 400 - 450 nm', common_name='coastal')
        },
        "B02": {
            "band": Band.create(name='B02', description='Blue: 450 - 500 nm', common_name='blue')
        },
        "B03": {
            "band": Band.create(name='B03', description='Green: 500 - 600 nm', common_name='green'),
        },
        "B04": {
            "band": Band.create(name='B04', description='Red: 600 - 700 nm', common_name='red'),
        },
        "B05": {
            "band": Band.create(name='B05', description='Vegetation Red Edge: 705 nm', common_name='rededge')
        },
        "B06": {
            "band": Band.create(name='B06', description='Vegetation Red Edge: 740 nm', common_name='rededge')
        },
        "B07": {
            "band": Band.create(name='B07', description='Vegetation Red Edge: 783 nm', common_name='rededge')
        },
        "B08": {
            "band": Band.create(name='B08', description='Near-IR: 750 - 1000 nm', common_name='nir')
        },
        "B8A": {
            "band": Band.create(name='B8A', description='Near-IR: 750 - 900 nm', common_name='nir08')
        },
        "B09": {
            "band": Band.create(name='B09', description='Water vapour: 850 - 1050 nm', common_name='nir09')
        },
        "B10": {
            "band": Band.create(name='B10', description='SWIR-Cirrus: 1350 - 1400 nm', common_name='cirrus')
        },
        "B11": {
            "band": Band.create(name='B11', description='SWIR16: 1550 - 1750 nm', common_name='swir16')
        },
        "B12": {
            "band": Band.create(name='B12', description='SWIR22: 2100 - 2300 nm', common_name='swir22')
        }
    }

    return bands

def init_client():
    """
        Initialize the boto3 s3 client that is used to get the Buckets from Allas

        -> boto3.client
    """

    # Create client with credentials. Allas-conf needed to be run for boto3 to get the credentials
    s3 = boto3.client(
        service_name = "s3",
        endpoint_url = "https://a3s.fi", 
        region_name = "regionOne"
    )

    return s3

def get_buckets(client):
    """
        client: boto3.client
        -> buckets: list of Buckets
        Get the Buckets from the client and two CSV files that have the buckets of two different projects
    """
    # Get the bucket names from the client and the CSVs
    bucket_information = client.list_buckets()
    buckets = [x['Name'] for x in bucket_information['Buckets'] if re.match(r"Sentinel2(?!.*segments)", x['Name'])]

    first_csv = pd.read_table("files/2000290_buckets.csv", header=None)
    first_buckets = list(chain.from_iterable(first_csv.to_numpy()))
    second_csv = pd.read_table("files/2001106_buckets.csv", header=None)
    second_buckets = list(chain.from_iterable(second_csv.to_numpy()))
    
    buckets = [*buckets, *first_buckets, *second_buckets]

    return buckets

def transform_crs(bounds, crs_string):
    
    """
        bounds: Bounding Box bounds from rasterio.open()
        crs_string: CRS string from CRS metadata
    """

    # Transform the bounds according to the CRS
    crs = CRS.from_epsg(4326)
    safecrs = CRS.from_epsg(int(crs_string))
    bounds_transformed = transform_bounds(safecrs, crs, bounds[0][0], bounds[0][1], bounds[0][2], bounds[0][3])
        
    return bounds_transformed

def get_crs(crsmetadatafile):

    """
        crsmetadatafile: The decoded content from the SAFEs CRS metadatafile
    """

    # Get CRS and resolution sizes from crsmetadatafile
    with minidom.parseString(crsmetadatafile) as doc:
        crsstring = get_xml_content(doc, 'HORIZONTAL_CS_CODE').split(':')[-1]
        sizes = doc.getElementsByTagName('Size')
        crsmetadata = { 
            'CRS': crsstring,
            'shapes': {}
        }
        for size in sizes:
            resolution = size.getAttribute('resolution')
            crsmetadata['shapes'][resolution] = (int(get_xml_content(size, 'NROWS')), int(get_xml_content(size, 'NCOLS')))

    return crsmetadata

def get_xml_content(doc, tagname):

    """
        doc: Parsed xml metadata file
        tagname: The wanted tag to be searched from the xml file
    """

    content = doc.getElementsByTagName(tagname)[0].firstChild.data
    return content

def get_metadata_content(bucket, metadatafile, client):

    """
        bucket: The bucket where the metadatafile is located
        metadatafile: The name of the metadatafile
        client: boto3.client
    """

    obj = client.get_object(Bucket = bucket, Key = metadatafile)['Body']
    metadatacontent = obj.read().decode()
    return metadatacontent

def get_metadata_from_xml(metadatabody):

    """
        metadatabody: The metadata content from boto3.client get_object call
    """

    with minidom.parseString(str(metadatabody)) as doc:
        metadatadict = {}
        metadatadict['cc_perc'] = int(float(get_xml_content(doc,'Cloud_Coverage_Assessment')))
        metadatadict['data_cover'] = 100 - int(float(get_xml_content(doc,'NODATA_PIXEL_PERCENTAGE')))
        metadatadict['start_time'] = get_xml_content(doc,'PRODUCT_START_TIME')
        metadatadict['end_time'] = get_xml_content(doc,'PRODUCT_STOP_TIME')
        metadatadict['orbit'] = get_xml_content(doc,'SENSING_ORBIT_NUMBER')
        metadatadict['baseline'] = get_xml_content(doc,'PROCESSING_BASELINE')

    return metadatadict