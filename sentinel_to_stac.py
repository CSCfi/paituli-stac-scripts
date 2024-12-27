import pystac as stac
import rasterio
import re
from datetime import datetime
from shapely.geometry import box, mapping, GeometryCollection, shape
from pystac.extensions.eo import EOExtension
from pystac.extensions.projection import ProjectionExtension
from pystac import CatalogType

from utils.allas_sentinel import get_sentinel2_bands, init_client, get_buckets, transform_crs, get_crs, get_metadata_content, get_metadata_from_xml

def create_collection(client, buckets):
    """
        client: boto3.client
        buckets: list of bucket names where data will be found
    """

    rootcollection = make_root_collection()
    rootcatalog = stac.Catalog(id='Sentinel-2 catalog', description='Sentinel 2 catalog.')
    rootcatalog.add_child(rootcollection)
    
    for bucket in buckets:

        # Usual list_objects_v2 function only lists up to 1000 objects so pagination is needed when using a client
        paginator = client.get_paginator('list_objects_v2')
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
        print('Bucket:', bucket)

        for safe in listofsafes:

            # SAFE-filename without the subfix
            safename = str(safe.split('.')[0])

            metadatafile = ''.join((x for x in bucketcontent_mtd if safename in x))
            crsmetadatafile = ''.join((x for x in bucketcontent_crs if safename in x))
            if not metadatafile or not crsmetadatafile:
                # If there is no metadatafile or CRS-metadatafile, the SAFE does not include data relevant to the script
                continue
            # THIS FAILS WITH FOLDER BUCKETS
            safecrs_metadata = get_crs(get_metadata_content(bucket, crsmetadatafile, client))
            
            # only jp2 that are image bands
            jp2images = [x for x in bucketcontent_jp2 if safe in x and 'IMG_DATA' in x]
            # if there are no jp2 imagefiles in the bucket, continue to the next bucket
            if not jp2images:
                continue
            # jp2 that are preview images
            previewimage = next(x for x in bucketcontent_jp2 if safe in x and 'PVI' in x)

            metadatacontent = get_metadata_content(bucket, metadatafile, client)
            
            for image in jp2images:

                uri = 'https://a3s.fi/' + bucket + '/' + image

                items = list(rootcollection.get_items())
                # Check if the item in question is already added to the collection
                if safename not in [x.id for x in items]:
                    item = make_item(uri, metadatacontent, safecrs_metadata)
                    rootcollection.add_item(item)
                    # add preview image 
                    add_asset(item, 'https://a3s.fi/' + bucket + '/' + previewimage, None, True)
                else:
                    item = [x for x in items if safename in x.id][0]
                    add_asset(item, uri, safecrs_metadata)

    rootcatalog.normalize_hrefs('Sentinel2-tileless')
    rootcatalog.validate_all()

    # Update the spatial and temporal extent
    print('Updating collection extent')
    rootbounds = [GeometryCollection([shape(s.geometry) for s in rootcollection.get_all_items()]).bounds]
    roottimes = [t.datetime for t in rootcollection.get_all_items()]
    roottemporal = [[min(roottimes), max(roottimes)]]
    rootcollection.extent.spatial = stac.SpatialExtent(rootbounds)
    rootcollection.extent.temporal = stac.TemporalExtent(roottemporal)

    rootcatalog.save(catalog_type=CatalogType.RELATIVE_PUBLISHED)

    print('Catalog saved')

def make_root_collection():

    # Preliminary apprx Finland, later with bbox of all tiles from bucketname
    sp_extent = stac.SpatialExtent([[0,0,0,0]])
    # Fill with general Sentinel-2 timeframe, later get from all safefiles
    capture_date = datetime.strptime('2015-06-29', '%Y-%m-%d')
    tmp_extent = stac.TemporalExtent([(capture_date, datetime.today())])
    extent = stac.Extent(sp_extent, tmp_extent)

    # Added optional stac_extension
    rootcollection = stac.Collection(
        id = 'sentinel2-l2a',
        title = 'Sentinel-2 L2A',
        description = 'Sentinel-2 products, processed to Level-2A (Surface Reflectance), a selection of mostly cloud-free products from Finland. More information: https://a3s.fi/sentinel-readme/README.txt',
        extent = extent, 
        stac_extensions = [
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
        ],
        license = 'CC-BY-3.0-IGO',
        providers = [stac.Provider(
            name = "CSC Finland",
            url = "https://www.csc.fi/",
            roles = ["host"]
        )],
        summaries = stac.Summaries(
            summaries={
                "eo:bands": [value['band'].to_dict() for k, value in s2_bands.items()],
                "gsd": [10, 20, 60]
            }
        )
    )
    # Add the link to the license
    rootcollection.add_link(
        link=stac.Link(
            rel = 'licence',
            target = 'https://sentinel.esa.int/documents/247904/690755/Sentinel_Data_Legal_Notice'
        )
    )
    # Add README as metadata assett
    rootcollection.add_asset(
        key="metadata",
        asset=stac.Asset(
            roles=['metadata'],
            href='https://a3s.fi/sentinel-readme/README.txt'
        )
    )

    print('Root collection made')

    return rootcollection

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

    stacItem = stac.Item(**params)

    # Adding the EO and Projecting Extensions to the item
    eo_ext = EOExtension.ext(stacItem, add_if_missing=True)
    eo_ext.bands = [s2_bands[band]['band'] for band in s2_bands]
    proj_ext = ProjectionExtension.ext(stacItem, add_if_missing=True)
    proj_ext.apply(epsg = int(crs_metadata['CRS']), transform = item_transform)

    print('Item made:', params['id'])

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
        asset = stac.Asset(
            href=uri,
            title=full_bandname,
            media_type=stac.MediaType.JPEG2000,
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
        asset = stac.Asset(
                href=uri,
                title=full_bandname,
                media_type=stac.MediaType.JPEG2000,
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
        asset = stac.Asset(
                href=uri,
                title="Thumbnail image",
                media_type=stac.MediaType.JPEG2000,
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

if __name__ == '__main__':

    s2_bands = get_sentinel2_bands()

    s3 = init_client()
    buckets = get_buckets(s3)
    create_collection(s3, buckets)