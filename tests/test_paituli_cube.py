import pystac_client
import pystac
import pytest
import xarray
import pyproj
import odc.stac

@pytest.fixture
def catalog_instance(app_host) -> pystac.Collection:
    # Use the update-script headers to not show up in logs
    test_catalog = pystac_client.Client.open(f"{app_host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    return test_catalog

@pytest.fixture
def collection_instance(catalog_instance, collection_id) -> pystac.Collection:
    test_collection = catalog_instance.get_collection(collection_id)
    return test_collection

@pytest.fixture
def mediatype_instance(collection_instance) -> str:
    items = collection_instance.get_items()
    media_type = items[0].assets[0]["type"]
    return media_type

@pytest.mark.xfail(mediatype_instance == "application/x-netcdf", reason="NetCDF not supported")
def test_cube(catalog_instance, collection_id) -> None:

    lon, lat = 24.945, 60.173
    search = catalog_instance.search(
        intersects=dict(type="Point", coordinates=[lon, lat]),
        collections=[collection_id],
        max_items=100
    )
    item_collection = search.item_collection()

    epsg = item_collection[0].properties["proj:epsg"]
    gsd = item_collection[0].properties["gsd"]
    asset_keys = list(item_collection[0].assets.keys())

    cube = odc.stac.load(
        item_collection,
        bands=asset_keys,
        crs=epsg,
        resolution=gsd,
        chunks={"time": 1, "band": 1, "y": 1024, "x": 1024}
    ).squeeze()

    assert type(cube) == xarray.Dataset, "Should be Dataset"
    assert cube.time.any(), "Should have time attribute"