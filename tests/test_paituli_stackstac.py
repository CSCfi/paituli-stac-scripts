import pystac_client
import requests
import datetime
import pystac
import pytest
import stackstac
import xarray
import pyproj

@pytest.fixture
def catalog_instance(app_host) -> pystac.Collection:
    test_catalog = pystac_client.Client.open(f"{app_host}/geoserver/ogc/stac/v1/")
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
def test_stackstac(catalog_instance, collection_id) -> None:

    lon, lat = 24.945, 60.173
    search = catalog_instance.search(
        intersects=dict(type="Point", coordinates=[lon, lat]),
        collections=[collection_id],
    )
    item_collection = search.item_collection()

    epsg = item_collection[0].properties["proj:epsg"]
    x, y = pyproj.Proj(f"EPSG:{epsg}")(lon, lat)
    buffer = 5000 
    asset_keys = list(item_collection[0].assets.keys())

    cube = stackstac.stack(
        items=item_collection,
        bounds=(x-buffer, y-buffer, x+buffer, y+buffer), 
        assets=asset_keys,
        epsg=epsg
    ).squeeze() 

    assert type(cube) == xarray.DataArray, "Should be DataArray"
    assert cube.time.any(), "Should have time attribute"
