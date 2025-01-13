import pystac_client
import requests
import datetime
import pystac
import pytest

@pytest.fixture
def collection_instance(app_host, collection_id) -> pystac.Collection:
    # Use the update-script headers to not show up in logs
    catalog = pystac_client.Client.open(f"{app_host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    test_collection = catalog.get_collection(collection_id)
    return test_collection

@pytest.fixture
def items_instance(collection_instance):
    items = collection_instance.get_items()
    return items

def test_item(items_instance, collection_instance) -> None:
    epsg_codes = {3067, 4123, 5048, 3902, 3903, 2391, 2392, 2394, 3386, 3387, 2393,
                  3901, 25832, 25833, 32634, 32636, 32635}
    asset_media_types = [
        "image/tiff; application=geotiff", 
        "image/png", 
        "image/jp2", 
        "application/x-netcdf",
        "image/tiff; application=geotiff; profile=cloud-optimized"
        ]
    
    for item in items_instance:
        assert pystac.validation.validate(item), "Should be valid STAC item"
        timespan = collection_instance.extent.temporal.intervals
        assert type(item.datetime) == datetime.datetime, "Should be datetime object"
        if collection_instance.id != "sentinel2-l2a":
            assert type(item.properties["gsd"]) == float, "Should be float"
            assert item.properties["gsd"] > 0, "Should be more than 0"
            assert item.properties["proj:transform"], "Item should include proj:transform"

        assert type(item.properties["proj:epsg"]) == int, "Should be int"
        assert item.properties["proj:epsg"] in epsg_codes, "Should be a valid EPSG code"
        assert timespan[0][0] <= item.datetime <= timespan[0][1], "Datetime should be within Collection timerange"

        assert len(item.assets) > 0, "Should be more than 0"
        for x in item.assets:
            asset = item.assets[x]
            if asset.href.startswith("/appl/data/geo"):
                continue
            r = requests.head(asset.href)
            assert r.status_code == 200, "The asset HREF should return status code 200"
            assert asset.title, "Should have a title"
            if "data" in asset.roles:
                assert asset.media_type in asset_media_types, "Asset media type should be one of listed ones"
            assert asset.roles, "Should have items in role list"
            if collection_instance.id != "sentinel2-l2a":
                assert type(asset.extra_fields["gsd"]) == float, "Should be float"
                assert asset.extra_fields["proj:transform"], "Asset should include proj:transform"
            assert asset.extra_fields["proj:shape"], "Asset should include proj:shape"