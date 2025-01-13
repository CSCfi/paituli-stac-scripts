import pystac_client
import datetime
import pystac
import pytest

@pytest.fixture
def catalog_instance(app_host) -> pystac_client.Client:
    # Use the update-script headers to not show up in logs
    catalog = pystac_client.Client.open(f"{app_host}/geoserver/ogc/stac/v1/", headers={"User-Agent":"update-script"})
    return catalog

@pytest.fixture
def collection_instance(catalog_instance, collection_id) -> pystac.Collection:
    test_collection = catalog_instance.get_collection(collection_id)
    return test_collection

@pytest.fixture
def time_instance(collection_instance) -> datetime.datetime:
    test_item = next(collection_instance.get_items())
    time_instance = test_item.datetime
    return time_instance

@pytest.mark.parametrize("coord_input, expected", [((24.945, 60.173), True), ((35.652, 139.839), False)])
def test_coord_search_intersect(catalog_instance, collection_instance, coord_input, expected) -> None:

    lon, lat = coord_input
    search = catalog_instance.search(
        intersects=dict(type="Point", coordinates=[lon, lat]),
        collections=[collection_instance.id],
    )
    flag = True if search.matched() > 0 else False
    assert flag == expected, "Wrong number of matches"

def test_coord_search_time(catalog_instance, collection_instance, time_instance) -> None:

    search = catalog_instance.search(
        collections=[collection_instance.id],
        datetime=time_instance
    )
    assert search.matched() > 0, "Wrong number of matches"

@pytest.mark.parametrize("bbox_input, expected", [([23.0,60.5,26.0,64.0], True), ([129.4,31.0,145.5,45.5], False)])
def test_bbox_search(catalog_instance, collection_instance, bbox_input, expected) -> None:

    search = catalog_instance.search(
        bbox=bbox_input,
        collections=[collection_instance.id],
    )
    flag = True if search.matched() > 0 else False
    assert flag == expected, "Wrong number of matches"