import pytest

def pytest_addoption(parser):
    parser.addoption("--collection", action="store", default="", help="Collection ID")
    parser.addoption("--host", action="store", default="", help="Catalog host")

@pytest.fixture
def collection_id(request):
    return request.config.getoption("--collection")

@pytest.fixture
def app_host(request):
    return request.config.getoption("--host")