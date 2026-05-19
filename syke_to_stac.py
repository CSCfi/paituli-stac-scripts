import pystac
import os
import json
import pandas as pd
import rasterio

from datetime import datetime
from functools import lru_cache
from rasterio.warp import transform_bounds
from shapely.geometry import box, mapping

dir_path = os.path.dirname(os.path.realpath(__file__))
pystac.version.set_stac_version('1.0.0')

# The files are named the same so just listing the filename and switching the file-extension should work. If files are named differently later, make a dictionary or db.
syke_collection_files = [
    "Harmonized_Landsat57_satellite_image_mosaic_timeseries",
    "Harmonized_Landsat89_satellite_image_mosaic_timeseries",
    "Sentinel2_reflectance_mosaic_2017_2021",
    "Sentinel2_reflectance_mosaic_2022_onwards",
    "harmonized_finnish_corine_land_cover_at_aquainfra"
]

# --- Load collection from JSON ---
def load_collection(filename):
    path = f"{dir_path}/files/{filename}"
    with open(path) as f:
        data = json.load(f)
    return pystac.read_dict(data)

# --- Load CSV with pandas ---
def load_csv(filename):
    path = f"{dir_path}/files/{filename}"
    return pd.read_csv(path, delimiter=";")

# --- Parse date string "1.5.1984" -> datetime ---
def parse_date(date_str):
    return datetime.strptime(date_str.strip(), "%d.%m.%Y")

# --- Extract geometry and bbox from a GeoTIFF file --- #
# --- Cache geometry lookups so each URL is only fetched once ---
@lru_cache(maxsize=None)
def get_geometry_from_tif(href):
    """Extract geometry, bbox, EPSG and transform from a remote GeoTIFF, cached by URL."""
    try:
        with rasterio.open(href) as src:
            bounds = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
            bbox = list(bounds)
            geometry = mapping(box(*bbox))
            epsg = src.crs.to_epsg()
            proj_transform = [
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
            proj_shape = src.shape

            return geometry, bbox, epsg, proj_transform, proj_shape
    except Exception as e:
        print(f"  Warning: could not read geometry from {href}: {e}")
        return None, None, None, None, None


# --- Create Items and Assets from CSV rows ---
def create_items_from_csv(collection, df):
    for item_id, group in df.groupby("item_id"):
        start_date = parse_date(group["start-date"].iloc[0])
        end_date = parse_date(group["end-date"].iloc[0])

        # Use first asset's GeoTIFF to extract geometry (cached)
        first_href = group["file"].iloc[0]
        geometry, bbox, epsg, proj_transform, proj_shape = get_geometry_from_tif(first_href)
        collection_gsd = int(collection.summaries.get_list("gsd")[0]) # Use the GSD from Collection Summaries

        # Create the Item
        item = pystac.Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=start_date,
            properties={
                "start_datetime": start_date.isoformat() + "Z",
                "end_datetime": end_date.isoformat() + "Z",
                "proj:epsg": epsg,
                "proj:transform": proj_transform,
                "gsd": collection_gsd
            }
        )

        # Add one Asset per band
        for _, row in group.iterrows():
            asset_href = row["file"]
            item.add_asset(
                key=row["asset"],
                asset=pystac.Asset(
                    href=asset_href,
                    title=row["asset"],
                    media_type="image/tiff; application=geotiff; profile=cloud-optimized",
                    roles=["data"],
                    extra_fields={
                        "proj:transform": proj_transform,
                        "proj:shape": proj_shape,
                        "gsd": collection_gsd
                    }
                )
            )

        collection.add_item(item)
        print(f"  Added item: {item_id} with {len(group)} assets, bbox: {bbox}")

# --- Create and populate catalog ---
def create_collections(root_catalog):
    collections = []

    for collection in syke_collection_files:
        collection_json = load_collection(collection + ".json")
        root_catalog.add_child(collection_json)
        collections.append((collection_json, collection + ".csv"))

    return collections

if __name__ == "__main__":
    root_catalog = pystac.Catalog(
        id="SYKE",
        description="SYKE catalog",
        catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED
    )

    collections = create_collections(root_catalog)

    for collection, csv_file in collections:
        print(f" Collection: {collection.id}")
        df = load_csv(csv_file)
        print(f"  Loaded {len(df)} rows, {df['item_id'].nunique()} unique items")
        create_items_from_csv(collection, df)
        collection.update_extent_from_items()

    # Normalize hrefs and save
    output_path = f"{dir_path}/syke"
    root_catalog.normalize_hrefs(output_path)
    root_catalog.save(catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)

    print(" Catalog saved to:", output_path)