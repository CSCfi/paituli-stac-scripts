from pystac import Catalog, Collection
import pystac
import rasterio
import urllib.request, json

fmi_collections = [
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_vuosi/Sentinel-2_global_mosaic_vuosi.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_dekadi/Sentinel-2_global_mosaic_dekadi.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_indeksimosaiikit/Sentinel-2_indeksimosaiikit.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_dekadi_mosaiikki/Sentinel-1_dekadi_mosaiikki.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_daily_mosaiikki/Sentinel-1_daily_mosaiikki.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_osakuvat/Sentinel-1_osakuvat.json",
    #"https://pta.data.lit.fmi.fi/stac/catalog/Landsat_pintaheijastus/Landsat_pintaheijastus.json", # THE CHILDREN ARE NOT RIGHT
    "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_indeksit/Landsat_indeksit.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Metsavarateema/Metsavarateema.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Latvuskorkeusmalli/Latvuskorkeusmalli.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/MML-DTM-2m/MML-DTM-2m.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Myrskytuhoriskikartta/Myrskytuhoriskikartta.json",
    "https://pta.data.lit.fmi.fi/stac/catalog/Tuulituhoriski/Tuulituhoriski.json"
]

collection_info = {
    "sentinel_2_annual_mosaics_at_fmi" : {
        "title":
            "Sentinel-2 annual surface reflectance mosaics (FMI Tuulituhohaukka)",
        "description": 
            "Sentinel-2 annual surface reflectance  mosaics. Scale: 10m. Original Sentinel-2 data from ESA Copernicus Sentinel Program, mosaic processing by Sentinel-2 Global Mosaic Service, mosaic postprocessing by SYKE. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://s2gm.land.copernicus.eu/help/documentation",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_vuosi/Sentinel-2_global_mosaic_vuosi.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "licensor",
                    "processor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="Sentinel-2 Global Mosaic service",
                url="https://s2gm.land.copernicus.eu/",
                roles=[
                    "processor"
                ]
            )
        ]
    },
    "sentinel_2_11_days_mosaics_at_fmi": {
        "title":
            "Sentinel-2 11-days surface reflectance mosaics (FMI Tuulituhohaukka)",
        "description":
            "Sentinel-2 11-days surface reflectance mosaics. Scale: 10m. Original Sentinel-2 data from ESA Copernicus Sentinel Program, mosaic processing by Sentinel-2 Global Mosaic Service. Mosaic postprocessing by SYKE. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://s2gm.land.copernicus.eu/help/documentation",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_global_mosaic_dekadi/Sentinel-2_global_mosaic_dekadi.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="Sentinel-2 Global Mosaic service",
                url="https://s2gm.land.copernicus.eu/",
                roles=[
                    "processor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            )
        ]
    },
    "sentinel_2_monthly_index_mosaics_at_fmi": {
        "title":
            "Sentinel-2 monthly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI (FMI Tuulituhohaukka)",
        "description":
            "Sentinel-2 monthly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI. Scale: 10m. Available each year for April-October. Original Sentinel-2 data from ESA Copernicus Sentinel Program, processing by SYKE and FMI. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-image-index-mosaics-s2ind-sentinel-2-kuvamosaiikit-s2ind",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-2-image-index-mosaics-s2ind-sentinel-2-kuvamosaiikit-s2ind",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-2_indeksimosaiikit/Sentinel-2_indeksimosaiikit.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
        ]
    },
    "sentinel_1_11_days_mosaics_at_fmi": {
        "title":
            "Sentinel-1 11-days backscatter mosaics: VV and VH polarisation (FMI Tuulituhohaukka)",
        "description":
            "Sentinel-1 11-days backscatter mosaics: VV and VH polarisation. Scale: 20m. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_dekadi_mosaiikki/Sentinel-1_dekadi_mosaiikki.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "sentinel_1_daily_mosaics_at_fmi": {
        "title":
            "Sentinel-1 daily backscatter mosaics: VV and VH polarisation (FMI Tuulituhohaukka)",
        "description":
            "Sentinel-1 daily backscatter mosaics: VV and VH polarisation. Scale: 20m. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/sentinel-1-sar-image-mosaic-s1sar-sentinel-1-sar-kuvamosaiikki-s1sar",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_daily_mosaiikki/Sentinel-1_daily_mosaiikki.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "sentinel_1_tiles_at_fmi": {
        "title":
            "Sentinel-1 backscatter tiles: VV and VH polarisation (FMI Tuulituhohaukka)",
        "description":
            "Sentinel-1 backscatter tiles: VV and VH polarisation. Scale: 20m. Original Sentinel-1 data from ESA Copernicus Sentinel Program, processing by FMI. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar",
        "licenseURL":
            "https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Sentinel-1_osakuvat/Sentinel-1_osakuvat.json",
        "license":
            "CC-BY-SA-3.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "processor",
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="ESA",
                url="https://www.esa.int/",
                roles=[
                    "producer"
                ]
            ),
        ]
    },
    "landsat_yearly_mosaics_at_fmi": {
        "title":
            "Landsat annual surface reflectance mosaics (FMI Tuulituhohaukka)",
        "description":
            "Landsat annual surface reflectance mosaics. Scale: 30m. Avaialble years: 1985, 1990 and 1995. Original Landsat imagery from USGS and ESA, processing by Blom Kartta. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-satellite-image-mosaics-href-historialliset-landsat-kuvamosaiikit-href",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-satellite-image-mosaics-href-historialliset-landsat-kuvamosaiikit-href",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_pintaheijastus/Landsat_pintaheijastus.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "licensor"
                ]
            ),
            pystac.Provider(
                name="USGS",
                url="https://www.usgs.gov/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="Blom Kartta",
                url="https://blomkartta.fi/",
                roles=[
                    "processor"
                ]
            ),
        ]
    },
    "landsat_annual_index_mosaics_at_fmi": {
        "title":
            "Landsat (4 and 5) yearly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI (FMI Tuulituhohaukka)",
        "description":
            "Landsat (4 and 5) yearly index mosaics: NDVI, NDBI, NDMI, NDSI, NDTI. Scale: 30m. Available for the years 1984-2011. Landsat-4/5 imagery from United States Geological Survey. Mosaics processed by SYKE at Finnish National Satellite Data Centre. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-image-index-mosaics-hind-historialliset-landsat-kuvaindeksimosaiikit-hind",
        "licenseURL":
            "https://ckan.ymparisto.fi/dataset/historical-landsat-image-index-mosaics-hind-historialliset-landsat-kuvaindeksimosaiikit-hind",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Landsat_indeksit/Landsat_indeksit.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="USGS",
                url="https://www.usgs.gov/",
                roles=[
                    "producer"
                ]
            ),
            pystac.Provider(
                name="SYKE",
                url="https://www.syke.fi/en-US",
                roles=[
                    "processor",
                    "licensor"
                ]
            ),
        ]
    },
    "forest_inventory_at_fmi": {
        "title":
            "Multi-source forest inventory products (FMI Tuulituhohaukka)",
        "description":
            "Multi-source forest inventory products. Scale: 20m. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/fin/catalog.search#/metadata/0e7ad446-2999-4c94-ad0d-095991d8f80a",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Metsavarateema/Metsavarateema.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="LUKE",
                url="https://www.luke.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "canopy_height_model_at_fmi": {
        "title":
            "Canopy height model (FMI Tuulituhohaukka)",
        "description":
            "Canopy height model. Scale: 1m. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/eng/catalog.search#/metadata/8f3b883b-a133-4eee-9f5d-bfd042d782bb",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Latvuskorkeusmalli/Latvuskorkeusmalli.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="Finnish Forest Centre",
                url="https://www.metsakeskus.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "2m_digital_terrain_model_products_at_fmi": {
        "title":
            "Digital terrain model products: DTM, aspect, slope (FMI Tuulituhohaukka)",
        "description":
            "Digital terrain model products: DTM, aspect, slope. Scale: 2m. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://www.paikkatietohakemisto.fi/geonetwork/srv/eng/catalog.search#/metadata/053a0a20-abfa-4bf9-ac74-270e845654d1",
        "licenseURL":
            "https://www.maanmittauslaitos.fi/en/opendata-licence-cc40",
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/MML-DTM-2m/MML-DTM-2m.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="NLS",
                url="https://www.maanmittauslaitos.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "forest_wind_damage_risk_at_fmi": {
        "title":
            "Forest wind damage risk map (FMI Tuulituhohaukka)",
        "description":
            "Forest storm damage risk map. Scale: 16m. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://metsainfo.luke.fi/fi/cms/tuulituhoriskikartta/tuulituhoriskit-kysymykset",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Myrskytuhoriskikartta/Myrskytuhoriskikartta.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host"
                ]
            ),
            pystac.Provider(
                name="LUKE",
                url="https://www.luke.fi/en",
                roles=[
                    "producer",
                    "licensor"
                ]
            )
        ]
    },
    "daily_wind_damage_risk_at_fmi": {
        "title":
            "Daily wind damage risk map (FMI Tuulituhohaukka)",
        "description":
            "Daily wind damage risk map. Scale: 500m. Coordinate system: ETRS-TM35FIN.",
        "metadata":
            "https://etsin.fairdata.fi/dataset/7a326b19-7a3c-42fc-9375-fcec1898fc6e",
        "licenseURL":
            None,
        "original_href":
            "https://pta.data.lit.fmi.fi/stac/catalog/Tuulituhoriski/Tuulituhoriski.json",
        "license":
            "CC-BY-4.0",
        "providers": [
            pystac.Provider(
                name="FMI",
                url="https://en.ilmatieteenlaitos.fi/",
                roles=[
                    "host",
                    "producer",
                    "licensor"
                ]
            )
        ]
    }

}

news_ids = {
    "Sentinel-2_global_mosaic_vuosi": "sentinel_2_annual_mosaics_at_fmi",
    "Sentinel-2_global_mosaic_dekadi": "sentinel_2_11_days_mosaics_at_fmi",
    "Sentinel-2_indeksimosaiikit": "sentinel_2_monthly_index_mosaics_at_fmi",
    "Sentinel-1_dekadi_mosaiikki": "sentinel_1_11_days_mosaics_at_fmi",
    "Sentinel-1_daily_mosaiikki": "sentinel_1_daily_mosaics_at_fmi",
    "Sentinel-1_osakuvat": "sentinel_1_tiles_at_fmi",
    "Landsat_pintaheijastus": "landsat_yearly_mosaics_at_fmi",
    "Landsat_indeksit": "landsat_annual_index_mosaics_at_fmi",
    "Latvuskorkeusmalli": "canopy_height_model_at_fmi",
    "Metsavarateema": "forest_inventory_at_fmi",
    "MML-DTM-2m": "2m_digital_terrain_model_products_at_fmi",
    "Myrskytuhoriskikartta": "forest_wind_damage_risk_at_fmi",
    "Tuulituhoriski": "daily_wind_damage_risk_at_fmi"
}

def retry_errors(list_of_items, list_of_errors):

    print(" - Retrying errors:")
    while len(list_of_errors) > 0:
        for i,item in enumerate(list_of_errors):
            try:
                list_of_items.append(pystac.Item.from_file(item))
                print(f" + Added item {item}")
                list_of_errors.remove(item)
            except Exception as e:
                print(f" - ERROR {e} in item {item} #{i}")

    return 0

def create_fmi_collections(root_catalog):

    collections = []
    for collection in fmi_collections:
        try:
            collections.append(Collection.from_file(collection))
        except ValueError:
            with urllib.request.urlopen(collection) as url:
                data = json.load(url)
                data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                collections.append(Collection.from_dict(data))

    for collection in collections:

        collection.id = news_ids[collection.id]
        print(f"Creating {collection.id}")
        
        collection_links = collection.get_child_links()

        sub_collections = []
        for link in collection_links:
            try:
                sub_collections.append(Collection.from_file(link.target))
            except ValueError:
                with urllib.request.urlopen(link.target) as url:
                    data = json.load(url)
                    data["extent"]["temporal"]["interval"] = [data["extent"]["temporal"]["interval"]]
                    sub_collections.append(Collection.from_dict(data))

        item_links = list(set([link.target for sub in sub_collections for link in sub.get_item_links()]))

        items = []
        errors = []

        for i,item in enumerate(item_links):
            try:
                items.append(pystac.Item.from_file(item))
            except Exception as e:
                print(f" - ERROR {e} in item {item} #{i}")
                errors.append(item)
        print(f" + Number of items: {len(items)}")

        collection.remove_links("child")
        collection.remove_links("license")

        collection.title = collection_info[collection.id]["title"]
        collection.description = collection_info[collection.id]["description"]
        collection.providers = collection_info[collection.id]["providers"]
        collection.extra_fields["derived_from"] = collection_info[collection.id]["original_href"]
        collection.license = collection_info[collection.id]["license"]
        if collection_info[collection.id]["metadata"]:
            collection.add_link(pystac.Link(
                rel="metadata",
                target=collection_info[collection.id]["metadata"],
                title="Metadata"
            ))
            collection.add_asset(
                key="metadata",
                asset=pystac.Asset(
                    href=collection_info[collection.id]["metadata"],
                    title="Metadata",
                    roles=[
                        "metadata"
                    ]
                )
            )
        if collection_info[collection.id]["licenseURL"]:
            collection.add_link(pystac.Link(
                rel="license",
                target=collection_info[collection.id]["licenseURL"],
                title="License"
            ))

        # If there were connection errors during the item making process, the item generation for errors is retried
        if len(errors) > 0:
            retry_errors(items, errors)
            print(" + All errors fixed")

        for i,item in enumerate(items):

            with rasterio.open(next(iter(item.assets.values())).href) as src:
                item.extra_fields["gsd"] = src.res[0]
                # 9391 EPSG code is false, replace by the standard 3067
                if src.crs.to_epsg() == 9391:
                    item.extra_fields["proj:epsg"] = 3067
                else:
                    item.extra_fields["proj:epsg"] = src.crs.to_epsg()
                item.extra_fields["proj:transform"] = [
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

            for asset in item.assets:
                if item.assets[asset].roles is not list:
                    item.assets[asset].roles = [item.assets[asset].roles]
        
            del item.extra_fields["license"]
            item.remove_links("license")

            collection.add_item(item)

        root_catalog.add_child(collection)
        print(" + All items added")

    root_catalog.normalize_and_save('FMI')
    print("Catalog normalized and saved")

if __name__ == "__main__":

    root_catalog = Catalog(id="FMI", description="FMI catalog", catalog_type= pystac.CatalogType.RELATIVE_PUBLISHED)
    create_fmi_collections(root_catalog)