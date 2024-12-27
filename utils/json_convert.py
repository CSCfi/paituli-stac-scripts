import json

def convert_json_to_geoserver(json_content):

    """
        json_content: json file or a python dict
        
        A function to map the STAC jsonfiles into the GeoServer database layout.
        There are different json layouts for Collections and Items. The function checks if the jsonfile is of type "Collection",
        or of type "Feature" (=Item). A number of properties are hardcoded into metadata as these are not collected in the STAC jsonfiles.
    """

    # Load the content if it's not a dict
    if not isinstance(json_content, dict):
        with open(json_content) as f:
            content = json.load(f)
    else:
        content = json_content
    
    if content["type"] == "Collection":

        bbox = content["extent"]["spatial"]["bbox"][0]
        time_interval = content["extent"]["temporal"]["interval"][0]

        new_json = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]],
                        [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]],
                        [bbox[2], bbox[1]]
                    ]
                ]
            },
            "properties": {
                "name": content["id"],
                "title": content["title"],
                "eo:identifier": content["id"],
                "description": content["description"],
                "timeStart": time_interval[0],
                "timeEnd": time_interval[1],
                "primary": True,
                "license": content["license"],
                "providers": content["providers"],
                "licenseLink": None,
                "queryables": [
                    "eo:identifier"
                ]
            }
        }

        if "assets" in content: new_json["properties"]["assets"] = content["assets"]
        if "summeries" in content: new_json["properties"]["summaries"] = content["summaries"]

        # Add Cloud Cover queryable for Sentinel 2 L2A
        if content["id"] == "sentinel2-l2a": new_json["properties"]["queryables"].append("eo:cloud_cover")

        if "derive_from" in content:
            new_json["properties"]["derivedFrom"] = {
                "href": content["derived_from"],
                "rel": "derived_from",
                "type": "application/json"
            }

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = {
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                }

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["start_datetime"],
                "timeEnd": content["properties"]["end_datetime"],
                "eop:resolution": content["gsd"],
                "crs": content["properties"]["proj:epsg"],
                "projTransform": content["properties"]["proj:transform"],
                "assets": content["assets"]
            }
        }

        if "eo:cloud_cover" in content["properties"]: new_json["properties"]["opt:cloudCover"] = int(content["properties"]["eo:cloud_cover"])
        if "thumbnail" in content["links"]: new_json["properties"]["thumbnailURL"] = content["links"]["thumbail"]["href"]

        # Fix for FMI Datetime
        if content["properties"]["start_datetime"] is None and content["properties"]["end_datetime"] is None and content["properties"]["datetime"] is not None:
            new_json["properties"]["timeStart"] = content["properties"]["datetime"]
            new_json["properties"]["timeEnd"] = content["properties"]["datetime"]

    return json.loads(json.dumps(new_json))