# Templates for new data collection to Paituli STAC

## Collection JSON

[Example Collection JSON](collection.json)

Comments about the elements:
* Do not change elements: `type`, `stac_version`, `extent` - correct bbox and temporal extent are calculated by import script. But these rows should not be removed.
* `id` -  Collection's shorttitle, use underscores, not spaces, no special characters, all letters lower cap. `id` is the machine-readable name of the collection. End the `id` with the name of service providing the data for example: `at_paituli`, `at_geocubes`, `at_fmi`, `at_aquainfra`.
* `title` - Human-readable name of the collection with spaces, if important include scale, but not year. Year is in temporal extent an for each item separately.
* `description` - Description of the dataset, one paragraph, a few rows long. Longer description should be behind the metadata link. End the description with CRS info. "Coordinate system: xxx."
* `assets`/`metadata` - URL link to longer metadata, can be any metadata service. If possible, use DOI, URN or other PID.
* `licenseURL` - link to license file, can be left empty
* `license` - See options: https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#license
* `providers` - See options: https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#provider-object
* `summaries`/`gsd` - pixel size
* `summaries`/`bands` - name for each band in the raster file
