# Templates for new data collection to Paituli STAC

For terminology see: https://paituli.csc.fi/stac.html

## Collection JSON

[Example Collection JSON](collection.json)

Comments about the elements:
* Do not change elements: `type`, `stac_version`, `extent` - correct bbox and temporal extent are calculated by import script. But these rows should not be removed.
* `id` -  Collection's shorttitle, use underscores, not spaces, no special characters, all letters lower cap. `id` is the machine-readable name of the collection. End the `id` with the name of service providing the data for example: `at_paituli`, `at_geocubes`, `at_fmi`, `at_aquainfra`.
* `title` - Human-readable name of the collection with spaces, if important include scale, but not year. Year is in temporal extent an for each item separately.
* `description` - Description of the dataset, one paragraph, a few rows long. Longer description should be behind the metadata link. End the description with CRS info. "Coordinate system: xxx."
* `assets`/`metadata` - URL link to longer metadata, can be any metadata service. If possible, use DOI, URN or other PID.
* `licenseURL` - link to license file, can be left empty
* `license` - [See STAC spec license](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#license)
* `providers` - [See STAC spec provides](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#provider-object)
* `summaries`/`gsd` - pixel size
* `summaries`/`bands` - name for each band in the raster file

## Items CSV

[Example Items CSV](items.csv)
* `file` - URL of the Item's Asset data file
* `start-date` and `end-date` - data start and end dates, if you only have at year level, then use 1.1.2013;31.12.2013
* `collection` - has to match the Collection's `id` in the JSON-file
* `asset` - name of the asset, no spaces, no special chars. In the collection Items have same asset names, but one Item can have several assets.
* `item_id` - name of the item, no spaces, no special chars. Unique for each time/bbox combination. If several assets are provided for the same Item, can be several times in the CSV.
