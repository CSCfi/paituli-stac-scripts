# Paituli STAC scripts

Scripts to create STAC Collections from different data sources. PyTest tests contained in the `tests` folder.

## Creating local STAC files

### Paituli

Run `paituli_to_stac.py` to create the Catalog and Collections. The script requires that you give the database port as an argument with `--port` and the database host address with `--db_host`. You can also provide the database password with `--pwd`, and if you want to only create specific collections, use `--collections`.
```bash
python paituli_to_stac.py --port <DB-port> --db_host <Database host address>
```

Run `add_puhti_assets.py` to add assets to each Item which have the local Puhti HREF. You need to provide the collection ID with `--collection` and the GeoServer host with `--host`.
```bash
python add_puhti_assets.py --host <Host address> --collection <Collection ID>
```

Run `update_paituli_stac.py` to update collection/s. Multiple collections can be given with the `--collections`, but atleast one needs to be given. The host address is given via `--host`. Give the database host address with `--db_host`. The DB port can be given with `--port` or with additional input. Using the `--local` flag, the script checks the local files for new files. Using the `--add_puhti` flag, the script will add Puhti assets for the new Items. Using the `--update_extents` flag, the script will update the Collection Extents even if no Items were added.
```bash
python update_paituli_stac.py --port <DB-port> --db_host <Database host address> --host <Host address> --collections <Collection ID>
```

### Sentinel

For getting the buckets through the boto3, you need read access to the CSC Project they are located in. Using the scripts on Linux, you need the allas_conf script for accessing Allas. The two CSV-files contain the buckets from these two CSC projects.

Create the local STAC files with:
```sh
python sentinel_to_stac.py
```

The update script is run with the selected host address.
```sh
python update_allas_sentinel.py --host <host-address>
```

### FMI

To turn the FMI's static STAC files into a local STAC Catalog:
```sh
python fmi_to_stac.py
```

The update script is these above two scripts combined without needing to save the STAC Catalog locally. To run the update script, you need to provide the host address through the `--host` argument and the GeoServer password via a password-file, through `--pwd` argument or the script will ask it afterwards.
```sh
python update_fmi.py --host <host address> --pwd <GeoServer password>
```

### GeoCubes

The collection information and translations are located in [karttatasot.csv](files/karttatasot.csv). If new datasets are added to GeoCubes, the translations of these datasets need to be added to `karttatasot.csv` before the script takes them into account.

Run `geocubes_stac.py` to turn the GeoCubes into STAC
```bash
python geocubes_stac.py 
```

Run `update_geocubes.py` to update the GeoCubes collections in the selected host. Provide the host address as an argument.
```bash
python update_geocubes.py --host <update-host-address>
```

The `check_new_datasets.py` script checks if there's any new datasets in GeoCubes.
```bash
python check_new_datasets.py --host <host-address-to-compare-against>
```

## Uploading local Catalog

Run `stac_to_geoserver.py` to upload the created Collections to GeoServer. You need to provide the GeoServer host with `--host` and the collection ID with `--collection`. You can also give the local Catalog folder name with `--catalog` or the script will ask it when run.
```bash
python stac_to_geoserver.py --host <Host address> --catalog <Catalog folder name> --collection <Collection ID>
```


## Testing

Run the PyTest tests from the tests-folder with the Collection ID and host address provided with `--collection` and `--host`:
```bash
pytest ./tests --collection <Collection ID to test> --host <Host address>
```