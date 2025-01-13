import requests

def get_datasets():
    """
        Datasets can be obtained from an API endpoint.
        Returns a dictionary containing the GeoCubes datasets and their relevant information.
    """

    data = requests.get("https://vm0160.kaj.pouta.csc.fi/geocubes/info/getDatasets")
    const_url = "https://vm0160.kaj.pouta.csc.fi"
    raw_datasets = data.text.split(";")
    split_datasets = [x.split(",") for x in raw_datasets]

    dataset_dict = {}
    for split in split_datasets:
        dataset_dict[split[0]] = dict(zip(["name", "layername", "years", "folder", "file_prefix", "max_resolution", "bit_depth", "producer", "metadata_URL"], split))

    for d in dataset_dict:
        year_split = dataset_dict[d]['years'].split(".")
        dataset_dict[d]['paths'] = []
        if len(year_split) == 1:
            dataset_dict[d]['paths'].append(f"{const_url}{dataset_dict[d]['folder']}{year_split[0]}/")
        else:
            for year in year_split:
                dataset_dict[d]['paths'].append(f"{const_url}{dataset_dict[d]['folder']}{year}/")
    
    return dataset_dict