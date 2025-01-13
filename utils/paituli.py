import requests
import datetime
import os
import re

from bs4 import BeautifulSoup

def recursive_filecheck(url: str, links: list[str], recursive_links: list) -> list:

    """
        Goes through the files in the given URL and recursively checks them for appropriate files
    """

    for link in links:
        if link["href"].startswith("?") or link["href"].startswith("/"):
            continue
        else:
            if link["href"].endswith("/"):
                page = requests.get(url+link["href"])
                data = page.text
                soup = BeautifulSoup(data, features="html.parser")
                for a in soup.find_all("a"):
                    if a["href"].startswith("?") or a["href"].startswith("/"):
                        continue
                    else:
                        a["href"] = link["href"] + a["href"]
                        links.append(a)
                links.remove(link)
                recursive_filecheck(url+link["href"], links, recursive_links)
            else:
                recursive_links.append(link)
    
    return recursive_links

def get_new_local_files() -> list:

    """
        Goes through the given directory and returns the files that have been modified recently (30 days).
    """

    new_paths = []
    directory = "/geodata/"
    target_date = datetime.datetime.now() - datetime.timedelta(days=30)

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            modification_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            split_path = file_path.split(".")
            if modification_time.date() >= target_date.date() and len(split_path) >= 2:
                modified_path = split_path[-2].replace(directory.replace(".", ""), "")
                new_paths.append(modified_path)

    return new_paths


def generate_timestamps(path: str, data_dict: dict, label: str | None) -> dict:

    """
    A bunch of different scenarios for timestamps:
     - If dataset has only the latest data, get the Last-Modified header from the file
     - If the database year has a span of multiple years, set the timespan accordingly
     - If the dataset contains monthly data, get the year and month from filename
     - Else, try to get the year from the full path of the file
     - Lastly, if the path does not contain the year, take the last year from the database year-field
    """

    year_pattern = r'(19\d{2}(?![\d_])|20\d{2}(?![\d_])|21\d{2}(?![\d_]))(-(?=\d{4}))?'
    number_pattern = r'^\d+$'
    label_pattern = r'(?<=\()18\d{2}(?=\))|(?<=\()19\d{2}(?=\))|(?<=\()20\d{2}(?=\))'
    
    # National Land Survey of Finland old maps has the year in the label
    if label:
        check_label = re.search(label_pattern, label)
    else:
        check_label = None

    if "nls_digital_elevation_model_2m" in data_dict['stac_id']: # This gets the time for 2m DEM, there's no better alternative
        resp = requests.head(path)
        modified = resp.headers["Last-Modified"]
        item_starttime = datetime.datetime.strptime(modified, "%a, %d %b %Y %H:%M:%S %Z")
        item_endtime = datetime.datetime.strptime(modified, "%a, %d %b %Y %H:%M:%S %Z")
        item_date = item_starttime.year
    elif "nls_topographic_map_42k" in data_dict['stac_id'] and "x" in data_dict['year']: #Some datasets have years as 192x
        item_starttime = datetime.datetime.strptime(f"1920-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"1930-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{item_starttime.year}_{item_endtime.year}"
    elif check_label: # If year in label, use that
        label_year = check_label.group(0)
        item_starttime = datetime.datetime.strptime(f"{label_year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{label_year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = label_year
    elif label and "(-)" in label and data_dict["org_eng"] == "National Land Survey of Finland": # If label year is blank, the year is unknown
        split_year = data_dict["year"].split("-")
        item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{split_year[0]}_{split_year[1]}"
    elif "-" not in data_dict["year"]: # If only one year in dataset, use that
        item_starttime = datetime.datetime.strptime(f"{data_dict['year']}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{data_dict['year']}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = data_dict["year"]
    elif "snow_load_on_trees" in data_dict["stac_id"]: # filename is type rcp**{startyear}{endyear}
        split_file = path.split("/")[-1].split(".")[0]
        start_year = split_file[-8:-4]
        end_year = split_file[-4:]
        item_starttime = datetime.datetime.strptime(f"{start_year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{end_year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{start_year}_{end_year}"
    elif "predictions" in data_dict["stac_id"]:
        # There are some datasets with parantheses in the years column
        if len(data_dict['year'].split('(')) > 1: # Monthly mean precipitation and temperature predictions
            split_fix = data_dict['year'].split('(')[0].strip()
            split_years = split_fix.split('-')
        else:
            split_years = data_dict['year'].split('-')
        item_starttime = datetime.datetime.strptime(f"{split_years[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_endtime = datetime.datetime.strptime(f"{split_years[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        item_date = f"{split_years[0]}_{split_years[1]}"
    elif "monthly_avg" in data_dict["stac_id"] or "monthly_precipitation_1km" in data_dict["stac_id"]:
        split_file = path.split("/")[-1].split(".")[0].split("_")
        for split in split_file:
            match = re.search(number_pattern, split)
            if match:
                numbers = match.group(0)
                item_starttime = datetime.datetime.strptime(f"{numbers}-01", "%Y%m-%d")
                # Calculate the last day of the corresponding month
                first_day_of_next_month = item_starttime + datetime.timedelta(days=32)
                lastday = first_day_of_next_month - datetime.timedelta(days=first_day_of_next_month.day)
                item_endtime = datetime.datetime.strptime(f"{numbers}-{lastday.day}", "%Y%m-%d")
                item_date = numbers
    else:
        match = re.search(year_pattern, path)
        # Some HY SPECTRE data items have the publication date in the path and not the data date
        if match and not data_dict['stac_id'].startswith("hy_spectre"):
            if match.group(1) not in data_dict['stac_id']:
                year = match.group(1)
                item_starttime = datetime.datetime.strptime(f"{year}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_endtime = datetime.datetime.strptime(f"{year}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_date = year
            else:
                split_year = data_dict["year"].split("-")
                item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
                item_date = f"{split_year[0]}_{split_year[1]}"
        else:
            split_year = data_dict["year"].split("-")
            item_starttime = datetime.datetime.strptime(f"{split_year[0]}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            item_endtime = datetime.datetime.strptime(f"{split_year[1]}-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")
            item_date = f"{split_year[0]}_{split_year[1]}"

    item_timestamps = {
        "item_start_time": item_starttime,
        "item_end_time": item_endtime,
        "item_date": item_date
    }

    return item_timestamps

def generate_item_id(path: str, data_dict: dict, item_date: str, label: str | None) -> str:

    """
        Generate the Item IDs from the given information. 
        This was done mainly through trial and error when any given dataset was widely different from previous ones.
        If new datasets give wrong or similar IDs for different items, feel free to add new rules
    """

    # This specific dataset has an incomplete filepath in the dataset
    if "ei_kkayria" in path and item_date == "2006":
        split_path = path.split(".")
        path = ".".join(split_path[:-1]) + "_RK2_2.tif"

    if label:
        if label == item_date:
            item_id = f"{data_dict['stac_id']}_{label}"
        else:
            item_id = f"{data_dict['stac_id']}_{label}_{item_date}"
    else:
        item_name = path.split("/")[-1].split(".")[0].split("_")[0].lower()
        item_name = item_name.replace("-", "_")
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"

    # For orthoimages, construct a different ID, which includes the dataset, elevation model, and the version number
    if "orthoimage" in data_dict['stac_id']:
        if label:
            item_id = f"{data_dict['stac_id']}_{label}_{item_date}_{path.split('/')[-6]}_{path.split('/')[-3]}_{path.split('/')[-2]}"
        else:
            leaf = path.split("/")[-1].split(".")[0]
            item_id = f"{data_dict['stac_id']}_{leaf}_{item_date}_{path.split('/')[-6]}_{path.split('/')[-3]}_{path.split('/')[-2]}"
    elif "general_map" in data_dict['stac_id']:
        split = path.split(".")[-2].split("_")
        item_name = "".join(split)
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"
    elif "predictions" in data_dict["stac_id"] and "monthly" in data_dict["stac_id"]:
        split = path.split("/")[-1].split(".")[-2].split("_")
        item_name = "_".join(split[0:3])
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"
    elif data_dict["stac_id"].startswith("hy"):
        item_id = f"{data_dict['stac_id']}_{item_date}"
    elif "nls_topographic_map_42k" in data_dict['stac_id'] and "x" in data_dict['year']:
        item_name = path.split("/")[-1].split(".")[0].split("_")[0].lower()
        item_name = item_name.replace("-", "_")
        item_id = f"{data_dict['stac_id']}_{item_name}_{item_date}"

    # Some IDs have periods in them, remove them
    if "." in item_id:
        item_id = item_id.replace(".", "")
    # Something fishy going with label having / in it even though it doesn't show up in database
    elif "/" in item_id:
        split = item_id.split("_")
        label_split = split[-2]
        fix = label_split.split("/")[-1]
        item_id = item_id.replace(label_split, fix)

    return item_id