import requests
import datetime
import os

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