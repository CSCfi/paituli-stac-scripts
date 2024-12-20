from pystac import Item

def retry_errors(list_of_items, list_of_errors):
    """
    Function to retry retrieving the items that were timed out during the process.
    
    list_of_items - List containing the STAC items from the source. The errored items will be appended to this list when successfully retrieved
    list_of_errors - List of links to the items that timed out during the retrieving process. Function will run until this list is empty
    """

    print(" * Trying to add items that timedout")
    while len(list_of_errors) > 0:
        for item in list_of_errors:
            try:
                list_of_items.append(Item.from_file(item))
                print(f" * Listed {item}")
                list_of_errors.remove(item)
            except Exception as e:
                print(f" ! {e} on {item}")

    return