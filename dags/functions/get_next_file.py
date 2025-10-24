from bs4 import BeautifulSoup
import os
import requests



base_url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"
outdir = "/opt/airflow/dags/data"
state_file = f"{outdir}/last_processed.txt"


def _get_next_file(ti):
    """Fetch the next unprocessed pageviews file from our url"""
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract filenames, search for the links, remove the .gz extension
    files = [
        a["href"].replace(".gz", "")
        for a in soup.find_all("a")
        if a["href"].endswith(".gz")
    ]
    files.sort()  

    # 
    last_processed = None
    if os.path.exists(state_file):
        with open(state_file) as f:
            last_processed = f.read().strip()

    # Pick the next file after the last processed file and avoid picking an already loaded file
    if last_processed and last_processed in files:
        file_num = files.index(last_processed)
        next_file = files[file_num + 1] if file_num + 1 < len(files) else None
    else:
        next_file = files[0] if files else None

    
    # if it has gone through the list, it would process nothing unless we change the date.
    if not next_file:
        print("No new files to process.")
        return None

    # save file being proceesed
    with open(state_file, "w") as f:
        f.write(next_file)
    
    ti.xcom_push(key="next_file", value=next_file)
    return next_file