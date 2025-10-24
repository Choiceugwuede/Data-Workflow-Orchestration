import requests
import os
import gzip
import csv


base_url = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"
outdir = "/opt/airflow/dags/data"
companies = ["amazon", "apple", "facebook", "google", "microsoft"]


os.makedirs(outdir, exist_ok=True)


def _extract_page_per_hour(ti):
    filename = ti.xcom_pull(key="next_file", task_ids="get_next_file")
    if not filename:
        print("No new file to process")
        return

    url = f"{base_url}{filename}.gz"
    output_csv = f"{outdir}/{filename}.csv"

    print(f"Downloading {url}")
    response = requests.get(url)
    gz_path = f"{outdir}/{filename}.gz"
    with open(gz_path, "wb") as f:
        f.write(response.content)
    print("Download complete.")

    print("Extracting requested fields")
    with gzip.open(gz_path, "rt", encoding="utf-8") as file, open(
        output_csv, "w", newline="", encoding="utf-8"
    ) as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["page_name", "views"])

        for line in file:
            parts = line.strip().split(" ")
            if len(parts) >= 3:
                page_name = parts[1].lower()
                views = parts[2]
                if any(c in page_name for c in companies):
                    writer.writerow([page_name, views])

    print(f"Extraction is done o. Saved to {output_csv}")
    ti.xcom_push(key="output_csv", value=output_csv)