import gzip
import os
#import logging 
import csv
import requests
from pendulum import datetime
#from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import DAG

outdir = "/opt/airflow/dags/data"
companies = ["amazon", "apple", "facebook", "google", "microsoft"]
filename = "pageviews-20251006-150000"
url = f"https://dumps.wikimedia.org/other/pageviews/2025/2025-10/{filename}.gz"
output_file = f"{outdir}/{filename}.csv"


def _extract_page_per_hour():


    print("Downloading file")
    response = requests.get(url)
    with open(f"{outdir}/{filename}.gz", "wb") as f:
        f.write(response.content)
    print("Download complete.")

    
    print("Extracting fields")
    with gzip.open(f"{outdir}/{filename}.gz", "rt", encoding="utf-8") as file, \
        open(output_file, "w", newline="", encoding="utf-8") as csvfile:

        writer = csv.writer(csvfile)
        writer.writerow(["page_name", "views"])

        for line in file:
            parts = line.strip().split(" ")
            if len(parts) >= 3:
                # Example: "" Category:Introduction/hi 1 0
                page_name = parts[1].lower()
                views = parts[2]
                if any(company in page_name for company in companies):
                    writer.writerow([page_name, views])

    print(f" Extraction complete! Saved to {output_file}")

def _load_to_postgres():

    df = pd.read_csv(f"{outdir}/{filename}.csv")

    # Connect to Postgres via Airflow Connection
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Load into Postgres
    df.to_sql("pageviews", con=engine, if_exists="replace", index=False)

    print("Data successfully loaded into Postgres (table: pageviews)")


with DAG(
    dag_id="core-sentiment",
    start_date=datetime(2025,10,21),
    description="Extracts hourly wikipedia pageviews for selected companies",
    schedule=None
):


    extract_page_per_hour = PythonOperator(
        task_id="extract_pageviews_hour",
        python_callable=_extract_page_per_hour,
    )

    load_to_postgres = PythonOperator(
    task_id="load_to_postgres",
    python_callable=_load_to_postgres
    )
    
    load_to_postgres