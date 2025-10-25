import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _load_to_postgres(ti):
    output_csv = ti.xcom_pull(key="output_csv", task_ids="extract_pageviews_hour")
    filename = ti.xcom_pull(key="next_file", task_ids="get_next_file")
    if not output_csv:
        print("No file to load.")
        return
    
    df = pd.read_csv(output_csv)
    df["file_name"] = filename

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql("pageviews", con=engine, if_exists="append", index=False)
    print(f"Data from {filename} appended to Postgres table 'pageviews'")