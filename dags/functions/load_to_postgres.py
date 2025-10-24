import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _load_to_postgres(ti):
    output_csv = ti.xcom_pull(key="output_csv", task_ids="extract_pageviews_hour")
    if not output_csv:
        print("No file to load.")
        return
    
    df = pd.read_csv(output_csv)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql("pageviews", con=engine, if_exists="append", index=False)
    print("Data appended to Postgres table 'pageviews'")