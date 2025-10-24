import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _get_top_company(ti):
    query = """
        select case when lower(page_name) like '%amazon%' then 'amazon'
        when lower(page_name) like '%apple%' then 'apple'
        when lower(page_name) like '%facebook%' then 'facebook'
        when lower(page_name) like '%google%' then 'google'
        when lower(page_name) like '%microsoft%' then 'microsoft'
        else 'other'
        end as company,
        sum(cast(views as integer)) as total_views
        from pageviews
        group by company
        order by total_views desc
        limit 1;
        """

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    top_company = cursor.fetchone()

    if top_company:
        company, total_views = top_company
        print(f"Top company: {company} with {total_views} total views.")
        ti.xcom_push(key="top_company", value=company)
        ti.xcom_push(key="views", value=total_views)
    else:
        print("No data found.")