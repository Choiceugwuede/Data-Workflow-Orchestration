from functions.get_top_company import _get_top_company
from functions.get_next_file import _get_next_file
from functions.load_to_postgres import _load_to_postgres
from functions.extract_page_per_hour import _extract_page_per_hour
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import DAG




with DAG(
    dag_id="core-sentiment-V2",
    start_date=datetime(2025, 10, 21),
    schedule="@daily",
    description="Dynmaically Extracts hourly wikipedia pageviews for selected companies",
):

    get_next_file = PythonOperator(
        task_id="get_next_file",
        python_callable=_get_next_file
    )

    extract_page_per_hour = PythonOperator(
        task_id="extract_pageviews_hour",
        python_callable=_extract_page_per_hour,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load_to_postgres,
    )

    get_top_company = PythonOperator(
        task_id="get_top_company",
        python_callable=_get_top_company,
    )

    send_notification = EmailOperator(
        task_id="send_notification",
        to="chidiogougwuede@gmail.com",
        subject="Sentiment Report for hour: {{ti.xcom_pull(task_ids='get_next_file', key='hour')}}",
        html_content="""
        <h3>Hourly Wikipedia Sentiment Report</h3>
        <p>Top Company: <b>{{ti.xcom_pull(task_ids='get_top_company', key='top_company')}}</b></p>
        <p>Number of Views: <b>{{ti.xcom_pull(task_ids='get_top_company', key='views')}}</b></p>
        <p>Kind Regards, </p>
        <p>CDE Airflow team</p>
        """,
        conn_id="smtp_connect"
    )

    get_next_file >> extract_page_per_hour >> load_to_postgres >> get_top_company >> send_notification
