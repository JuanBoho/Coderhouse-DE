from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import AirQualityETL


LOCATION_IDS = {
    "Argentina": 403227,
    "Chile": 229724,
    "Brasil": 1638491,
    "Bolivia": 1369322,
    "Peru": 287700,
    "Ecuador": 234583,
    "Colombia": 228248,
}
SCHEMA = "juanbohorquez_ar_coderhouse"

# Report email recipients
RECIPIENTS = ["python.demo.fastapi@gmail.com"]

aq_etl = AirQualityETL(SCHEMA, LOCATION_IDS, "/opt/airflow/config/config.ini")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_aq_measurements",
    catchup=False,
    start_date=datetime(2023,12,5),
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift_coder",
        sql="sql/create_tables.sql",
        hook_params={	
            "options": "-c search_path=juanbohorquez_ar_coderhouse"
        }
    )

    load_locations_data_task = PythonOperator(
        task_id="load_locations_data",
        python_callable=aq_etl.load_locations_data
    )

    load_aq_measuraments_data_task = PythonOperator(
        task_id="load_aq_measuraments_data",
        python_callable=aq_etl.load_aq_measurements_data
    )

    build_and_email_report = PythonOperator(
        task_id="build_and_email_report",
        python_callable=aq_etl.build_and_email_report,
            op_kwargs={
        "recipients": RECIPIENTS
        }
    )

    create_tables_task >> load_locations_data_task >> load_aq_measuraments_data_task >> build_and_email_report