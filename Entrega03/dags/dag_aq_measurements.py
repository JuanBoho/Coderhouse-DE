from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import load_locations_data, load_aq_measuraments_data

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
        task_id="load_stations_data",
        python_callable=load_locations_data,
        op_kwargs={
        "config_file": "/opt/airflow/config/config.ini"
        }
    )

    load_aq_measuraments_data_task = PythonOperator(
        task_id="load_aq_measuraments_data",
        python_callable=load_aq_measuraments_data,
            op_kwargs={
        "config_file": "/opt/airflow/config/config.ini"
        }
    )

    create_tables_task >> load_locations_data_task >> load_aq_measuraments_data_task