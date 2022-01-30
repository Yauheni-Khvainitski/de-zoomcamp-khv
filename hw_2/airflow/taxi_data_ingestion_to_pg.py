from airflow import DAG
from datetime import datetime
import os
from airflow.hooks.base import BaseHook

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from modules.ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = BaseHook.get_connection("postgres").host
PG_PORT = BaseHook.get_connection("postgres").port
PG_USER = BaseHook.get_connection("postgres").login
PG_PASSWORD = BaseHook.get_connection("postgres").password
PG_DATABASE = 'ny_taxi'

def test_task():
    pg_port = BaseHook.get_connection("postgres").port
    print(pg_port)
    return pg_port

default_args = {
    "owner": "ykhvainitski",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1
}

with DAG (
    dag_id="taxi_data_ingest_to_pg",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["de-zoomcamp"]
) as dag:

    URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
    FILE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
    URL_TEMPLATE = URL_PREFIX + FILE
    OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_' + FILE
    TABLE_NAME_TEMPLATE = TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

    download_csv = BashOperator(
        task_id="download_csv",
        bash_command=f'curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_to_pg = PythonOperator(
        task_id="ingest_to_pg",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    download_csv >> ingest_to_pg
