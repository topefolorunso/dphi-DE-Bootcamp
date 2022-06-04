import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from ingest_script import ingest_callable

present_month = "{{ execution_date.strftime(\'%Y-%m\') }}"

url_prefix = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
url = url_prefix + f'fhv_tripdata_{present_month}.parquet'

local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

parquet_name = f"file_{present_month}.parquet"
csv_name = f"file_{present_month}.csv"

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

table_name = f"fhv_trip_record_{present_month}"

ingestion_workflow = DAG(
    "PostgresIngestionDAG",
    schedule_interval="0 16 28 * *",
    start_date=datetime(2022, 1, 1)
)

with ingestion_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {url} > {local_home}/{parquet_name}'
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs={
            "user": PG_USER,
            'password': PG_PASSWORD,
            'host': PG_HOST,
            'port': PG_PORT,
            "db": PG_DATABASE,
            'table_name': table_name,
            "parquet_name": parquet_name,
            "csv_name": csv_name
        },
    )

    wget_task >> ingest_task