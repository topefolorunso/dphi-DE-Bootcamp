import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from datetime import datetime
from ingestion_script import upload_to_gcs

url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csv_name = f"zones_lookup_table.csv"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

ingestion_workflow = DAG(
    "Zones-Lookup-GCP-Ingestion-DAG",
    schedule_interval="@once",
    start_date=days_ago(1)
)

with ingestion_workflow:

    download_task = BashOperator(
        task_id='curl_dataset',
        bash_command=f'curl -sSL {url} > {local_home}/{csv_name}'
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/zones_lookup/{csv_name}",
            "local_file": f"{local_home}/{csv_name}",
        },
    )

    remove_task = BashOperator(
        task_id='delete_dataset',
        bash_command=f'rm {local_home}/{csv_name}'
    )

    download_task >> upload_to_gcs_task >> remove_task