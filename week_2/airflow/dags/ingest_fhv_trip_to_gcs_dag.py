import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from datetime import datetime
from ingestion_script import upload_to_gcs

present_month = "{{ execution_date.strftime(\'%Y-%m\') }}"
present_year = "{{ execution_date.strftime(\'%Y\') }}"

url_prefix = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
url = url_prefix + f'fhv_tripdata_{present_month}.parquet'

local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_name = f"fhv_trips_{present_month}.parquet"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

ingestion_workflow = DAG(
    "FHV-GCP-Ingestion-DAG",
    schedule_interval="0 0 1 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    max_active_runs=3,
    catchup=True,
    default_args = {
    "retries": 1
    }
)

with ingestion_workflow:

    download_task = BashOperator(
        task_id='curl_dataset',
        bash_command=f'curl -sSL {url} > {local_home}/{parquet_name}'
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/fhv_trips/{present_year}/{parquet_name}",
            "local_file": f"{local_home}/{parquet_name}",
        },
    )

    remove_task = BashOperator(
        task_id='delete_dataset',
        bash_command=f'rm {local_home}/{parquet_name}'
    )

    download_task >> upload_to_gcs_task >> remove_task