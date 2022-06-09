import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from datetime import datetime
from ingestion_script import upload_to_gcs

present_month = "{{ execution_date.strftime(\'%Y-%m\') }}"
present_year = "{{ execution_date.strftime(\'%Y\') }}"

local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

url_prefix = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'



ingestion_workflow = DAG(
    f"Taxi-Trips-GCP-Ingestion-DAG",
    schedule_interval="0 0 1 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 1),
    max_active_runs=3,
    catchup=True,
    default_args = {
    "retries": 1
    }
)

with ingestion_workflow:

    for color in ['yellow', 'green']:
        
        url = url_prefix + f'{color}_tripdata_{present_month}.parquet'
        parquet_name = f"{color}_trips_{present_month}.parquet"

        download_task = BashOperator(
            task_id=f'curl_{color}_dataset',
            bash_command=f'curl -sSL {url} > {local_home}/{parquet_name}'
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        upload_to_gcs_task = PythonOperator(
            task_id=f"upload_{color}_data_to_gcs",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{color}_trips/{present_year}/{parquet_name}",
                "local_file": f"{local_home}/{parquet_name}",
            },
        )

        remove_task = BashOperator(
            task_id=f'delete_{color}_dataset',
            bash_command=f'rm {local_home}/{parquet_name}'
        )

        download_task >> upload_to_gcs_task >> remove_task