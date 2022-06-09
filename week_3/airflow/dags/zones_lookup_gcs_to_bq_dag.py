import os

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="load_zones_lookup_data_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dphi-de-bc'],
    ) as dag:

    gcs_to_bq_table = BigQueryCreateExternalTableOperator(
        task_id=f"load_bigquery_zones_lookup_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "zones_lookup_table",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/raw/zones_lookup/*"],
            },
        },
    )

    gcs_to_bq_table