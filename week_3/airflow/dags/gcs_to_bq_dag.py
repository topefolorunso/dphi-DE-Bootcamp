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
    dag_id="gcs_load_data_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dphi-de-bc'],
    ) as dag:

    for year in ('2019', '2020'):

        gcs_to_bq_table = BigQueryCreateExternalTableOperator(
            task_id=f"load_bigquery_yellow_{year}_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"yellow_trips_data_{year}",
                },
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/yellow_trips/{year}/*"],
                },
            },
        )

        CREATE_PARTITION_TABLE_QUERY = \
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_trips_data_{year}_partitioned \
            PARTITION BY DATE(tpep_pickup_datetime) AS \
            SELECT * FROM {BIGQUERY_DATASET}.yellow_trips_data_{year}"
        
        partition_bq_table = BigQueryInsertJobOperator(
            task_id=f"partition_bq_yellow_{year}_table_task",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE_QUERY,
                    "useLegacySql": False,
                }
            },
        )

        gcs_to_bq_table >> partition_bq_table
    
    #for fhv trips
    gcs_to_bq_table = BigQueryCreateExternalTableOperator(
        task_id="load_bigquery_fhv_2019_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "fhv_trips_data_2019",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/fhv_trips/2019/*"],
            },
        },
    )

    CREATE_PARTITION_TABLE_QUERY = \
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.fhv_trips_data_2019_partitioned \
        PARTITION BY DATE(pickup_datetime) AS \
        SELECT * FROM {BIGQUERY_DATASET}.fhv_trips_data_2019"
    
    partition_bq_table = BigQueryInsertJobOperator(
        task_id="partition_bq_fhv_2019_table_task",
        configuration={
            "query": {
                "query": CREATE_PARTITION_TABLE_QUERY,
                "useLegacySql": False,
            }
        },
    )

    gcs_to_bq_table >> partition_bq_table

    #for zones lookup table
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

    gcs_to_bq_table``