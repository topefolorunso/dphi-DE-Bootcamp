import os
import pandas as pd

from google.cloud import storage
from sqlalchemy import create_engine
from time import time

def ingest_to_postgres(user, password, host, port, db, table_name, parquet_name, csv_name):

    print(table_name, csv_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    parquet_file = pd.read_parquet(parquet_name)
    parquet_file.to_csv(csv_name, index=False)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    t_start = time()

    try:
        while True:
            loop_start = time()

            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            loop_end = time()

            print('inserted another chunk..., took %.3f seconds' % (loop_end - loop_start))
    except StopIteration:
        t_end = time()
        
        print('completed data ingestion..., took %.3f minutes' % ((t_end - t_start)/60))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)