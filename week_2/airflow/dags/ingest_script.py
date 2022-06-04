import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def ingest_callable(user, password, host, port, db, table_name, parquet_name, csv_name):

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