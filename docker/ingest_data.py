import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'file.parquet'
    csv_name = 'output.csv'

    #download csv
    os.system(f'wget {url} -O {parquet_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write data to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)