services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always


docker run -it \
  -e POSTGRES_USER='root' \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB='ny_taxi' \
  -v c:/Users/HP/Desktop/PROJECT\ xED/Data/Projects/DE/dphi/docker/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name=pg-database \
postgres:13


https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL='tope@folorunso.com' \
  -e PGADMIN_DEFAULT_PASSWORD='root' \
  -p 8080:80 \
  --network=pg-network \
  --name=pgadmin \
dpage/pgadmin4

URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --url=${URL}


docker build -t taxi_ingest:v001 .

URL="http://172.25.224.1:8000/yellow_tripdata_2021-01.parquet"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}