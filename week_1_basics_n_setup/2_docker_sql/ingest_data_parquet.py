#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq
import pyarrow as pa

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file_name = 'output.parquet'

    #downloading data
    os.system(f"wget {url} -O {parquet_file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # reading first n rows from a parquet file into a dataframe
    pf = pq.ParquetFile(parquet_file_name)
    first_ten_rows = next(pf.iter_batches(batch_size=100))
    df = pa.Table.from_batches([first_ten_rows]).to_pandas()

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # creating an empty table first(will create a new table if table already exists)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    for batch in pf.iter_batches(batch_size = 100000):  # batch_zize = 100000
        t_start = time()
        batch_df = batch.to_pandas()

        batch_df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        batch_df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)




# $URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
#
# python ingest_data_par.py \
#     --user=root \
#     --password=root \
#     --host=localhost \
#     --port=5432 \
#     --db=ny_taxi \
#     --table_name=yellow_taxi_data \
#     --url=$URL
#
#
#
# sudo docker build -t taxi_ingest:v001 .
#
# URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
# sudo docker run -it \
#     --network=pg-network \
#     taxi_ingest:v001 \
#         --user=root \
#         --password=root \
#         --host=pg-database \
#         --port=5432 \
#         --db=ny_taxi \
#         --table_name=yellow_taxi_data \
#         --url=$URL

