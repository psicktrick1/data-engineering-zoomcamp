#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq
import pyarrow as pa

def ingest_callable(user, password, host, port, db, table_name, file_name):
    print(table_name, file_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()


    # reading first n rows from a parquet file into a dataframe
    pf = pq.ParquetFile(file_name)
    first_ten_rows = next(pf.iter_batches(batch_size=100))
    df = pa.Table.from_batches([first_ten_rows]).to_pandas()

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # creating an empty table first(will create a new table if table already exists)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    for batch in pf.iter_batches(batch_size = 100000):
        t_start = time()
        batch_df = batch.to_pandas()

        batch_df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        batch_df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))


