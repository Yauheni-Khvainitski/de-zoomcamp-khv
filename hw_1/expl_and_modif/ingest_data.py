#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import sys


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = table_name + '_output.csv'

    os.system(f"curl -o {csv_name} '{url}'")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('Success! Connected to postgres')

    if table_name == 'yellow_taxi_data':
        print(f'Loading {table_name}')

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        print(f'Success! Created table {table_name}')

        df.to_sql(name=table_name, con=engine, if_exists='append')
        print(f'Success! Appended the first chunk')

        while True:
            try:
                t_start = time()

                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))
            except StopIteration as e:
                print(e, 'End of iterations! Data should be loaded succesfully! Check db!')
                break
    elif table_name == 'taxi_zone_lookup':
        print(f'Loading {table_name}')

        df = pd.read_csv(csv_name)

        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    else:
        print(f'There is no config for such table{table_name}. Exit execution')
        sys.exit(1)





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument('--user', help='pg user name')
    parser.add_argument('--password', help='pg user pass')
    parser.add_argument('--host', help='pg host')
    parser.add_argument('--port', help='pg port')
    parser.add_argument('--db', help='pg db name')
    parser.add_argument('--table_name', help='target table to load data')
    parser.add_argument('--url', help='CSV url')

    args = parser.parse_args()

    main(args)
