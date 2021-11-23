import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import sys
from datetime import datetime

# path needs to be absolute so it gets executed correctly by the airflow worker. For some reason..

# read file
df=pd.read_csv('/my_app/extract_load_scripts/get_crypto_coins/input/coin_list.csv')

# add loaded_at col
df['loaded_at'] = datetime.now()

# set is_benchmark and is_farmed as booleans
df['is_benchmark'] = df['is_benchmark'].astype('bool')
df['is_benchmark'] = df['is_farmed'].astype('bool')

# instantiate connection to bigquery
client = bigquery.Client()

# specify bigquery dataset
dataset_ref = client.dataset('sources')

# specify bigquery table
table_ref = dataset_ref.table('coins')

# delete table
query_str = """delete from sources.coins where true"""
ticker_array = (
    client.query(query_str)
)

# send it to bigquery
client.load_table_from_dataframe(df, table_ref).result()