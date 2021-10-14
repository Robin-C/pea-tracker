import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import sys
from datetime import datetime

#path needs to be absolute so it gets executed correctly by the worker. For some reason..

#open xlsx file
read_file = pd.read_excel ('/my_app/extract_load_scripts/get_tickers/input/ticker_list.xlsx')

#save file to csv
read_file.to_csv ('/my_app/extract_load_scripts/get_tickers/output/ticker_list.csv', index = None, header=True)

#read file
df=pd.read_csv('/my_app/extract_load_scripts/get_tickers/output/ticker_list.csv')

#add loaded_at col
df['loaded_at'] = datetime.now()

#instantiate connection to bigquery
client = bigquery.Client()

#specify bigquery dataset
dataset_ref = client.dataset('sources')

#specify bigquery table
table_ref = dataset_ref.table('tickers')

#instantiate bq client
bqclient = bigquery.Client()

#delete table
query_str = """delete from sources.tickers where true"""
ticker_array = (
    bqclient.query(query_str)
)

#send it to bigquery
client.load_table_from_dataframe(df, table_ref).result()