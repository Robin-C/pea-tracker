import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import sys
from datetime import datetime

#read file
df=pd.read_csv('/my_app/extract_load_scripts/get_crypto_transactions/input/transactions.csv', parse_dates=['date_transaction'], dayfirst=True)

# CSV to be filled manually: 
#    coin,date_transaction,quantity,price
#    ETH,04/05/2021,10,265


# add loaded_at col
df['loaded_at'] = datetime.now()

df['was_farmed'] = df['was_farmed'].astype('bool')
df['was_airdropped'] = df['was_airdropped'].astype('bool')

# instantiate connection to bigquery
client = bigquery.Client()

# specify bigquery dataset
dataset_name = 'sources'
dataset_ref = client.dataset(dataset_name)

# specify bigquery table
table_name = 'crypto_transactions'
table_ref = dataset_ref.table(table_name)

# delete table
query_str = """delete from sources.crypto_transactions where true"""
ticker_array = (
    client.query(query_str)
)

# df.shape[0] gives number of rows while [1] gives number of columns
count_row = df.shape[0]

# send it to bigquery
client.load_table_from_dataframe(df, table_ref).result()

# print result
print(f"sucessfully loaded {count_row} transactions to dataset {dataset_name} , table {table_name}.")