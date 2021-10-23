import yfinance as yf
import numpy as np
import pandas as pd
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import sys
from datetime import datetime

#get the ticker list from bigquery
bqclient = bigquery.Client()
query_str = """select ticker
from sources.tickers"""
ticker_array = (
    bqclient.query(query_str)
    .result()
    .to_dataframe()
)

#initiat empty list
df_list = list()

#loop thru the dataframe to flatten it with a ticker column
#to improve design, we could first get the lastest date from bigquery's ticket_data table so we only query the missing dates from the last time we last ran the script...

for ticker in ticker_array['ticker']:

    data = yf.download(ticker, group_by="Ticker", period='3y')
    data['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
    df_list.append(data)
    
# combine all dataframes into a single dataframe
df = pd.concat(df_list)

#lower case col names
df.columns= df.columns.str.lower()

#only select the adjusted close and ticker columns
df = df[['ticker', 'adj close']]
df.rename(columns={'adj close': 'adj_close'}, inplace=True)

#add loaded_at col
df['loaded_at'] = datetime.now()

# save to csv
df.to_csv('prices.csv')

#read file
df=pd.read_csv(r'prices.csv', parse_dates=['Date', 'loaded_at'])

#instantiate connection to bigquery
client = bigquery.Client()

#specify bigquery dataset
dataset_ref = client.dataset('sources')

#specify bigquery table
table_ref = dataset_ref.table('prices')

#delete prices table
bqclient = bigquery.Client()
query_str = """delete from sources.prices where true"""

bqclient.query(query_str)


#send it to bigquery
client.load_table_from_dataframe(df, table_ref).result()