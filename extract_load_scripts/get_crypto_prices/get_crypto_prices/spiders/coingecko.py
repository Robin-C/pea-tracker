import scrapy
from datetime import date, datetime
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import pandas as pd

class pmuSpider(scrapy.Spider):
    name = 'coingecko'

    def __init__(self):
      self.rows = list()  # That list will be used to store prices before being injected in a dataframe
      self.bqclient = bigquery.Client()
      # Get the coin list from bigquery
      self.query_str = """select coinGecko_name
        from sources.coins"""
      self.response = (
            self.bqclient.query(self.query_str)
            .result()
        )
      self.urls = list()  # Create list to compile url list to be scraped
      self.base_url = 'https://www.coingecko.com/en/coins/'

    def start_requests(self):
        for coin in self.response:
          self.urls.append(
            { 'url': self.base_url + coin.get('coinGecko_name'),
            'coinGecko_name': coin.get('coinGecko_name')}
          )
        for url in self.urls:
          yield scrapy.Request(url=url['url'], callback=self.parse, meta={'coinGecko_name': url['coinGecko_name']})

    def parse(self, response):
      price = float(response.xpath('//html/body/div[4]/div[4]/div[1]/div/div[1]/div[4]/div/div[1]/span[1]/span/text()').get().replace('$', '').replace(',',''))
      coinGecko_name = response.meta['coinGecko_name']
      date_of_the_day = datetime.today().strftime('%Y-%m-%d')
      loaded_at = datetime.now()
      self.rows.append([date_of_the_day, coinGecko_name, price, loaded_at])

    def closed(self, reason):
      if reason == 'finished':
        self.bqclient.query("""delete from sources.prices_crypto where true""" ) # Delete table
        dataset_ref = self.bqclient.dataset('sources')
        table_ref = dataset_ref.table('prices_crypto')
        df = pd.DataFrame(self.rows, columns =['date', 'coinGecko_name', 'price', 'loaded_at'])
        df['date'] = pd.to_datetime(df["date"]).dt.date  # Transform col to date type
        self.bqclient.load_table_from_dataframe(df, table_ref).result()  # Send it to BQ