import scrapy
from datetime import date, datetime
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
import pandas as pd

class pmuSpider(scrapy.Spider):
    name = "coingecko"
    rows = list()
    bqclient = bigquery.Client()

    def start_requests(self):
        # Get the coin list from bigquery
        query_str = """select coinGecko_name
        from sources.coins"""
        response = (
            self.bqclient.query(query_str)
            .result()
        )

        urls = list()  # Create list to compile url list to be scraped
        base_url = 'https://www.coingecko.com/en/coins/'
 
        for coin in response:
          urls.append(
            { 'url': base_url + coin.get('coinGecko_name'),
            'coin': coin.get('coinGecko_name')}
          )

        for url in urls:
          yield scrapy.Request(url=url['url'], callback=self.parse, meta={'coin': url['coin']})

    def parse(self, response):
      price = response.xpath('//html/body/div[4]/div[4]/div[1]/div/div[1]/div[4]/div/div[1]/span[1]/span/text()').get()
      coin = response.meta['coin']
      date_of_the_day = datetime.today().strftime('%Y-%m-%d')
      loaded_at = datetime.now()
      self.rows.append([date_of_the_day, response.meta['coin'], price, loaded_at])
      print(self.rows)

    def closed(self, reason):
      if reason == 'finished':
        dataset_ref = self.bqclient.dataset('sources')  # specify bigquery dataset
        table_ref = dataset_ref.table('prices_crypto') # specify bigquery table
        # send it to bigquery
        df = pd.DataFrame(self.rows, columns =['date', 'coin', 'price', 'loaded_at'], dtype = float)
        df['date'] = pd.to_datetime(df["date"]).dt.date
        self.bqclient.load_table_from_dataframe(df, table_ref).result()
        print(df)