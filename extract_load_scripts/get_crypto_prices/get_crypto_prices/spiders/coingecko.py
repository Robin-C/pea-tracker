import scrapy
from datetime import datetime
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/.dbt/pea-tracker-5b072ff475b4.json"
  

class pmuSpider(scrapy.Spider):
    name = "coingecko"
    def start_requests(self):
        # get the coin list from bigquery
        bqclient = bigquery.Client()
        query_str = """select coinGecko_name
        from sources.coins"""
        response = (
            bqclient.query(query_str)
            .result()
            
        )
       # create list to compile url list to be scraped
        urls = list()
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
      yield {
      # specify bigquery dataset
      dataset_ref = client.dataset('sources')
      # specify bigquery table
      table_ref = dataset_ref.table('crypto_prices')
      }