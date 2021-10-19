with tickers as (
  select ticker_sk
  from {{ref('stg_sk_transactions')}}
  group by 1
),

date_list as (
   select date_id
   from {{ ref('stg_dates') }}
),

tickers_dates_crossjoin as (
select *
from date_list
cross join tickers 
),

final as (
select date_id as date, ticker_sk, concat(date_id, ticker_sk) as pk 
    , (select sum(quantity) from {{ref('stg_sk_transactions')}} transactions_subquery where transactions_subquery.ticker_sk = tickers_dates_crossjoin.ticker_sk and transactions_subquery.date_transaction <= tickers_dates_crossjoin.date_id) as cum_qty
    , (select sum(quantity*price) from {{ref('stg_sk_transactions')}} transactions_subquery where transactions_subquery.ticker_sk = tickers_dates_crossjoin.ticker_sk and transactions_subquery.date_transaction <= tickers_dates_crossjoin.date_id) as cum_cost
    , (select sum(quantity*price) / sum(quantity) from {{ref('stg_sk_transactions')}} transactions_subquery where transactions_subquery.ticker_sk = tickers_dates_crossjoin.ticker_sk and transactions_subquery.date_transaction <= tickers_dates_crossjoin.date_id) as cum_average_cost
from tickers_dates_crossjoin
)

select *
from final
where cum_qty is not 