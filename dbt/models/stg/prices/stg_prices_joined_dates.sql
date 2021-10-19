with tickers as (
  select dbt_scd_id
  from {{ ref('dw_dim_tickers')}}
  group by 1
),

date_list as (
   select date_id
   from {{ ref('stg_dates')}}
   where date_id <= CURRENT_DATE()
),

tickers_dates_crossjoin as (
select *
from date_list
cross join tickers 
),

prices_on_date as (
select date, ticker_sk
from {{ ref('stg_sk_prices')}}
),

/* magic is in the subquery in the where clause */
prev_max_date as (
select tickers_dates_crossjoin.date_id, tickers_dates_crossjoin.dbt_scd_id, (select max(date) from prices_on_date t2 where t2.date <= tickers_dates_crossjoin.date_id and t2.ticker_sk = tickers_dates_crossjoin.dbt_scd_id) as prev_max_date
from tickers_dates_crossjoin
left join prices_on_date on date_id = prices_on_date.date and dbt_scd_id = ticker_sk
)

/* we go look the price at the max_prev_date date and we also create a new PK since the old one would have been duplicated on weekends and holidays, we filter the nulls since it means the ticker was not live yet */
select date_id as date, prices.ticker_sk, adj_close, concat(date_id, ticker_sk) as pk, prev_max_date.prev_max_date
from prev_max_date
left join {{ ref('stg_sk_prices')}} prices on prev_max_date.prev_max_date = prices.date and prev_max_date.dbt_scd_id = prices.ticker_sk
where prices.ticker_sk is not null