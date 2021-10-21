with source_data as (
select concat(stg_prices.date, tickers_snapshot.dbt_scd_id) as pk
, stg_prices.date, stg_prices.adj_close, stg_prices.loaded_at,tickers_snapshot.dbt_scd_id as ticker_sk
from {{ ref('stg_prices') }} stg_prices
left join {{ ref('stg_tickers_snapshot_clean') }} tickers_snapshot on stg_prices.ticker_id =tickers_snapshot.ticker_id 
where stg_prices.date between tickers_snapshot.dbt_valid_from and coalesce(tickers_snapshot.dbt_valid_to, '2999-12-31')

),

final as (
	select *
	from source_data
)

select *
from final