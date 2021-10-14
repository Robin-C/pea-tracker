with source_data as (
select stg_prices.pk, stg_prices.date, stg_prices.adj_close, stg_prices.loaded_at,tickers_snapshot.dbt_scd_id as ticker_surrkey
from {{ ref('stg_prices') }} stg_prices
left join {{ ref('tickers_snapshot') }} tickers_snapshot on stg_prices.ticker_id =tickers_snapshot.ticker_id 
where stg_prices.Date between tickers_snapshot.dbt_valid_from and tickers_snapshot.dbt_valid_to

),

final as (
	select *
	from source_data
)

select *
from final