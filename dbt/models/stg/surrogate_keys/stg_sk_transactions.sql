with source_data as (
	
select	  tickers_snapshot.dbt_scd_id as ticker_sk
        , stg_transactions.date_transaction
		, stg_transactions.quantity
		, stg_transactions.price
		, stg_transactions.loaded_at

from {{ ref('stg_transactions') }} stg_transactions
left join {{ ref('tickers_snapshot') }} tickers_snapshot on stg_transactions.ticker_id = tickers_snapshot.ticker_id 
where stg_transactions.date_transaction between tickers_snapshot.dbt_valid_from and tickers_snapshot.dbt_valid_to

),

final as (
	select *
	from source_data
)

select *
from final