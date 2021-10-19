with source_data as (
	
select	  tickers_snapshot.dbt_scd_id as ticker_sk
        , stg_transactions.date_transaction
		, stg_transactions.quantity
		, stg_transactions.price
		, stg_transactions.loaded_at

from {{ ref('stg_transactions') }} stg_transactions
left join {{ ref('stg_tickers_snapshot_clean') }} tickers_snapshot on stg_transactions.ticker_id = tickers_snapshot.ticker_id 
where stg_transactions.date_transaction between tickers_snapshot.dbt_valid_from and coalesce(tickers_snapshot.dbt_valid_to, '2999-12-31')

),

final as (
	select *
	from source_data
)

select *
from final