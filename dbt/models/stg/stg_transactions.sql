with source_data as (
    select tickers as ticker_id, cast(date_transaction as date) as date_transaction, quantity, price, loaded_at
    from {{ source('sources', 'transactions')}}

),

final as (
	select *
	from source_data
)

select *
from final