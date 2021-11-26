/* We aggregate because we can sometimes have several transactions for the same coin on the same day */

with source_data as (
    select coin as coin_id
    , cast(date_transaction as date) as date_transaction
    , sum(quantity) as quantity
    , sum(price*quantity) / sum(quantity) as price
    , loaded_at
    , concat(coin, cast(date_transaction as date)) as pk
    from {{ source('sources', 'crypto_transactions')}}
    group by coin, cast(date_transaction as date), loaded_at, concat(coin, cast(date_transaction as date))
),

final as (
	select *
	from source_data
)

select *
from final