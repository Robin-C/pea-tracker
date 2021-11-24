with source_data as (
    select date, coinGecko_name, price, loaded_at, concat(date, coinGecko_name) as pk
    from {{ source('sources', 'prices_crypto')}}

),

final as (
	select *
	from source_data
)

select *
from final