with source_data as (
    select Date as date, ticker as ticker_id, adj_close, loaded_at, concat(date, ticker) as pk
    from {{ source('sources', 'prices')}}

),

final as (
	select *
	from source_data
)

select *
from final