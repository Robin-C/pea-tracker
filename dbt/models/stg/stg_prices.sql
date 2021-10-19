with source_data as (
    select cast(Date as date) as date, ticker as ticker_id, adj_close, loaded_at, concat(cast(date as date), ticker) as pk
    from {{ source('sources', 'prices')}}

),

final as (
	select *
	from source_data
)

select *
from final