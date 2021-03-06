{% snapshot tickers_snapshot %}


with source_data as (
    select ticker as ticker_id, description, index, loaded_at, is_benchmark, country
    from {{ source('sources', 'tickers')}}

),

final as (
	select *
	from source_data
)

select *
from final

{% endsnapshot %}