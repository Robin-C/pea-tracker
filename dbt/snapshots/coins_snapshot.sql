{% snapshot coins_snapshot %}


with source_data as (
    select coin as coin_id, coinGecko_name, is_benchmark, is_farmed, loaded_at
    from {{ source('sources', 'coins')}}

),

final as (
	select *
	from source_data
)

select *
from final

{% endsnapshot %}