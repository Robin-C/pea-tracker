with source_data as (
    select coin_id
    , coinGecko_name
    , is_benchmark
    , is_farmed
    , loaded_at
    , dbt_scd_id
    , dbt_updated_at
    , cast(dbt_valid_from as date) as dbt_valid_from
    , date_sub(cast(dbt_valid_to as date) , interval 1 day) as dbt_valid_to
    from {{ ref('coins_snapshot')}}

),

final as (
	select *
	from source_data
)

select *
from final