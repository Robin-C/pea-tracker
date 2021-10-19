with source_data as (
	select *
	from {{ ref('stg_tickers_snapshot_clean')}}
),

final as (
	select *
	from source_data
)

select *
from final