with source_date as (
	select *
	from {{ ref('stg_tickers_snapshot_clean')}}
),

final as (
	select *
	from source_date
)

select *
from final