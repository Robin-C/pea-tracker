with source_date as (
	select *
	from {{ ref('tickers_snapshot')}}
),

final as (
	select *
	from source_date
)

select *
from final