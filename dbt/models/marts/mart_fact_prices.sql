with source_date as (
	select *
	from {{ ref('dw_fact_prices')}}
),

final as (
	select *
	from source_date
)

select *
from final