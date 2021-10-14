with source_date as (
	select *
	from {{ ref('surr_prices')}}
),

final as (
	select *
	from source_date
)

select *
from final