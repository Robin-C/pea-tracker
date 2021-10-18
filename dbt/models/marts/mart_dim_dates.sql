with source_date as (
	select *
	from {{ ref('dw_dim_dates')}}
),

final as (
	select *
	from source_date
)

select *
from final