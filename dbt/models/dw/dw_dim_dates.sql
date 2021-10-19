with source_data as (
	select *
	from {{ ref('stg_dates')}}
),

final as (
	select *
	from source_data
)

select *
from final