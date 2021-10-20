with source_data as (
	select *
	from {{ ref('stg_portfolio_monthly_performance')}}
),

final as (
	select *
	from source_data
)

select *
from final