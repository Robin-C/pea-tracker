with source_date as (
	select *
	from {{ ref('dw_fact_transactions_daily_performance')}}
),

final as (
	select *
	from source_date
)

select *
from final