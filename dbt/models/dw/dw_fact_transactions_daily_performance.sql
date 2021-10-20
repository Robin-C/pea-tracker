with source_data as (
	select *
	from {{ ref('stg_transactions_daily_performance')}}
),

final as (
	select *
	from source_data
)

select *
from final