with source_date as (
	select *
	from {{ ref('dw_fact_crypto_portfolio_daily_performance')}}
),

final as (
	select *
	from source_date
)

select *
from final