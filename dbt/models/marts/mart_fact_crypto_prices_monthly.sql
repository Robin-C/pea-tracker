with source_date as (
	select *
	from {{ ref('dw_fact_crypto_prices_monthly')}}
),

final as (
	select *
	from source_date
)

select *
from final