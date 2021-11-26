with source_data as (
	select *
	from {{ ref('stg_crypto_transactions_cum_joined_dates')}}
),

final as (
	select *
	from source_data
)

select *
from final