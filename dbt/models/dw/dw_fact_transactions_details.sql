with source_data as (
	select *
	from {{ ref('stg_sk_transactions')}}
),

final as (
	select *
	from source_data
)

select *
from final