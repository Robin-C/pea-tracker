with source_date as (
	select *
	from {{ ref('stg_sk_transactions')}}
),

final as (
	select *
	from source_date
)

select *
from final