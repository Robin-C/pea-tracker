with source_date as (
	select *
	from {{ ref('dw_dim_coins')}}
),

final as (
	select *
	from source_date
)

select *
from final