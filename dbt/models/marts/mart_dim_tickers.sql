with source_date as (
	select *
	from {{ ref('dw_dim_tickers')}}
),

final as (
	select *
	from source_date
)

select *
from final