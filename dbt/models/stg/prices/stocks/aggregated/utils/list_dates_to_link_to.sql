/* Created this view since I had a ressource too intensive error when querying stg_prices_monthly_performance */

with list_dates_to_link_to as (
    select 
      case when DATE_SUB(DATE_TRUNC(DATE_ADD(date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) > current_date() 
      then current_date() 
      else DATE_SUB(DATE_TRUNC(DATE_ADD(date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end as date_to_link_to
    from {{ ref('stg_prices_daily_performance') }}
    where 1=1
    group by 1
),

final as (
    select *
    from list_dates_to_link_to
)

select *
from final