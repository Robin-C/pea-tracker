with monthly_performance as (
    select year_month, ticker_sk, sum(daily_performance) as monthly_performance
    from {{ ref('stg_prices_daily_performance') }} prices
    inner join {{ ref('stg_dates') }} on date_id = date
    group by 1,2
),

price_on_first_of_the_month as (
    select year_month, ticker_sk, adj_close
    from {{ ref('stg_prices_daily_performance') }}  prices
    inner join {{ ref('stg_dates') }} on date_id = date
    where date_id = date_trunc(date_id, month)
),

final as (
    select price_on_first_of_the_month.*
           , monthly_performance.monthly_performance
           , (price_on_first_of_the_month.adj_close + monthly_performance.monthly_performance) / price_on_first_of_the_month.adj_close - 1 as monthly_performance_percent
    from price_on_first_of_the_month
    inner join monthly_performance on monthly_performance.year_month = price_on_first_of_the_month.year_month 
    and monthly_performance.ticker_sk = price_on_first_of_the_month.ticker_sk
    order by price_on_first_of_the_month.year_month
)

select *
from final


