with monthly_performance as (
    select year_month, ticker_sk, sum(daily_performance) as monthly_performance
    from {{ ref('stg_prices_daily_performance') }} prices
    inner join {{ ref('stg_dates') }} on date_id = date
    group by 1,2
),

price_on_last_day_of_prev_month as (
    select year_month, prices.ticker_sk
    , prev_month_prices.adj_close as price_last_month
    from {{ ref('stg_prices_daily_performance') }} prices
    inner join {{ ref('stg_prices_daily_performance') }} prev_month_prices on prev_month_prices.date = date_trunc(prices.date, month) - 1 and prices.ticker_sk = prev_month_prices.ticker_sk
    inner join {{ ref('stg_dates') }} on prices.date = date_id
    where 1=1
    and prices.date = date_trunc(prices.date, month)

),


final as (
    select price_on_last_day_of_prev_month.*
           , monthly_performance.monthly_performance
           , (price_on_last_day_of_prev_month.price_last_month + monthly_performance.monthly_performance) / price_on_last_day_of_prev_month.price_last_month - 1 as monthly_performance_percent
           , concat(price_on_last_day_of_prev_month.year_month, price_on_last_day_of_prev_month.ticker_sk) as pk
    from price_on_last_day_of_prev_month
    inner join monthly_performance on monthly_performance.year_month = price_on_last_day_of_prev_month.year_month 
    and monthly_performance.ticker_sk = price_on_last_day_of_prev_month.ticker_sk
    order by price_on_last_day_of_prev_month.year_month
)

select *
from final


