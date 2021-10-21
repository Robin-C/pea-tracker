with monthly_performance as (
    select year_month, ticker_sk, sum(daily_performance) as monthly_performance
    from {{ ref('stg_prices_daily_performance') }} prices
    inner join {{ ref('stg_dates') }} on date_id = date
    group by 1,2
),

/* We get the price of the last day of the month so we can calculate the price at the beginning of the month.
price at the beginning of the period = price of last day of month - performance during the month.
last day of month = first day next month minus 1
DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
if last day of the month is at future date (ie we are still halfway thru the month) then take ticker value at current_date())
*/
price_on_last_day_current__month as (
    select year_month, prices.ticker_sk
    , price_on_last_day_current__month.adj_close as price_last_day_of_month
    from {{ ref('stg_prices_daily_performance') }} prices
    inner join {{ ref('stg_prices_daily_performance') }} price_on_last_day_current__month on price_on_last_day_current__month.date = case when DATE_SUB(DATE_TRUNC(DATE_ADD(prices.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) > current_date() then current_date() else DATE_SUB(DATE_TRUNC(DATE_ADD(prices.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end and prices.ticker_sk = price_on_last_day_current__month.ticker_sk
    inner join {{ ref('stg_dates') }} on prices.date = date_id
    where 1=1
    and prices.date = case when DATE_SUB(DATE_TRUNC(DATE_ADD(prices.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) > current_date() then current_date() else DATE_SUB(DATE_TRUNC(DATE_ADD(prices.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end
),


final as (
    select price_on_last_day_current__month.*
           , monthly_performance.monthly_performance
           , monthly_performance.monthly_performance / (price_on_last_day_current__month.price_last_day_of_month - monthly_performance.monthly_performance)  as monthly_performance_percent
           , concat(price_on_last_day_current__month.year_month, price_on_last_day_current__month.ticker_sk) as pk
    from price_on_last_day_current__month
    inner join monthly_performance on monthly_performance.year_month = price_on_last_day_current__month.year_month 
    and monthly_performance.ticker_sk = price_on_last_day_current__month.ticker_sk
    order by price_on_last_day_current__month.year_month
)

select *
from final


