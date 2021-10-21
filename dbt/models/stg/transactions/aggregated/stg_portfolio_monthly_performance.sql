/* we calculate the monthly portfolio performance */
with monthly_portfolio_performance as (
    select year_month, ticker_sk, sum(portfolio_daily_performance.portfolio_daily_performance) as monthly_performance
    from {{ ref('stg_portfolio_daily_performance') }} portfolio_daily_performance
    inner join {{ref('stg_dates') }} dates on portfolio_daily_performance.date = date_id
    group by 1,2
),

/* We get the ticker value of the last day of the month so we can calculate the ticker value at the beginning of the month.
ticker value at the beginning of the month = ticker value of last day of month - performance during the month.
last day of month = first day next month minus 1
DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
if last day of the month is at future date (ie we are still halfway thru the month) then take ticker value at current_date())
*/

portfolio_value_on_last_day_of_current_month as (
    select year_month, porfolio_daily_performance.ticker_sk, portfolio_value_on_last_day_of_current_month.ticker_value as ticker_value_last_day_of_month
    from {{ ref('stg_portfolio_daily_performance') }} porfolio_daily_performance
    inner join {{ ref('stg_portfolio_daily_performance') }} portfolio_value_on_last_day_of_current_month on portfolio_value_on_last_day_of_current_month.date = case when DATE_SUB(DATE_TRUNC(DATE_ADD(porfolio_daily_performance.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) > current_date() then current_date() else DATE_SUB(DATE_TRUNC(DATE_ADD(porfolio_daily_performance.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end and porfolio_daily_performance.ticker_sk = portfolio_value_on_last_day_of_current_month.ticker_sk
    inner join {{ ref('stg_dates') }} on porfolio_daily_performance.date = date_id
    where porfolio_daily_performance.date = case when DATE_SUB(DATE_TRUNC(DATE_ADD(porfolio_daily_performance.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) > current_date() then current_date() else DATE_SUB(DATE_TRUNC(DATE_ADD(porfolio_daily_performance.date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) end
    order by year_month
),


/* We join the 2 tables together and finally calculate the % increase */
final as (
    select monthly_portfolio_performance.year_month, monthly_portfolio_performance.ticker_sk
    , ticker_value_last_day_of_month
    , monthly_portfolio_performance.monthly_performance
    , monthly_performance / (ticker_value_last_day_of_month - monthly_performance) as monthly_portfolio_performance_percent
    , concat(monthly_portfolio_performance.year_month, monthly_portfolio_performance.ticker_sk) as pk
    from monthly_portfolio_performance 
    inner join portfolio_value_on_last_day_of_current_month on monthly_portfolio_performance.ticker_sk = portfolio_value_on_last_day_of_current_month.ticker_sk and monthly_portfolio_performance.year_month = portfolio_value_on_last_day_of_current_month.year_month
)

select *
from final