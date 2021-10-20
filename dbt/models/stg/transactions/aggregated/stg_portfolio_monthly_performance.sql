/* we calculate the monthly portfolio performance */
with monthly_portfolio_performance as (
    select year_month, ticker_sk, sum(portfolio_daily_performance.portfolio_daily_performance) as monthly_performance
    from {{ ref('stg_portfolio_daily_performance') }} portfolio_daily_performance
    inner join {{ref('stg_dates') }} dates on portfolio_daily_performance.date = date_id
    group by 1,2
),

/* then we need to get the portfolio value on the last day of the previous month so we can calculate % increase/decrease */
/* portfolio value on the last day of the month is cum_qty */
portfolio_value_on_last_day_of_prev_month as (
    select year_month, porfolio_daily_performance.ticker_sk, prev_porfolio_daily_performance.ticker_value
    from {{ ref('stg_portfolio_daily_performance') }} porfolio_daily_performance
    inner join {{ ref('stg_portfolio_daily_performance') }} prev_porfolio_daily_performance on prev_porfolio_daily_performance.date = date_trunc(porfolio_daily_performance.date, month) - 1 and porfolio_daily_performance.ticker_sk = prev_porfolio_daily_performance.ticker_sk
    inner join {{ref('stg_dates') }} on porfolio_daily_performance.date = date_id
    where porfolio_daily_performance.date = date_trunc(porfolio_daily_performance.date, month) 
    order by year_month
),


/* We join the 2 tables together and finally calculate the % increase */
final as (
    select monthly_portfolio_performance.year_month, monthly_portfolio_performance.ticker_sk
    , monthly_portfolio_performance.monthly_performance
    , monthly_performance / ticker_value as monthly_portfolio_performance_percent
    , concat(monthly_portfolio_performance.year_month, monthly_portfolio_performance.ticker_sk) as pk
    from monthly_portfolio_performance 
    inner join portfolio_value_on_last_day_of_prev_month on monthly_portfolio_performance.ticker_sk = portfolio_value_on_last_day_of_prev_month.ticker_sk and monthly_portfolio_performance.year_month = portfolio_value_on_last_day_of_prev_month.year_month
)

select *
from final