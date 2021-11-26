with monthly_performances as (
    select year_month, coin_sk, sum(daily_performance) as monthly_performance
    from {{ ref('stg_crypto_daily_performance') }}
    inner join {{ ref('stg_dates') }} on date_id = date
    group by 1,2
),

dates_to_link_to as (
    select date, date_sub(date_trunc(date, month), interval 1 day) as to_link_to
    from {{ ref('stg_crypto_daily_performance') }}
    where extract(month from date) <> extract(month from date_add(date, INTERVAL 1 DAY))
    group by 1, 2
),

price_on_last_day_current__month as (
    select 
      dates.year_month
    , prices.coin_sk
    , prices.price as price_last_day_of_month
    , monthly_performances.monthly_performance
    from {{ ref('stg_crypto_daily_performance') }} prices
    inner join {{ ref('stg_dates') }} dates on prices.date = date_id
    inner join dates_to_link_to on prices.date =  dates_to_link_to.to_link_to
    inner join monthly_performances on dates.year_month = monthly_performances.year_month and monthly_performances.coin_sk = prices.coin_sk
    where 1=1
),

final as (
    select price_on_last_day_current__month.*
           , price_on_last_day_current__month.monthly_performance / (price_on_last_day_current__month.price_last_day_of_month - price_on_last_day_current__month.monthly_performance)  as monthly_performance_percent
           , concat(price_on_last_day_current__month.year_month, price_on_last_day_current__month.coin_sk) as pk
    from price_on_last_day_current__month
    order by price_on_last_day_current__month.year_month
)

select *
from final