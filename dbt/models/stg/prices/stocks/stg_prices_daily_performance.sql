with source as (
    select prices.pk, prices.date, prices.ticker_sk
    , prices.adj_close
    , prices.adj_close - prev_prices.adj_close as daily_performance
    from {{ ref('stg_prices_joined_dates') }} prices
    inner join {{ ref('stg_prices_joined_dates') }} prev_prices on prev_prices.ticker_sk = prices.ticker_sk
    and prev_prices.date = date_sub(prices.date, INTERVAL 1 day)
),

final as (
    select *
    from source
    order by date
)

select *
from final