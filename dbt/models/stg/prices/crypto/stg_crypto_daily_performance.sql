with source as (
    select prices.date
    , prices.coin_sk
    , prices.price
    , prices.price - prev_prices.price as daily_performance
    , prices.pk
    , prices.loaded_at
    from {{ ref('stg_sk_crypto_prices') }} prices
    left join {{ ref('stg_sk_crypto_prices') }} prev_prices on prev_prices.coin_sk = prices.coin_sk
    and prev_prices.date = date_sub(prices.date, INTERVAL 1 day)
),

final as (
    select *
    from source
    order by date
)

select *
from final