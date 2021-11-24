with coins as (
    select * from {{ref('stg_coins_snapshot_clean')}}
),

crypto_prices as (
    select *
    from {{ ref('stg_crypto_prices') }}
)