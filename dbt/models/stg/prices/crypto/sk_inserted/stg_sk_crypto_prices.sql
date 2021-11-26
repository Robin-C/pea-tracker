with source_data as (
select coins_snapshot.dbt_scd_id as coin_sk
, concat(stg_crypto_prices.date, coins_snapshot.dbt_scd_id) as pk
, stg_crypto_prices.date
, stg_crypto_prices.price
, stg_crypto_prices.loaded_at

from {{ ref('stg_crypto_prices') }} stg_crypto_prices
left join {{ ref('stg_coins_snapshot_clean') }} coins_snapshot on stg_crypto_prices.coinGecko_name = coins_snapshot.coinGecko_name 
where stg_crypto_prices.date between coins_snapshot.dbt_valid_from and coalesce(coins_snapshot.dbt_valid_to, '2999-12-31')

),

final as (
	select *
	from source_data
)

select *
from final