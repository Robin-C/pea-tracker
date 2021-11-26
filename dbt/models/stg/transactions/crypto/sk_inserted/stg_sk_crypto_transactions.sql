with source_data as (
	
select	  coins_snapshot.dbt_scd_id as coin_sk
        , stg_crypto_transactions.date_transaction
		, stg_crypto_transactions.quantity
		, stg_crypto_transactions.price
		, stg_crypto_transactions.loaded_at
		, concat(coins_snapshot.dbt_scd_id, stg_crypto_transactions.date_transaction) as pk
from {{ ref('stg_crypto_transactions') }} stg_crypto_transactions
left join {{ ref('stg_coins_snapshot_clean') }} coins_snapshot on stg_crypto_transactions.coin_id = coins_snapshot.coin_id 
where stg_crypto_transactions.date_transaction between coins_snapshot.dbt_valid_from and coalesce(coins_snapshot.dbt_valid_to, '2999-12-31')

),

final as (
	select *
	from source_data
)

select *
from final