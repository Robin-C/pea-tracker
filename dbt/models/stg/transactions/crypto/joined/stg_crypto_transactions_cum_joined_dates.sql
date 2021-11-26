with coins as (
  select coin_sk
  from {{ ref('stg_sk_crypto_transactions') }}
  group by 1
),

date_list as (
   select date_id
   from {{ ref('stg_dates') }}
   where date_id <= CURRENT_DATE()
),

coins_dates_crossjoin as (
select *
from date_list
cross join coins 
),

final as (
select date_id as date, coin_sk, concat(date_id, coin_sk) as pk 
    , (select sum(quantity) from {{ ref('stg_sk_crypto_transactions') }} transactions_subquery where transactions_subquery.coin_sk = coins_dates_crossjoin.coin_sk and transactions_subquery.date_transaction <= coins_dates_crossjoin.date_id) as cum_qty
    , (select sum(quantity*price) from {{ref('stg_sk_crypto_transactions')}} transactions_subquery where transactions_subquery.coin_sk = coins_dates_crossjoin.coin_sk and transactions_subquery.date_transaction <= coins_dates_crossjoin.date_id) as cum_cost
    , (select sum(quantity*price) / sum(quantity) from {{ref('stg_sk_crypto_transactions')}} transactions_subquery where transactions_subquery.coin_sk = coins_dates_crossjoin.coin_sk and transactions_subquery.date_transaction <= coins_dates_crossjoin.date_id) as cum_average_cost
from coins_dates_crossjoin
)

select *
from final
where cum_qty is not null