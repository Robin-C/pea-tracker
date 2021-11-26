/* goal is to multiply the daily performance of the tickers by the amount of share of the portfolio */

with source_data as (
	
select daily_transactions.date, daily_transactions.coin_sk, cum_qty, cum_average_cost
, daily_prices.daily_performance * cum_qty as portfolio_daily_performance
, (price * cum_qty) - cum_cost as unrealized_gains_losses
, cum_qty * price as coin_value
, concat(daily_transactions.date, daily_transactions.coin_sk) as pk
from {{ ref('stg_crypto_transactions_cum_joined_dates') }} daily_transactions
inner join {{ ref('stg_crypto_daily_performance') }} daily_prices on daily_prices.date = daily_transactions.date and daily_prices.coin_sk = daily_transactions.coin_sk
order by date
),

final as (
	select *
	from source_data
)

select *
from final