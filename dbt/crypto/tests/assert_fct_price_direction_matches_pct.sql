-- Test: assert_fct_price_direction_matches_pct
--
-- Validates that the price_direction derived column is consistent with the
-- sign of price_change_pct_24h in fct_crypto_prices.
--
-- Business rules:
--   price_change_pct_24h > 0  ⟹  price_direction = 'up'
--   price_change_pct_24h < 0  ⟹  price_direction = 'down'
--   price_change_pct_24h = 0  ⟹  price_direction = 'flat'
--   price_change_pct_24h IS NULL  ⟹  price_direction = 'flat'
--
-- A non-zero result means at least one row violates these rules.

select
    coin_id,
    coin_name,
    price_change_pct_24h,
    price_direction
from {{ ref('fct_crypto_prices') }}
where
    -- percentage is positive but direction is not 'up'
    (price_change_pct_24h > 0  and price_direction != 'up')
    -- percentage is negative but direction is not 'down'
    or (price_change_pct_24h < 0  and price_direction != 'down')
    -- percentage is zero or null but direction is not 'flat'
    or (
        (price_change_pct_24h = 0 or price_change_pct_24h is null)
        and price_direction != 'flat'
    )
