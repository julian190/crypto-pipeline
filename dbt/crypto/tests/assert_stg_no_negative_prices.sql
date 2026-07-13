-- Test: assert_stg_no_negative_prices
--
-- Ensures that no row in stg_crypto_data has a negative value for any of the
-- three price columns. A non-zero result means the test fails.
--
-- Columns checked:
--   current_price_nzd  — filtered to NOT NULL by the staging model itself
--   low_24h_ndz        — may be null; only checked when populated
--   high_24_nzd        — may be null; only checked when populated

select
    coin_id,
    coin_name,
    current_price_nzd,
    low_24h_nzd,
    high_24_nzd
from {{ ref('stg_crypto_data') }}
where
    current_price_nzd < 0
    or (low_24h_nzd  is not null and low_24h_nzd  < 0)
    or (high_24_nzd  is not null and high_24_nzd  < 0)
