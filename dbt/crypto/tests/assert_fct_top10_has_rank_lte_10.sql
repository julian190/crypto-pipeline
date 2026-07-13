-- Test: assert_fct_top10_has_rank_lte_10
--
-- Validates that the is_top_10 derived column in fct_crypto_prices is
-- consistent with the market_cap_rank value.
--
-- Business rule:
--   is_top_10 = true   ⟹   market_cap_rank <= 10
--   is_top_10 = false  ⟹   market_cap_rank > 10
--
-- A non-zero result means at least one row violates this rule.

select
    coin_id,
    coin_name,
    market_cap_rank,
    is_top_10
from {{ ref('fct_crypto_prices') }}
where
    -- is_top_10 is true but rank is outside the top 10
    (is_top_10 = true  and market_cap_rank > 10)
    -- is_top_10 is false but rank is within the top 10
    or (is_top_10 = false and market_cap_rank <= 10)
