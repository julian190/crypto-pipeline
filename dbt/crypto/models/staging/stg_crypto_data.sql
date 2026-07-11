with source as (
    select * from {{ source('crypto', 'crypto_data') }}
),


cleaned as (
select 
id                    as coin_Id,
symbol                as coin_symbol,
name                  as coin_name,

-- Prices (Cast as numtic)
CAST(current_price AS numeric) as current_price_nzd,
CAST(low_24h AS numeric) as low_24h_ndz,
CAST(high_24h AS numeric) as high_24_nzd,
CAST(price_change_24h AS numeric) as price_change_24h_nzd,

 -- market data

CAST(market_cap as numeric)             as market_cap_nzd,
CAST(market_cap_rank as integer)        as market_cap_rank,
CAST(total_volume as numeric)           as total_volume_nzd,

    -- percentage changes

cast(price_change_percentage_24h as numeric) as price_change_pct_24h,

    -- metadata
cast(last_updated as timestamp)         as last_updated_at,
ingested_at
from source
where current_price IS NOT NULL
)

select * from cleaned
