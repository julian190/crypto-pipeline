with staging as (
    select * from {{ ref('stg_crypto_data') }}
)

select
    coin_id,
    coin_symbol,
    coin_name,
    current_price_nzd,
    market_cap_rank,
    price_change_pct_24h,

    case when market_cap_rank <= 10 then true else false end as is_top_10,
    case 
        when price_change_pct_24h > 0 then 'up'
        when price_change_pct_24h < 0 then 'down'
        else 'flat'
    end as price_direction,

    ingested_at

from staging