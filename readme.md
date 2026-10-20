# Crypto Pipeline

A containerised data pipeline that collects live cryptocurrency market data from the CoinGecko API every 5 minutes, keeps the full price history in PostgreSQL, models it with dbt, and orchestrates every step with Apache Airflow. Everything runs locally with Docker Compose.

**Patterns demonstrated:** scheduled API ingestion, append-only raw storage, layered dbt modelling (raw → staging → marts), latest-snapshot-per-key logic, task orchestration with dependencies, and data quality testing inside the pipeline run.

---

## The Problem

Crypto prices move constantly, so checking them by hand tells you where the market is now but not how it got there. Answering simple questions such as "which coins are up or down, and how has a price moved over the last day?" needs two things the API does not provide on its own:

- **History.** The API returns only the current state. Trends need a snapshot stored every few minutes.
- **A clean, current view.** Raw API responses use API field names and loose types, and every refresh produces another copy of every coin. Analysts need one tidy row per coin for "right now" questions.

This project solves both. It stores every snapshot, and it exposes two layers: the full cleaned history for trend analysis, and a latest-snapshot table for current-state reporting and BI tools such as Power BI or Tableau.

## What It Delivers

| Output | Contents | Use |
|---|---|---|
| `raw.crypto_data` | Every API response, appended on each run | Source of truth and replayable history |
| `stg_crypto_data` (view) | Cleaned, typed, snake_case rows; full history retained | Trends and price history |
| `fct_crypto_prices` (table) | Latest snapshot per coin, with `is_top_10` and `price_direction` | Current prices, top movers, dashboards |

---

## Architecture & Data Flow

```mermaid
graph TD
    A[CoinGecko API] -->|Python + Requests| B[Extract: DataFrame]
    B -->|Pandas + SQLAlchemy, append| C[(PostgreSQL: raw.crypto_data)]
    C -->|dbt staging| D[View: stg_crypto_data<br/>full history]
    D -->|dbt mart: DISTINCT ON latest per coin| E[Table: fct_crypto_prices<br/>latest snapshot]
    E -->|dbt tests| F[Data quality validation]
    E --> G[BI tools: Power BI / Tableau]
    H[Apache Airflow DAG<br/>every 5 minutes] -.->|extract_and_load| B
    H -.->|dbt_run| D
    H -.->|dbt_test| F

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#96f,stroke:#333,stroke-width:2px
    style E fill:#6f9,stroke:#333,stroke-width:2px
    style H fill:#f96,stroke:#333,stroke-width:2px
```

| Stage | Tool | Description |
|---|---|---|
| **Extract** | Python, Requests | One call to the CoinGecko markets endpoint per run, returned as a DataFrame |
| **Load** | Pandas, SQLAlchemy | Ensures the `raw` schema exists, then appends the snapshot to `raw.crypto_data` |
| **Transform** | dbt Core | Staging view (clean, cast, rename) and a mart table (latest snapshot per coin plus flags) |
| **Test** | dbt singular tests | Three custom tests run after every build as the last DAG task |
| **Orchestrate** | Apache Airflow 2.10 | `extract_and_load → dbt_run → dbt_test` |

---

## Design Decisions: Why We Do It This Way

| Decision | Why | Trade-off |
|---|---|---|
| **Append-only raw table** | Keeps every snapshot, so trends are possible and the mart can be rebuilt from history without calling the API again. An earlier version replaced the table on each run, and Postgres refused because the staging view depended on it; appending fixed that and preserved history. | The table grows with every run (see Known Limitations) |
| **Staging keeps full history; the mart keeps only the latest row per coin** | Trend questions need history, while "what is the price now" questions need one row per coin. Two layers serve both without duplicating logic. | Current-state queries and trend queries hit different tables |
| **`DISTINCT ON (id)` ordered by `ingested_at DESC`** | Returns the whole latest row per coin in one step. `GROUP BY` with `MAX()` would return the latest timestamp but not the matching values. | `DISTINCT ON` is PostgreSQL-specific; on another warehouse the equivalent is `ROW_NUMBER()` with a filter |
| **24-hour change taken from the API, not recalculated** | CoinGecko already supplies the 24-hour change, so recomputing it would duplicate logic and could disagree with the source. | The pipeline depends on the source's definition of the 24-hour window |
| **Casting and cleaning only in staging** | Types, names, and null handling live in one layer, so downstream models stay simple. | Rows without a price are filtered out in staging |
| **One API call per run, every 5 minutes** | Gives fine-grained history while staying well inside the free public tier's rate limit. | Only the top coins returned by the endpoint are covered |
| **Tests run inside the DAG** | A failed test is visible in the same run that produced the data. | A failing test does not stop data already loaded into raw |
| **PostgreSQL 18 with a versioned volume path** | Postgres 18 needs the data directory mounted at `/var/lib/postgresql/data/18`, otherwise the container fails on start-up. | The path is version-specific and must change with major upgrades |

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Requests |
| Storage | PostgreSQL 18 |
| Loading | Pandas, SQLAlchemy |
| Transformation | dbt Core (dbt-postgres) |
| Orchestration | Apache Airflow 2.10 |
| Infrastructure | Docker, Docker Compose |

## Project Structure

```
crypto-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py   # Airflow DAG definition
├── extract/
│   └── coingecko_extract.py     # CoinGecko API call, returns a DataFrame
├── load/
│   └── postgres_load.py         # SQLAlchemy load to the raw schema
├── dbt/
│   └── crypto/
│       ├── models/
│       │   ├── staging/
│       │   │   └── stg_crypto_data.sql
│       │   └── marts/
│       │       └── fct_crypto_prices.sql
│       ├── tests/               # Custom singular dbt tests
│       └── schema.yml
├── Screenshots/                 # Run evidence used in this README
├── docker-compose.yml
├── requirements.txt
├── .env.example                 # Template for local environment variables
└── .gitignore                   # Excludes .env, Data/, logs/, venv/
```

---

## Data Model

**`raw.crypto_data`**: the raw API response, append-only, one row per coin per run, with an `ingested_at` timestamp added at load time.

**`stg_crypto_data`** (view): cleaned and typed, full history retained (one row per coin per ingestion run). Columns are renamed to snake_case, prices and market figures are cast to numeric, and rows without a current price are filtered out.

| Column | Description |
|---|---|
| `coin_id`, `coin_symbol`, `coin_name` | Coin identifiers |
| `current_price_nzd` | Current price in NZD |
| `low_24h_nzd`, `high_24_nzd`, `price_change_24h_nzd` | 24-hour range and change in NZD |
| `market_cap_nzd`, `market_cap_rank`, `total_volume_nzd` | Market data |
| `price_change_pct_24h` | 24-hour percentage change, supplied by the API |
| `last_updated_at`, `ingested_at` | When CoinGecko last updated the coin, and when this pipeline loaded it |

**`fct_crypto_prices`** (table): the latest snapshot per coin, deduplicated with `DISTINCT ON`, plus two derived columns:

| Column | Logic |
|---|---|
| `is_top_10` | `true` when `market_cap_rank <= 10` |
| `price_direction` | `up` when `price_change_pct_24h > 0`, `down` when `< 0`, otherwise `flat` |

---

## Data Quality Tests

Three custom singular tests in `dbt/crypto/tests/` run as the last task of the DAG. Each test passes when its query returns no rows.

| Test | What it checks |
|---|---|
| `assert_stg_no_negative_prices` | No row in `stg_crypto_data` has a negative price |
| `assert_fct_price_direction_matches_pct` | `price_direction` agrees with the sign of `price_change_pct_24h` |
| `assert_fct_top10_has_rank_lte_10` | Every coin flagged `is_top_10` has `market_cap_rank <= 10` |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git

## Setup

**1. Clone the repo**

```bash
git clone https://github.com/julian190/crypto-pipeline.git
cd crypto-pipeline
```

**2. Create your `.env` file** from the template:

```bash
cp .env.example .env   # Windows CMD: copy .env.example .env
```

Then fill in your own values:

```dotenv
POSTGRES_PASSWORD=your_password
databaseURL=postgresql://postgres:your_password@localhost:5432/Crypto
DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true
AIR_FLOW_USER=admin
AIR_FLOW_PASSWORD=change_me
AIR_FLOW_FIRSTNAME=First
AIR_FLOW_LASTNAME=Last
AIR_FLOW_EMAIL=admin@example.com
```

> The `.env` file is gitignored and must never be committed.

**3. Start all services**

```bash
docker compose up -d
```

This starts PostgreSQL, runs the Airflow migrations (`airflow-init`), then starts the Airflow webserver and scheduler. The first start can take several minutes while dependencies install inside the containers.

**4. Verify services are running**

```bash
docker compose ps
```

All services should show `running`, and `airflow-init` should show `exited 0`.

**5. Open the Airflow UI**

Go to <http://localhost:8080> and log in with `AIR_FLOW_USER` / `AIR_FLOW_PASSWORD` from your `.env`.

**6. Run the pipeline**

Find the `crypto_pipeline` DAG, enable it, and optionally trigger a manual run. The loader creates the `raw` schema if it does not exist. The three tasks run in sequence:

```
extract_and_load → dbt_run → dbt_test
```

## Querying the Output

Once the pipeline has run, connect with DBeaver or `psql` and query the mart:

```sql
-- Top 10 coins by market cap
SELECT coin_name, current_price_nzd, market_cap_rank, price_direction
FROM marts.fct_crypto_prices
WHERE is_top_10 = true
ORDER BY market_cap_rank;

-- Biggest movers in the latest snapshot
SELECT coin_name, price_change_pct_24h
FROM marts.fct_crypto_prices
ORDER BY price_change_pct_24h DESC
LIMIT 10;
```

Query the staging view for history:

```sql
-- Bitcoin price over the last 24 hours
SELECT ingested_at, current_price_nzd
FROM dbt.stg_crypto_data
WHERE coin_id = 'bitcoin'
  AND ingested_at >= now() - interval '24 hours'
ORDER BY ingested_at;
```

## Stopping the Pipeline

```bash
docker compose down        # stops containers, keeps data
docker compose down -v     # stops containers and deletes volumes
```

---

## Evidence

**Airflow DAG run status**

![Airflow DAG status](Screenshots/AirFlow_dag_status.png)

**Final data in PostgreSQL**

![Final data in the Postgres table](Screenshots/PG_FinalDataInTable.png)

---

## Known Limitations

- **Unbounded growth.** Each run appends about 100 rows. At one run every 5 minutes that is roughly 29,000 rows a day, or about 10 million a year. There is no retention policy yet. At this cadence the next step would be date partitioning on the raw table and archiving or dropping old partitions.
- **Rate-limited free tier.** The pipeline uses CoinGecko's public API without a key. One call per run is far below the limit, but the tier is rate-limited, and a free demo key is available if more calls are needed.
- **NZD only.** Prices are requested in NZD (`vs_currency=nzd`).
- **Local single-node setup.** Airflow runs with default local credentials in Docker Compose and is not hardened for production.
- **No failure alerting.** Failed runs are visible in the Airflow UI, but no notifications are configured.

## Data Source

Market data comes from the [CoinGecko API](https://docs.coingecko.com) (public tier, no key required). Data provided by CoinGecko. See CoinGecko's API terms for attribution and usage limits.