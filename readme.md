# Crypto Pipeline

An end-to-end data pipeline that ingests live cryptocurrency market data from the CoinGecko API, loads it into PostgreSQL, transforms it with dbt, and orchestrates everything with Apache Airflow — all running locally via Docker.

## Architecture

```
CoinGecko API
      │
      ▼
 Extract (Python + Requests)
      │
      ▼
 Load (Pandas + SQLAlchemy)
      │
      ▼
 PostgreSQL ── raw.crypto_data
      │
      ▼
 Transform (dbt)
      ├── staging/stg_coin_prices       (clean, cast, rename)
      └── marts/fct_crypto_prices       (business logic, flags)
      │
      ▼
 Orchestrate (Apache Airflow DAG)
      └── extract_load → dbt_run → dbt_test
```

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Requests |
| Storage | PostgreSQL |
| Loading | Pandas, SQLAlchemy |
| Transformation | dbt Core |
| Orchestration | Apache Airflow 2.10 |
| Infrastructure | Docker, Docker Compose |

## Project Structure

```
crypto-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py   # Airflow DAG definition
├── extract/
│   └── coingecko_extract.py     # CoinGecko API call, returns DataFrame
├── load/
│   └── postgres_load.py         # SQLAlchemy load to raw schema
├── dbt/
│   └── crypto/
│       ├── models/
│       │   ├── staging/
│       │   │   └── stg_coin_prices.sql
│       │   └── marts/
│       │       └── fct_crypto_prices.sql
│       └── schema.yml
├── docker-compose.yml
├── requirements.txt
├── .env                         # Not committed — see setup below
└── .gitignore
```

## Data Model

**raw.crypto_data** — raw API response, append-only, one row per coin per run

**staging.stg_coin_prices** — cleaned and typed: nulls handled, columns renamed to snake_case, prices cast to numeric

**marts.fct_crypto_prices** — business-ready: adds `price_change_pct_24h`, `is_top_10` flag, and `price_direction` (up/down/flat)

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git

## Setup

**1. Clone the repo**

```bash
git clone https://github.com/julian190/crypto-pipeline.git
cd crypto-pipeline
```

**2. Create your `.env` file** in the project root:

```dotenv
databaseURL=postgresql://postgres:postgres@localhost:5432/Crypto
DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true
POSTGRES_PASSWORD=postgres
AIR_FLOW_USER=admin
AIR_FLOW_PASSWORD=admin
AIR_FLOW_FIRSTNAME=Julian
AIR_FLOW_LASTNAME=Metias
AIR_FLOW_EMAIL=info@jmetias.nz
```

> ⚠️ The `.env` file is gitignored and must be created manually — never commit credentials.

**3. Start all services**

```bash
docker compose up -d
```

This starts PostgreSQL, runs Airflow migrations (`airflow-init`), then starts the Airflow webserver and scheduler. First run takes a few minutes while pip installs dependencies inside the containers.

**4. Verify services are running**

```bash
docker compose ps
```

All services should show `running` or `exited 0` for `airflow-init`.

**5. Create the raw schema in Postgres**

Connect to Postgres via DBeaver or psql and run:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
```

**6. Open the Airflow UI**

Go to [http://localhost:8080](http://localhost:8080) and log in with the credentials from your `.env` file (`AIR_FLOW_USER` / `AIR_FLOW_PASSWORD`).

**7. Run the pipeline**

Find the `crypto_pipeline` DAG, enable it, and trigger a manual run. The three tasks run in sequence:

```
extract_and_load → dbt_run → dbt_test
```

## Querying the Output

Once the pipeline has run, connect to Postgres and query the mart:

```sql
-- Top 10 coins by market cap
SELECT coin_name, current_price_nzd, market_cap_rank, price_direction
FROM marts.fct_crypto_prices
WHERE is_top_10 = true
ORDER BY market_cap_rank;

-- Biggest movers in the last run
SELECT coin_name, price_change_pct_24h
FROM marts.fct_crypto_prices
ORDER BY price_change_pct_24h DESC
LIMIT 10;
```

## Stopping the Pipeline

```bash
docker compose down        # stops containers, keeps data
docker compose down -v     # stops containers and deletes all data
```

## Notes

- Data source: [CoinGecko Public API](https://docs.coingecko.com) — no API key required
- Currency: NZD (`vs_currency=nzd`)
- The pipeline is scheduled to run daily (`@daily` in the DAG)
- Each run appends a new snapshot to `raw.crypto_data` — historical prices accumulate over time