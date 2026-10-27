# Finance ETL Pipeline

A finance-oriented ETL pipeline that extracts daily stock prices from Yahoo Finance,
transforms and validates the data, and loads it into a local PostgreSQL database.
Orchestrated with Apache Airflow via Astro CLI.

## Architecture

```
Yahoo Finance (yfinance)
    |
    v
[EXTRACT] -- Fetch OHLCV data for AAPL, MSFT, GOOGL, NVDA, AMZN
    |
    v
[TRANSFORM] -- Type casting, deduplication, validation,
    |           derived columns (daily return, moving averages)
    v
[LOAD] -- Upsert into PostgreSQL (idempotent via ON CONFLICT)
    |
    v
[QUALITY CHECKS] -- Null checks, price consistency, reconciliation
```

## Tables in `finance_db`

| Table | Purpose |
|-------|---------|
| `raw_daily_prices` | Append-only audit trail of every extraction |
| `fact_daily_stock_prices` | Cleaned, enriched records (PK: symbol + trade_date) |
| `rejected_records` | Rows that failed validation, with rejection reasons |

## Quick Start

### 1. Prerequisites
- Python 3.11+
- PostgreSQL 17 (running as Windows service)
- Docker Desktop (for Airflow via Astro CLI)

### 2. Setup
```powershell
cd D:\Projects\ApacheAirflow

# Create and activate virtual environment
py -m venv .venv
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure
Edit `.env` with your PostgreSQL credentials:
```env
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=finance_db
PG_USER=postgres
PG_PASSWORD=your_password_here
```

### 4. Test Database Connection
```powershell
python scripts\test_connection.py
```

### 5. Run the Pipeline
```powershell
# Full run (5 symbols, 1 year of data)
python run_pipeline.py

# Custom run
python run_pipeline.py --symbols AAPL TSLA --period 3mo
```

### 6. Run Tests
```powershell
python -m pytest tests\ -v
```

### 7. Analytical Queries
Open `sql\analytical_queries.sql` in pgAdmin or any SQL client. Includes:
- Latest closing prices
- Best/worst daily returns
- 52-week highs and lows
- Monthly aggregations
- Volatility rankings
- Golden cross detection (MA7 crosses MA30)

## Project Structure

```
ApacheAirflow/
  .env                    # PostgreSQL credentials (git-ignored)
  .gitignore
  requirements.txt        # Python dependencies
  run_pipeline.py         # Main ETL entry point
  pipeline/
    __init__.py           # Config loader (reads .env)
    extract.py            # Yahoo Finance data extraction
    transform.py          # Cleaning, validation, enrichment
    load.py               # PostgreSQL loading with upsert
    quality_checks.py     # Post-load data quality checks
  scripts/
    test_connection.py    # DB connectivity test
  sql/
    analytical_queries.sql # 10 ready-to-use analytical queries
  tests/
    test_transform.py     # Unit tests for transform logic
```

## Key Design Decisions

- **Yahoo Finance via yfinance**: No API key needed, no rate limits, public data
- **Upsert (ON CONFLICT DO UPDATE)**: Re-running the pipeline for the same dates
  overwrites stale data instead of creating duplicates
- **Rejected records table**: Bad data is logged, not silently dropped
- **CHECK constraints on fact table**: Database enforces prices > 0, high >= low
- **Derived columns**: Daily returns, 7-day and 30-day moving averages computed
  during transform for immediate analytical use
