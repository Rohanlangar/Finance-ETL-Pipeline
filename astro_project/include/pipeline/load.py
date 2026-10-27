"""
LOAD module -- Write clean stock data into PostgreSQL (finance_db).

Tables:
  raw_daily_prices       -- Raw extracted records (append-only audit trail).
  fact_daily_stock_prices -- Cleaned & enriched records (upsert on symbol+date).

Idempotency:
  Uses INSERT ... ON CONFLICT (symbol, trade_date) DO UPDATE
  so re-running the pipeline for the same dates overwrites
  with the latest values instead of creating duplicates.
"""
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

try:
    from pipeline import get_pg_connection_params
except ImportError:
    from include.pipeline import get_pg_connection_params

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# DDL -- Table definitions
# -------------------------------------------------------------------
RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_daily_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(10)    NOT NULL,
    trade_date      DATE           NOT NULL,
    open_price      NUMERIC(12,4),
    high_price      NUMERIC(12,4),
    low_price       NUMERIC(12,4),
    close_price     NUMERIC(12,4),
    volume          BIGINT,
    extracted_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    loaded_at       TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_date
    ON raw_daily_prices (symbol, trade_date);
"""

FACT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS fact_daily_stock_prices (
    symbol              VARCHAR(10)    NOT NULL,
    trade_date          DATE           NOT NULL,
    open_price          NUMERIC(12,4)  NOT NULL CHECK (open_price > 0),
    high_price          NUMERIC(12,4)  NOT NULL CHECK (high_price > 0),
    low_price           NUMERIC(12,4)  NOT NULL CHECK (low_price > 0),
    close_price         NUMERIC(12,4)  NOT NULL CHECK (close_price > 0),
    volume              BIGINT         NOT NULL CHECK (volume >= 0),
    daily_return_pct    NUMERIC(10,6),
    ma_7                NUMERIC(12,4),
    ma_30               NUMERIC(12,4),
    intraday_range      NUMERIC(12,4),
    loaded_at           TIMESTAMP      NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, trade_date),
    CHECK (high_price >= low_price)
);
"""

REJECTED_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS rejected_records (
    id                SERIAL PRIMARY KEY,
    symbol            VARCHAR(10),
    trade_date        DATE,
    open_price        NUMERIC(12,4),
    high_price        NUMERIC(12,4),
    low_price         NUMERIC(12,4),
    close_price       NUMERIC(12,4),
    volume            BIGINT,
    rejection_reason  TEXT,
    rejected_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
"""


def create_tables(use_airflow_host: bool = False) -> None:
    """Create all required tables if they don't exist."""
    conn_params = get_pg_connection_params(use_airflow_host)
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()

    for ddl in [RAW_TABLE_DDL, FACT_TABLE_DDL, REJECTED_TABLE_DDL]:
        cur.execute(ddl)

    cur.close()
    conn.close()
    logger.info("Tables created/verified in '%s'.", conn_params["dbname"])


def load_raw(raw_df: pd.DataFrame, use_airflow_host: bool = False) -> int:
    """
    Append raw extracted records to raw_daily_prices (audit trail).
    Returns the number of rows inserted.
    """
    if raw_df.empty:
        return 0

    conn = psycopg2.connect(**get_pg_connection_params(use_airflow_host))
    cur = conn.cursor()

    rows = [
        (
            row["symbol"],
            row["date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            int(row["volume"]),
            row["extracted_at"],
        )
        for _, row in raw_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO raw_daily_prices
            (symbol, trade_date, open_price, high_price, low_price, close_price, volume, extracted_at)
        VALUES %s
    """
    execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()
    count = len(rows)
    cur.close()
    conn.close()
    logger.info("Loaded %d rows into raw_daily_prices.", count)
    return count


def load_clean(clean_df: pd.DataFrame, use_airflow_host: bool = False) -> int:
    """
    Upsert clean records into fact_daily_stock_prices.
    ON CONFLICT (symbol, trade_date) DO UPDATE -- idempotent.
    Returns the number of rows upserted.
    """
    if clean_df.empty:
        return 0

    conn = psycopg2.connect(**get_pg_connection_params(use_airflow_host))
    cur = conn.cursor()

    rows = [
        (
            row["symbol"],
            row["date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            int(row["volume"]),
            row.get("daily_return_pct"),
            row.get("ma_7"),
            row.get("ma_30"),
            row.get("intraday_range"),
        )
        for _, row in clean_df.iterrows()
    ]

    upsert_sql = """
        INSERT INTO fact_daily_stock_prices
            (symbol, trade_date, open_price, high_price, low_price,
             close_price, volume, daily_return_pct, ma_7, ma_30, intraday_range, loaded_at)
        VALUES %s
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            open_price       = EXCLUDED.open_price,
            high_price       = EXCLUDED.high_price,
            low_price        = EXCLUDED.low_price,
            close_price      = EXCLUDED.close_price,
            volume           = EXCLUDED.volume,
            daily_return_pct = EXCLUDED.daily_return_pct,
            ma_7             = EXCLUDED.ma_7,
            ma_30            = EXCLUDED.ma_30,
            intraday_range   = EXCLUDED.intraday_range,
            loaded_at        = NOW()
    """

    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
    execute_values(cur, upsert_sql, rows, template=template, page_size=500)
    conn.commit()
    count = len(rows)
    cur.close()
    conn.close()
    logger.info("Upserted %d rows into fact_daily_stock_prices.", count)
    return count


def load_rejected(rejected_df: pd.DataFrame, use_airflow_host: bool = False) -> int:
    """Log rejected rows for auditing."""
    if rejected_df.empty:
        return 0

    conn = psycopg2.connect(**get_pg_connection_params(use_airflow_host))
    cur = conn.cursor()

    rows = [
        (
            row.get("symbol"),
            row.get("date"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            int(row.get("volume", 0)),
            row.get("rejection_reason", ""),
        )
        for _, row in rejected_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO rejected_records
            (symbol, trade_date, open_price, high_price, low_price,
             close_price, volume, rejection_reason)
        VALUES %s
    """
    execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()
    count = len(rows)
    cur.close()
    conn.close()
    logger.info("Logged %d rejected rows into rejected_records.", count)
    return count


# ---------------------------------------------------------------------------
# Quick standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from pipeline.extract import extract_stock_data
    from pipeline.transform import transform_stock_data

    # Create tables
    create_tables()

    # Extract
    raw = extract_stock_data(symbols=["AAPL"], period="5d")

    # Load raw
    raw_count = load_raw(raw)
    print(f"\nRaw rows loaded: {raw_count}")

    # Transform
    clean, rejected = transform_stock_data(raw)

    # Load clean
    clean_count = load_clean(clean)
    print(f"Clean rows upserted: {clean_count}")

    # Load rejected
    rej_count = load_rejected(rejected)
    print(f"Rejected rows logged: {rej_count}")

    print("\nLoad complete!")
