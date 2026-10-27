"""
Finance Stock Prices ETL DAG
============================

Orchestrates daily stock extraction from Yahoo Finance (yfinance), data transformation,
upsert loading into PostgreSQL (finance_db), and automated data quality checks.

Schedule: Daily at 18:00 UTC (after US market close, Monday to Friday)
Orchestration: Apache Airflow 2.x/3.x via Astro CLI
Database: PostgreSQL on host.docker.internal:5432
"""

from datetime import datetime, timedelta
import logging
import os
import shutil

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowException
import pandas as pd

# Pipeline logic imports (from include/pipeline or pipeline)
try:
    from pipeline import SYMBOLS, HISTORY_PERIOD, get_pg_connection_params
    from pipeline.extract import extract_stock_data
    from pipeline.transform import transform_stock_data
    from pipeline.load import create_tables as db_create_tables, load_raw, load_clean, load_rejected
    from pipeline.quality_checks import run_quality_checks
except ImportError:
    from include.pipeline import SYMBOLS, HISTORY_PERIOD, get_pg_connection_params
    from include.pipeline.extract import extract_stock_data
    from include.pipeline.transform import transform_stock_data
    from include.pipeline.load import create_tables as db_create_tables, load_raw, load_clean, load_rejected
    from include.pipeline.quality_checks import run_quality_checks

logger = logging.getLogger(__name__)

# Base staging directory for intermediate file persistence
STAGING_DIR = "/usr/local/airflow/include/staging"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="finance_stock_etl_dag",
    default_args=DEFAULT_ARGS,
    description="Extracts stock OHLCV data from Yahoo Finance, validates, enriches, and upserts into PostgreSQL.",
    schedule="0 18 * * 1-5",  # 18:00 UTC, Mon-Fri
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "stocks", "production"],
    params={
        "symbols": Param(
            SYMBOLS,
            type="array",
            description="List of ticker symbols to extract (e.g. AAPL, MSFT, NVDA).",
        ),
        "period": Param(
            "1y",
            type="string",
            description="Yahoo Finance lookback period: '5d', '1mo', '3mo', '6mo', '1y', '2y'.",
        ),
    },
    doc_md=__doc__,
)
def finance_stock_etl():
    """Defines the TaskFlow DAG pipeline."""

    @task(task_id="check_database_connectivity")
    def check_db() -> dict:
        """Verify network connection and auth to PostgreSQL."""
        import psycopg2
        params = get_pg_connection_params(use_airflow_host=True)
        logger.info("Connecting to PostgreSQL at %(host)s:%(port)s (db: %(dbname)s, user: %(user)s)...", params)
        try:
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            cur.close()
            conn.close()
            logger.info("Successfully connected to PostgreSQL! Version: %s", version)
            return {"status": "connected", "version": version, "host": params["host"]}
        except Exception as exc:
            logger.error("Failed to connect to PostgreSQL: %s", exc)
            raise AirflowException(f"Database connection error: {exc}")

    @task(task_id="ensure_database_tables")
    def init_tables(db_status: dict) -> str:
        """Create database tables and indexes if they do not exist."""
        logger.info("Initializing schema tables in PostgreSQL...")
        db_create_tables(use_airflow_host=True)
        return "tables_initialized"

    @task(task_id="extract_stock_prices")
    def extract(tables_ready: str, **context) -> dict:
        """Download raw OHLCV prices from Yahoo Finance and persist to staging."""
        dag_params = context.get("params", {})
        symbols = dag_params.get("symbols", SYMBOLS)
        period = dag_params.get("period", HISTORY_PERIOD)
        run_id = context.get("run_id", "manual_run").replace(":", "_").replace("+", "_")

        logger.info("Extracting stock data for symbols=%s, period=%s", symbols, period)
        raw_df = extract_stock_data(symbols=symbols, period=period)

        if raw_df.empty:
            raise AirflowException("Extraction returned 0 rows! Aborting DAG run.")

        os.makedirs(STAGING_DIR, exist_ok=True)
        raw_staging_path = os.path.join(STAGING_DIR, f"{run_id}_raw.pkl")
        raw_df.to_pickle(raw_staging_path)

        # Also insert into raw_daily_prices audit log table immediately
        raw_inserted = load_raw(raw_df, use_airflow_host=True)
        logger.info("Saved %d raw records to %s and logged %d rows to raw_daily_prices.", len(raw_df), raw_staging_path, raw_inserted)

        return {
            "raw_staging_path": raw_staging_path,
            "raw_rows": len(raw_df),
            "raw_inserted_table": raw_inserted,
            "symbols": symbols,
            "run_id": run_id,
        }

    @task(task_id="transform_stock_prices")
    def transform(extract_meta: dict) -> dict:
        """Clean, validate, calculate technical indicators (return, MA7, MA30)."""
        raw_path = extract_meta["raw_staging_path"]
        run_id = extract_meta["run_id"]

        logger.info("Reading raw data from %s...", raw_path)
        raw_df = pd.read_pickle(raw_path)

        clean_df, rejected_df = transform_stock_data(raw_df)

        clean_path = os.path.join(STAGING_DIR, f"{run_id}_clean.pkl")
        rejected_path = os.path.join(STAGING_DIR, f"{run_id}_rejected.pkl")

        clean_df.to_pickle(clean_path)
        rejected_df.to_pickle(rejected_path)

        logger.info(
            "Transformation complete: %d clean records, %d rejected records.",
            len(clean_df),
            len(rejected_df),
        )

        return {
            "clean_staging_path": clean_path,
            "rejected_staging_path": rejected_path,
            "clean_count": len(clean_df),
            "rejected_count": len(rejected_df),
            "run_id": run_id,
        }

    @task(task_id="load_stock_prices")
    def load(transform_meta: dict) -> dict:
        """Upsert clean stock records and insert rejected audit records into PostgreSQL."""
        clean_path = transform_meta["clean_staging_path"]
        rej_path = transform_meta["rejected_staging_path"]

        clean_df = pd.read_pickle(clean_path)
        rejected_df = pd.read_pickle(rej_path)

        logger.info("Upserting %d clean records into fact_daily_stock_prices...", len(clean_df))
        clean_upserted = load_clean(clean_df, use_airflow_host=True)

        logger.info("Logging %d rejected records into rejected_records...", len(rejected_df))
        rejected_inserted = load_rejected(rejected_df, use_airflow_host=True)

        logger.info(
            "Load complete -> Upserted Clean: %d | Inserted Rejected: %d",
            clean_upserted,
            rejected_inserted,
        )

        return {
            "upserted_clean": clean_upserted,
            "inserted_rejected": rejected_inserted,
            "run_id": transform_meta["run_id"],
        }

    @task(task_id="run_data_quality_checks")
    def quality_checks(load_meta: dict) -> dict:
        """Run post-load data quality suite against fact_daily_stock_prices."""
        expected_count = load_meta.get("upserted_clean", 0)
        logger.info("Executing quality checks with expected clean count = %d...", expected_count)

        results = run_quality_checks(expected_clean_count=expected_count, use_airflow_host=True)
        logger.info("Quality check results: %s", results)

        if not results.get("all_checks_pass", False):
            raise AirflowException(
                f"Data quality checks failed! Results: {results}"
            )

        logger.info("All data quality checks passed successfully!")
        return results

    @task(task_id="cleanup_staging_files", trigger_rule="all_done")
    def cleanup(transform_meta: dict, extract_meta: dict) -> str:
        """Remove intermediate staging parquet files to preserve disk space."""
        paths = [
            extract_meta.get("raw_staging_path"),
            transform_meta.get("clean_staging_path"),
            transform_meta.get("rejected_staging_path"),
        ]
        removed_count = 0
        for p in paths:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                    removed_count += 1
                except Exception as e:
                    logger.warning("Could not remove %s: %s", p, e)

        logger.info("Cleaned up %d temporary staging files.", removed_count)
        return f"cleaned_{removed_count}_files"

    # Define task dependencies
    db_status = check_db()
    tables_ready = init_tables(db_status)
    ext_data = extract(tables_ready)
    trans_data = transform(ext_data)
    loaded_data = load(trans_data)
    qc_data = quality_checks(loaded_data)
    clean_task = cleanup(trans_data, ext_data)

    # Ensure cleanup runs after quality checks
    qc_data >> clean_task


# Instantiate DAG
finance_stock_etl_dag = finance_stock_etl()
