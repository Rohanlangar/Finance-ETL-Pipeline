"""
run_pipeline.py -- Execute the full ETL pipeline locally (no Airflow).

Usage:
    .venv\Scripts\python.exe run_pipeline.py
    .venv\Scripts\python.exe run_pipeline.py --symbols AAPL MSFT --period 1mo
"""
import argparse
import logging
import sys

from pipeline import SYMBOLS, HISTORY_PERIOD
from pipeline.extract import extract_stock_data
from pipeline.transform import transform_stock_data
from pipeline.load import create_tables, load_raw, load_clean, load_rejected
from pipeline.quality_checks import run_quality_checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline.runner")


def main(symbols: list[str], period: str) -> None:
    logger.info("=" * 60)
    logger.info("FINANCE ETL PIPELINE -- Starting")
    logger.info("Symbols: %s | Period: %s", symbols, period)
    logger.info("=" * 60)

    # Step 0: Ensure tables exist
    logger.info("\n--- STEP 0: Creating tables ---")
    create_tables()

    # Step 1: EXTRACT
    logger.info("\n--- STEP 1: EXTRACT ---")
    raw_df = extract_stock_data(symbols=symbols, period=period)
    if raw_df.empty:
        logger.error("No data extracted. Aborting pipeline.")
        sys.exit(1)
    logger.info("Extracted %d raw rows.", len(raw_df))

    # Step 2: TRANSFORM
    logger.info("\n--- STEP 2: TRANSFORM ---")
    clean_df, rejected_df = transform_stock_data(raw_df)
    logger.info("Clean: %d rows | Rejected: %d rows", len(clean_df), len(rejected_df))

    # Step 3: LOAD
    logger.info("\n--- STEP 3: LOAD ---")
    raw_count = load_raw(raw_df)
    clean_count = load_clean(clean_df)
    rej_count = load_rejected(rejected_df)
    logger.info(
        "Loaded -> Raw: %d | Clean: %d | Rejected: %d",
        raw_count, clean_count, rej_count,
    )

    # Step 4: QUALITY CHECKS
    logger.info("\n--- STEP 4: DATA QUALITY CHECKS ---")
    results = run_quality_checks(expected_clean_count=clean_count)

    logger.info("\n" + "=" * 60)
    if results["all_checks_pass"]:
        logger.info("PIPELINE COMPLETE -- All checks passed!")
    else:
        logger.warning("PIPELINE COMPLETE -- Some checks failed. Review logs above.")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finance ETL Pipeline")
    parser.add_argument(
        "--symbols", nargs="+", default=SYMBOLS,
        help="Ticker symbols to fetch (default: %(default)s)",
    )
    parser.add_argument(
        "--period", default=HISTORY_PERIOD,
        help="History period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y (default: %(default)s)",
    )
    args = parser.parse_args()
    main(symbols=args.symbols, period=args.period)
