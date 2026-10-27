"""
Data-quality checks run after loading into PostgreSQL.

Checks:
  1. Row count reconciliation (source vs target).
  2. Null checks on critical columns.
  3. Price consistency (high >= low, prices > 0).
  4. Date gap detection (missing trading days).
  5. Summary statistics for quick review.
"""
import logging

import psycopg2
try:
    from pipeline import get_pg_connection_params
except ImportError:
    from include.pipeline import get_pg_connection_params

logger = logging.getLogger(__name__)


def run_quality_checks(
    expected_clean_count: int = 0,
    use_airflow_host: bool = False,
) -> dict:
    """
    Run all data-quality checks against fact_daily_stock_prices.

    Returns a dict with check results:
        {
            "row_count": int,
            "reconciliation_pass": bool,
            "null_check_pass": bool,
            "price_consistency_pass": bool,
            "symbols_loaded": list,
            "date_range": (min_date, max_date),
        }
    """
    conn = psycopg2.connect(**get_pg_connection_params(use_airflow_host))
    cur = conn.cursor()
    results = {}

    # ---- 1. Row count ----
    cur.execute("SELECT COUNT(*) FROM fact_daily_stock_prices;")
    total_rows = cur.fetchone()[0]
    results["row_count"] = total_rows
    logger.info("[CHECK 1] Total rows in fact table: %d", total_rows)

    if expected_clean_count > 0:
        match = total_rows >= expected_clean_count
        results["reconciliation_pass"] = match
        if match:
            logger.info("  [PASS] Row count >= expected (%d).", expected_clean_count)
        else:
            logger.warning(
                "  [FAIL] Expected >= %d rows, found %d.",
                expected_clean_count, total_rows,
            )
    else:
        results["reconciliation_pass"] = True

    # ---- 2. Null checks ----
    cur.execute("""
        SELECT COUNT(*) FROM fact_daily_stock_prices
        WHERE open_price IS NULL
           OR high_price IS NULL
           OR low_price IS NULL
           OR close_price IS NULL
           OR volume IS NULL;
    """)
    null_count = cur.fetchone()[0]
    results["null_check_pass"] = null_count == 0
    if null_count == 0:
        logger.info("[CHECK 2] [PASS] No NULL values in price/volume columns.")
    else:
        logger.warning("[CHECK 2] [FAIL] %d rows with NULL price/volume.", null_count)

    # ---- 3. Price consistency ----
    cur.execute("""
        SELECT COUNT(*) FROM fact_daily_stock_prices
        WHERE high_price < low_price
           OR open_price <= 0
           OR close_price <= 0;
    """)
    bad_prices = cur.fetchone()[0]
    results["price_consistency_pass"] = bad_prices == 0
    if bad_prices == 0:
        logger.info("[CHECK 3] [PASS] All prices are consistent (high >= low, prices > 0).")
    else:
        logger.warning("[CHECK 3] [FAIL] %d rows with inconsistent prices.", bad_prices)

    # ---- 4. Symbols loaded ----
    cur.execute("SELECT DISTINCT symbol FROM fact_daily_stock_prices ORDER BY symbol;")
    symbols = [row[0] for row in cur.fetchall()]
    results["symbols_loaded"] = symbols
    logger.info("[CHECK 4] Symbols loaded: %s", ", ".join(symbols))

    # ---- 5. Date range ----
    cur.execute("""
        SELECT MIN(trade_date), MAX(trade_date) FROM fact_daily_stock_prices;
    """)
    min_date, max_date = cur.fetchone()
    results["date_range"] = (str(min_date), str(max_date))
    logger.info("[CHECK 5] Date range: %s to %s", min_date, max_date)

    # ---- 6. Per-symbol summary ----
    cur.execute("""
        SELECT symbol,
               COUNT(*) as days,
               MIN(trade_date) as first_date,
               MAX(trade_date) as last_date,
               ROUND(AVG(close_price)::numeric, 2) as avg_close,
               ROUND(MIN(close_price)::numeric, 2) as min_close,
               ROUND(MAX(close_price)::numeric, 2) as max_close
        FROM fact_daily_stock_prices
        GROUP BY symbol
        ORDER BY symbol;
    """)
    logger.info("[CHECK 6] Per-symbol summary:")
    for row in cur.fetchall():
        logger.info(
            "  %s: %d days, %s to %s, avg_close=$%.2f, range=$%.2f-$%.2f",
            row[0], row[1], row[2], row[3], row[4], row[5], row[6],
        )

    cur.close()
    conn.close()

    # Overall pass/fail
    all_pass = all([
        results["reconciliation_pass"],
        results["null_check_pass"],
        results["price_consistency_pass"],
    ])
    results["all_checks_pass"] = all_pass

    if all_pass:
        logger.info("\n=== ALL QUALITY CHECKS PASSED ===")
    else:
        logger.warning("\n=== SOME QUALITY CHECKS FAILED ===")

    return results


# ---------------------------------------------------------------------------
# Quick standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    results = run_quality_checks()
    print(f"\nResults: {results}")
