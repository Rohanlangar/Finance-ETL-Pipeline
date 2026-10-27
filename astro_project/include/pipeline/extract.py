"""
EXTRACT module -- Fetch daily stock prices from Yahoo Finance.

Uses the `yfinance` library which scrapes public Yahoo Finance data.
No API key needed, no strict rate limits.

Returns raw DataFrames with original column names and types
so the Transform step can handle all cleaning.
"""
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf

try:
    from pipeline import SYMBOLS, HISTORY_PERIOD
except ImportError:
    from include.pipeline import SYMBOLS, HISTORY_PERIOD

logger = logging.getLogger(__name__)


def extract_stock_data(
    symbols: Optional[list[str]] = None,
    period: str = HISTORY_PERIOD,
) -> pd.DataFrame:
    """
    Download daily OHLCV data for each symbol from Yahoo Finance.

    Args:
        symbols: List of ticker symbols (defaults to config.SYMBOLS).
        period:  How far back to fetch, e.g. '1y', '6mo', '5d'.

    Returns:
        A single DataFrame with columns:
            symbol, date, open, high, low, close, volume
        One row per symbol per trading day.
    """
    symbols = symbols or SYMBOLS
    all_frames: list[pd.DataFrame] = []

    for symbol in symbols:
        logger.info("Extracting %s (period=%s)...", symbol, period)
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, timeout=30)

            if hist.empty:
                logger.warning("No data returned for %s -- skipping.", symbol)
                continue

            # Reset index so Date becomes a regular column
            hist = hist.reset_index()

            # Keep only the columns we need and rename to snake_case
            hist = hist[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
            hist.columns = ["date", "open", "high", "low", "close", "volume"]

            # Tag each row with the symbol
            hist.insert(0, "symbol", symbol)

            # Record when we extracted this data
            hist["extracted_at"] = datetime.utcnow()

            all_frames.append(hist)
            logger.info("  -> %d rows for %s", len(hist), symbol)

        except Exception as exc:
            logger.error("Failed to extract %s: %s", symbol, exc)
            # Continue with remaining symbols instead of crashing
            continue

    if not all_frames:
        logger.error("No data extracted for any symbol!")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    logger.info(
        "Extraction complete: %d total rows for %d symbols.",
        len(combined),
        len(all_frames),
    )
    return combined


# ---------------------------------------------------------------------------
# Quick standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    df = extract_stock_data(symbols=["AAPL"], period="5d")
    if not df.empty:
        print("\n--- Extracted Data Sample ---")
        print(df.to_string(index=False))
        print(f"\nShape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Dtypes:\n{df.dtypes}")
    else:
        print("No data extracted.")
