"""
TRANSFORM module -- Clean, validate, and enrich raw stock data.

Responsibilities:
  1. Cast types (date -> date, prices -> Decimal-safe float, volume -> int).
  2. Drop exact duplicates.
  3. Validate business rules (prices > 0, volume >= 0, high >= low).
  4. Compute derived columns (daily_return, ma_7, ma_30).
  5. Flag invalid rows instead of silently dropping them.
"""
import logging
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def transform_stock_data(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform raw extracted data into clean records.

    Args:
        raw_df: DataFrame from extract_stock_data().

    Returns:
        (clean_df, rejected_df)
        clean_df:    Validated rows ready for loading.
        rejected_df: Rows that failed validation with a 'rejection_reason' column.
    """
    if raw_df.empty:
        logger.warning("Empty DataFrame received -- nothing to transform.")
        return pd.DataFrame(), pd.DataFrame()

    df = raw_df.copy()
    initial_count = len(df)
    logger.info("Transforming %d raw rows...", initial_count)

    # ---------------------------------------------------------------
    # 1. TYPE CASTING
    # ---------------------------------------------------------------
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")

    # ---------------------------------------------------------------
    # 2. DEDUPLICATION (same symbol + same date = duplicate)
    # ---------------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    dupes_removed = before_dedup - len(df)
    if dupes_removed:
        logger.info("  Removed %d duplicate rows.", dupes_removed)

    # ---------------------------------------------------------------
    # 3. VALIDATION -- flag bad rows
    # ---------------------------------------------------------------
    rejection_reasons = []

    # Null prices
    null_mask = df[["open", "high", "low", "close"]].isnull().any(axis=1)
    rejection_reasons.append((null_mask, "NULL price value"))

    # Prices must be positive
    neg_mask = (df[["open", "high", "low", "close"]] <= 0).any(axis=1)
    rejection_reasons.append((neg_mask, "Non-positive price"))

    # High must be >= Low
    hl_mask = df["high"] < df["low"]
    rejection_reasons.append((hl_mask, "High < Low"))

    # Volume must be non-negative
    vol_mask = df["volume"] < 0
    rejection_reasons.append((vol_mask, "Negative volume"))

    # Combine rejection masks
    combined_reject_mask = pd.Series(False, index=df.index)
    combined_reasons = pd.Series("", index=df.index)

    for mask, reason in rejection_reasons:
        combined_reject_mask = combined_reject_mask | mask
        combined_reasons = combined_reasons.where(~mask, combined_reasons + reason + "; ")

    rejected_df = df[combined_reject_mask].copy()
    rejected_df["rejection_reason"] = combined_reasons[combined_reject_mask]

    clean_df = df[~combined_reject_mask].copy()

    logger.info(
        "  Validation: %d clean, %d rejected out of %d.",
        len(clean_df), len(rejected_df), len(df),
    )

    # ---------------------------------------------------------------
    # 4. DERIVED COLUMNS (computed on clean data only)
    # ---------------------------------------------------------------
    # Daily return: (close - prev_close) / prev_close, per symbol
    clean_df = clean_df.sort_values(["symbol", "date"])
    clean_df["daily_return_pct"] = (
        clean_df.groupby("symbol")["close"]
        .pct_change()
        .round(6)
    )

    # 7-day and 30-day simple moving averages of close price
    clean_df["ma_7"] = (
        clean_df.groupby("symbol")["close"]
        .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
        .round(4)
    )
    clean_df["ma_30"] = (
        clean_df.groupby("symbol")["close"]
        .transform(lambda s: s.rolling(window=30, min_periods=1).mean())
        .round(4)
    )

    # Intraday price range
    clean_df["intraday_range"] = (clean_df["high"] - clean_df["low"]).round(4)

    # Replace NaN returns (first row per symbol) with 0
    clean_df["daily_return_pct"] = clean_df["daily_return_pct"].fillna(0.0)

    logger.info("  Derived columns added: daily_return_pct, ma_7, ma_30, intraday_range.")
    logger.info("Transform complete: %d clean rows ready to load.", len(clean_df))

    return clean_df, rejected_df


# ---------------------------------------------------------------------------
# Quick standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from pipeline.extract import extract_stock_data

    raw = extract_stock_data(symbols=["AAPL"], period="1mo")
    clean, rejected = transform_stock_data(raw)

    print("\n--- Clean Data Sample (first 5 rows) ---")
    print(clean.head().to_string(index=False))
    print(f"\nClean shape: {clean.shape}")
    print(f"Clean columns: {list(clean.columns)}")

    if not rejected.empty:
        print(f"\n--- Rejected Rows ({len(rejected)}) ---")
        print(rejected.to_string(index=False))
    else:
        print("\nNo rejected rows.")
