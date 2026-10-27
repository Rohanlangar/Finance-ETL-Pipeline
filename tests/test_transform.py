"""Unit tests for the Finance ETL pipeline."""
import pandas as pd
import numpy as np
import pytest
from datetime import date, datetime

# Add project root to path
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.transform import transform_stock_data


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------
@pytest.fixture
def sample_raw_df():
    """Create a realistic raw DataFrame as extract would produce."""
    return pd.DataFrame({
        "symbol": ["AAPL"] * 5 + ["MSFT"] * 5,
        "date": pd.date_range("2024-01-02", periods=5).tolist() * 2,
        "open":  [185.0, 186.5, 187.2, 184.0, 188.5,  375.0, 376.5, 377.2, 374.0, 378.5],
        "high":  [187.0, 188.5, 189.2, 186.0, 190.5,  377.0, 378.5, 379.2, 376.0, 380.5],
        "low":   [183.0, 184.5, 185.2, 182.0, 186.5,  373.0, 374.5, 375.2, 372.0, 376.5],
        "close": [186.0, 187.5, 188.2, 185.0, 189.5,  376.0, 377.5, 378.2, 375.0, 379.5],
        "volume": [50000000, 48000000, 52000000, 45000000, 55000000,
                   30000000, 28000000, 32000000, 25000000, 35000000],
        "extracted_at": [datetime.utcnow()] * 10,
    })


@pytest.fixture
def bad_data_df():
    """DataFrame with intentional quality issues."""
    return pd.DataFrame({
        "symbol": ["BAD1", "BAD2", "BAD3", "GOOD"],
        "date": pd.date_range("2024-01-02", periods=4).tolist(),
        "open":  [-10.0, 100.0, 100.0, 150.0],    # BAD1: negative price
        "high":  [50.0,  90.0,  110.0, 155.0],     # BAD2: high < low
        "low":   [40.0,  100.0, 100.0, 148.0],
        "close": [45.0,  95.0,  105.0, 152.0],
        "volume": [1000, 2000, 3000, 4000],
        "extracted_at": [datetime.utcnow()] * 4,
    })


# -------------------------------------------------------------------
# Tests for Transform
# -------------------------------------------------------------------
class TestTransform:
    def test_returns_clean_and_rejected(self, sample_raw_df):
        """Transform should return a tuple of (clean, rejected)."""
        clean, rejected = transform_stock_data(sample_raw_df)
        assert isinstance(clean, pd.DataFrame)
        assert isinstance(rejected, pd.DataFrame)

    def test_clean_has_derived_columns(self, sample_raw_df):
        """Clean data should include daily_return_pct, ma_7, ma_30, intraday_range."""
        clean, _ = transform_stock_data(sample_raw_df)
        expected_cols = {"daily_return_pct", "ma_7", "ma_30", "intraday_range"}
        assert expected_cols.issubset(set(clean.columns))

    def test_no_duplicates_in_clean(self, sample_raw_df):
        """No duplicate (symbol, date) pairs in clean output."""
        # Add a duplicate row
        dup_df = pd.concat([sample_raw_df, sample_raw_df.iloc[[0]]], ignore_index=True)
        clean, _ = transform_stock_data(dup_df)
        assert clean.duplicated(subset=["symbol", "date"]).sum() == 0

    def test_rejects_negative_prices(self, bad_data_df):
        """Rows with negative prices should be rejected."""
        clean, rejected = transform_stock_data(bad_data_df)
        rejected_symbols = rejected["symbol"].tolist()
        assert "BAD1" in rejected_symbols

    def test_rejects_high_less_than_low(self, bad_data_df):
        """Rows where high < low should be rejected."""
        _, rejected = transform_stock_data(bad_data_df)
        rejected_symbols = rejected["symbol"].tolist()
        assert "BAD2" in rejected_symbols

    def test_good_rows_pass_through(self, bad_data_df):
        """Valid rows should appear in clean output."""
        clean, _ = transform_stock_data(bad_data_df)
        assert "GOOD" in clean["symbol"].values

    def test_daily_return_first_row_is_zero(self, sample_raw_df):
        """First row per symbol should have daily_return_pct = 0."""
        clean, _ = transform_stock_data(sample_raw_df)
        for symbol in clean["symbol"].unique():
            sym_data = clean[clean["symbol"] == symbol].sort_values("date")
            assert sym_data.iloc[0]["daily_return_pct"] == 0.0

    def test_intraday_range_calculation(self, sample_raw_df):
        """intraday_range should equal high - low."""
        clean, _ = transform_stock_data(sample_raw_df)
        expected = clean["high"] - clean["low"]
        pd.testing.assert_series_equal(
            clean["intraday_range"].reset_index(drop=True),
            expected.round(4).reset_index(drop=True),
            check_names=False,
        )

    def test_empty_input(self):
        """Empty DataFrame should return two empty DataFrames."""
        clean, rejected = transform_stock_data(pd.DataFrame())
        assert clean.empty
        assert rejected.empty

    def test_volume_is_int(self, sample_raw_df):
        """Volume should be cast to int64."""
        clean, _ = transform_stock_data(sample_raw_df)
        assert clean["volume"].dtype == np.int64
