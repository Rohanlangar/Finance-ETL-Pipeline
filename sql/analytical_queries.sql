-- =====================================================================
-- Analytical SQL Queries for finance_db
-- Run these in psql, pgAdmin, or any PostgreSQL client.
-- =====================================================================

-- 1. Latest closing prices for all symbols
SELECT symbol, trade_date, close_price, volume
FROM fact_daily_stock_prices
WHERE trade_date = (SELECT MAX(trade_date) FROM fact_daily_stock_prices)
ORDER BY symbol;


-- 2. Top 10 highest daily returns (biggest single-day gainers)
SELECT symbol, trade_date, close_price,
       ROUND(daily_return_pct * 100, 2) AS return_pct
FROM fact_daily_stock_prices
WHERE daily_return_pct IS NOT NULL
ORDER BY daily_return_pct DESC
LIMIT 10;


-- 3. Top 10 worst daily returns (biggest single-day losers)
SELECT symbol, trade_date, close_price,
       ROUND(daily_return_pct * 100, 2) AS return_pct
FROM fact_daily_stock_prices
WHERE daily_return_pct IS NOT NULL
ORDER BY daily_return_pct ASC
LIMIT 10;


-- 4. Average daily volume per symbol (most traded stocks)
SELECT symbol,
       ROUND(AVG(volume)) AS avg_daily_volume,
       ROUND(AVG(close_price)::numeric, 2) AS avg_close
FROM fact_daily_stock_prices
GROUP BY symbol
ORDER BY avg_daily_volume DESC;


-- 5. 52-week high and low per symbol
SELECT symbol,
       MIN(low_price) AS year_low,
       MAX(high_price) AS year_high,
       ROUND((MAX(high_price) - MIN(low_price)) / MIN(low_price) * 100, 2)
           AS range_pct
FROM fact_daily_stock_prices
GROUP BY symbol
ORDER BY range_pct DESC;


-- 6. Monthly average close price (time-series aggregation)
SELECT symbol,
       DATE_TRUNC('month', trade_date)::DATE AS month,
       ROUND(AVG(close_price)::numeric, 2) AS avg_close,
       ROUND(AVG(volume)) AS avg_volume
FROM fact_daily_stock_prices
GROUP BY symbol, DATE_TRUNC('month', trade_date)
ORDER BY symbol, month;


-- 7. Volatility ranking (std deviation of daily returns)
SELECT symbol,
       COUNT(*) AS trading_days,
       ROUND(STDDEV(daily_return_pct)::numeric * 100, 4) AS volatility_pct,
       ROUND(AVG(daily_return_pct)::numeric * 100, 4) AS avg_return_pct
FROM fact_daily_stock_prices
WHERE daily_return_pct != 0
GROUP BY symbol
ORDER BY volatility_pct DESC;


-- 8. Days where intraday range exceeded 5% of close
SELECT symbol, trade_date, open_price, high_price, low_price, close_price,
       intraday_range,
       ROUND((intraday_range / close_price * 100)::numeric, 2) AS range_pct
FROM fact_daily_stock_prices
WHERE intraday_range / close_price > 0.05
ORDER BY range_pct DESC;


-- 9. Golden cross detection: MA_7 crosses above MA_30
-- (simplified: days where MA_7 > MA_30 but previous day MA_7 <= MA_30)
WITH lagged AS (
    SELECT symbol, trade_date, close_price, ma_7, ma_30,
           LAG(ma_7) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_ma_7,
           LAG(ma_30) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_ma_30
    FROM fact_daily_stock_prices
)
SELECT symbol, trade_date, close_price, ma_7, ma_30
FROM lagged
WHERE ma_7 > ma_30
  AND prev_ma_7 <= prev_ma_30
ORDER BY trade_date DESC;


-- 10. Source-to-target reconciliation: compare raw vs fact row counts
SELECT 'raw_daily_prices' AS table_name, COUNT(*) AS row_count
FROM raw_daily_prices
UNION ALL
SELECT 'fact_daily_stock_prices', COUNT(*)
FROM fact_daily_stock_prices
UNION ALL
SELECT 'rejected_records', COUNT(*)
FROM rejected_records;
