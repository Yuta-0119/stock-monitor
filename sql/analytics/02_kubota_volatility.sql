-- STEP 2: ボラティリティ評価
-- ATR・HV・レンジ収縮でボラティリティスコアを算出
CREATE OR REPLACE VIEW `onitsuka-app.analytics.kubota_volatility` AS
WITH daily AS (
  SELECT
    code, date, high, low, close,
    LAG(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
  FROM `onitsuka-app.stock_raw.daily_quotes`
  WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 120 DAY)
),
tr AS (
  SELECT *,
    GREATEST(
      high - low,
      ABS(high - IFNULL(prev_close, high)),
      ABS(low - IFNULL(prev_close, low))
    ) AS true_range
  FROM daily
),
rolling AS (
  SELECT
    code, date, close,
    AVG(true_range) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS atr_14,
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 21 PRECEDING AND CURRENT ROW
    ) AS high_22d,
    MIN(low) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 21 PRECEDING AND CURRENT ROW
    ) AS low_22d,
    STDDEV(SAFE_DIVIDE(close - prev_close, prev_close)) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) * SQRT(250) AS hv_20d,
    STDDEV(SAFE_DIVIDE(close - prev_close, prev_close)) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) * SQRT(250) AS hv_60d,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM tr
)
SELECT
  code, date, close,
  ROUND(atr_14, 1) AS atr_14,
  ROUND(SAFE_DIVIDE(atr_14, close) * 100, 2) AS atr_pct,
  ROUND(SAFE_DIVIDE(high_22d - low_22d, low_22d) * 100, 1) AS range_1m_pct,
  ROUND(hv_20d * 100, 1) AS hv_20d_pct,
  ROUND(hv_60d * 100, 1) AS hv_60d_pct,
  (CASE WHEN SAFE_DIVIDE(atr_14, close) >= 0.015 THEN 2 ELSE 0 END)
  + (CASE WHEN hv_20d < hv_60d THEN 2 ELSE 0 END)
  + (CASE
      WHEN SAFE_DIVIDE(high_22d - low_22d, low_22d) BETWEEN 0.15 AND 0.80 THEN 1
      ELSE 0
    END)
  AS volatility_score,
  CASE WHEN hv_20d < hv_60d THEN TRUE ELSE FALSE END AS hv_contraction
FROM rolling
WHERE rn = 1;
