-- ============================================================
-- 08_backtest_signals: 過去シグナルのバックテスト（窪田条件版）
-- ============================================================
-- 条件: MA200上 + ATR>=1.5% + HV収縮 + 値幅収縮 + 出来高急増(1.5x)
-- 過去500日のプライム銘柄を対象に+5/+10/+20日後リターンを計算
-- ============================================================
CREATE OR REPLACE VIEW `onitsuka-app.analytics.backtest_signals` AS
WITH prime_codes AS (
  SELECT code
  FROM `onitsuka-app.stock_master.equity_master`
  WHERE market_segment LIKE '%プライム%'
),
daily AS (
  SELECT
    dq.code, dq.date, dq.close, dq.volume, dq.high, dq.low,
    LAG(dq.close) OVER (PARTITION BY dq.code ORDER BY dq.date) AS prev_close
  FROM `onitsuka-app.stock_raw.daily_quotes` dq
  INNER JOIN prime_codes pc ON dq.code = pc.code
  WHERE dq.date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 500 DAY)
    AND dq.close > 0 AND dq.volume > 0
),
with_indicators AS (
  SELECT
    code, date, close, volume, high, low, prev_close,
    AVG(close) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
    AVG(volume) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_vol_20d,
    AVG(GREATEST(
      high - low,
      ABS(high - IFNULL(prev_close, high)),
      ABS(low  - IFNULL(prev_close, low))
    )) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr_14,
    STDDEV(SAFE_DIVIDE(close - prev_close, prev_close)) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) * SQRT(250) AS hv_20d,
    STDDEV(SAFE_DIVIDE(close - prev_close, prev_close)) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) * SQRT(250) AS hv_60d,
    SAFE_DIVIDE(
      MAX(high) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) -
      MIN(low)  OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
      NULLIF(
        MAX(high) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) -
        MIN(low)  OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        0
      )
    ) AS range_contraction,
    COUNT(*) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS row_cnt,
    LEAD(close, 5)  OVER (PARTITION BY code ORDER BY date) AS close_5d,
    LEAD(close, 10) OVER (PARTITION BY code ORDER BY date) AS close_10d,
    LEAD(close, 20) OVER (PARTITION BY code ORDER BY date) AS close_20d
  FROM daily
),
signals AS (
  SELECT * FROM with_indicators
  WHERE row_cnt >= 150
    AND close > ma200
    AND SAFE_DIVIDE(atr_14, close) >= 0.015
    AND hv_20d < hv_60d
    AND range_contraction < 0.5
    AND SAFE_DIVIDE(volume, avg_vol_20d) >= 1.5
    AND close_5d IS NOT NULL
)
SELECT
  code,
  date AS signal_date,
  ROUND(close, 0) AS entry_price,
  ROUND(SAFE_DIVIDE(volume, avg_vol_20d), 2) AS volume_ratio,
  ROUND(SAFE_DIVIDE(atr_14, close) * 100, 2) AS atr_pct,
  ROUND(hv_20d * 100, 1) AS hv_20d_pct,
  ROUND(hv_60d * 100, 1) AS hv_60d_pct,
  ROUND(range_contraction, 3) AS range_contraction,
  ROUND(SAFE_DIVIDE(close_5d  - close, close) * 100, 2) AS return_5d_pct,
  ROUND(SAFE_DIVIDE(close_10d - close, close) * 100, 2) AS return_10d_pct,
  ROUND(SAFE_DIVIDE(close_20d - close, close) * 100, 2) AS return_20d_pct,
  CASE WHEN close_5d  > close THEN 1 ELSE 0 END AS win_5d,
  CASE WHEN close_10d > close THEN 1 ELSE 0 END AS win_10d,
  CASE WHEN close_20d IS NOT NULL AND close_20d > close THEN 1
       WHEN close_20d IS NOT NULL THEN 0
       ELSE NULL END AS win_20d
FROM signals
ORDER BY signal_date DESC, code
