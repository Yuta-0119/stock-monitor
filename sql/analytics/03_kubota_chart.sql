-- STEP 3: チャートシグナル
-- MA200トレンド・レンジ収縮・出来高サージ・52週高値付近を評価
CREATE OR REPLACE VIEW `onitsuka-app.analytics.kubota_chart` AS
WITH daily AS (
  SELECT code, date, open, high, low, close, volume, turnover_value
  FROM `onitsuka-app.stock_raw.daily_quotes`
  WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 300 DAY)
),
ma AS (
  SELECT *,
    AVG(close) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS ma_200,
    AVG(close) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 219 PRECEDING AND 20 PRECEDING
    ) AS ma_200_20d_ago,
    AVG(close) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) AS ma_25,
    AVG(volume) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_volume_20d,
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS high_10d,
    MIN(low) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS low_10d,
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS high_30d,
    MIN(low) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS low_30d,
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    ) AS high_52w,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM daily
)
SELECT
  code, date, close,
  ROUND(ma_200, 1) AS ma_200,
  ROUND(ma_25, 1) AS ma_25,
  CASE
    WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) > 0.005 THEN 'UP'
    WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) < -0.005 THEN 'DOWN'
    ELSE 'FLAT'
  END AS ma200_trend,
  CASE WHEN close > ma_200 THEN 'ABOVE' ELSE 'BELOW' END AS price_vs_ma200,
  ROUND(SAFE_DIVIDE(volume, avg_volume_20d), 2) AS volume_ratio,
  CASE WHEN SAFE_DIVIDE(volume, avg_volume_20d) >= 1.5 THEN TRUE ELSE FALSE END AS volume_surge,
  ROUND(SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d), 3) AS range_contraction,
  CASE WHEN SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d) < 0.5 THEN TRUE ELSE FALSE END AS consolidation,
  CASE WHEN close >= high_52w * 0.98 THEN TRUE ELSE FALSE END AS near_52w_high,
  (CASE WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) > 0.005 AND close > ma_200 THEN 2 ELSE 0 END)
  + (CASE WHEN SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d) < 0.5 THEN 2 ELSE 0 END)
  + (CASE WHEN SAFE_DIVIDE(volume, avg_volume_20d) >= 1.5 THEN 3
          WHEN SAFE_DIVIDE(volume, avg_volume_20d) >= 1.2 THEN 1
          ELSE 0 END)
  AS chart_score
FROM ma
WHERE rn = 1;
