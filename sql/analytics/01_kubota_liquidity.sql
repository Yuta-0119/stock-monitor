-- STEP 1: 流動性フィルター
-- 窪田フレームワーク: 日次売買代金20日平均・変動係数(CV)でスクリーニング
CREATE OR REPLACE VIEW `onitsuka-app.analytics.kubota_liquidity` AS
WITH daily AS (
  SELECT
    code,
    date,
    turnover_value,
    volume
  FROM `onitsuka-app.stock_raw.daily_quotes`
  WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 60 DAY)
),
rolling AS (
  SELECT
    code,
    date,
    turnover_value,
    AVG(turnover_value) OVER (
      PARTITION BY code ORDER BY date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_turnover_20d,
    STDDEV(turnover_value) OVER (
      PARTITION BY code ORDER BY date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS std_turnover_20d,
    COUNT(*) OVER (
      PARTITION BY code ORDER BY date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS cnt_20d
  FROM daily
),
latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM rolling
  WHERE cnt_20d >= 15
)
SELECT
  code,
  date,
  turnover_value AS latest_turnover,
  ROUND(avg_turnover_20d / 100000000, 2) AS avg_turnover_20d_oku,
  ROUND(SAFE_DIVIDE(std_turnover_20d, avg_turnover_20d), 3) AS cv_value,
  CASE
    WHEN avg_turnover_20d >= 5000000000 THEN 'PASS_A'
    WHEN avg_turnover_20d >= 3000000000 THEN 'PASS_B'
    WHEN avg_turnover_20d >= 1000000000 THEN 'PASS_C'
    ELSE 'FAIL'
  END AS liquidity_grade,
  CASE
    WHEN SAFE_DIVIDE(std_turnover_20d, avg_turnover_20d) < 1.0 THEN 'PASS'
    WHEN SAFE_DIVIDE(std_turnover_20d, avg_turnover_20d) < 1.5 THEN 'WARN'
    ELSE 'FAIL'
  END AS cv_grade
FROM latest
WHERE rn = 1;
