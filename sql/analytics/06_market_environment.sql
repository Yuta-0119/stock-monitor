-- STEP 6: 相場環境（TOPIX版）
-- TOPIX MA200トレンドで相場フェーズを判定
-- 注意: 日経225はAPIで取得不可のためTOPIXで代替
CREATE OR REPLACE VIEW `onitsuka-app.analytics.market_environment` AS
WITH topix AS (
  SELECT
    date, close,
    AVG(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS topix_ma200,
    AVG(close) OVER (ORDER BY date ROWS BETWEEN 219 PRECEDING AND 20 PRECEDING) AS topix_ma200_20d_ago,
    ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
  FROM `onitsuka-app.stock_raw.topix_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 300 DAY)
)
SELECT
  date,
  ROUND(close, 2) AS topix_close,
  ROUND(topix_ma200, 2) AS topix_ma200,
  CASE
    WHEN SAFE_DIVIDE(topix_ma200 - topix_ma200_20d_ago, topix_ma200_20d_ago) > 0.005 THEN 'BULL'
    WHEN SAFE_DIVIDE(topix_ma200 - topix_ma200_20d_ago, topix_ma200_20d_ago) < -0.005 THEN 'BEAR'
    ELSE 'NEUTRAL'
  END AS market_phase,
  CASE
    WHEN close > topix_ma200
      AND SAFE_DIVIDE(topix_ma200 - topix_ma200_20d_ago, topix_ma200_20d_ago) > 0.005 THEN 3
    WHEN close > topix_ma200 THEN 2
    ELSE 1
  END AS environment_score
FROM topix
WHERE rn = 1;
