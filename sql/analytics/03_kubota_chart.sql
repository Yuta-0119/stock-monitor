-- STEP 3: チャートシグナル
-- 窪田フレームワーク準拠版（2024-03修正）
-- MA200上向き(1pt) + MA200上(1pt) + もみ合い収縮(2pt) + 出来高急増(1pt) = 5点満点
-- 放れ判定(breakout): 終値が直近10日高値を更新 → ENTRY SIGNALの必須条件
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
    -- 直近10日・30日の高値/安値（もみ合い収縮判定）
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
    -- 前日の10日高値（放れ判定用：今日の終値 > 前日時点の10日高値）
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS high_10d_prev,
    -- 52週高値（価格強度スコア用）
    MAX(high) OVER (
      PARTITION BY code ORDER BY date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
    ) AS high_52w,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM daily
)
SELECT
  code, date, close,
  ROUND(ma_200, 1) AS ma_200,
  ROUND(ma_25, 1)  AS ma_25,

  -- MA200トレンド（20日前比 +0.5%超 = 上向き）
  CASE
    WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) > 0.005 THEN 'UP'
    WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) < -0.005 THEN 'DOWN'
    ELSE 'FLAT'
  END AS ma200_trend,

  -- 終値 vs MA200
  CASE WHEN close > ma_200 THEN 'ABOVE' ELSE 'BELOW' END AS price_vs_ma200,

  -- 出来高指標
  ROUND(SAFE_DIVIDE(volume, avg_volume_20d), 2) AS volume_ratio,
  CASE WHEN SAFE_DIVIDE(volume, avg_volume_20d) >= 1.5 THEN TRUE ELSE FALSE END AS volume_surge,

  -- もみ合い収縮（10日値幅 / 30日値幅 < 0.5）
  ROUND(SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d), 3) AS range_contraction,
  CASE WHEN SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d) < 0.5
    THEN TRUE ELSE FALSE END AS consolidation,

  -- 放れ判定：終値が前日時点の10日高値を更新（もみ合いからのブレイクアウト）
  CASE WHEN close >= high_10d_prev AND high_10d_prev IS NOT NULL
    THEN TRUE ELSE FALSE END AS breakout,

  -- 52週高値付近
  CASE WHEN close >= high_52w * 0.98 THEN TRUE ELSE FALSE END AS near_52w_high,

  -- ★ チャートスコア（5点満点）
  -- 修正前: MA200かつ条件2点 + もみ合い2点 + 出来高最大3点 = 7点満点（出来高過大）
  -- 修正後: MA200上向き1点 + MA200上1点 + もみ合い2点 + 出来高急増1点 = 5点満点
  (CASE WHEN SAFE_DIVIDE(ma_200 - ma_200_20d_ago, ma_200_20d_ago) > 0.005 THEN 1 ELSE 0 END)
  + (CASE WHEN close > ma_200 THEN 1 ELSE 0 END)
  + (CASE WHEN SAFE_DIVIDE(high_10d - low_10d, high_30d - low_30d) < 0.5 THEN 2 ELSE 0 END)
  + (CASE WHEN SAFE_DIVIDE(volume, avg_volume_20d) >= 1.5 THEN 1 ELSE 0 END)
  AS chart_score

FROM ma
WHERE rn = 1;
