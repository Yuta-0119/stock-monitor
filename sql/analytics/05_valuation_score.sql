-- STEP 5: バリュエーションスコア
-- PER・PBRをEPS/BPSから計算し、セクター平均との相対評価
-- equity_master カラム名: sector33_name, market_segment
CREATE OR REPLACE VIEW `onitsuka-app.analytics.valuation_score` AS
WITH latest_price AS (
  SELECT
    code, date, close,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
  FROM `onitsuka-app.stock_raw.daily_quotes`
  WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 10 DAY)
),
price AS (
  SELECT code, date, close FROM latest_price WHERE rn = 1
),
with_valuation AS (
  SELECT
    p.code,
    p.date,
    p.close,
    f.eps,
    f.bps,
    CASE WHEN f.eps > 0 THEN ROUND(SAFE_DIVIDE(p.close, f.eps), 1) ELSE NULL END AS per,
    CASE WHEN f.bps > 0 THEN ROUND(SAFE_DIVIDE(p.close, f.bps), 2) ELSE NULL END AS pbr
  FROM price p
  LEFT JOIN `onitsuka-app.analytics.fundamental_score` f ON p.code = f.code
),
sector_avg AS (
  SELECT
    em.sector33_name,
    AVG(wv.per) AS sector_avg_per
  FROM with_valuation wv
  JOIN `onitsuka-app.stock_master.equity_master` em ON wv.code = em.code
  WHERE wv.per > 0 AND wv.per < 200
  GROUP BY em.sector33_name
)
SELECT
  wv.code,
  wv.date,
  wv.close,
  wv.per,
  wv.pbr,
  sa.sector_avg_per,
  -- PER相対スコア（5点満点）
  CASE
    WHEN wv.per > 0 AND wv.per < sa.sector_avg_per * 0.8 THEN 5
    WHEN wv.per > 0 AND wv.per <= sa.sector_avg_per THEN 3
    WHEN wv.per > 0 AND wv.per <= sa.sector_avg_per * 1.3 THEN 1
    ELSE 0
  END AS per_score,
  -- PBRスコア（5点満点）
  (CASE WHEN wv.pbr > 0 AND wv.pbr < 1.5 THEN 3 ELSE 0 END)
  + (CASE WHEN wv.pbr > 0 AND wv.pbr < 1.0 THEN 2 ELSE 0 END)
  AS pbr_score
FROM with_valuation wv
LEFT JOIN `onitsuka-app.stock_master.equity_master` em ON wv.code = em.code
LEFT JOIN sector_avg sa ON em.sector33_name = sa.sector33_name;
