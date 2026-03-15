-- STEP 4: ファンダメンタルスコア
-- 売上CAGR・営業利益CAGR・ROE・ROIC・財務健全性を評価
-- 注意: type_of_document の通期値はSTEP 0で確認後に調整すること
CREATE OR REPLACE VIEW `onitsuka-app.analytics.fundamental_score` AS
WITH yearly AS (
  SELECT
    code,
    disclosed_date,
    type_of_document,
    net_sales,
    operating_profit,
    profit,
    equity,
    total_assets,
    earnings_per_share,
    book_value_per_share,
    equity_to_asset_ratio,
    EXTRACT(YEAR FROM disclosed_date) AS disc_year,
    ROW_NUMBER() OVER (
      PARTITION BY code, EXTRACT(YEAR FROM disclosed_date)
      ORDER BY disclosed_date DESC
    ) AS rn
  FROM `onitsuka-app.stock_raw.financial_summary`
  WHERE
    (type_of_document LIKE '%FY%'
     OR type_of_document LIKE '%Annual%'
     OR type_of_document LIKE '%通期%')
    AND net_sales > 0
),
annual AS (
  SELECT * FROM yearly WHERE rn = 1
),
latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY disc_year DESC) AS yr_rank
  FROM annual
),
cagr AS (
  SELECT
    a.code,
    a.disc_year AS latest_year,
    a.net_sales AS latest_sales,
    a.operating_profit AS latest_op,
    a.profit,
    a.equity,
    a.total_assets,
    a.equity_to_asset_ratio,
    a.earnings_per_share AS eps,
    a.book_value_per_share AS bps,
    b.net_sales AS sales_3y_ago,
    b.operating_profit AS op_3y_ago,
    SAFE_DIVIDE(a.profit, a.equity) AS roe,
    SAFE_DIVIDE(a.operating_profit * 0.7, a.total_assets - a.equity) AS roic_approx,
    CASE
      WHEN b.net_sales > 0 AND a.net_sales > 0
      THEN POW(SAFE_DIVIDE(a.net_sales, b.net_sales), 1.0/3) - 1
      ELSE NULL
    END AS sales_cagr_3y,
    CASE
      WHEN b.operating_profit > 0 AND a.operating_profit > 0
      THEN POW(SAFE_DIVIDE(a.operating_profit, b.operating_profit), 1.0/3) - 1
      ELSE NULL
    END AS op_cagr_3y,
    SAFE_DIVIDE(a.operating_profit, a.net_sales) AS op_margin
  FROM latest a
  LEFT JOIN annual b ON a.code = b.code AND a.disc_year = b.disc_year + 3
  WHERE a.yr_rank = 1
)
SELECT
  code,
  latest_year,
  eps,
  bps,
  ROUND(sales_cagr_3y * 100, 1) AS sales_cagr_3y_pct,
  ROUND(op_cagr_3y * 100, 1) AS op_cagr_3y_pct,
  ROUND(roe * 100, 1) AS roe_pct,
  ROUND(roic_approx * 100, 1) AS roic_pct,
  ROUND(op_margin * 100, 1) AS op_margin_pct,
  ROUND(equity_to_asset_ratio * 100, 1) AS equity_ratio_pct,
  -- 売上CAGRスコア（8点満点）
  CASE
    WHEN sales_cagr_3y >= 0.15 THEN 8
    WHEN sales_cagr_3y >= 0.10 THEN 6
    WHEN sales_cagr_3y >= 0.05 THEN 3
    ELSE 0
  END AS sales_cagr_score,
  -- 営業利益CAGRスコア（8点満点）
  CASE
    WHEN op_cagr_3y >= 0.20 THEN 8
    WHEN op_cagr_3y >= 0.10 THEN 6
    WHEN op_cagr_3y >= 0.05 THEN 3
    ELSE 0
  END AS op_cagr_score,
  -- ROEスコア（7点満点）
  CASE
    WHEN roe >= 0.15 THEN 7
    WHEN roe >= 0.10 THEN 5
    WHEN roe >= 0.08 THEN 3
    ELSE 0
  END AS roe_score,
  -- ROICスコア（6点満点）
  CASE
    WHEN roic_approx >= 0.12 THEN 6
    WHEN roic_approx >= 0.08 THEN 4
    WHEN roic_approx >= 0.05 THEN 2
    ELSE 0
  END AS roic_score,
  CASE
    WHEN equity_to_asset_ratio < 0.20 THEN 'FAIL'
    WHEN op_margin < 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS financial_health
FROM cagr;
