-- ============================================================
-- 09_earnings_next: 次回決算発表予定日
-- ============================================================
CREATE OR REPLACE VIEW `onitsuka-app.analytics.earnings_next` AS
WITH future AS (
  SELECT
    code,
    date AS earnings_date,
    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date ASC) AS rn
  FROM `onitsuka-app.stock_raw.earnings_calendar`
  WHERE date >= CURRENT_DATE('Asia/Tokyo')
)
SELECT
  code,
  earnings_date                                                             AS next_earnings_date,
  DATE_DIFF(earnings_date, CURRENT_DATE('Asia/Tokyo'), DAY)                AS days_to_earnings
FROM future
WHERE rn = 1
