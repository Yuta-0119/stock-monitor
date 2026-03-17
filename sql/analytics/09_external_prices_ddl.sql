-- STEP 9: external_prices テーブル定義（CREATE TABLE IF NOT EXISTS）
-- 米国株・投資信託の時価を格納するテーブル。
-- データは scripts/fetch_external_prices.py により毎日 WRITE_TRUNCATE でロード。
-- このステートメントはテーブルが存在しない初回のみ実際に作成される。
CREATE TABLE IF NOT EXISTS `onitsuka-app.analytics.external_prices` (
  fetch_date     DATE    OPTIONS(description="取得日"),
  ticker         STRING  OPTIONS(description="ティッカーシンボル（米国株: GOOGL 等, 投資信託: 0331418A.T 等）"),
  asset_name     STRING  OPTIONS(description="資産名（holdings.company_name と一致させること）"),
  asset_type     STRING  OPTIONS(description="資産種別（米国株 / 投資信託）"),
  price_original FLOAT64 OPTIONS(description="元通貨建て価格（USD または JPY）"),
  currency       STRING  OPTIONS(description="元通貨（USD / JPY）"),
  usdjpy_rate    FLOAT64 OPTIONS(description="取得時の USD/JPY レート（米国株のみ）"),
  price_jpy      FLOAT64 OPTIONS(description="JPY 換算後の価格")
);
