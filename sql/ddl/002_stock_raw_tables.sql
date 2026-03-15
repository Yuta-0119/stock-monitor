-- ============================================================
-- stock_raw テーブル定義
-- J-Quants API v2 Standardプランのデータを格納
-- ============================================================

-- 日次株価OHLC
CREATE TABLE IF NOT EXISTS stock_raw.daily_quotes (
  date            DATE          NOT NULL,
  code            STRING        NOT NULL,
  open            FLOAT64,
  high            FLOAT64,
  low             FLOAT64,
  close           FLOAT64,
  volume          FLOAT64,
  turnover_value  FLOAT64,
  adjustment_factor    FLOAT64,
  adjustment_open      FLOAT64,
  adjustment_high      FLOAT64,
  adjustment_low       FLOAT64,
  adjustment_close     FLOAT64,
  adjustment_volume    FLOAT64,
  _ingested_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY code
OPTIONS (
  description = 'J-Quants /equities/bars/daily - 全上場銘柄の日次株価',
  require_partition_filter = false
);

-- 財務サマリー
CREATE TABLE IF NOT EXISTS stock_raw.financial_summary (
  disclosed_date              DATE          NOT NULL,
  code                        STRING        NOT NULL,
  fiscal_year_end             STRING,
  type_of_document            STRING,
  net_sales                   FLOAT64,
  operating_profit            FLOAT64,
  ordinary_profit             FLOAT64,
  profit                      FLOAT64,
  earnings_per_share          FLOAT64,
  diluted_earnings_per_share  FLOAT64,
  book_value_per_share        FLOAT64,
  return_on_equity            FLOAT64,
  total_assets                FLOAT64,
  equity                      FLOAT64,
  equity_to_asset_ratio       FLOAT64,
  number_of_issued_and_outstanding_shares_at_the_end_of_fiscal_year STRING,
  forecast_net_sales          FLOAT64,
  forecast_operating_profit   FLOAT64,
  forecast_ordinary_profit    FLOAT64,
  forecast_profit             FLOAT64,
  forecast_earnings_per_share FLOAT64,
  forecast_dividend_per_share STRING,
  material_changes_in_subsidiaries STRING,
  _ingested_at                TIMESTAMP     DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY disclosed_date
CLUSTER BY code
OPTIONS (
  description = 'J-Quants /fins/summary - 決算短信サマリー'
);

-- TOPIX四本値
CREATE TABLE IF NOT EXISTS stock_raw.topix_daily (
  date    DATE      NOT NULL,
  open    FLOAT64,
  high    FLOAT64,
  low     FLOAT64,
  close   FLOAT64,
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
OPTIONS (
  description = 'J-Quants /indices/bars/daily/topix - TOPIX指数'
);

-- 指数四本値（日経225等）
CREATE TABLE IF NOT EXISTS stock_raw.index_daily (
  date        DATE      NOT NULL,
  index_code  STRING    NOT NULL,
  open        FLOAT64,
  high        FLOAT64,
  low         FLOAT64,
  close       FLOAT64,
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY index_code
OPTIONS (
  description = 'J-Quants /indices/bars/daily - 各種指数四本値（Standard以上）'
);

-- 信用取引週末残高
CREATE TABLE IF NOT EXISTS stock_raw.margin_interest (
  date                  DATE      NOT NULL,
  code                  STRING    NOT NULL,
  long_margin_trade_volume   FLOAT64,
  long_margin_trade_value    FLOAT64,
  short_margin_trade_volume  FLOAT64,
  short_margin_trade_value   FLOAT64,
  long_negotiable_margin_trade_volume  FLOAT64,
  long_negotiable_margin_trade_value   FLOAT64,
  short_negotiable_margin_trade_volume FLOAT64,
  short_negotiable_margin_trade_value  FLOAT64,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY code
OPTIONS (
  description = 'J-Quants /markets/margin-interest - 信用取引週末残高（Standard以上）'
);

-- 業種別空売り比率
CREATE TABLE IF NOT EXISTS stock_raw.short_selling_ratio (
  date                  DATE      NOT NULL,
  sector33_code         STRING    NOT NULL,
  selling_value         FLOAT64,
  short_selling_with_restrictions_value   FLOAT64,
  short_selling_without_restrictions_value FLOAT64,
  _ingested_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
OPTIONS (
  description = 'J-Quants /markets/short-ratio - 業種別空売り比率（Standard以上）'
);

-- 投資部門別売買動向
CREATE TABLE IF NOT EXISTS stock_raw.investor_types (
  published_date  DATE    NOT NULL,
  start_date      DATE,
  end_date        DATE,
  section         STRING,
  proprietors_sell_value      FLOAT64,
  proprietors_buy_value       FLOAT64,
  foreigners_sell_value       FLOAT64,
  foreigners_buy_value        FLOAT64,
  individuals_sell_value      FLOAT64,
  individuals_buy_value       FLOAT64,
  securities_cos_sell_value   FLOAT64,
  securities_cos_buy_value    FLOAT64,
  investment_trusts_sell_value FLOAT64,
  investment_trusts_buy_value  FLOAT64,
  other_corps_sell_value      FLOAT64,
  other_corps_buy_value       FLOAT64,
  _ingested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY published_date
OPTIONS (
  description = 'J-Quants /equities/investor-types - 投資部門別売買動向'
);

-- 決算発表予定日
CREATE TABLE IF NOT EXISTS stock_raw.earnings_calendar (
  code              STRING    NOT NULL,
  company_name      STRING,
  date              DATE,
  fiscal_year_end   STRING,
  _ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY code
OPTIONS (
  description = 'J-Quants /equities/earnings-calendar - 決算発表予定日'
);

-- 取引カレンダー
CREATE TABLE IF NOT EXISTS stock_raw.trading_calendar (
  date                DATE      NOT NULL,
  holiday_division    STRING,
  _ingested_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
  description = 'J-Quants /markets/calendar - 取引カレンダー'
);
