-- ============================================================
-- stock_master テーブル定義
-- ============================================================

-- 上場銘柄マスター
CREATE TABLE IF NOT EXISTS stock_master.equity_master (
  code                    STRING    NOT NULL,
  company_name            STRING,
  company_name_english    STRING,
  sector17_code           STRING,
  sector17_name           STRING,
  sector33_code           STRING,
  sector33_name           STRING,
  scale_category          STRING,
  market_code             STRING,
  market_segment          STRING,
  _updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY code
OPTIONS (
  description = 'J-Quants /equities/master - 上場銘柄マスター'
);

-- 手動補完用（ROIC・FCF等）
CREATE TABLE IF NOT EXISTS stock_master.manual_fundamentals (
  code                      STRING    NOT NULL,
  fiscal_year               STRING    NOT NULL,
  roic                      FLOAT64,
  fcf                       FLOAT64,
  equity_ratio              FLOAT64,
  interest_bearing_debt     FLOAT64,
  interest_coverage_ratio   FLOAT64,
  notes                     STRING,
  updated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY code
OPTIONS (
  description = 'ROIC・FCF等の手動補完データ（株探等から取得）'
);

-- ============================================================
-- portfolio テーブル定義
-- ============================================================

-- 保有銘柄
CREATE TABLE IF NOT EXISTS portfolio.holdings (
  code              STRING    NOT NULL,
  account_type      STRING,     -- NISA成長投資枠 / NISA積立投資枠 / 特定口座
  quantity          INT64,
  avg_cost          FLOAT64,
  purchase_date     DATE,
  stop_loss_price   FLOAT64,
  target_price_1    FLOAT64,
  target_price_2    FLOAT64,
  notes             STRING,
  is_active         BOOL      DEFAULT TRUE,
  _updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY code
OPTIONS (
  description = '保有銘柄管理'
);

-- ウォッチリスト
CREATE TABLE IF NOT EXISTS portfolio.watchlist (
  code              STRING    NOT NULL,
  added_date        DATE,
  framework_score   FLOAT64,
  entry_trigger     STRING,
  priority          STRING,     -- A / B / C
  status            STRING,     -- 監視中 / エントリー済 / 見送り
  notes             STRING,
  _updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY code
OPTIONS (
  description = 'ウォッチリスト管理'
);

-- 資産推移（FIRE進捗）
CREATE TABLE IF NOT EXISTS portfolio.asset_history (
  month                 DATE      NOT NULL,
  nisa_growth           FLOAT64,
  nisa_tsumitate        FLOAT64,
  individual_stocks     FLOAT64,
  cash                  FLOAT64,
  other_assets          FLOAT64,
  total_assets          FLOAT64,
  housing_loan_balance  FLOAT64,
  net_assets            FLOAT64,
  notes                 STRING,
  _updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY month
OPTIONS (
  description = 'FIRE進捗 - 月次資産推移'
);

-- アラート履歴
CREATE TABLE IF NOT EXISTS portfolio.alerts (
  alert_date    DATE      NOT NULL,
  code          STRING,
  alert_type    STRING,     -- 損切り接近 / 出来高ブレイク / 決算接近 etc
  priority      STRING,     -- 緊急 / 高 / 中 / 低
  message       STRING,
  is_resolved   BOOL      DEFAULT FALSE,
  _created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY alert_date
OPTIONS (
  description = 'アラート履歴'
);
