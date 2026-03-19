-- STEP 10: 保有銘柄サマリービュー
-- analytics.holdings（生トランザクション）を銘柄ごとに集計し、
-- integrated_score / external_prices と結合して評価額・損益を算出する。
--
-- 価格参照の優先順位:
--   国内株  → integrated_score.latest_close（J-Quants 日次 JPY）
--   米国株  → external_prices.price_jpy（yfinance USD → JPY 換算）
--   投資信託 → external_prices.price_jpy（yfinance 基準価額 JPY）
--
-- 通貨・単位の扱い:
--   国内株の purchase_amount は JPY（円/株）
--   投資信託の purchase_amount は shares(口) × unit_price(円/万口) で格納（実円 = ÷10,000 が必要）
--   米国株の purchase_amount は USD → 現在の USDJPY で近似換算
CREATE OR REPLACE VIEW `onitsuka-app.analytics.holdings_summary` AS
WITH

-- ─── USD/JPY レート（external_prices から最新を取得）──────
fx AS (
  SELECT MAX(usdjpy_rate) AS usdjpy_rate
  FROM `onitsuka-app.analytics.external_prices`
  WHERE usdjpy_rate IS NOT NULL
),

-- ─── 銘柄ごとに集計（全行が買付済み）────────────────────────────
-- analytics.holdings は load_holdings.py が買付のみを書き込む（trade_type カラムなし）
-- product_type / account_type が正式カラム名（旧名 product_category / account は廃止）
-- 投資信託の保有単位: 口数を万口に変換（SBI証券の標準表示に合わせる）
-- 万口単位にすることで (現在値円/万口 - 取得単価円/万口) × 保有万口 = 含み損益円 が成立する
agg AS (
  SELECT
    product_type,
    company_name,
    code,
    account_type,
    CASE
      WHEN product_type = '投資信託'
      THEN ROUND(SUM(shares) / 10000.0, 4)  -- 口 → 万口
      ELSE SUM(shares)
    END                                                    AS total_shares,     -- 投資信託:万口 / それ以外:株数
    SUM(purchase_amount)                                   AS total_cost_orig,  -- 元通貨建て取得金額
    SAFE_DIVIDE(SUM(purchase_amount), SUM(shares))         AS avg_cost_orig,    -- 元通貨建て平均取得単価（投資信託:円/万口）
    MIN(purchase_date)                                     AS first_buy_date,
    MAX(latest_purchase_date)                              AS last_buy_date
  FROM `onitsuka-app.analytics.holdings`
  GROUP BY product_type, company_name, code, account_type
),

-- ─── 取得金額を JPY 統一（米国株: USDJPY 換算 / 投資信託: /10000 補正）──────────
-- 投資信託の purchase_amount は shares(口) × unit_price(円/万口) で格納されているため
-- 実際の円建て金額に変換するには 10,000 で除算する必要がある。
-- avg_cost_orig は SUM(purchase_amount)/SUM(shares) = 円/万口 スケール（= ETF proxy NAV と同単位）
agg_jpy AS (
  SELECT
    a.*,
    CASE
      WHEN a.product_type = '米国株'
        THEN ROUND(a.total_cost_orig * COALESCE(fx.usdjpy_rate, 150), 0)
      WHEN a.product_type = '投資信託'
        THEN ROUND(a.total_cost_orig / 10000.0, 0)  -- 円/万口 → 実円換算
      ELSE a.total_cost_orig
    END AS total_cost,
    CASE
      WHEN a.product_type = '米国株'
        THEN ROUND(a.avg_cost_orig * COALESCE(fx.usdjpy_rate, 150), 0)
      WHEN a.product_type = '投資信託'
        THEN a.avg_cost_orig  -- 円/万口 のまま（latest_close と同単位で比較可能）
      ELSE a.avg_cost_orig
    END AS avg_cost_per_share,
    fx.usdjpy_rate
  FROM agg a
  LEFT JOIN fx ON TRUE
)

SELECT
  aj.product_type        AS product_category,  -- app.py との互換性維持
  aj.company_name,
  aj.code,
  aj.account_type        AS account,           -- app.py との互換性維持
  aj.total_shares,
  aj.total_cost                                             AS total_cost,
  aj.avg_cost_per_share,
  aj.first_buy_date,
  aj.last_buy_date,

  -- ★ 最新価格（国内株 / 米国株 / 投資信託の優先順）
  COALESCE(sc.latest_close, ep_us.price_jpy, ep_fund.price_jpy) AS latest_close,

  -- 窪田スコア・シグナル（国内株のみ有効）
  sc.kubota_signal,
  sc.kubota_trade_score,
  sc.growth_invest_score,
  sc.next_earnings_date,
  sc.days_to_earnings,
  sc.price_strength_score,

  -- ★ 評価額（円）
  -- 投資信託: total_shares=万口, price_jpy=円/万口 → 万口 × 円/万口 = 円（直接計算可能）
  -- 米国株・国内株: total_shares=株数, price=円/株 → そのまま
  CASE
    WHEN ep_fund.price_jpy IS NOT NULL AND aj.product_category = '投資信託'
      THEN ROUND(aj.total_shares * ep_fund.price_jpy, 0)
    WHEN COALESCE(sc.latest_close, ep_us.price_jpy) IS NOT NULL
      THEN ROUND(aj.total_shares * COALESCE(sc.latest_close, ep_us.price_jpy), 0)
    ELSE NULL
  END AS current_value,

  -- ★ 含み損益（円）
  -- 投資信託: (現在値円/万口 - 取得単価円/万口) × 保有万口 = 含み損益円
  CASE
    WHEN ep_fund.price_jpy IS NOT NULL AND aj.product_category = '投資信託'
      AND aj.total_cost IS NOT NULL
      THEN ROUND(aj.total_shares * ep_fund.price_jpy - aj.total_cost, 0)
    WHEN COALESCE(sc.latest_close, ep_us.price_jpy) IS NOT NULL
      AND aj.total_cost IS NOT NULL
      THEN ROUND(aj.total_shares * COALESCE(sc.latest_close, ep_us.price_jpy) - aj.total_cost, 0)
    ELSE NULL
  END AS unrealized_pnl,

  -- ★ 損益率（%）
  CASE
    WHEN ep_fund.price_jpy IS NOT NULL AND aj.product_category = '投資信託'
      AND aj.total_cost > 0
      THEN ROUND(
        SAFE_DIVIDE(aj.total_shares * ep_fund.price_jpy - aj.total_cost, aj.total_cost) * 100, 2
      )
    WHEN COALESCE(sc.latest_close, ep_us.price_jpy) IS NOT NULL
      AND aj.total_cost > 0
      THEN ROUND(
        SAFE_DIVIDE(
          aj.total_shares * COALESCE(sc.latest_close, ep_us.price_jpy) - aj.total_cost,
          aj.total_cost
        ) * 100, 2
      )
    ELSE NULL
  END AS return_pct

FROM agg_jpy aj

-- 国内株: integrated_score（5桁コード → 先頭4桁 = holdings.code）
LEFT JOIN `onitsuka-app.analytics.integrated_score` sc
  ON LEFT(sc.code, 4) = aj.code
  AND aj.product_type = '国内株'

-- 米国株: external_prices（ticker = holdings.code: "GOOGL", "TSLA" 等）
LEFT JOIN `onitsuka-app.analytics.external_prices` ep_us
  ON ep_us.ticker = aj.code
  AND aj.product_type = '米国株'
  AND ep_us.asset_type = '米国株'

-- 投資信託: external_prices（asset_name = holdings.company_name）
LEFT JOIN `onitsuka-app.analytics.external_prices` ep_fund
  ON ep_fund.asset_name = aj.company_name
  AND aj.product_type = '投資信託'
  AND ep_fund.asset_type = '投資信託';
