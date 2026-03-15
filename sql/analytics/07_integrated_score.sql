-- STEP 7: 統合スコア（最重要ビュー）
-- 流動性・ボラティリティ・チャート・ファンダメンタル・バリュエーション・相場環境を統合
-- equity_master カラム名: company_name, sector33_name, market_segment
CREATE OR REPLACE VIEW `onitsuka-app.analytics.integrated_score` AS
SELECT
  em.code,
  em.company_name,
  em.sector33_name,
  em.market_segment,
  -- 流動性
  liq.avg_turnover_20d_oku,
  liq.cv_value,
  liq.liquidity_grade,
  liq.cv_grade,
  -- ボラティリティ
  vol.atr_pct,
  vol.range_1m_pct,
  vol.hv_contraction,
  vol.volatility_score,
  -- チャート
  cht.close AS latest_close,
  cht.ma200_trend,
  cht.price_vs_ma200,
  cht.volume_ratio,
  cht.volume_surge,
  cht.consolidation,
  cht.near_52w_high,
  cht.chart_score,
  -- ファンダメンタル
  fnd.sales_cagr_3y_pct,
  fnd.op_cagr_3y_pct,
  fnd.roe_pct,
  fnd.roic_pct,
  fnd.equity_ratio_pct,
  fnd.sales_cagr_score,
  fnd.op_cagr_score,
  fnd.roe_score,
  fnd.roic_score,
  fnd.financial_health,
  IFNULL(fnd.sales_cagr_score, 0) + IFNULL(fnd.op_cagr_score, 0)
    + IFNULL(fnd.roe_score, 0) + IFNULL(fnd.roic_score, 0) AS fundamental_total,
  -- バリュエーション
  val.per,
  val.pbr,
  val.per_score,
  val.pbr_score,
  IFNULL(val.per_score, 0) + IFNULL(val.pbr_score, 0) AS valuation_total,
  -- 相場環境
  env.topix_close,
  env.market_phase,
  env.environment_score,
  -- ★ 窪田トレードスコア（ボラ2点 + チャート7点 = 9点満点）
  IFNULL(vol.volatility_score, 0) + IFNULL(cht.chart_score, 0) AS kubota_trade_score,
  -- ★ 成長株投資スコア（29点満点）
  IFNULL(fnd.sales_cagr_score, 0) + IFNULL(fnd.op_cagr_score, 0)
    + IFNULL(fnd.roe_score, 0) + IFNULL(fnd.roic_score, 0)
    + IFNULL(val.per_score, 0) + IFNULL(val.pbr_score, 0) AS growth_invest_score,
  -- ★ 総合判定
  CASE
    WHEN fnd.financial_health = 'FAIL' THEN 'FAIL_FINANCIAL'
    WHEN liq.liquidity_grade = 'FAIL' THEN 'FAIL_LIQUIDITY'
    ELSE 'ACTIVE'
  END AS screening_status,
  -- ★ 窪田エントリーシグナル
  CASE
    WHEN liq.liquidity_grade IN ('PASS_A', 'PASS_B')
      AND vol.volatility_score >= 3
      AND cht.chart_score >= 6
      AND cht.volume_surge = TRUE
      AND env.market_phase = 'BULL'
    THEN 'ENTRY SIGNAL'
    WHEN liq.liquidity_grade IN ('PASS_A', 'PASS_B', 'PASS_C')
      AND vol.volatility_score >= 2
      AND cht.consolidation = TRUE
    THEN 'WATCH（放れ待ち）'
    ELSE '-'
  END AS kubota_signal
FROM `onitsuka-app.stock_master.equity_master` em
LEFT JOIN `onitsuka-app.analytics.kubota_liquidity`  liq ON em.code = liq.code
LEFT JOIN `onitsuka-app.analytics.kubota_volatility` vol ON em.code = vol.code
LEFT JOIN `onitsuka-app.analytics.kubota_chart`      cht ON em.code = cht.code
LEFT JOIN `onitsuka-app.analytics.fundamental_score` fnd ON em.code = fnd.code
LEFT JOIN `onitsuka-app.analytics.valuation_score`   val ON em.code = val.code
CROSS JOIN `onitsuka-app.analytics.market_environment` env
WHERE em.market_segment LIKE '%プライム%';
