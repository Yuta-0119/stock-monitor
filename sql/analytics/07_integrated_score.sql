-- STEP 7: integrated_score (strategy.* views)
CREATE OR REPLACE VIEW `onitsuka-app.analytics.integrated_score` AS
SELECT
  m.code,
  m.company_name,
  m.sector33_name,
  m.market_name AS market_segment,
  liq.avg_turnover_20d_oku,
  liq.cv_value,
  liq.liquidity_grade,
  liq.cv_grade,
  vol.atr_pct,
  vol.range_1m_pct,
  vol.hv_contraction,
  vol.volatility_score,
  cht.close AS latest_close,
  cht.ma200_trend,
  cht.price_vs_ma200,
  cht.volume_ratio,
  cht.volume_surge,
  cht.consolidation,
  cht.breakout,
  cht.near_52w_high,
  cht.chart_score,
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
  val.per,
  val.pbr,
  val.per_score,
  val.pbr_score,
  IFNULL(val.per_score, 0) + IFNULL(val.pbr_score, 0) AS valuation_total,
  env.topix_close,
  env.market_phase,
  env.environment_score,
  ern.next_earnings_date,
  ern.days_to_earnings,
  IFNULL(vol.volatility_score, 0) + IFNULL(cht.chart_score, 0) AS kubota_trade_score,
  IFNULL(fnd.sales_cagr_score, 0) + IFNULL(fnd.op_cagr_score, 0)
    + IFNULL(fnd.roe_score, 0) + IFNULL(fnd.roic_score, 0)
    + IFNULL(val.per_score, 0) + IFNULL(val.pbr_score, 0) AS growth_invest_score,
  CASE
    WHEN fnd.financial_health = 'FAIL' THEN 'FAIL_FINANCIAL'
    WHEN liq.liquidity_grade = 'D' THEN 'FAIL_LIQUIDITY'
    ELSE 'ACTIVE'
  END AS screening_status,
  CASE
    WHEN liq.liquidity_grade IN ('A', 'B')
      AND vol.volatility_score >= 3
      AND cht.breakout = TRUE AND cht.volume_surge = TRUE
      AND (cht.ma200_trend = 'UP' OR cht.price_vs_ma200 = 'ABOVE')
      AND env.market_phase = 'BULL'
    THEN 'buy_signal'
    WHEN liq.liquidity_grade IN ('A', 'B', 'C')
      AND vol.volatility_score >= 2
      AND cht.consolidation = TRUE AND cht.breakout = FALSE
    THEN 'wait'
    ELSE '-'
  END AS kubota_signal,
  CAST(NULL AS INT64) AS signal_confidence,
  (CASE WHEN cht.near_52w_high = TRUE THEN 2 ELSE 0 END)
  + (CASE WHEN cht.volume_ratio >= 1.5 THEN 1 ELSE 0 END) AS price_strength_score
FROM `onitsuka-app.raw.equities_master` m
LEFT JOIN `onitsuka-app.strategy.liquidity_screening`      liq ON m.code = liq.code
LEFT JOIN `onitsuka-app.strategy.volatility_screening`     vol ON m.code = vol.code
LEFT JOIN `onitsuka-app.strategy.chart_pattern_screening`  cht ON m.code = cht.code
LEFT JOIN `onitsuka-app.strategy.fundamental_growth_score` fnd ON m.code = fnd.code
LEFT JOIN `onitsuka-app.strategy.valuation_assessment`     val ON m.code = val.code
LEFT JOIN `onitsuka-app.strategy.earnings_schedule`        ern ON m.code = ern.code
CROSS JOIN `onitsuka-app.strategy.market_condition`        env
WHERE m.market_name = 'プライム';
