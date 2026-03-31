"""
raw.cache_integrated_ranking を analytics.integrated_score から再作成するスクリプト。

ダッシュボードは raw.cache_integrated_ranking を参照するため、
Analytics Layer 更新後にこのスクリプトで最新データへ同期する。
列の差異: market_name（キャッシュ）← market_segment（ビュー）
"""

import os
import sys
from google.cloud import bigquery

BQ_PROJECT  = os.environ.get("BQ_PROJECT", "onitsuka-app")
BQ_LOCATION = "asia-northeast1"

REFRESH_SQL = f"""
CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_integrated_ranking` AS
SELECT
  code,
  company_name,
  sector33_name,
  market_segment AS market_name,
  avg_turnover_20d_oku,
  cv_value,
  liquidity_grade,
  cv_grade,
  atr_pct,
  range_1m_pct,
  hv_contraction,
  volatility_score,
  latest_close,
  ma200_trend,
  price_vs_ma200,
  volume_ratio,
  volume_surge,
  consolidation,
  breakout,
  near_52w_high,
  chart_score,
  sales_cagr_3y_pct,
  op_cagr_3y_pct,
  roe_pct,
  roic_pct,
  equity_ratio_pct,
  sales_cagr_score,
  op_cagr_score,
  roe_score,
  roic_score,
  financial_health,
  fundamental_total,
  per,
  pbr,
  per_score,
  pbr_score,
  valuation_total,
  topix_close,
  market_phase,
  environment_score,
  next_earnings_date,
  days_to_earnings,
  kubota_trade_score,
  growth_invest_score,
  screening_status,
  kubota_signal,
  price_strength_score
FROM `{BQ_PROJECT}.analytics.integrated_score`
"""


def main():
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    print(f"Refreshing raw.cache_integrated_ranking from analytics.integrated_score ...")
    job = client.query(REFRESH_SQL)
    job.result()
    table = client.get_table(f"{BQ_PROJECT}.raw.cache_integrated_ranking")
    print(f"  完了: {table.num_rows:,} 行 → raw.cache_integrated_ranking")


if __name__ == "__main__":
    main()
