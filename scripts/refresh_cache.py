"""
Cache table refresh script.

Refreshes cache tables actually consumed by downstream systems:
1. Dashboard + trading watchlist SQL -> raw.cache_integrated_ranking
2. Trading morning session (Nikkei gap filter) -> raw.cache_market_condition

Previously this also refreshed cache_market_overview, cache_backtest_stats,
and cache_minute_profile, but the Phase B cleanup (2026-04-11) confirmed
that none of those three tables are read by any live trading code. They
have been dropped and are no longer produced here.
"""

import os
import sys
import time
import json
import requests
from google.cloud import bigquery

BQ_PROJECT = os.environ.get("BQ_PROJECT", "onitsuka-app")
BQ_LOCATION = "asia-northeast1"

# Slack notification (optional)
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")


def send_slack(message, color="good"):
    if not SLACK_WEBHOOK:
        return
    try:
        payload = {"attachments": [{"color": color, "text": message}]}
        requests.post(SLACK_WEBHOOK, data=json.dumps(payload),
                      headers={"Content-Type": "application/json"}, timeout=10)
    except Exception:
        pass


def main():
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    t0 = time.time()
    errors = []

    # 1. cache_integrated_ranking (from analytics.integrated_score)
    print("Refreshing cache_integrated_ranking...")
    try:
        client.query(f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_integrated_ranking` AS
            SELECT
              code, company_name, sector33_name,
              market_segment AS market_name,
              avg_turnover_20d_oku, cv_value, liquidity_grade, cv_grade,
              atr_pct, range_1m_pct, hv_contraction, volatility_score,
              latest_close, ma200_trend, price_vs_ma200,
              volume_ratio, volume_surge, consolidation, breakout, near_52w_high,
              chart_score,
              sales_cagr_3y_pct, op_cagr_3y_pct, roe_pct, roic_pct, equity_ratio_pct,
              sales_cagr_score, op_cagr_score, roe_score, roic_score, financial_health,
              fundamental_total,
              per, pbr, per_score, pbr_score, valuation_total,
              topix_close, market_phase, environment_score,
              next_earnings_date, days_to_earnings,
              kubota_trade_score, growth_invest_score,
              screening_status, kubota_signal, price_strength_score
            FROM `{BQ_PROJECT}.analytics.integrated_score`
        """).result()
        t = client.get_table(f"{BQ_PROJECT}.raw.cache_integrated_ranking")
        print(f"  OK: {t.num_rows:,} rows")
    except Exception as e:
        print(f"  FAILED: {e}")
        errors.append(f"cache_integrated_ranking: {e}")

    # 2. cache_market_condition
    print("Refreshing cache_market_condition...")
    try:
        client.query(f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_market_condition` AS
            SELECT * FROM `{BQ_PROJECT}.strategy.market_condition`
        """).result()
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        errors.append(f"cache_market_condition: {e}")

    elapsed = time.time() - t0
    print(f"\nCache refresh completed in {elapsed:.1f}s")

    # Slack notification
    if errors:
        send_slack(f"ETL Cache Refresh: {len(errors)} errors\n" + "\n".join(errors), "danger")
    else:
        send_slack(f"ETL Cache Refresh: All 2 tables refreshed ({elapsed:.0f}s)", "good")


if __name__ == "__main__":
    main()
