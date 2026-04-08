"""
Cache table refresh script.

Refreshes all cache tables needed by:
1. Dashboard (raw.cache_integrated_ranking)
2. Auto-trading system (cache_market_condition, cache_market_overview,
   cache_backtest_stats, cache_minute_profile)
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

    # 2. cache_market_overview
    print("Refreshing cache_market_overview...")
    try:
        client.query(f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_market_overview` AS
            SELECT * FROM `{BQ_PROJECT}.mart.market_overview`
            ORDER BY date DESC LIMIT 30
        """).result()
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        errors.append(f"cache_market_overview: {e}")

    # 3. cache_market_condition
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

    # 4. cache_backtest_stats
    print("Refreshing cache_backtest_stats...")
    try:
        client.query(f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_backtest_stats` AS
            SELECT code,
                COUNT(*) AS past_signals,
                ROUND(AVG(return_5d_pct), 2) AS avg_5d,
                ROUND(AVG(return_10d_pct), 2) AS avg_10d,
                ROUND(AVG(return_20d_pct), 2) AS avg_20d,
                ROUND(SAFE_DIVIDE(COUNTIF(return_5d_pct > 0), COUNT(return_5d_pct)) * 100, 1) AS winrate_5d,
                ROUND(SAFE_DIVIDE(COUNTIF(return_10d_pct > 0), COUNT(return_10d_pct)) * 100, 1) AS winrate_10d,
                ROUND(SAFE_DIVIDE(COUNTIF(return_20d_pct > 0), COUNT(return_20d_pct)) * 100, 1) AS winrate_20d,
                ROUND(AVG(CASE WHEN return_20d_pct > 0 THEN return_20d_pct END), 2) AS avg_win_20d,
                ROUND(AVG(CASE WHEN return_20d_pct <= 0 THEN return_20d_pct END), 2) AS avg_loss_20d
            FROM `{BQ_PROJECT}.strategy.signal_backtest`
            GROUP BY code
        """).result()
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        errors.append(f"cache_backtest_stats: {e}")

    # 5. cache_minute_profile
    print("Refreshing cache_minute_profile...")
    try:
        client.query(f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.raw.cache_minute_profile` AS
            SELECT code, time,
                ROUND(AVG(high - low), 2) AS avg_range,
                ROUND(AVG(volume), 0) AS avg_volume,
                COUNT(*) AS sample_days
            FROM `{BQ_PROJECT}.raw.stock_prices_minute`
            WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 60 DAY)
            GROUP BY code, time
        """).result()
        print("  OK")
    except Exception as e:
        print(f"  FAILED: {e}")
        errors.append(f"cache_minute_profile: {e}")

    elapsed = time.time() - t0
    print(f"\nCache refresh completed in {elapsed:.1f}s")

    # Slack notification
    if errors:
        send_slack(f"ETL Cache Refresh: {len(errors)} errors\n" + "\n".join(errors), "danger")
    else:
        send_slack(f"ETL Cache Refresh: All 5 tables refreshed ({elapsed:.0f}s)", "good")


if __name__ == "__main__":
    main()
