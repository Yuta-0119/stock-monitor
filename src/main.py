"""stock-monitor メインエントリーポイント

使用方法:
  # 日次バッチ（平日18:00 JSTに実行）
  python -m src.main --mode daily

  # 初期ロード（全データ一括取込み）
  python -m src.main --mode init

  # API疎通確認
  python -m src.main --mode check

  # DDL実行（テーブル作成）
  python -m src.main --mode setup

  # Analytics Layer 作成（BigQuery Viewの作成・更新）
  python -m src.main --mode analytics

  # バックフィル（期間指定）
  python -m src.main --mode backfill --from 20200101 --to 20240101
"""
import argparse
from dotenv import load_dotenv
load_dotenv()
import logging
import sys
from datetime import datetime

from src.config import Config
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock-monitor")


def run_check(client: JQuantsClient):
    """API疎通確認"""
    logger.info("=== API疎通確認 ===")
    if client.health_check():
        logger.info("✅ J-Quants API接続OK")
    else:
        logger.error("❌ J-Quants API接続失敗")
        sys.exit(1)


def run_setup(loader: BQLoader):
    """BigQueryテーブル作成"""
    logger.info("=== BigQuery DDL実行 ===")
    import glob
    ddl_files = sorted(glob.glob("sql/ddl/*.sql"))
    for f in ddl_files:
        logger.info(f"Executing: {f}")
        loader.execute_sql_file(f)
    logger.info("✅ テーブル作成完了")


def run_snapshot(config: Config):
    """score_history テーブルへ当日スナップショットを追記"""
    logger.info("=== スコア履歴スナップショット開始 ===")

    from google.cloud import bigquery
    bq = bigquery.Client(project=config.bq_project, location=config.bq_location)

    # ① テーブルが存在しなければ作成
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{config.bq_project}.{config.ds_analytics}.score_history` (
      snapshot_date         DATE,
      code                  STRING,
      company_name          STRING,
      sector33_name         STRING,
      market_segment        STRING,
      latest_close          FLOAT64,
      avg_turnover_20d_oku  FLOAT64,
      liquidity_grade       STRING,
      volatility_score      INT64,
      chart_score           INT64,
      kubota_trade_score    INT64,
      sales_cagr_3y_pct     FLOAT64,
      op_cagr_3y_pct        FLOAT64,
      roe_pct               FLOAT64,
      roic_pct              FLOAT64,
      growth_invest_score   INT64,
      per                   FLOAT64,
      pbr                   FLOAT64,
      market_phase          STRING,
      kubota_signal         STRING,
      screening_status      STRING
    )
    PARTITION BY snapshot_date
    OPTIONS(require_partition_filter = FALSE)
    """
    bq.query(ddl).result()

    # ② 本日分がまだ未挿入の場合のみ INSERT
    check_df = bq.query(f"""
        SELECT COUNT(*) AS cnt
        FROM `{config.bq_project}.{config.ds_analytics}.score_history`
        WHERE snapshot_date = CURRENT_DATE('Asia/Tokyo')
    """).to_dataframe(create_bqstorage_client=False)

    if check_df["cnt"].iloc[0] > 0:
        logger.info("  本日のスナップショットは取込み済みのためスキップ")
        return

    insert_sql = f"""
    INSERT INTO `{config.bq_project}.{config.ds_analytics}.score_history`
    SELECT
      CURRENT_DATE('Asia/Tokyo') AS snapshot_date,
      code, company_name, sector33_name, market_segment,
      latest_close, avg_turnover_20d_oku, liquidity_grade,
      CAST(volatility_score  AS INT64),
      CAST(chart_score       AS INT64),
      CAST(kubota_trade_score AS INT64),
      sales_cagr_3y_pct, op_cagr_3y_pct, roe_pct, roic_pct,
      CAST(growth_invest_score AS INT64),
      per, pbr, market_phase, kubota_signal, screening_status
    FROM `{config.bq_project}.{config.ds_analytics}.integrated_score`
    WHERE screening_status = 'ACTIVE'
    """
    job = bq.query(insert_sql)
    job.result()
    logger.info(f"  スナップショット完了: {job.num_dml_affected_rows} 行挿入")
    logger.info("✅ スコア履歴スナップショット完了")


def run_analytics(loader: BQLoader, config: Config):
    """Analytics Layer 作成（BigQuery View群の作成・更新）"""
    logger.info("=== Analytics Layer 構築開始 ===")

    # analyticsデータセット作成
    from google.cloud import bigquery
    client_bq = bigquery.Client(project=config.bq_project, location=config.bq_location)
    dataset_id = f"{config.bq_project}.{config.ds_analytics}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = config.bq_location
    client_bq.create_dataset(dataset, exists_ok=True)
    logger.info(f"Dataset ready: {dataset_id}")

    # sql/analytics/ 内のSQLを順番に実行
    import glob as glob_module
    import os
    sql_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", "analytics")
    sql_files = sorted(glob_module.glob(os.path.join(sql_dir, "*.sql")))
    if not sql_files:
        logger.error(f"No SQL files found in {sql_dir}")
        sys.exit(1)

    for f in sql_files:
        logger.info(f"Executing: {os.path.basename(f)}")
        try:
            with open(f, "r", encoding="utf-8") as fh:
                sql = fh.read().strip()
            job = client_bq.query(sql)
            job.result()
            logger.info(f"  OK: {os.path.basename(f)}")
        except Exception as e:
            logger.error(f"  FAILED: {os.path.basename(f)} — {e}")
            raise

    logger.info("✅ Analytics Layer 構築完了")

    # スコア履歴スナップショットを追記
    try:
        run_snapshot(config)
    except Exception as e:
        logger.warning(f"スナップショット失敗（スキップ）: {e}")


def run_init(client: JQuantsClient, loader: BQLoader, config: Config):
    """初期ロード（全データ一括取込み）"""
    logger.info("=== 初期データロード開始 ===")
    results = {}

    # 1. 銘柄マスター
    from src.ingest.equity_master import ingest as ingest_master
    results["equity_master"] = ingest_master(client, loader, config)

    # 2. 株価OHLC（CSV一括ダウンロード）
    from src.ingest.daily_quotes import ingest_bulk
    results["daily_quotes"] = ingest_bulk(client, loader, config)

    # 3. 財務サマリー（CSV一括ダウンロード）
    from src.ingest.financial_summary import ingest_bulk as ingest_fin_bulk
    results["financial_summary"] = ingest_fin_bulk(client, loader, config)

    # 4. TOPIX
    from src.ingest.index_data import ingest_topix
    results["topix"] = ingest_topix(client, loader, config)

    # 5. 指数データ
    from src.ingest.index_data import ingest_indices
    try:
        results["indices"] = ingest_indices(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_indices skipped: {e}")
        results["indices"] = 0

    # 6. 信用取引残高
    from src.ingest.market_data import ingest_margin_interest
    try:
        results["margin_interest"] = ingest_margin_interest(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_margin_interest skipped: {e}")
        results["margin_interest"] = 0

    # 7. 空売り比率
    from src.ingest.market_data import ingest_short_selling
    try:
        results["short_selling"] = ingest_short_selling(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_short_selling skipped: {e}")
        results["short_selling"] = 0

    # 8. 投資部門別
    from src.ingest.market_data import ingest_investor_types
    try:
        results["investor_types"] = ingest_investor_types(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_investor_types skipped: {e}")
        results["investor_types"] = 0

    # 9. 決算カレンダー
    from src.ingest.market_data import ingest_earnings_calendar
    try:
        results["earnings_calendar"] = ingest_earnings_calendar(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_earnings_calendar skipped: {e}")
        results["earnings_calendar"] = 0

    logger.info("=== 初期ロード結果 ===")
    for name, count in results.items():
        logger.info(f"  {name}: {count:,} rows")
    logger.info("✅ 初期データロード完了")


def run_daily(client: JQuantsClient, loader: BQLoader, config: Config):
    """日次バッチ"""
    logger.info(f"=== 日次バッチ開始: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    results = {}

    # 1. 銘柄マスター更新
    from src.ingest.equity_master import ingest as ingest_master
    results["equity_master"] = ingest_master(client, loader, config)

    # 2. 株価OHLC
    from src.ingest.daily_quotes import ingest_daily
    results["daily_quotes"] = ingest_daily(client, loader, config)

    # 3. 財務サマリー
    from src.ingest.financial_summary import ingest as ingest_fin
    results["financial_summary"] = ingest_fin(client, loader, config)

    # 4. TOPIX
    from src.ingest.index_data import ingest_topix
    results["topix"] = ingest_topix(client, loader, config)

    # 5. 指数データ
    from src.ingest.index_data import ingest_indices
    results["indices"] = ingest_indices(client, loader, config)

    # 6. 空売り比率
    from src.ingest.market_data import ingest_short_selling
    try:
        results["short_selling"] = ingest_short_selling(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_short_selling skipped: {e}")
        results["short_selling"] = 0

    # 7. 決算カレンダー
    from src.ingest.market_data import ingest_earnings_calendar
    try:
        results["earnings_calendar"] = ingest_earnings_calendar(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_earnings_calendar skipped: {e}")
        results["earnings_calendar"] = 0

    logger.info("=== 日次バッチ結果 ===")
    total = 0
    for name, count in results.items():
        logger.info(f"  {name}: {count:,} rows")
        total += count
    logger.info(f"  合計: {total:,} rows")
    logger.info("✅ 日次バッチ完了")


def run_weekly(client: JQuantsClient, loader: BQLoader, config: Config):
    """週次バッチ（信用取引・投資部門別）"""
    logger.info("=== 週次バッチ開始 ===")
    results = {}

    from src.ingest.market_data import ingest_margin_interest
    results["margin_interest"] = ingest_margin_interest(client, loader, config)

    from src.ingest.market_data import ingest_investor_types
    try:
        results["investor_types"] = ingest_investor_types(client, loader, config)
    except Exception as e:
        logger.warning(f"ingest_investor_types skipped: {e}")
        results["investor_types"] = 0

    logger.info("=== 週次バッチ結果 ===")
    for name, count in results.items():
        logger.info(f"  {name}: {count:,} rows")
    logger.info("✅ 週次バッチ完了")


def main():
    parser = argparse.ArgumentParser(description="stock-monitor データパイプライン")
    parser.add_argument("--mode", required=True,
                        choices=["check", "setup", "init", "daily", "weekly", "backfill", "analytics", "snapshot", "export"],
                        help="実行モード")
    parser.add_argument("--from", dest="from_date", help="開始日 (YYYYMMDD)")
    parser.add_argument("--to", dest="to_date", help="終了日 (YYYYMMDD)")
    args = parser.parse_args()

    config = Config.from_env()
    client = JQuantsClient(api_key=config.jquants_api_key)
    loader = BQLoader(project=config.bq_project, location=config.bq_location)

    if args.mode == "check":
        run_check(client)

    elif args.mode == "setup":
        run_setup(loader)

    elif args.mode == "init":
        run_init(client, loader, config)

    elif args.mode == "daily":
        run_daily(client, loader, config)

    elif args.mode == "weekly":
        run_weekly(client, loader, config)

    elif args.mode == "analytics":
        run_analytics(loader, config)

    elif args.mode == "export":
        from src.export.sheets_exporter import export as run_export
        logger.info("=== Google Sheets エクスポート開始 ===")
        results = run_export(config)
        for sheet, count in results.items():
            logger.info(f"  {sheet}: {count} rows")
        logger.info("✅ エクスポート完了")

    elif args.mode == "snapshot":
        run_snapshot(config)

    elif args.mode == "backfill":
        if not args.from_date or not args.to_date:
            parser.error("--from と --to はbackfillモードで必須です")
        from src.ingest.daily_quotes import ingest_backfill
        ingest_backfill(client, loader, config, args.from_date, args.to_date)


if __name__ == "__main__":
    main()
