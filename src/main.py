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
                        choices=["check", "setup", "init", "daily", "weekly", "backfill"],
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

    elif args.mode == "backfill":
        if not args.from_date or not args.to_date:
            parser.error("--from と --to はbackfillモードで必須です")
        from src.ingest.daily_quotes import ingest_backfill
        ingest_backfill(client, loader, config, args.from_date, args.to_date)


if __name__ == "__main__":
    main()
