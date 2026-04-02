"""日次株価OHLC取込み

- 初期ロード: CSV一括ダウンロードで過去10年分を取得
- 日次更新: date指定のAPIコールで当日分を取得
"""
import logging
from datetime import datetime, timedelta

import pandas as pd

from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "Date": "date",
    "Code": "code",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "TurnoverValue": "turnover_value",
    "AdjustmentFactor": "adjustment_factor",
    "AdjustmentOpen": "adjustment_open",
    "AdjustmentHigh": "adjustment_high",
    "AdjustmentLow": "adjustment_low",
    "AdjustmentClose": "adjustment_close",
    "AdjustmentVolume": "adjustment_volume",
}

# バルクCSV用カラムマップ（省略形）
COLUMN_MAP_BULK = {
    "Date": "date",
    "Code": "code",
    "O":    "open",
    "H":    "high",
    "L":    "low",
    "C":    "close",
    "Vo":   "volume",
    "Va":   "turnover_value",
}

KEEP_COLS = [
    "date", "code", "open", "high", "low", "close", "volume",
    "turnover_value", "adjustment_factor",
    "adjustment_open", "adjustment_high", "adjustment_low",
    "adjustment_close", "adjustment_volume",
]


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """データ変換"""
    # フォーマット自動判別（省略形 or フル名）
    cmap = COLUMN_MAP_BULK if "O" in df.columns else COLUMN_MAP
    rename = {k: v for k, v in cmap.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [c for c in KEEP_COLS if c in df.columns]
    df = df[keep]

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    # 数値カラムの型変換
    float_cols = [c for c in df.columns if c not in ("date", "code")]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def ingest_daily(client: JQuantsClient, loader: BQLoader, config,
                 target_date: str | None = None) -> int:
    """日次更新: 指定日（または最新営業日）の株価データを取得

    Args:
        target_date: 取得日（YYYYMMDD形式）。Noneの場合は最新日を自動判定
    """
    if target_date is None:
        # BigQueryの最新日付の翌日から取得
        latest = loader.get_latest_date(f"{config.ds_raw}.stock_prices_daily")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            target_date = next_date.strftime("%Y%m%d")
        else:
            # テーブルが空なら直近営業日
            target_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"Fetching daily quotes for date={target_date}")
    data = client.get_daily_quotes(date=target_date)
    if not data:
        logger.info(f"No data for {target_date} (holiday or not yet updated)")
        return 0

    df = _transform(pd.DataFrame(data))

    return loader.merge_dataframe(
        df,
        f"{config.ds_raw}.stock_prices_daily",
        merge_keys=["date", "code"],
        staging_table=f"{config.ds_raw}.stock_prices_daily_staging",
    )


def ingest_bulk(client: JQuantsClient, loader: BQLoader, config,
                from_date: str | None = None,
                to_date: str | None = None) -> int:
    """初期ロード / バックフィル: CSV一括ダウンロードまたはAPI日付範囲指定

    bulk APIでCSVダウンロード → 全件洗い替え
    """
    # CSVファイル一覧を取得
    files = client.bulk_list("/equities/bars/daily")
    if files:
        logger.info(f"Found {len(files)} CSV files for daily quotes")
        total_rows = 0
        first_write_done = False

        for i, file_info in enumerate(files):
            key = file_info.get("Key", "")
            logger.info(f"[{i+1}/{len(files)}] Downloading: {key}")

            try:
                df = client.bulk_download(key)
                df = _transform(df)

                # 日付フィルター
                if from_date and "date" in df.columns:
                    from_dt = datetime.strptime(from_date, "%Y%m%d").date()
                    df = df[df["date"] >= from_dt]
                if to_date and "date" in df.columns:
                    to_dt = datetime.strptime(to_date, "%Y%m%d").date()
                    df = df[df["date"] <= to_dt]

                if not df.empty:
                    # 初回は WRITE_TRUNCATE、以降は WRITE_APPEND
                    if not first_write_done:
                        mode = "WRITE_TRUNCATE"
                        first_write_done = True
                    else:
                        mode = "WRITE_APPEND"
                    rows = loader.load_dataframe(
                        df,
                        f"{config.ds_raw}.stock_prices_daily",
                        write_disposition=mode,
                    )
                    total_rows += rows
            except Exception as e:
                logger.error(f"Failed to process {key}: {e}")
                continue

        return total_rows
    else:
        # CSVが取得できない場合はAPI経由でフォールバック
        logger.info("No CSV files available, falling back to API")
        if not from_date:
            from_date = "20160101"  # Standard: 10年分
        if not to_date:
            to_date = datetime.now().strftime("%Y%m%d")

        data = client.get_daily_quotes(from_date=from_date, to_date=to_date)
        if not data:
            return 0

        df = _transform(pd.DataFrame(data))
        return loader.load_dataframe(
            df,
            f"{config.ds_raw}.stock_prices_daily",
            write_disposition="WRITE_TRUNCATE",
        )


def ingest_backfill(client: JQuantsClient, loader: BQLoader, config,
                    from_date: str, to_date: str) -> int:
    """期間指定でのバックフィル（差分取込み）"""
    logger.info(f"Backfilling daily quotes: {from_date} to {to_date}")
    data = client.get_daily_quotes(from_date=from_date, to_date=to_date)
    if not data:
        return 0

    df = _transform(pd.DataFrame(data))
    return loader.merge_dataframe(
        df,
        f"{config.ds_raw}.stock_prices_daily",
        merge_keys=["date", "code"],
        staging_table=f"{config.ds_raw}.stock_prices_daily_staging",
    )
