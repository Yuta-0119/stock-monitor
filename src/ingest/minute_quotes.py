"""分足株価OHLC取込み

- 日次更新: date指定のAPIコールで当日分を取得
- BigQueryテーブル: raw.stock_prices_minute
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
    "Time": "time",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "TurnoverValue": "turnover_value",
}

# Bulk CSV uses short names
COLUMN_MAP_BULK = {
    "Date": "date",
    "Code": "code",
    "Time": "time",
    "O": "open",
    "H": "high",
    "L": "low",
    "C": "close",
    "Vo": "volume",
    "Va": "turnover_value",
}

KEEP_COLS = ["date", "code", "time", "open", "high", "low", "close", "volume", "turnover_value"]
INT_COLS = {"volume"}


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """データ変換"""
    cmap = COLUMN_MAP_BULK if "O" in df.columns else COLUMN_MAP
    rename = {k: v for k, v in cmap.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [c for c in KEEP_COLS if c in df.columns]
    df = df[keep]

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    float_cols = [c for c in df.columns if c not in ("date", "code", "time")]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if col in INT_COLS:
            df[col] = df[col].fillna(0).astype("int64")

    return df


def _fetch_and_load_one_day(
    client: JQuantsClient,
    loader: BQLoader,
    config,
    target_date: str,
) -> int:
    """Fetch and load minute quotes for a single day."""
    logger.info(f"Fetching minute quotes for date={target_date}")
    data = client.get_minute_quotes(date=target_date)
    if not data:
        logger.info(f"No minute data for {target_date}")
        return 0

    df = _transform(pd.DataFrame(data))
    if df.empty:
        return 0

    return loader.load_dataframe(
        df,
        f"{config.ds_raw}.stock_prices_minute",
        write_disposition="WRITE_APPEND",
    )


def ingest_minute(client: JQuantsClient, loader: BQLoader, config,
                  target_date: str | None = None) -> int:
    """日次更新: 指定日以降の不足分を一括取得

    Args:
        target_date: 取得日（YYYYMMDD形式）。
                     Noneの場合は、最新日の翌日から今日まで順番にbackfillする。
                     明示的に指定した場合は、その日一日分のみ取得する。
    """
    if target_date is not None:
        # Explicit single-day mode
        return _fetch_and_load_one_day(client, loader, config, target_date)

    # Auto-backfill mode: fill any gap from (latest+1) to today.
    # Use loader.get_latest_date() which handles require_partition_filter=true
    # via INFORMATION_SCHEMA.PARTITIONS. Previously this was an inline
    # `SELECT MAX(date) FROM stock_prices_minute` that silently failed on
    # partition-filter-enforced tables → fallback to today-only fetch.
    table_id = f"{config.ds_raw}.stock_prices_minute"
    latest_str = loader.get_latest_date(table_id, date_column="date")
    if latest_str:
        try:
            latest_date = datetime.strptime(latest_str, "%Y-%m-%d").date()
        except ValueError:
            latest_date = None
    else:
        latest_date = None

    logger.info(f"DEBUG: MAX(date)={latest_date!r} from {loader.project}.{table_id}")

    if latest_date is None:
        # No data in table → fetch today only (init-like behavior)
        today = datetime.now().strftime("%Y%m%d")
        return _fetch_and_load_one_day(client, loader, config, today)

    # Iterate from (latest+1) up to today. Cap at 10 days to avoid runaway.
    start = latest_date + timedelta(days=1)
    today = datetime.now().date()
    total_loaded = 0
    max_iterations = 10

    iter_date = start
    count = 0
    while iter_date <= today and count < max_iterations:
        target_str = iter_date.strftime("%Y%m%d")
        try:
            loaded = _fetch_and_load_one_day(client, loader, config, target_str)
            total_loaded += loaded
        except Exception as e:
            logger.warning(f"ingest_minute backfill for {target_str} failed: {e}")
        iter_date += timedelta(days=1)
        count += 1

    if count >= max_iterations:
        logger.warning(
            f"Minute backfill reached max iterations ({max_iterations}). "
            f"Some dates may still be missing."
        )

    return total_loaded
