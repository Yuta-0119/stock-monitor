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


def ingest_minute(client: JQuantsClient, loader: BQLoader, config,
                  target_date: str | None = None) -> int:
    """日次更新: 指定日の分足データを取得

    Args:
        target_date: 取得日（YYYYMMDD形式）。Noneの場合は最新日を自動判定
    """
    if target_date is None:
        latest = loader.get_latest_date(f"{config.ds_raw}.stock_prices_minute")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            target_date = next_date.strftime("%Y%m%d")
        else:
            target_date = datetime.now().strftime("%Y%m%d")

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
