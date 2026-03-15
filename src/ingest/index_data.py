"""TOPIX・指数データ取込み"""
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)


def ingest_topix(client: JQuantsClient, loader: BQLoader, config,
                 from_date: str | None = None, to_date: str | None = None) -> int:
    """TOPIX四本値の取込み"""
    if from_date is None:
        latest = loader.get_latest_date(f"{config.ds_raw}.topix_daily")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            from_date = next_date.strftime("%Y%m%d")

    data = client.get_topix(from_date=from_date, to_date=to_date)
    if not data:
        return 0

    df = pd.DataFrame(data)
    # 新フォーマット（O/H/L/C）と旧フォーマット（Open/High/Low/Close）の両方をサポート
    col_map = {
        "Date": "date",
        "O": "open", "H": "high", "L": "low", "C": "close",       # 新フォーマット
        "Open": "open", "High": "high", "Low": "low", "Close": "close",  # 旧フォーマット
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = list(dict.fromkeys(v for v in col_map.values() if v in df.columns))
    df = df[keep]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ["open", "high", "low", "close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return loader.load_dataframe(
        df, f"{config.ds_raw}.topix_daily",
        write_disposition="WRITE_TRUNCATE"
    )


def ingest_indices(client: JQuantsClient, loader: BQLoader, config,
                   from_date: str | None = None, to_date: str | None = None) -> int:
    """指数四本値の取込み（Standard以上）"""
    if from_date is None:
        latest = loader.get_latest_date(f"{config.ds_raw}.index_daily")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            from_date = next_date.strftime("%Y%m%d")

    try:
        data = client.get_indices(from_date=from_date, to_date=to_date)
    except requests.exceptions.HTTPError as e:
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 400:
            logger.info("indices/bars/daily: 400 Bad Request (not available on this plan or date) - skipped")
            return 0
        raise
    if not data:
        return 0

    df = pd.DataFrame(data)
    col_map = {"Date": "date", "IndexCode": "index_code",
               "Open": "open", "High": "high", "Low": "low", "Close": "close"}
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ["open", "high", "low", "close"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.index_daily",
        merge_keys=["date", "index_code"],
        staging_table=f"{config.ds_raw}.index_daily_staging",
    )
