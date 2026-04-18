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
        latest = loader.get_latest_date(f"{config.ds_raw}.index_prices_daily")
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

    df["index_code"] = "0000"
    keep = ["date", "index_code"] + [c for c in ["open", "high", "low", "close"] if c in df.columns]
    df = df[keep]
    return loader.merge_dataframe(
        df, f"{config.ds_raw}.index_prices_daily",
        merge_keys=["date", "index_code"],
        staging_table=f"{config.ds_raw}.index_prices_daily_staging",
    )


def ingest_indices(client: JQuantsClient, loader: BQLoader, config,
                   from_date: str | None = None,
                   to_date: str | None = None,
                   max_catchup_days: int = 30) -> int:
    """指数四本値の取込み（Standard以上）— per-day iteration.

    The J-Quants endpoint requires ``date`` OR ``code``; passing only
    from/to returns 400. This implementation iterates day by day using
    ``date=YYYYMMDD`` so one call retrieves every index (~79 codes) for
    that date.

    Cursor logic:
      * ``from_date``/``to_date`` supplied     → iterate that explicit window.
      * Neither supplied                        → cursor = the latest date on
        which we already hold non-0000 codes + 1. This is important because
        ``ingest_topix`` updates code='0000' daily and would otherwise push
        the MAX(date) of the whole table past every actual industry-index
        cutoff, leaving ``ingest_indices`` to perpetually find "nothing new"
        even when the industry data is stale.

    Response fields are short-form in v2 (``Date/Code/O/H/L/C``); the
    legacy long-form rename (``IndexCode``/``Open``/...) is kept for
    backward compatibility.
    """
    from datetime import date as _date, timedelta as _td
    today = (datetime.utcnow() + _td(hours=9)).date()
    tgt_table = f"{config.ds_raw}.index_prices_daily"

    if from_date is None:
        # Cursor derived from a MAX(date) WHERE index_code != '0000'
        # query so the topix-updater does not advance our watermark.
        latest_non_zero = _latest_non_zero_index_date(loader, config, tgt_table)
        if latest_non_zero:
            start = latest_non_zero + _td(days=1)
        else:
            start = today - _td(days=max_catchup_days - 1)
    else:
        start = datetime.strptime(from_date, "%Y%m%d").date()

    end = (datetime.strptime(to_date, "%Y%m%d").date()
           if to_date else today - _td(days=1))
    if start > end:
        logger.info("indices already up to date (start=%s > end=%s)", start, end)
        return 0

    # Cap runaway backfills in case the cursor is way behind.
    span = (end - start).days + 1
    if span > max_catchup_days:
        logger.warning(
            "indices catch-up span %d > cap %d — limiting to last %d days",
            span, max_catchup_days, max_catchup_days,
        )
        start = end - _td(days=max_catchup_days - 1)

    logger.info("indices catch-up window: %s → %s", start, end)

    all_rows: list[dict] = []
    cur = start
    while cur <= end:
        # Skip weekends — J-Quants returns empty but still charges latency.
        if cur.weekday() < 5:
            ymd = cur.strftime("%Y%m%d")
            try:
                day_rows = client.get_indices(date=ymd)
                all_rows.extend(day_rows or [])
            except requests.exceptions.HTTPError as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status == 400:
                    logger.info("indices %s: 400 (likely holiday) — skipped", ymd)
                else:
                    logger.warning("indices %s: %s — continuing", ymd, e)
            except Exception as e:
                logger.warning("indices %s: %s — continuing", ymd, e)
        cur += _td(days=1)

    if not all_rows:
        logger.info("indices: no new data over window %s..%s", start, end)
        return 0

    df = pd.DataFrame(all_rows)
    # Short-form (v2) + long-form (legacy) simultaneously supported.
    col_map = {
        # Short form (current)
        "Date": "date",
        "Code": "index_code",
        "O": "open", "H": "high", "L": "low", "C": "close",
        # Long form (historical backward-compat)
        "IndexCode": "index_code",
        "Open": "open", "High": "high", "Low": "low", "Close": "close",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = list(dict.fromkeys(v for v in col_map.values() if v in df.columns))
    df = df[keep]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("open", "high", "low", "close"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.drop_duplicates(subset=["date", "index_code"])

    return loader.merge_dataframe(
        df, tgt_table,
        merge_keys=["date", "index_code"],
        staging_table=f"{config.ds_raw}.index_prices_daily_staging",
    )


def _latest_non_zero_index_date(loader: BQLoader, config, table_id: str):
    """Return MAX(date) WHERE index_code != '0000'.

    Separated so ``ingest_indices`` does not race with ``ingest_topix`` —
    topix only updates code='0000', so its watermark must not gate the
    industry indices update.
    """
    import logging as _lg
    _log = _lg.getLogger(__name__)
    try:
        sql = (
            f"SELECT MAX(date) AS d FROM `{loader.project}.{table_id}` "
            f"WHERE index_code != '0000'"
        )
        rows = list(loader.client.query(sql).result())
        if not rows:
            return None
        return rows[0][0]
    except Exception as e:
        _log.warning("_latest_non_zero_index_date failed: %s", e)
        return None
