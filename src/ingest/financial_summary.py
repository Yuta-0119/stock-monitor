"""財務サマリー取込み"""
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "DisclosedDate": "disclosed_date",
    "DisclosedTime": "_disclosed_time",
    "Code": "code",
    "FiscalYearEnd": "fiscal_year_end",
    "TypeOfDocument": "type_of_document",
    "NetSales": "net_sales",
    "OperatingProfit": "operating_profit",
    "OrdinaryProfit": "ordinary_profit",
    "Profit": "profit",
    "EarningsPerShare": "earnings_per_share",
    "DilutedEarningsPerShare": "diluted_earnings_per_share",
    "BookValuePerShare": "book_value_per_share",
    "ReturnOnEquity": "return_on_equity",
    "TotalAssets": "total_assets",
    "Equity": "equity",
    "EquityToAssetRatio": "equity_to_asset_ratio",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYear":
        "number_of_issued_and_outstanding_shares_at_the_end_of_fiscal_year",
    "ForecastNetSales": "forecast_net_sales",
    "ForecastOperatingProfit": "forecast_operating_profit",
    "ForecastOrdinaryProfit": "forecast_ordinary_profit",
    "ForecastProfit": "forecast_profit",
    "ForecastEarningsPerShare": "forecast_earnings_per_share",
    "ForecastDividendPerShare": "forecast_dividend_per_share",
    "MaterialChangesInSubsidiaries": "material_changes_in_subsidiaries",
}

# 旧バルクCSV用カラムマップ（省略形）
COLUMN_MAP_BULK = {
    "DiscDate":  "disclosed_date",
    "DiscTime":  "_disclosed_time",
    "Code":      "code",
    "DocType":   "type_of_document",
    "Sales":     "net_sales",
    "OP":        "operating_profit",
    "OdP":       "ordinary_profit",
    "NP":        "profit",
    "EPS":       "earnings_per_share",
    "DEPS":      "diluted_earnings_per_share",
    "BPS":       "book_value_per_share",
    "TA":        "total_assets",
    "Eq":        "equity",
    "EqAR":      "equity_to_asset_ratio",
    "ShOutFY":   "number_of_issued_and_outstanding_shares_at_the_end_of_fiscal_year",
    "FSales":    "forecast_net_sales",
    "FOP":       "forecast_operating_profit",
    "FOdP":      "forecast_ordinary_profit",
    "FNP":       "forecast_profit",
    "FEPS":      "forecast_earnings_per_share",
    "FDivAnn":   "forecast_dividend_per_share",
    "MatChgSub": "material_changes_in_subsidiaries",
}


def _ingest_one_day(client: JQuantsClient, loader: BQLoader, config,
                    target_date: str) -> int:
    """Single-day fetch (YYYYMMDD). Swallows 400 (holiday/no disclosure)."""
    try:
        data = client.get_financial_summary(date=target_date)
    except requests.exceptions.HTTPError as e:
        if hasattr(e, "response") and e.response is not None and e.response.status_code == 400:
            logger.info(f"No financial data for {target_date} (400 - holiday or no disclosure)")
            return 0
        raise
    if not data:
        logger.info(f"No financial data for {target_date}")
        return 0

    df = pd.DataFrame(data)
    # Surface the actual column set so we can diagnose silent J-Quants schema drift
    logger.info("financial_summary %s incoming columns: %s", target_date, list(df.columns))
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    keep = [v for v in COLUMN_MAP.values() if v in df.columns and not v.startswith("_")]
    df = df[keep]
    if "disclosed_date" not in df.columns:
        # Without the merge key the BQ MERGE is guaranteed to 400; log loudly and skip.
        logger.warning(
            "financial_summary %s: response missing disclosed_date after rename (cols=%s) -- skipping",
            target_date, list(df.columns),
        )
        return 0

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "disclosed_date" in df.columns:
        df["disclosed_date"] = pd.to_datetime(df["disclosed_date"]).dt.date

    numeric_cols = [
        "net_sales", "operating_profit", "ordinary_profit", "profit",
        "earnings_per_share", "diluted_earnings_per_share",
        "book_value_per_share", "return_on_equity",
        "total_assets", "equity", "equity_to_asset_ratio",
        "forecast_net_sales", "forecast_operating_profit",
        "forecast_ordinary_profit", "forecast_profit",
        "forecast_earnings_per_share",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return loader.merge_dataframe(
        df,
        f"{config.ds_raw}.financial_summary",
        merge_keys=["disclosed_date", "code", "type_of_document"],
        staging_table=f"{config.ds_raw}.financial_summary_staging",
    )


def ingest(client: JQuantsClient, loader: BQLoader, config,
           target_date: str | None = None,
           max_catchup_days: int = 30) -> int:
    """Daily financial-summary update with auto catch-up loop.

    target_date is not None -> ingest just that day (legacy behavior).
    target_date is None     -> loop from BQ latest+1 to today (JST), one
                               day at a time. Empty days are silently
                               skipped, so multi-day gaps fill themselves.

    The previous implementation only tried a single day after the latest,
    so a holiday or disclosure-less day froze the table forever.
    """
    if target_date is not None:
        return _ingest_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(
        f"{config.ds_raw}.financial_summary", "disclosed_date"
    )
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    logger.info("financial_summary BQ latest disclosed_date = %r (today=%s)", latest, today)
    if latest:
        # get_latest_date may return either 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' depending on
        # whether pandas materialised as date or Timestamp. Take the first 10 chars to be safe.
        latest_str = str(latest)[:10]
        start = datetime.strptime(latest_str, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start = today
    if start > today:
        logger.info("financial_summary already up to date (latest=%s)", latest)
        return 0
    logger.info("financial_summary catch-up window: %s -> %s", start, today)

    span = (today - start).days + 1
    if span > max_catchup_days:
        logger.warning(
            "financial_summary catch-up span %d > cap %d, limiting to last %d days",
            span, max_catchup_days, max_catchup_days,
        )
        start = today - timedelta(days=max_catchup_days - 1)

    total = 0
    cur = start
    days_done = 0
    while cur <= today:
        ymd = cur.strftime("%Y%m%d")
        try:
            n = _ingest_one_day(client, loader, config, ymd)
            total += n
        except Exception as e:
            logger.warning("financial_summary day %s failed: %s -- continuing", ymd, e)
        days_done += 1
        cur += timedelta(days=1)
    logger.info("financial_summary catch-up: %d rows over %d days", total, days_done)
    return total

def ingest_bulk(client: JQuantsClient, loader: BQLoader, config) -> int:
    """CSVまたはAPIで全期間の財務サマリーを取得"""
    files = client.bulk_list("/fins/summary")
    if files:
        total = 0
        for i, f_info in enumerate(files):
            try:
                df = client.bulk_download(f_info.get("Key", ""))
                # 新旧フォーマットを自動判別
                cmap = COLUMN_MAP_BULK if "DiscDate" in df.columns else COLUMN_MAP
                rename = {k: v for k, v in cmap.items() if k in df.columns}
                df = df.rename(columns=rename)
                keep = [v for v in cmap.values() if v in df.columns and not v.startswith("_")]
                df = df[keep]

                # disclosed_date が存在しない古いCSVはスキップ（パーティションキー必須）
                if "disclosed_date" not in df.columns:
                    logger.warning(f"[financial_summary] disclosed_date missing, skipping (key={f_info.get('Key','')})")
                    continue
                # disclosed_date が全行NaT/NaNの場合も除外
                df = df[df["disclosed_date"].notna()].copy()
                if df.empty:
                    logger.warning(f"[financial_summary] all rows null disclosed_date, skipping")
                    continue

                if "code" in df.columns:
                    df["code"] = df["code"].astype(str).str.zfill(5)
                if "disclosed_date" in df.columns:
                    df["disclosed_date"] = pd.to_datetime(df["disclosed_date"]).dt.date
                mode = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
                total += loader.load_dataframe(
                    df, f"{config.ds_raw}.financial_summary", write_disposition=mode
                )
            except Exception as e:
                logger.error(f"Failed: {e}")
        return total
    return 0
