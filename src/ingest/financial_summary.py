"""財務サマリー取込み"""
import logging
from datetime import datetime, timedelta
import pandas as pd
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


def ingest(client: JQuantsClient, loader: BQLoader, config,
           target_date: str | None = None) -> int:
    """財務サマリーの日次更新

    Args:
        target_date: 開示日（YYYYMMDD）。Noneなら最新日を自動判定
    """
    if target_date is None:
        latest = loader.get_latest_date(
            f"{config.ds_raw}.financial_summary", "disclosed_date"
        )
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            target_date = next_date.strftime("%Y%m%d")

    data = client.get_financial_summary(date=target_date)
    if not data:
        logger.info(f"No financial data for {target_date}")
        return 0

    df = pd.DataFrame(data)
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # 不要カラム除外
    keep = [v for v in COLUMN_MAP.values() if v in df.columns and not v.startswith("_")]
    df = df[keep]

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "disclosed_date" in df.columns:
        df["disclosed_date"] = pd.to_datetime(df["disclosed_date"]).dt.date

    # 数値カラムの型変換
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
