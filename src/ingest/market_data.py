"""マーケットデータ取込み（信用取引・空売り・投資部門別）"""
import logging
from datetime import datetime, timedelta
import pandas as pd
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)


def ingest_margin_interest(client: JQuantsClient, loader: BQLoader, config,
                           target_date: str | None = None) -> int:
    """信用取引週末残高の取込み（Standard以上）"""
    if target_date is None:
        latest = loader.get_latest_date(f"{config.ds_raw}.margin_interest")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            target_date = next_date.strftime("%Y%m%d")

    data = client.get_margin_interest(date=target_date)
    if not data:
        return 0

    df = pd.DataFrame(data)
    col_map = {
        "Date": "date", "Code": "code",
        "LongMarginTradeVolume": "long_margin_trade_volume",
        "LongMarginTradeValue": "long_margin_trade_value",
        "ShortMarginTradeVolume": "short_margin_trade_volume",
        "ShortMarginTradeValue": "short_margin_trade_value",
        "LongNegotiableMarginTradeVolume": "long_negotiable_margin_trade_volume",
        "LongNegotiableMarginTradeValue": "long_negotiable_margin_trade_value",
        "ShortNegotiableMarginTradeVolume": "short_negotiable_margin_trade_volume",
        "ShortNegotiableMarginTradeValue": "short_negotiable_margin_trade_value",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep]
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.margin_interest",
        merge_keys=["date", "code"],
        staging_table=f"{config.ds_raw}.margin_interest_staging",
    )


def ingest_short_selling(client: JQuantsClient, loader: BQLoader, config,
                         target_date: str | None = None) -> int:
    """業種別空売り比率の取込み（Standard以上）"""
    if target_date is None:
        latest = loader.get_latest_date(f"{config.ds_raw}.short_selling_ratio")
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            target_date = next_date.strftime("%Y%m%d")

    data = client.get_short_selling_ratio(date=target_date)
    if not data:
        return 0

    df = pd.DataFrame(data)
    col_map = {
        "Date": "date", "Sector33Code": "sector33_code",
        "SellingValue": "selling_value",
        "ShortSellingWithRestrictionsValue": "short_selling_with_restrictions_value",
        "ShortSellingWithoutRestrictionsValue": "short_selling_without_restrictions_value",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.short_selling_ratio",
        merge_keys=["date", "sector33_code"],
        staging_table=f"{config.ds_raw}.short_selling_ratio_staging",
    )


def ingest_investor_types(client: JQuantsClient, loader: BQLoader, config,
                          from_date: str | None = None,
                          to_date: str | None = None) -> int:
    """投資部門別売買動向の取込み"""
    if from_date is None:
        latest = loader.get_latest_date(
            f"{config.ds_raw}.investor_types", "published_date"
        )
        if latest:
            next_date = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            from_date = next_date.strftime("%Y%m%d")

    data = client.get_investor_types(from_date=from_date, to_date=to_date)
    if not data:
        return 0

    df = pd.DataFrame(data)
    col_map = {
        "PublishedDate": "published_date",
        "StartDate": "start_date", "EndDate": "end_date",
        "Section": "section",
        "ProprietorsSellValue": "proprietors_sell_value",
        "ProprietorsBuyValue": "proprietors_buy_value",
        "ForeignersSellValue": "foreigners_sell_value",
        "ForeignersBuyValue": "foreigners_buy_value",
        "IndividualsSellValue": "individuals_sell_value",
        "IndividualsBuyValue": "individuals_buy_value",
        "SecuritiesCosSellValue": "securities_cos_sell_value",
        "SecuritiesCosBuyValue": "securities_cos_buy_value",
        "InvestmentTrustsSellValue": "investment_trusts_sell_value",
        "InvestmentTrustsBuyValue": "investment_trusts_buy_value",
        "OtherCorpsSellValue": "other_corps_sell_value",
        "OtherCorpsBuyValue": "other_corps_buy_value",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep]

    for c in ["published_date", "start_date", "end_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.investor_types",
        merge_keys=["published_date", "section"],
        staging_table=f"{config.ds_raw}.investor_types_staging",
    )


def ingest_earnings_calendar(client: JQuantsClient, loader: BQLoader, config) -> int:
    """決算発表予定日の取込み（全件洗い替え）"""
    data = client.get_earnings_calendar()
    if not data:
        return 0

    df = pd.DataFrame(data)
    col_map = {
        "Code": "code", "CompanyName": "company_name",
        "Date": "date", "FiscalYearEnd": "fiscal_year_end",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep]
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    return loader.load_dataframe(
        df, f"{config.ds_raw}.earnings_calendar",
        write_disposition="WRITE_TRUNCATE",
    )
