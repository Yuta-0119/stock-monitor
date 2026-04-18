"""先物・オプション関連データ取込み."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pandas as pd

from src.bq_loader import BQLoader
from src.jquants_client import JQuantsClient

logger = logging.getLogger(__name__)


# 日経225オプション の短縮形 → BQ列名
_INDEX_OPT_COLMAP = {
    "Date": "date",
    "Code": "code",
    "EmMrgnTrgDiv": "em_mrgn_trg_div",
    # Day session OHLC (main)
    "O": "day_open", "H": "day_high", "L": "day_low", "C": "day_close",
    # Night session OHLC (E = evening)
    "EO": "night_open", "EH": "night_high", "EL": "night_low", "EC": "night_close",
    # Whole-day / session-split long form
    "WhDayOP": "whole_day_open", "WhDayHiP": "whole_day_high",
    "WhDayLoP": "whole_day_low", "WhDayCP": "whole_day_close",
    "NgtSsnOP": "night_session_open", "NgtSsnHiP": "night_session_high",
    "NgtSsnLoP": "night_session_low", "NgtSsnCP": "night_session_close",
    "DaySsnOP": "day_session_open", "DaySsnHiP": "day_session_high",
    "DaySsnLoP": "day_session_low", "DaySsnCP": "day_session_close",
    # Derived / market fields
    "SettlementPrice": "settlement_price",
    "TheoreticalPrice": "theoretical_price",
    "BaseVolatility": "base_volatility",
    "UnderlyingPrice": "underlying_price",
    "ImpliedVolatility": "implied_volatility",
    "InterestRate": "interest_rate",
    "OpenInterest": "open_interest",
    "TurnoverValue": "turnover_value",
    "TradingVolume": "trading_volume",
    "ContractMonth": "contract_month",
    "StrikePrice": "strike_price",
    "PutCallDivision": "put_call_division",
    "LastTradingDay": "last_trading_day",
    "SpecialQuotationDay": "special_quotation_day",
    "VolumeOnlyAuction": "volume_only_auction",
    "CentralContractMonthFlag": "central_contract_month_flag",
}


def ingest_index_options_225(client: JQuantsClient, loader: BQLoader, config,
                              target_date: str | None = None,
                              max_catchup_days: int = 30) -> int:
    """日経225オプション四本値の取込み (Standard以上).

    target_date is not None -> single-day ingest (YYYYMMDD).
    target_date is None     -> catch-up from BQ latest+1 to yesterday.
    """
    if target_date is not None:
        return _index_opt_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(f"{config.ds_raw}.index_option_prices_daily")
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    if latest:
        start = datetime.strptime(str(latest)[:10], "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start = today - timedelta(days=max_catchup_days - 1)
    end = today - timedelta(days=1)
    if start > end:
        logger.info("index_options already up to date (latest=%s)", latest)
        return 0

    span = (end - start).days + 1
    if span > max_catchup_days:
        start = end - timedelta(days=max_catchup_days - 1)
    logger.info("index_options catch-up: %s .. %s", start, end)

    total = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            try:
                total += _index_opt_one_day(client, loader, config, cur.strftime("%Y%m%d"))
            except Exception as e:
                logger.warning("index_options %s failed: %s", cur, e)
        cur += timedelta(days=1)
    return total


def _index_opt_one_day(client: JQuantsClient, loader: BQLoader, config,
                       target_date: str) -> int:
    try:
        data = client.get_index_options_225(date=target_date)
    except Exception as e:
        msg = str(e)
        if "400" in msg or "403" in msg:
            logger.info("No index_options for %s", target_date)
            return 0
        raise
    if not data:
        return 0

    df = pd.DataFrame(data)
    rename = {k: v for k, v in _INDEX_OPT_COLMAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = [v for v in dict.fromkeys(_INDEX_OPT_COLMAP.values()) if v in df.columns]
    df = df[keep]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("last_trading_day", "special_quotation_day"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in list(df.columns):
        if c in ("date", "code", "em_mrgn_trg_div", "contract_month",
                 "put_call_division", "last_trading_day", "special_quotation_day",
                 "central_contract_month_flag", "volume_only_auction"):
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "em_mrgn_trg_div" not in df.columns:
        df["em_mrgn_trg_div"] = ""
    else:
        df["em_mrgn_trg_div"] = df["em_mrgn_trg_div"].fillna("").astype(str)
    df = df.drop_duplicates(subset=["date", "code", "em_mrgn_trg_div"])

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.index_option_prices_daily",
        merge_keys=["date", "code", "em_mrgn_trg_div"],
        staging_table=f"{config.ds_raw}.index_option_prices_daily_staging",
    )
