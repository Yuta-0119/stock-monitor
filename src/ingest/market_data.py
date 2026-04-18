"""マーケットデータ取込み（信用取引・空売り・投資部門別）"""
import logging
from datetime import date, datetime, timedelta
import pandas as pd
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)


def _iter_fridays(start: date, end: date):
    """Yield each Friday (weekday=4) in the inclusive [start, end] range."""
    cur = start + timedelta(days=(4 - start.weekday()) % 7)
    while cur <= end:
        yield cur
        cur += timedelta(days=7)


def ingest_margin_interest(client: JQuantsClient, loader: BQLoader, config,
                           target_date: str | None = None,
                           max_catchup_weeks: int = 8) -> int:
    """信用取引週末残高の取込み（Standard以上）.

    margin_interest は金曜日締めの週次データ。土日や祝日を date= で渡すと
    J-Quants は 200 + data=[] を返すため、エンドポイントを叩く日付は必ず
    「直近の金曜日」に丸める必要がある。

    target_date is not None -> single-day ingest (caller-specified YYYYMMDD).
    target_date is None     -> iterate Fridays in (BQ latest .. today]. On
                                first run (table missing), back-fills up to
                                ``max_catchup_weeks`` Fridays so we don't
                                no-op forever.
    """
    if target_date is not None:
        return _margin_interest_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(f"{config.ds_raw}.margin_interest")
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    # Most recent Friday (today if today is Friday; last Friday otherwise).
    last_fri = today - timedelta(days=(today.weekday() - 4) % 7)
    logger.info(
        "margin_interest BQ latest=%r today=%s last_friday=%s",
        latest, today, last_fri,
    )

    if latest:
        latest_str = str(latest)[:10]
        prev = datetime.strptime(latest_str, "%Y-%m-%d").date()
        start = prev + timedelta(days=1)
    else:
        # Initial run: back-fill the last N Fridays.
        start = last_fri - timedelta(weeks=max_catchup_weeks - 1)

    fridays = list(_iter_fridays(start, last_fri))
    if not fridays:
        logger.info("margin_interest already up to date (latest=%s)", latest)
        return 0
    logger.info(
        "margin_interest catch-up Fridays: %s..%s (%d weeks)",
        fridays[0], fridays[-1], len(fridays),
    )

    total = 0
    for d in fridays:
        ymd = d.strftime("%Y%m%d")
        try:
            n = _margin_interest_one_day(client, loader, config, ymd)
            total += n
        except Exception as e:
            logger.warning("margin_interest %s failed: %s -- continuing", ymd, e)
    logger.info("margin_interest catch-up: %d rows over %d Fridays", total, len(fridays))
    return total


def _margin_interest_one_day(client: JQuantsClient, loader: BQLoader, config,
                             target_date: str) -> int:
    """Single-day ingest. Swallows 400/403-holiday and empty-data cases."""
    try:
        data = client.get_margin_interest(date=target_date)
    except Exception as e:
        msg = str(e)
        # 400 = malformed / no-data; 403 with "invalid or expired" is a separate
        # bug (see user .env), but 403 can also mean "no data for this date".
        if "400" in msg or "403" in msg:
            logger.info(f"No margin_interest for {target_date} ({msg[:80]})")
            return 0
        raise
    if not data:
        logger.info(f"margin_interest {target_date}: empty response")
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
                         target_date: str | None = None,
                         max_catchup_days: int = 30) -> int:
    """Sector short-selling ratio update with catch-up loop.

    target_date is not None -> single-day ingest (legacy).
    target_date is None     -> loop from BQ latest+1 to today (JST).
    """
    if target_date is not None:
        return _short_selling_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(f"{config.ds_raw}.short_selling_ratio")
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    logger.info("short_selling_ratio BQ latest date = %r (today=%s)", latest, today)
    if latest:
        latest_str = str(latest)[:10]
        start = datetime.strptime(latest_str, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start = today
    if start > today:
        logger.info("short_selling_ratio already up to date (latest=%s)", latest)
        return 0
    logger.info("short_selling_ratio catch-up window: %s -> %s", start, today)

    span = (today - start).days + 1
    if span > max_catchup_days:
        logger.warning(
            "short_selling_ratio catch-up span %d > cap %d, limiting", span, max_catchup_days,
        )
        start = today - timedelta(days=max_catchup_days - 1)

    total = 0
    cur = start
    days_done = 0
    while cur <= today:
        ymd = cur.strftime("%Y%m%d")
        try:
            n = _short_selling_one_day(client, loader, config, ymd)
            total += n
        except Exception as e:
            logger.warning("short_selling day %s failed: %s -- continuing", ymd, e)
        days_done += 1
        cur += timedelta(days=1)
    logger.info("short_selling_ratio catch-up: %d rows over %d days", total, days_done)
    return total


def _short_selling_one_day(client: JQuantsClient, loader: BQLoader, config,
                           target_date: str) -> int:
    """Single-day ingest. Swallows 400 (holiday)."""
    try:
        data = client.get_short_selling_ratio(date=target_date)
    except Exception as e:
        # J-Quants returns 400 for holidays / no-data days
        msg = str(e)
        if "400" in msg:
            logger.info(f"No short_selling for {target_date} (400 - holiday)")
            return 0
        raise
    if not data:
        return 0

    df = pd.DataFrame(data)
    logger.info("short_selling %s incoming columns: %s", target_date, list(df.columns))
    # J-Quants now returns short-form column names. Support both for backward compatibility.
    col_map = {
        # Long form (legacy J-Quants response)
        "Date": "date", "Sector33Code": "sector33_code",
        "SellingValue": "sell_excluding_short_value",
        "ShortSellingWithRestrictionsValue": "short_with_restriction_value",
        "ShortSellingWithoutRestrictionsValue": "short_no_restriction_value",
        # Short form (current J-Quants response)
        "S33": "sector33_code",
        "SellExShortVa": "sell_excluding_short_value",
        "ShrtWithResVa": "short_with_restriction_value",
        "ShrtNoResVa": "short_no_restriction_value",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = [v for v in dict.fromkeys(col_map.values()) if v in df.columns]
    df = df[keep]
    if "sector33_code" not in df.columns or "date" not in df.columns:
        logger.warning(
            "short_selling %s: missing merge-key columns after rename (cols=%s) -- skipping",
            target_date, list(df.columns),
        )
        return 0
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
