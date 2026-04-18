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
    # J-Quants v2 returns short-form column names (schema drift 2026-04).
    # Support both legacy long-form and current short-form; the Value columns
    # are no longer returned but we keep the mapping for forward-compat.
    col_map = {
        # Common
        "Date": "date", "Code": "code",
        # Legacy long-form (kept for backward compat)
        "LongMarginTradeVolume": "long_margin_trade_volume",
        "LongMarginTradeValue": "long_margin_trade_value",
        "ShortMarginTradeVolume": "short_margin_trade_volume",
        "ShortMarginTradeValue": "short_margin_trade_value",
        "LongNegotiableMarginTradeVolume": "long_negotiable_margin_trade_volume",
        "LongNegotiableMarginTradeValue": "long_negotiable_margin_trade_value",
        "ShortNegotiableMarginTradeVolume": "short_negotiable_margin_trade_volume",
        "ShortNegotiableMarginTradeValue": "short_negotiable_margin_trade_value",
        # Current short-form (2026-04+)
        "LongVol": "long_margin_trade_volume",
        "ShrtVol": "short_margin_trade_volume",
        "LongNegVol": "long_negotiable_margin_trade_volume",
        "ShrtNegVol": "short_negotiable_margin_trade_volume",
        "LongStdVol": "long_standard_margin_trade_volume",
        "ShrtStdVol": "short_standard_margin_trade_volume",
        "IssType": "issue_type",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = [v for v in dict.fromkeys(col_map.values()) if v in df.columns]
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
    """決算発表予定日の取込み（全件洗い替え）。

    J-Quants `/equities/earnings-calendar` は「発表予定」のフォワード
    スナップショットを返す (過去の履歴ではない)。WRITE_TRUNCATE が
    正しい書込モード。

    V2 レスポンス フィールドは短縮形 (CoName / FY / FQ / SectorNm /
    Section)。legacy 長形 (CompanyName / FiscalYearEnd) も別マップで
    受理しておく。
    """
    from datetime import datetime as _dt, timezone as _tz
    data = client.get_earnings_calendar()
    if not data:
        logger.info("earnings_calendar: empty response (no upcoming announcements)")
        return 0

    df = pd.DataFrame(data)
    col_map = {
        # Common
        "Date": "date",
        "Code": "code",
        # V2 short form (current)
        "CoName": "company_name",
        "FY": "fiscal_year",
        "FQ": "fiscal_quarter",
        "SectorNm": "sector_name",
        "Section": "section",
        # Legacy long form (kept for forward/backward compat)
        "CompanyName": "company_name",
        "FiscalYearEnd": "fiscal_year",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = list(dict.fromkeys(v for v in col_map.values() if v in df.columns))
    df = df[keep]
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    df["_fetched_at"] = pd.Timestamp(_dt.now(_tz.utc))

    return loader.load_dataframe(
        df, f"{config.ds_raw}.earnings_calendar",
        write_disposition="WRITE_TRUNCATE",
    )


_SHORT_SALE_COLMAP = {
    "DiscDate": "disc_date", "CalcDate": "calc_date", "Code": "code",
    "SSName": "ss_name", "SSAddr": "ss_addr",
    "DICName": "dic_name", "DICAddr": "dic_addr",
    "FundName": "fund_name",
    "ShrtPosToSO": "shrt_pos_to_so",
    "ShrtPosShares": "shrt_pos_shares",
    "ShrtPosUnits": "shrt_pos_units",
    "PrevRptDate": "prev_rpt_date",
    "PrevRptRatio": "prev_rpt_ratio",
    "Notes": "notes",
}


def ingest_short_sale_report(client: JQuantsClient, loader: BQLoader, config,
                              target_date: str | None = None,
                              max_catchup_days: int = 30) -> int:
    """空売り残高報告の取込み (Standard以上).

    Publishes rows when short position ≥ 0.5% of SO is filed.
    target_date is None → catch-up from BQ latest disc_date + 1 to yesterday.
    """
    if target_date is not None:
        return _short_sale_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(
        f"{config.ds_raw}.short_sale_report", "disc_date"
    )
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    if latest:
        start = datetime.strptime(str(latest)[:10], "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start = today - timedelta(days=max_catchup_days - 1)
    end = today - timedelta(days=1)
    if start > end:
        logger.info("short_sale_report already up to date")
        return 0
    span = (end - start).days + 1
    if span > max_catchup_days:
        start = end - timedelta(days=max_catchup_days - 1)
    logger.info("short_sale_report catch-up: %s .. %s", start, end)

    total = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            try:
                total += _short_sale_one_day(client, loader, config,
                                             cur.strftime("%Y%m%d"))
            except Exception as e:
                logger.warning("short_sale %s failed: %s", cur, e)
        cur += timedelta(days=1)
    return total


def _short_sale_one_day(client: JQuantsClient, loader: BQLoader, config,
                        target_date: str) -> int:
    try:
        data = client.get_short_sale_report(disc_date=target_date)
    except Exception as e:
        msg = str(e)
        if "400" in msg or "403" in msg:
            logger.info("No short_sale_report for %s", target_date)
            return 0
        raise
    if not data:
        return 0

    df = pd.DataFrame(data)
    rename = {k: v for k, v in _SHORT_SALE_COLMAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = [v for v in dict.fromkeys(_SHORT_SALE_COLMAP.values()) if v in df.columns]
    df = df[keep]

    for c in ("disc_date", "calc_date", "prev_rpt_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ("shrt_pos_to_so", "shrt_pos_shares", "shrt_pos_units", "prev_rpt_ratio"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("code", "ss_name", "ss_addr", "dic_name", "dic_addr",
              "fund_name", "notes"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    df = df.drop_duplicates(subset=["disc_date", "code", "ss_name", "fund_name"])

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.short_sale_report",
        merge_keys=["disc_date", "code", "ss_name", "fund_name"],
        staging_table=f"{config.ds_raw}.short_sale_report_staging",
    )


_DAILY_MARGIN_COLMAP = {
    "PubDate": "pub_date", "Code": "code", "AppDate": "app_date",
    "ShrtOut": "shrt_out", "ShrtOutChg": "shrt_out_chg",
    "ShrtOutRatio": "shrt_out_ratio",
    "LongOut": "long_out", "LongOutChg": "long_out_chg",
    "LongOutRatio": "long_out_ratio",
    "SLRatio": "sl_ratio",
    "ShrtNegOut": "shrt_neg_out", "ShrtNegOutChg": "shrt_neg_out_chg",
    "ShrtStdOut": "shrt_std_out", "ShrtStdOutChg": "shrt_std_out_chg",
    "LongNegOut": "long_neg_out", "LongNegOutChg": "long_neg_out_chg",
    "LongStdOut": "long_std_out", "LongStdOutChg": "long_std_out_chg",
    "TSEMrgnRegCls": "tse_mrgn_reg_cls",
}


def _flatten_pub_reason(rows: list[dict]) -> None:
    for r in rows:
        pr = r.pop("PubReason", None) or {}
        r["pub_reason_restricted"] = str(pr.get("Restricted", ""))
        r["pub_reason_daily_publication"] = str(pr.get("DailyPublication", ""))
        r["pub_reason_monitoring"] = str(pr.get("Monitoring", ""))
        r["pub_reason_restricted_by_jsf"] = str(pr.get("RestrictedByJSF", ""))
        r["pub_reason_precaution_by_jsf"] = str(pr.get("PrecautionByJSF", ""))
        r["pub_reason_unclear_or_sec_on_alert"] = str(pr.get("UnclearOrSecOnAlert", ""))


def ingest_daily_margin_interest(client: JQuantsClient, loader: BQLoader, config,
                                  target_date: str | None = None,
                                  max_catchup_days: int = 30) -> int:
    """日々公表信用取引残高 (margin-alert) の取込み (Standard以上)."""
    if target_date is not None:
        return _daily_margin_one_day(client, loader, config, target_date)

    latest = loader.get_latest_date(
        f"{config.ds_raw}.daily_margin_interest", "pub_date"
    )
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    if latest:
        start = datetime.strptime(str(latest)[:10], "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start = today - timedelta(days=max_catchup_days - 1)
    end = today - timedelta(days=1)
    if start > end:
        logger.info("daily_margin_interest already up to date")
        return 0
    span = (end - start).days + 1
    if span > max_catchup_days:
        start = end - timedelta(days=max_catchup_days - 1)
    logger.info("daily_margin_interest catch-up: %s .. %s", start, end)

    total = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            try:
                total += _daily_margin_one_day(client, loader, config,
                                               cur.strftime("%Y%m%d"))
            except Exception as e:
                logger.warning("daily_margin %s failed: %s", cur, e)
        cur += timedelta(days=1)
    return total


def _daily_margin_one_day(client: JQuantsClient, loader: BQLoader, config,
                          target_date: str) -> int:
    try:
        data = client.get_daily_margin_interest(date=target_date)
    except Exception as e:
        msg = str(e)
        if "400" in msg or "403" in msg:
            logger.info("No daily_margin for %s", target_date)
            return 0
        raise
    if not data:
        return 0

    # Mutates data in place, flattening the PubReason nested struct.
    _flatten_pub_reason(data)

    df = pd.DataFrame(data)
    rename = {k: v for k, v in _DAILY_MARGIN_COLMAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    keep = [v for v in dict.fromkeys(_DAILY_MARGIN_COLMAP.values()) if v in df.columns]
    keep += [c for c in df.columns if c.startswith("pub_reason_")]
    df = df[list(dict.fromkeys(keep))]

    for c in ("pub_date", "app_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    for c in ("shrt_out_ratio", "long_out_ratio", "tse_mrgn_reg_cls"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    for c in list(df.columns):
        if c in ("pub_date", "app_date", "code", "tse_mrgn_reg_cls",
                 "shrt_out_ratio", "long_out_ratio") or c.startswith("pub_reason_"):
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "code" in df.columns:
        df["code"] = df["code"].astype(str)

    df = df.drop_duplicates(subset=["pub_date", "code", "app_date"])

    return loader.merge_dataframe(
        df, f"{config.ds_raw}.daily_margin_interest",
        merge_keys=["pub_date", "code", "app_date"],
        staging_table=f"{config.ds_raw}.daily_margin_interest_staging",
    )


def ingest_trading_calendar(client: JQuantsClient, loader: BQLoader, config) -> int:
    """取引カレンダーの取込み（/markets/calendar, 全件洗い替え）。

    J-Quants は 2016-04 以降〜翌年末までの holiday division を 1 レスポンス
    で返す（~4,200 行）。WRITE_TRUNCATE で定期的にフルリフレッシュする。
    """
    from datetime import datetime as _dt, timezone as _tz
    data = client.get_trading_calendar()
    if not data:
        logger.info("trading_calendar: empty response")
        return 0

    df = pd.DataFrame(data)
    df = df.rename(columns={"Date": "date", "HolDiv": "hol_div"})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "hol_div" in df.columns:
        df["hol_div"] = df["hol_div"].astype(str)
    df["_fetched_at"] = pd.Timestamp(_dt.now(_tz.utc))
    df = df.drop_duplicates(subset=["date"]).sort_values("date")

    return loader.load_dataframe(
        df, f"{config.ds_raw}.trading_calendar",
        write_disposition="WRITE_TRUNCATE",
    )
