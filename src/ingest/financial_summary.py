"""財務サマリー取込み (J-Quants /fins/summary).

J-Quants は 2026-04 頃から **短縮形カラム名** (DiscDate, DocType, Sales, OP, ...)
で response を返すように変わった。BQ テーブル onitsuka-app.raw.financial_summary は
108 列の長形式 (disclosed_date, document_type, net_sales, operating_profit, ...) で
作成されているため、両者を結ぶ完全な COLUMN_MAP が必要。

本モジュールが扱う列:
  J-Quants response (短縮形)        : 107 列  (logged via logger.info(df.columns))
  BQ raw.financial_summary (長形式) : 108 列  (107 user + 1 system column _fetched_at)

COLUMN_MAP は 107 短縮 -> 107 長形式 を 1:1 で網羅。重複・孤児なし。
merge_keys は disclosed_date / code / document_type の 3 つで PRIMARY 相当。
"""
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)


# ===================================================================
# COLUMN_MAP — J-Quants short-form -> raw.financial_summary BQ column
# ===================================================================
# Coverage: 107 / 107 J-Quants response columns -> all user columns of BQ schema.
# Validated by tools/validate_financial_summary_map.py before each commit.
COLUMN_MAP = {
    # --- header / identifiers ---
    "DiscDate": "disclosed_date",
    "DiscTime": "disclosed_time",
    "Code": "code",
    "DiscNo": "disclosure_number",
    "DocType": "document_type",
    # --- period descriptors ---
    "CurPerType": "current_period_type",
    "CurPerSt": "current_period_start",
    "CurPerEn": "current_period_end",
    "CurFYSt": "current_fy_start",
    "CurFYEn": "current_fy_end",
    "NxtFYSt": "next_fy_start",
    "NxtFYEn": "next_fy_end",
    # --- consolidated PL / BS (current actual) ---
    "Sales": "net_sales",
    "OP": "operating_profit",
    "OdP": "ordinary_profit",
    "NP": "net_income",
    "EPS": "eps",
    "DEPS": "diluted_eps",
    "TA": "total_assets",
    "Eq": "equity",
    "EqAR": "equity_to_asset_ratio",
    "BPS": "bps",
    # --- cash flow ---
    "CFO": "cf_operating",
    "CFI": "cf_investing",
    "CFF": "cf_financing",
    "CashEq": "cash_and_equivalents",
    # --- dividends (current FY actual) ---
    "Div1Q": "dividend_q1",
    "Div2Q": "dividend_q2",
    "Div3Q": "dividend_q3",
    "DivFY": "dividend_fy_end",
    "DivAnn": "dividend_annual",
    "DivUnit": "distributions_per_unit",
    "DivTotalAnn": "total_dividend_paid",
    "PayoutRatioAnn": "payout_ratio",
    # --- dividends (current FY forecast) ---
    "FDiv1Q": "forecast_dividend_q1",
    "FDiv2Q": "forecast_dividend_q2",
    "FDiv3Q": "forecast_dividend_q3",
    "FDivFY": "forecast_dividend_fy",
    "FDivAnn": "forecast_dividend_annual",
    "FDivUnit": "forecast_distributions_unit",
    "FDivTotalAnn": "forecast_total_dividend_paid",
    "FPayoutRatioAnn": "forecast_payout_ratio",
    # --- dividends (next FY forecast) ---
    "NxFDiv1Q": "next_forecast_dividend_q1",
    "NxFDiv2Q": "next_forecast_dividend_q2",
    "NxFDiv3Q": "next_forecast_dividend_q3",
    "NxFDivFY": "next_forecast_dividend_fy",
    "NxFDivAnn": "next_forecast_dividend_annual",
    "NxFDivUnit": "next_forecast_distributions_unit",
    # NOTE: J-Quants response has no NxFDivTotalAnn and BQ has no
    #   next_forecast_total_dividend_paid; both sides agree on the gap.
    "NxFPayoutRatioAnn": "next_forecast_payout_ratio",
    # --- forecast PL (current FY 2Q / interim) ---
    "FSales2Q": "forecast_sales_2q",
    "FOP2Q": "forecast_op_2q",
    "FOdP2Q": "forecast_ordinary_2q",
    "FNP2Q": "forecast_net_income_2q",
    "FEPS2Q": "forecast_eps_2q",
    # --- forecast PL (next FY 2Q) ---
    "NxFSales2Q": "next_forecast_sales_2q",
    "NxFOP2Q": "next_forecast_op_2q",
    "NxFOdP2Q": "next_forecast_ordinary_2q",
    "NxFNp2Q": "next_forecast_net_income_2q",   # note J-Quants uses lowercase 'p' here
    "NxFEPS2Q": "next_forecast_eps_2q",
    # --- forecast PL (current FY full year) ---
    "FSales": "forecast_sales",
    "FOP": "forecast_op",
    "FOdP": "forecast_ordinary",
    "FNP": "forecast_net_income",
    "FEPS": "forecast_eps",
    # --- forecast PL (next FY full year) ---
    "NxFSales": "next_forecast_sales",
    "NxFOP": "next_forecast_op",
    "NxFOdP": "next_forecast_ordinary",
    "NxFNp": "next_forecast_net_income",        # lowercase 'p' again
    "NxFEPS": "next_forecast_eps",
    # --- material changes / disclosures ---
    "MatChgSub": "material_changes_subsidiaries",
    "SigChgInC": "changes_consolidation_scope",   # SigChgInC = Significant Change in Consolidation
    "ChgByASRev": "changes_accounting_standard",  # changes by Accounting Standard revision
    "ChgNoASRev": "changes_other_accounting",     # changes not from AS revision
    "ChgAcEst": "changes_accounting_estimates",
    "RetroRst": "retrospective_restatement",
    # --- shares ---
    "ShOutFY": "shares_outstanding",
    "TrShFY": "treasury_stock",
    "AvgSh": "avg_shares",
    # --- non-consolidated PL/BS (current actual) ---
    "NCSales": "nc_net_sales",
    "NCOP": "nc_operating_profit",
    "NCOdP": "nc_ordinary_profit",
    "NCNP": "nc_net_income",
    "NCEPS": "nc_eps",
    "NCTA": "nc_total_assets",
    "NCEq": "nc_equity",
    "NCEqAR": "nc_equity_to_asset_ratio",
    "NCBPS": "nc_bps",
    # --- non-consolidated forecast (current FY 2Q) ---
    "FNCSales2Q": "forecast_nc_sales_2q",
    "FNCOP2Q": "forecast_nc_op_2q",
    "FNCOdP2Q": "forecast_nc_ordinary_2q",
    "FNCNP2Q": "forecast_nc_net_income_2q",
    "FNCEPS2Q": "forecast_nc_eps_2q",
    # --- non-consolidated forecast (next FY 2Q) ---
    "NxFNCSales2Q": "next_forecast_nc_sales_2q",
    "NxFNCOP2Q": "next_forecast_nc_op_2q",
    "NxFNCOdP2Q": "next_forecast_nc_ordinary_2q",
    "NxFNCNP2Q": "next_forecast_nc_net_income_2q",
    "NxFNCEPS2Q": "next_forecast_nc_eps_2q",
    # --- non-consolidated forecast (current FY full year) ---
    "FNCSales": "forecast_nc_sales",
    "FNCOP": "forecast_nc_op",
    "FNCOdP": "forecast_nc_ordinary",
    "FNCNP": "forecast_nc_net_income",
    "FNCEPS": "forecast_nc_eps",
    # --- non-consolidated forecast (next FY full year) ---
    "NxFNCSales": "next_forecast_nc_sales",
    "NxFNCOP": "next_forecast_nc_op",
    "NxFNCOdP": "next_forecast_nc_ordinary",
    "NxFNCNP": "next_forecast_nc_net_income",
    "NxFNCEPS": "next_forecast_nc_eps",
}


# ===================================================================
# Type coercion
# ===================================================================

# DATE columns in BQ (the rest of the targets are NUMERIC / FLOAT / STRING).
DATE_COLUMNS = {
    "disclosed_date",
    "current_period_start",
    "current_period_end",
    "current_fy_start",
    "current_fy_end",
    "next_fy_start",
    "next_fy_end",
}

# STRING columns -- never coerce to numeric.
STRING_COLUMNS = {
    "disclosed_time",
    "code",
    "disclosure_number",
    "document_type",
    "current_period_type",
    "material_changes_subsidiaries",
    "changes_consolidation_scope",
    "changes_accounting_standard",
    "changes_other_accounting",
    "changes_accounting_estimates",
    "retrospective_restatement",
}


# ===================================================================
# Single-day fetch / MERGE
# ===================================================================

def _ingest_one_day(client: JQuantsClient, loader: BQLoader, config,
                    target_date: str, dry_run: bool = False) -> int:
    """単一日 (YYYYMMDD) の財務サマリーを取込む。

    Args:
        target_date: YYYYMMDD
        dry_run: True なら BQ に書き込まず、rename 後の dataframe 形状だけ
                 ログに出して 0 を返す。スキーマ整合性確認用。
    """
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
    incoming_cols = list(df.columns)
    logger.info("financial_summary %s incoming cols: %d", target_date, len(incoming_cols))

    # Surface unknown columns once (J-Quants might add new ones in future)
    unknown = [c for c in incoming_cols if c not in COLUMN_MAP]
    if unknown:
        logger.warning("financial_summary %s unmapped J-Quants columns (will be dropped): %s",
                       target_date, unknown)

    # Rename to BQ schema
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Keep only mapped (BQ schema) columns; drop anything we don't recognise
    mapped_targets = [v for v in COLUMN_MAP.values() if v in df.columns]
    df = df[mapped_targets]

    # Required merge keys must be present after rename
    for needed in ("disclosed_date", "code", "document_type"):
        if needed not in df.columns:
            logger.warning(
                "financial_summary %s: required column %s missing after rename (cols=%s) -- skipping",
                target_date, needed, list(df.columns),
            )
            return 0

    # Type coercion
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)
    for dc in DATE_COLUMNS:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce").dt.date
    # Numeric: everything that isn't DATE / STRING and is in the target schema
    for col in df.columns:
        if col in DATE_COLUMNS or col in STRING_COLUMNS:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if dry_run:
        logger.info(
            "financial_summary %s DRY-RUN: would MERGE %d rows x %d cols (cols=%s)",
            target_date, len(df), len(df.columns), list(df.columns),
        )
        # Sample dtype report for visual verification
        for c in list(df.columns)[:10]:
            logger.info("  %s -> %s", c, df[c].dtype)
        return 0

    return loader.merge_dataframe(
        df,
        f"{config.ds_raw}.financial_summary",
        merge_keys=["disclosed_date", "code", "document_type"],
        staging_table=f"{config.ds_raw}.financial_summary_staging",
    )


def ingest(client: JQuantsClient, loader: BQLoader, config,
           target_date: str | None = None,
           max_catchup_days: int = 30,
           dry_run: bool = False) -> int:
    """財務サマリーの日次更新 (catch-up loop).

    target_date: YYYYMMDD or None.
      - 指定あり: その日のみ取込み。
      - None    : 最新日 + 1 日 〜 today (JST) を 1 日ずつループ。空日は黙ってスキップ。
    max_catchup_days: 自動取り込みの最大遡行日数 (暴走防止)。
    dry_run: BQ へ書き込まずに dataframe 形状だけログに出す。
    """
    if target_date is not None:
        return _ingest_one_day(client, loader, config, target_date, dry_run=dry_run)

    latest = loader.get_latest_date(
        f"{config.ds_raw}.financial_summary", "disclosed_date"
    )
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    logger.info("financial_summary BQ latest disclosed_date = %r (today=%s)", latest, today)
    if latest:
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
            n = _ingest_one_day(client, loader, config, ymd, dry_run=dry_run)
            total += n
        except Exception as e:
            logger.warning("financial_summary day %s failed: %s -- continuing", ymd, e)
        days_done += 1
        cur += timedelta(days=1)
    logger.info("financial_summary catch-up: %d rows over %d days", total, days_done)
    return total


# ===================================================================
# Bulk loader (CSV / multi-day) - kept for backfill use-cases.
# Uses the same COLUMN_MAP so bulk and daily paths agree on schema.
# ===================================================================

# Old short-form alias map (kept for legacy CSV files that still use these).
# Most aliases are already covered by COLUMN_MAP; this only adds extras the
# CSV may emit that the API does not.
COLUMN_MAP_BULK = {
    **COLUMN_MAP,
    # Legacy CSV-only column aliases (rarely used)
}


def ingest_bulk(client: JQuantsClient, loader: BQLoader, config) -> int:
    """CSV / API bulk download for backfill."""
    files = client.bulk_list("/fins/summary")
    if not files:
        return 0

    total = 0
    for i, f_info in enumerate(files):
        try:
            df = client.bulk_download(f_info.get("Key", ""))
            cmap = COLUMN_MAP_BULK
            rename = {k: v for k, v in cmap.items() if k in df.columns}
            df = df.rename(columns=rename)
            keep = list(dict.fromkeys(v for v in cmap.values() if v in df.columns))
            df = df[keep]

            if "disclosed_date" not in df.columns:
                logger.warning(f"[financial_summary] disclosed_date missing in bulk file {f_info.get(chr(34)+chr(75)+chr(101)+chr(121)+chr(34),chr(34)+chr(34))}, skipping")
                continue
            df = df[df["disclosed_date"].notna()].copy()
            if df.empty:
                continue

            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.zfill(5)
            for dc in DATE_COLUMNS:
                if dc in df.columns:
                    df[dc] = pd.to_datetime(df[dc], errors="coerce").dt.date
            mode = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
            total += loader.load_dataframe(
                df, f"{config.ds_raw}.financial_summary", write_disposition=mode
            )
        except Exception as e:
            logger.error(f"Bulk load failed: {e}")
    return total
