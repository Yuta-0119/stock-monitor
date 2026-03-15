"""BigQuery → Google Sheets エクスポーター

毎日の --mode export で呼び出す。
integrated_score ビューからデータを取得し、スプレッドシートに書き込む。
"""
import logging
import os
from datetime import datetime

import gspread
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# エクスポートするシート構成
SHEETS_CONFIG = [
    {
        "sheet_name": "スクリーニング結果",
        "query": """
            SELECT
              code,
              company_name,
              sector33_name,
              latest_close,
              avg_turnover_20d_oku,
              liquidity_grade,
              volatility_score,
              chart_score,
              kubota_trade_score,
              sales_cagr_3y_pct,
              op_cagr_3y_pct,
              roe_pct,
              roic_pct,
              growth_invest_score,
              per,
              pbr,
              market_phase,
              kubota_signal,
              screening_status
            FROM `onitsuka-app.analytics.integrated_score`
            WHERE screening_status = 'ACTIVE'
            ORDER BY kubota_trade_score DESC, growth_invest_score DESC
            LIMIT 200
        """,
        "jp_headers": [
            "銘柄コード", "会社名", "セクター",
            "終値", "平均売買代金(億円)",
            "流動性グレード",
            "ボラスコア", "チャートスコア", "窪田スコア",
            "売上CAGR3年(%)", "営業利益CAGR3年(%)",
            "ROE(%)", "ROIC(%)", "成長株スコア",
            "PER", "PBR",
            "相場フェーズ", "エントリーシグナル", "スクリーニング判定",
        ],
    },
    {
        "sheet_name": "エントリーシグナル",
        "query": """
            SELECT
              code,
              company_name,
              sector33_name,
              latest_close,
              avg_turnover_20d_oku,
              liquidity_grade,
              volatility_score,
              chart_score,
              kubota_trade_score,
              sales_cagr_3y_pct,
              roe_pct,
              growth_invest_score,
              per,
              pbr,
              kubota_signal,
              market_phase
            FROM `onitsuka-app.analytics.integrated_score`
            WHERE kubota_signal != '-'
            ORDER BY kubota_trade_score DESC, growth_invest_score DESC
        """,
        "jp_headers": [
            "銘柄コード", "会社名", "セクター",
            "終値", "平均売買代金(億円)",
            "流動性グレード",
            "ボラスコア", "チャートスコア", "窪田スコア",
            "売上CAGR3年(%)", "ROE(%)", "成長株スコア",
            "PER", "PBR",
            "エントリーシグナル", "相場フェーズ",
        ],
    },
    {
        "sheet_name": "相場環境",
        "query": """
            SELECT
              date,
              topix_close,
              topix_ma200,
              market_phase,
              environment_score
            FROM `onitsuka-app.analytics.market_environment`
        """,
        "jp_headers": [
            "日付", "TOPIX終値", "TOPIX200日MA", "相場フェーズ", "環境スコア",
        ],
    },
]


def _get_gspread_client(creds_path: str) -> gspread.Client:
    """サービスアカウントで gspread クライアントを初期化"""
    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
        ],
    )
    return gspread.authorize(creds)


def _ensure_worksheet(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> gspread.Worksheet:
    """シートが存在しなければ作成して返す"""
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logger.info(f"  Creating new sheet: {sheet_name}")
        return spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=30)


def _df_to_sheet(ws: gspread.Worksheet, df, jp_headers: list = None) -> None:
    """DataFrameをシートに全件書き込み（1行目:英語カラム名、2行目:日本語カラム名）"""
    ws.clear()
    if df.empty:
        logger.warning(f"  Empty DataFrame, cleared sheet only")
        return

    df = df.copy()
    # db_dtypes.DateDtype 等の特殊型は fillna("") が使用不可のため astype(str) で直接変換
    for col in df.columns:
        df[col] = df[col].astype(str).replace({"nan": "", "None": "", "NaT": "", "<NA>": ""})

    en_header = df.columns.tolist()
    rows = df.values.tolist()
    # 空文字をNoneに戻す（USER_ENTEREDモードが空文字を日付としてパースするのを防ぐ）
    rows = [[None if v == "" else v for v in row] for row in rows]

    all_rows = [en_header]
    if jp_headers:
        all_rows.append(jp_headers)
    all_rows.extend(rows)

    ws.update(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"  Written {len(rows)} rows + header")


def export(config) -> dict:
    """BigQuery → Google Sheets エクスポートを実行

    Returns:
        dict: {sheet_name: row_count} の結果サマリー
    """
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID 環境変数が設定されていません")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "sa-key.json")

    # BigQuery クライアント
    bq_client = bigquery.Client(project=config.bq_project, location=config.bq_location)

    # gspread クライアント
    gc = _get_gspread_client(creds_path)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    logger.info(f"Spreadsheet: {spreadsheet.title}")

    # 最終更新日時をシート名に付記するための日付
    today = datetime.now().strftime("%Y-%m-%d")

    results = {}
    for config_item in SHEETS_CONFIG:
        sheet_name = config_item["sheet_name"]
        query = config_item["query"]
        logger.info(f"Exporting: {sheet_name}")

        try:
            df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
            ws = _ensure_worksheet(spreadsheet, sheet_name)
            _df_to_sheet(ws, df, jp_headers=config_item.get("jp_headers"))
            results[sheet_name] = len(df)
            logger.info(f"  OK: {len(df)} rows -> '{sheet_name}'")
        except Exception as e:
            logger.error(f"  FAILED: {sheet_name} — {e}")
            results[sheet_name] = -1

    # 最終更新シートに書き込み
    try:
        ws_meta = _ensure_worksheet(spreadsheet, "更新履歴")
        existing = ws_meta.get_all_values()
        if not existing:
            ws_meta.update([["更新日時", "スクリーニング結果", "エントリーシグナル", "相場環境"]])
        new_row = [
            today,
            results.get("スクリーニング結果", 0),
            results.get("エントリーシグナル", 0),
            results.get("相場環境", 0),
        ]
        ws_meta.append_row(new_row)
    except Exception as e:
        logger.warning(f"更新履歴の書き込みに失敗: {e}")

    return results
