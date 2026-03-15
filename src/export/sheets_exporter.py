"""BigQuery → Google Sheets エクスポーター

毎日の --mode export で呼び出す。
integrated_score ビューからデータを取得し、スプレッドシートに書き込む。

シート構成:
  1. スクリーニング結果   - 全ACTIVE銘柄スコアランキング（上位200件）
  2. エントリーシグナル   - kubota_signal が ENTRY/WATCH の銘柄
  3. 相場環境            - TOPIXのMA200・マーケットフェーズ
  4. 株価推移（60日）     - 上位50銘柄 × 直近60営業日の終値（横持ち形式）
  5. バックテスト         - 過去シグナルの勝率・平均リターン統計
  6. スコア推移           - score_history テーブルの直近30日分
  7. 更新履歴            - 毎回の実行結果ログ
"""
import logging
import os
from datetime import datetime

import pandas as pd
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 通常シート設定（query + jp_headers のみ必要）
# ──────────────────────────────────────────────
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
              screening_status,
              next_earnings_date,
              days_to_earnings
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
            "次回決算日", "決算まで(日)",
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
              market_phase,
              next_earnings_date,
              days_to_earnings
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
            "次回決算日", "決算まで(日)",
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
    {
        "sheet_name": "バックテスト",
        "query": """
            SELECT
              COUNT(*)                                                     AS total_signals,
              MIN(signal_date)                                             AS earliest_signal,
              MAX(signal_date)                                             AS latest_signal,
              ROUND(AVG(win_5d)  * 100, 1)                                AS win_rate_5d_pct,
              ROUND(AVG(return_5d_pct),  2)                               AS avg_return_5d_pct,
              ROUND(AVG(win_10d) * 100, 1)                                AS win_rate_10d_pct,
              ROUND(AVG(return_10d_pct), 2)                               AS avg_return_10d_pct,
              ROUND(AVG(win_20d) * 100, 1)                                AS win_rate_20d_pct,
              ROUND(AVG(return_20d_pct), 2)                               AS avg_return_20d_pct,
              ROUND(MIN(return_20d_pct), 2)                               AS min_return_20d_pct,
              ROUND(MAX(return_20d_pct), 2)                               AS max_return_20d_pct,
              ROUND(STDDEV(return_20d_pct), 2)                            AS std_return_20d_pct
            FROM `onitsuka-app.analytics.backtest_signals`
            WHERE signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 365 DAY)
        """,
        "jp_headers": [
            "シグナル数(1年)", "最古シグナル日", "最新シグナル日",
            "勝率5日後(%)", "平均リターン5日後(%)",
            "勝率10日後(%)", "平均リターン10日後(%)",
            "勝率20日後(%)", "平均リターン20日後(%)",
            "最大損失20日後(%)", "最大利益20日後(%)", "リターン標準偏差20日後(%)",
        ],
    },
    {
        "sheet_name": "スコア推移",
        "query": """
            SELECT
              snapshot_date,
              code,
              company_name,
              sector33_name,
              latest_close,
              kubota_trade_score,
              growth_invest_score,
              kubota_signal,
              market_phase
            FROM `onitsuka-app.analytics.score_history`
            WHERE snapshot_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 30 DAY)
            ORDER BY snapshot_date DESC, kubota_trade_score DESC, growth_invest_score DESC
            LIMIT 3000
        """,
        "jp_headers": [
            "スナップショット日", "銘柄コード", "会社名", "セクター",
            "終値", "窪田スコア", "成長株スコア", "エントリーシグナル", "相場フェーズ",
        ],
        "allow_empty": True,   # テーブル未作成でもエラーにしない
    },
]

# ──────────────────────────────────────────────
# 株価推移（60日）専用クエリ（Pythonでピボット）
# ──────────────────────────────────────────────
PRICE_HISTORY_QUERY = """
SELECT
  dq.date,
  dq.code,
  em.company_name,
  em.sector33_name,
  ROUND(dq.close, 1) AS close
FROM `onitsuka-app.stock_raw.daily_quotes` dq
JOIN `onitsuka-app.stock_master.equity_master` em ON dq.code = em.code
WHERE dq.code IN (
  SELECT code
  FROM `onitsuka-app.analytics.integrated_score`
  WHERE screening_status = 'ACTIVE'
  ORDER BY kubota_trade_score DESC, growth_invest_score DESC
  LIMIT 50
)
AND dq.date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 90 DAY)
AND dq.close IS NOT NULL
ORDER BY dq.date, dq.code
"""


# ──────────────────────────────────────────────
# 内部ユーティリティ
# ──────────────────────────────────────────────

def _get_gspread_client(creds_path: str) -> gspread.Client:
    """サービスアカウントで gspread クライアントを初期化"""
    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def _ensure_worksheet(spreadsheet: gspread.Spreadsheet, sheet_name: str,
                       rows: int = 2000, cols: int = 60) -> gspread.Worksheet:
    """シートが存在しなければ作成して返す"""
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logger.info(f"  新規シート作成: {sheet_name}")
        return spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)


def _df_to_sheet(ws: gspread.Worksheet, df: pd.DataFrame,
                 jp_headers: list = None) -> None:
    """DataFrame をシートに全件書き込み。

    行構成:
      1行目: 英語カラム名（内部キー）
      2行目: 日本語カラム名（jp_headers 指定時）
      3行目以降: データ
    """
    ws.clear()
    if df.empty:
        logger.warning("  DataFrame が空のためヘッダーのみ書き込み")
        en_header = df.columns.tolist()
        all_rows = [en_header]
        if jp_headers:
            all_rows.append(jp_headers)
        ws.update(all_rows, value_input_option="RAW")
        return

    df = df.copy()

    # --- 型変換: db_dtypes.DateDtype 等を文字列に統一 ---
    for col in df.columns:
        df[col] = df[col].astype(str).replace(
            {"nan": "", "None": "", "NaT": "", "<NA>": ""}
        )

    en_header = df.columns.tolist()
    rows = df.values.tolist()

    # 空文字 → None に戻す（USER_ENTERED モードが空文字を日付パースしてエラーになるのを防ぐ）
    rows = [[None if v == "" else v for v in row] for row in rows]

    all_rows = [en_header]
    if jp_headers:
        all_rows.append(jp_headers)
    all_rows.extend(rows)

    ws.update(all_rows, value_input_option="USER_ENTERED")
    logger.info(f"  {len(rows)} 行 + ヘッダー書き込み完了")


def _col_letter(idx: int) -> str:
    """0-based 列インデックスを Sheets の列文字に変換（A, B, ..., Z, AA, ...）"""
    result = ""
    n = idx + 1  # 1-based
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _apply_formatting(spreadsheet: gspread.Spreadsheet, ws: gspread.Worksheet,
                       sheet_name: str, has_jp_header: bool) -> None:
    """シートに条件付き書式を適用する"""
    sheet_id = ws.id

    # --- 既存の条件付き書式ルールを全削除 ---
    while True:
        try:
            spreadsheet.batch_update({"requests": [
                {"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}}
            ]})
        except Exception:
            break  # ルールがなくなったら終了

    # データ開始行（0-based）: jp_headerあり → 行2(0-based) = 行3(1-based)
    data_start_row = 2 if has_jp_header else 1

    requests = []

    def _gradient_rule(col_idx, start_color, end_color, max_row=1000):
        """白→色のグラデーションルール"""
        col = col_idx
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row,
                        "endRowIndex": max_row,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    }],
                    "gradientRule": {
                        "minpoint": {
                            "color": start_color,
                            "type": "MIN",
                        },
                        "maxpoint": {
                            "color": end_color,
                            "type": "MAX",
                        },
                    },
                },
                "index": 0,
            }
        }

    def _formula_row_rule(formula, bg_color, max_row=1000):
        """数式ベース・行全体カラーリング"""
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row,
                        "endRowIndex": max_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": 30,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": formula}],
                        },
                        "format": {
                            "backgroundColor": bg_color,
                        },
                    },
                },
                "index": 0,
            }
        }

    def _formula_cell_rule(formula, col_idx, bg_color, max_row=1000):
        """数式ベース・セル単体カラーリング"""
        col = col_idx
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row,
                        "endRowIndex": max_row,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": formula}],
                        },
                        "format": {
                            "backgroundColor": bg_color,
                        },
                    },
                },
                "index": 0,
            }
        }

    white = {"red": 1.0, "green": 1.0, "blue": 1.0}
    green = {"red": 0.565, "green": 0.933, "blue": 0.565}
    blue  = {"red": 0.565, "green": 0.773, "blue": 0.933}
    light_green  = {"red": 0.7,  "green": 0.93, "blue": 0.7}
    light_yellow = {"red": 1.0,  "green": 0.95, "blue": 0.7}
    cell_red     = {"red": 1.0,  "green": 0.7,  "blue": 0.7}
    cell_orange  = {"red": 1.0,  "green": 0.85, "blue": 0.6}

    if sheet_name == "スクリーニング結果":
        # idx=8 kubota_trade_score: white→green gradient
        requests.append(_gradient_rule(8, white, green))
        # idx=13 growth_invest_score: white→blue gradient
        requests.append(_gradient_rule(13, white, blue))
        # idx=17 kubota_signal (R column): ENTRY SIGNAL → 行ライトグリーン
        # data_start_row=2(0-based) → row3(1-based) → $R3
        requests.append(_formula_row_rule('=$R3="ENTRY SIGNAL"', light_green))
        # WATCH → 行ライトイエロー
        requests.append(_formula_row_rule('=$R3="WATCH（放れ待ち）"', light_yellow))
        # idx=20 days_to_earnings (U column): <=10 → セル赤
        requests.append(_formula_cell_rule('=AND($U3<>"",$U3<=10)', 20, cell_red))
        # <=20 → セルオレンジ
        requests.append(_formula_cell_rule('=AND($U3<>"",$U3<=20)', 20, cell_orange))

    elif sheet_name == "エントリーシグナル":
        # idx=8 kubota_trade_score: white→green gradient
        requests.append(_gradient_rule(8, white, green))
        # idx=14 kubota_signal (O column): ENTRY SIGNAL → 行ライトグリーン
        # data_start_row=2(0-based) → $O3
        requests.append(_formula_row_rule('=$O3="ENTRY SIGNAL"', light_green))
        # idx=17 days_to_earnings (R column): <=10 → セル赤
        requests.append(_formula_cell_rule('=AND($R3<>"",$R3<=10)', 17, cell_red))
        # <=20 → セルオレンジ
        requests.append(_formula_cell_rule('=AND($R3<>"",$R3<=20)', 17, cell_orange))

    if not requests:
        return

    spreadsheet.batch_update({"requests": requests})
    logger.info(f"  条件付き書式 {len(requests)} 件 適用完了: {sheet_name}")


def _build_price_pivot(df_long: pd.DataFrame):
    """株価長形式 → 縦持ち転置（銘柄×日付）にピボット

    列構成:
      列A "銘柄":   "7203_トヨタ自動車"、"6758_ソニーグループ"… （銘柄コード＋会社名）
      列B "セクター": "輸送用機器"、"電気機器"…
      列C〜 日付:   "2025-12-17"、"2025-12-18"… （昇順）

    Returns:
        tuple[pd.DataFrame, list]:
            df_t  転置済みDataFrame（jp_headers は不要なため空リストを返す）
            []    （互換性維持用の空リスト）
    """
    if df_long.empty:
        return df_long, []

    df_long = df_long.copy()
    df_long["date"] = df_long["date"].astype(str)
    df_long["col"] = df_long["code"].astype(str) + "_" + df_long["company_name"].astype(str)

    # col → sector33_name のマッピング
    sector_map = (
        df_long.drop_duplicates("col")
        .set_index("col")["sector33_name"]
        .to_dict()
    )

    # ① 日付×銘柄でピボット
    df_pivot = df_long.pivot_table(
        index="date",
        columns="col",
        values="close",
        aggfunc="first",
    )
    df_pivot.columns.name = None

    # 直近60日のみ（90日取得して最新60日に絞る）
    df_pivot = df_pivot.sort_index(ascending=False).head(60)
    df_pivot = df_pivot.sort_index(ascending=True)

    # ② 転置: 銘柄×日付
    df_t = df_pivot.T                    # index=銘柄, columns=日付文字列
    df_t.index.name = "銘柄"
    df_t = df_t.reset_index()            # "銘柄" 列が先頭に

    # ③ 「セクター」列を2列目に挿入
    df_t.insert(1, "セクター", df_t["銘柄"].map(sector_map).fillna(""))

    return df_t, []


# ──────────────────────────────────────────────
# メインエクスポート関数
# ──────────────────────────────────────────────

def export(config) -> dict:
    """BigQuery → Google Sheets エクスポートを実行

    Returns:
        dict: {sheet_name: row_count}  (-1 は失敗)
    """
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID 環境変数が設定されていません")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "sa-key.json")

    bq_client = bigquery.Client(project=config.bq_project, location=config.bq_location)
    gc = _get_gspread_client(creds_path)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    logger.info(f"スプレッドシート: {spreadsheet.title}")

    today = datetime.now().strftime("%Y-%m-%d")
    results = {}

    # ──────────────────────────────
    # ① 通常シート（SHEETS_CONFIG）
    # ──────────────────────────────
    for cfg in SHEETS_CONFIG:
        sheet_name = cfg["sheet_name"]
        logger.info(f"エクスポート中: {sheet_name}")
        try:
            df = bq_client.query(cfg["query"]).to_dataframe(create_bqstorage_client=False)
            ws = _ensure_worksheet(spreadsheet, sheet_name)
            _df_to_sheet(ws, df, jp_headers=cfg.get("jp_headers"))
            results[sheet_name] = len(df)
            logger.info(f"  OK: {len(df)} 行 → '{sheet_name}'")
            # 条件付き書式の適用
            try:
                _apply_formatting(spreadsheet, ws, sheet_name, bool(cfg.get("jp_headers")))
            except Exception as fmt_err:
                logger.warning(f"  書式設定スキップ: {sheet_name} — {fmt_err}")
        except Exception as e:
            # allow_empty=True のシートは警告のみ（score_history 未作成時など）
            if cfg.get("allow_empty"):
                logger.warning(f"  SKIP: {sheet_name} — {e}")
                results[sheet_name] = 0
            else:
                logger.error(f"  FAILED: {sheet_name} — {e}")
                results[sheet_name] = -1

    # ──────────────────────────────
    # ② 株価推移（60日）— 転置形式
    #    行: 銘柄  列A=銘柄名 列B=セクター 列C〜=日付
    # ──────────────────────────────
    sheet_price = "株価推移（60日）"
    logger.info(f"エクスポート中: {sheet_price}")
    try:
        df_long = bq_client.query(PRICE_HISTORY_QUERY).to_dataframe(
            create_bqstorage_client=False
        )
        df_t, _ = _build_price_pivot(df_long)
        # 列数 = 銘柄(1) + セクター(1) + 日付数(最大60)
        n_cols = len(df_t.columns) + 2
        ws = _ensure_worksheet(spreadsheet, sheet_price, rows=60, cols=n_cols)
        _df_to_sheet(ws, df_t)   # 1行目=英語列名（銘柄/セクター/日付）、データのみ

        n_stocks = len(df_t)
        n_days   = max(len(df_t.columns) - 2, 0)
        results[sheet_price] = n_stocks
        logger.info(f"  OK: {n_stocks} 銘柄 × {n_days} 日分 → '{sheet_price}'")
    except Exception as e:
        logger.error(f"  FAILED: {sheet_price} — {e}")
        results[sheet_price] = -1

    # ──────────────────────────────
    # ③ 更新履歴
    # ──────────────────────────────
    try:
        ws_meta = _ensure_worksheet(spreadsheet, "更新履歴", rows=500, cols=15)
        existing = ws_meta.get_all_values()
        meta_headers = [
            "更新日時",
            "スクリーニング結果", "エントリーシグナル", "相場環境",
            "バックテスト", "スコア推移", "株価推移（60日）",
        ]
        if not existing:
            ws_meta.update([meta_headers], value_input_option="RAW")
        new_row = [
            today,
            results.get("スクリーニング結果", 0),
            results.get("エントリーシグナル", 0),
            results.get("相場環境", 0),
            results.get("バックテスト", 0),
            results.get("スコア推移", 0),
            results.get("株価推移（60日）", 0),
        ]
        ws_meta.append_row(new_row)
        logger.info("  更新履歴 追記完了")
    except Exception as e:
        logger.warning(f"更新履歴の書き込みに失敗: {e}")

    return results
