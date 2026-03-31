"""
保有銘柄データをGoogle Sheetsから読み込み、BigQueryにロードするスクリプト。

タブ別シート構造（共通: 1行=更新日ラベル, 2行=日付+URL, 3行=ヘッダー, 4行以降=データ）:
  - 国内株式: 銘柄コード(C) / 銘柄名(D) / 口座区分(F) / 売買区分(H) / 数量[株](K) / 単価[円](L)
  - 米国株式: ティッカー(C) / 銘柄名(D) / 口座(E) / 売買区分(G) / 数量[株](K) / 単価[USドル](L) / 為替レート(N)
  - 投資信託: ファンド名(C) / 口座(E) / 取引(F) / 数量[口](H) / 単価(I) / 受渡金額(M)

処理:
  - 買付取引のみ対象
  - 銘柄コード（または銘柄名）ごとに集計（総株数・VWAP取得単価・最初/最新購入日・合計金額）
  - BigQuery テーブル onitsuka-app.analytics.holdings へ WRITE_TRUNCATE でロード
"""

import re
import datetime
import sys
import os

import pandas as pd
import gspread
import google.auth
from google.oauth2 import service_account
from google.cloud import bigquery

# ─── 設定 ───────────────────────────────────────────────
SPREADSHEET_ID = "1NHwYw9EeFApinVMfvEBrJm39WislzgL9h76U9I3RTuo"
HEADER_ROW_IDX = 2   # 0-indexed: 行3がヘッダー
DATA_START_IDX = 3   # 0-indexed: 行4からデータ

BQ_PROJECT  = "onitsuka-app"
BQ_DATASET  = "analytics"
BQ_TABLE    = "holdings"
BQ_LOCATION = "asia-northeast1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
SA_KEY_PATH = os.path.join(SCRIPT_DIR, "..", "sa-key.json")


# ─── 認証 ───────────────────────────────────────────────

def _get_credentials(scopes=None):
    if os.path.exists(SA_KEY_PATH):
        if scopes:
            return service_account.Credentials.from_service_account_file(SA_KEY_PATH, scopes=scopes)
        return service_account.Credentials.from_service_account_file(SA_KEY_PATH)
    creds, _ = google.auth.default(scopes=scopes)
    return creds


# ─── ユーティリティ ────────────────────────────────────

def _parse_number(val) -> float | None:
    """'1,900.00' / '¥104,600' / '5,299口' などを float に変換"""
    if not val or not str(val).strip() or str(val).strip() in ("-", ""):
        return None
    cleaned = re.sub(r"[¥$,\s口株]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date(val: str) -> datetime.date | None:
    val = str(val).strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


def _find_col(headers: list[str], keyword: str) -> int | None:
    """ヘッダー行からキーワードを含む最初の列インデックスを返す"""
    for i, h in enumerate(headers):
        if keyword in str(h):
            return i
    return None


def _pad_row(row: list, min_len: int) -> list:
    return list(row) + [""] * max(0, min_len - len(row))


def _open_spreadsheet():
    creds = _get_credentials(scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ─── 各タブ読み込み ────────────────────────────────────

def _read_domestic(ws) -> pd.DataFrame:
    """国内株式タブを読み込む（買付のみ）"""
    all_rows = ws.get_all_values()
    if len(all_rows) <= DATA_START_IDX:
        return pd.DataFrame()

    headers = all_rows[HEADER_ROW_IDX]
    idx = {
        "trade_date": _find_col(headers, "約定日"),
        "code":       _find_col(headers, "銘柄コード"),
        "company":    _find_col(headers, "銘柄名"),
        "account":    _find_col(headers, "口座区分"),
        "buy_sell":   _find_col(headers, "売買区分"),
        "quantity":   _find_col(headers, "数量"),
        "unit_price": _find_col(headers, "単価"),
    }
    required = ["trade_date", "buy_sell", "quantity"]
    if any(idx[k] is None for k in required):
        print(f"  警告: 国内株式の必須列が見つかりません: {[k for k in required if idx[k] is None]}")
        return pd.DataFrame()

    max_col = max(v for v in idx.values() if v is not None)
    records = []
    for row in all_rows[DATA_START_IDX:]:
        row = _pad_row(row, max_col + 1)
        if not any(str(c).strip() for c in row):
            continue
        if str(row[idx["buy_sell"]]).strip() != "買付":
            continue
        trade_date = _parse_date(row[idx["trade_date"]])
        if not trade_date:
            continue

        quantity   = _parse_number(row[idx["quantity"]])
        unit_price = _parse_number(row[idx["unit_price"]]) if idx["unit_price"] is not None else None
        amount     = (quantity * unit_price) if (quantity and unit_price) else None

        records.append({
            "trade_date":   trade_date,
            "code":         str(row[idx["code"]]).strip() if idx["code"] is not None else None,
            "company_name": str(row[idx["company"]]).strip() if idx["company"] is not None else "",
            "product_type": "国内株式",
            "account_type": str(row[idx["account"]]).strip() if idx["account"] is not None else "",
            "quantity":     quantity,
            "unit_price":   unit_price,
            "amount":       amount,
        })
    return pd.DataFrame(records)


def _read_us(ws) -> pd.DataFrame:
    """米国株式タブを読み込む（買付のみ・JPY換算）"""
    all_rows = ws.get_all_values()
    if len(all_rows) <= DATA_START_IDX:
        return pd.DataFrame()

    headers = all_rows[HEADER_ROW_IDX]
    idx = {
        "trade_date": _find_col(headers, "約定日"),
        "code":       _find_col(headers, "ティッカー"),
        "company":    _find_col(headers, "銘柄名"),
        "account":    _find_col(headers, "口座"),
        "buy_sell":   _find_col(headers, "売買区分"),
        "quantity":   _find_col(headers, "数量"),
        "unit_price": _find_col(headers, "単価"),
        "fx_rate":    _find_col(headers, "為替レート"),
    }
    required = ["trade_date", "buy_sell", "quantity"]
    if any(idx[k] is None for k in required):
        print(f"  警告: 米国株式の必須列が見つかりません: {[k for k in required if idx[k] is None]}")
        return pd.DataFrame()

    max_col = max(v for v in idx.values() if v is not None)
    records = []
    for row in all_rows[DATA_START_IDX:]:
        row = _pad_row(row, max_col + 1)
        if not any(str(c).strip() for c in row):
            continue
        if str(row[idx["buy_sell"]]).strip() != "買付":
            continue
        trade_date = _parse_date(row[idx["trade_date"]])
        if not trade_date:
            continue

        quantity   = _parse_number(row[idx["quantity"]])
        unit_price = _parse_number(row[idx["unit_price"]]) if idx["unit_price"] is not None else None
        fx_rate    = _parse_number(row[idx["fx_rate"]])    if idx["fx_rate"]    is not None else None

        # purchase_amount を円換算
        if quantity and unit_price and fx_rate:
            amount = round(quantity * unit_price * fx_rate, 0)
        elif quantity and unit_price:
            amount = quantity * unit_price  # 為替不明時はUSDのまま
        else:
            amount = None

        records.append({
            "trade_date":   trade_date,
            "code":         str(row[idx["code"]]).strip() if idx["code"] is not None else None,
            "company_name": str(row[idx["company"]]).strip() if idx["company"] is not None else "",
            "product_type": "米国株式",
            "account_type": str(row[idx["account"]]).strip() if idx["account"] is not None else "",
            "quantity":     quantity,
            "unit_price":   unit_price,
            "amount":       amount,
        })
    return pd.DataFrame(records)


def _read_trust(ws) -> pd.DataFrame:
    """投資信託タブを読み込む（買付のみ）"""
    all_rows = ws.get_all_values()
    if len(all_rows) <= DATA_START_IDX:
        return pd.DataFrame()

    headers = all_rows[HEADER_ROW_IDX]
    idx = {
        "trade_date": _find_col(headers, "約定日"),
        "company":    _find_col(headers, "ファンド名"),
        "account":    _find_col(headers, "口座"),
        "buy_sell":   _find_col(headers, "取引"),
        "quantity":   _find_col(headers, "数量"),
        "unit_price": _find_col(headers, "単価"),
        "amount":     _find_col(headers, "受渡金額"),
    }
    required = ["trade_date", "buy_sell"]
    if any(idx[k] is None for k in required):
        print(f"  警告: 投資信託の必須列が見つかりません: {[k for k in required if idx[k] is None]}")
        return pd.DataFrame()

    max_col = max(v for v in idx.values() if v is not None)
    records = []
    for row in all_rows[DATA_START_IDX:]:
        row = _pad_row(row, max_col + 1)
        if not any(str(c).strip() for c in row):
            continue
        if str(row[idx["buy_sell"]]).strip() != "買付":
            continue
        trade_date = _parse_date(row[idx["trade_date"]])
        if not trade_date:
            continue

        quantity   = _parse_number(row[idx["quantity"]])   if idx["quantity"]   is not None else None
        unit_price = _parse_number(row[idx["unit_price"]]) if idx["unit_price"] is not None else None
        # 受渡金額を優先、なければ quantity * unit_price
        amount_direct = _parse_number(row[idx["amount"]]) if idx["amount"] is not None else None
        amount = amount_direct if amount_direct else (
            (quantity * unit_price) if (quantity and unit_price) else None
        )

        records.append({
            "trade_date":   trade_date,
            "code":         None,  # 投資信託にコードなし
            "company_name": str(row[idx["company"]]).strip() if idx["company"] is not None else "",
            "product_type": "投資信託",
            "account_type": str(row[idx["account"]]).strip() if idx["account"] is not None else "",
            "quantity":     quantity,
            "unit_price":   unit_price,
            "amount":       amount,
        })
    return pd.DataFrame(records)


# ─── 集計 ─────────────────────────────────────────────

def aggregate_holdings(df_tx: pd.DataFrame) -> pd.DataFrame:
    """
    トランザクションを銘柄コード（または銘柄名）ごとに集計して保有銘柄を生成。

    BigQuery スキーマ:
        code / company_name / product_type / account_type /
        shares / purchase_price / purchase_date / latest_purchase_date /
        purchase_amount / tx_count
    """
    if df_tx.empty:
        return pd.DataFrame()

    df_tx = df_tx.copy()
    # コードがない場合は銘柄名をキーに使う
    df_tx["key"] = df_tx.apply(
        lambda r: r["code"] if (r["code"] and str(r["code"]).strip()) else r["company_name"],
        axis=1,
    )

    records = []
    for key, grp in df_tx.groupby("key"):
        grp = grp.sort_values("trade_date")
        total_qty    = grp["quantity"].sum() if grp["quantity"].notna().any() else None
        total_amount = grp["amount"].sum()   if grp["amount"].notna().any()   else None

        purchase_price = None
        if total_qty and total_qty > 0 and total_amount:
            purchase_price = round(total_amount / total_qty, 2)

        records.append({
            "code":                 grp["code"].iloc[0],
            "company_name":         grp["company_name"].iloc[0],
            "product_type":         grp["product_type"].iloc[0],
            "account_type":         grp["account_type"].iloc[0],
            "shares":               float(total_qty)    if total_qty    is not None else None,
            "purchase_price":       purchase_price,
            "purchase_date":        grp["trade_date"].min(),
            "latest_purchase_date": grp["trade_date"].max(),
            "purchase_amount":      float(total_amount) if total_amount is not None else None,
            "tx_count":             len(grp),
        })

    return pd.DataFrame(records)


# ─── BigQuery ロード ───────────────────────────────────

def load_to_bigquery(df: pd.DataFrame) -> None:
    print(f"\nBigQuery ロード中: {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    creds  = _get_credentials()
    client = bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)

    schema = [
        bigquery.SchemaField("code",                 "STRING",  description="銘柄コード（国内株:4-5桁, 米国株:ティッカー, 投資信託:None）"),
        bigquery.SchemaField("company_name",         "STRING",  description="会社名・ファンド名"),
        bigquery.SchemaField("product_type",         "STRING",  description="商品区分（国内株式/米国株式/投資信託）"),
        bigquery.SchemaField("account_type",         "STRING",  description="口座区分"),
        bigquery.SchemaField("shares",               "FLOAT64", description="保有株数・口数"),
        bigquery.SchemaField("purchase_price",       "FLOAT64", description="取得単価（VWAP、円換算）"),
        bigquery.SchemaField("purchase_date",        "DATE",    description="最初の購入日"),
        bigquery.SchemaField("latest_purchase_date", "DATE",    description="最新の購入日"),
        bigquery.SchemaField("purchase_amount",      "FLOAT64", description="取得金額合計（円換算）"),
        bigquery.SchemaField("tx_count",             "INT64",   description="購入回数"),
    ]

    table_id   = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    df = df.copy()
    for col in ["purchase_date", "latest_purchase_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"  ロード完了: {table.num_rows} 行")


# ─── エントリーポイント ────────────────────────────────

def main():
    # スプレッドシートを開く
    try:
        ss = _open_spreadsheet()
    except Exception as e:
        print(f"警告: Google Spreadsheetsへの接続に失敗しました（スキップ）: {e}")
        sys.exit(0)

    # 各タブを読み込む
    readers = {
        "国内株式": _read_domestic,
        "米国株式": _read_us,
        "投資信託": _read_trust,
    }

    all_dfs = []
    for sheet_name, reader_fn in readers.items():
        try:
            ws = ss.worksheet(sheet_name)
            df = reader_fn(ws)
            print(f"  {sheet_name}: {len(df)} 件の買付トランザクション")
            if not df.empty:
                all_dfs.append(df)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  警告: ワークシート '{sheet_name}' が見つかりません（スキップ）")
        except Exception as e:
            print(f"  警告: '{sheet_name}' の読み込み失敗（スキップ）: {e}")

    if not all_dfs:
        print("警告: 読み込める買付データがありませんでした。")
        sys.exit(0)

    df_all = pd.concat(all_dfs, ignore_index=True)
    print(f"\n合計買付トランザクション: {len(df_all)} 件")

    df_holdings = aggregate_holdings(df_all)
    print(f"集計後の保有銘柄数: {len(df_holdings)} 件")
    print(df_holdings.to_string())

    load_to_bigquery(df_holdings)
    print("\n完了！")


if __name__ == "__main__":
    main()
