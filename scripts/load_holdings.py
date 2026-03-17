"""
保有銘柄データをGoogle Sheetsから読み込み、BigQueryにロードするスクリプト。

Google Sheets「保有銘柄」タブの列構成:
  A: 約定日/受領日
  B: 約定日
  C: 受渡日
  D: 取引 (購入/売却)
  E: 商品 (国内株式/米国株式/投資信託)
  F: 銘柄 ("会社名\nコード" の形式)
  G: 口座区分
  H: 数量 ("100株", "5,949口", "1株" など)
  I: 受渡金額（換算） ("¥104,600" など)

処理:
  - 購入取引のみ対象
  - 銘柄コードと会社名をF列から抽出
  - 数量・金額をパース
  - 銘柄コードごとに集計（総株数・VWAP取得単価・最初の購入日・合計金額）
  - BigQuery テーブル onitsuka-app.analytics.holdings へ WRITE_TRUNCATE でロード

実行方法:
  .venv\\Scripts\\python scripts/load_holdings.py
"""

import re
import datetime
import sys
import os

import pandas as pd
import gspread
from google.oauth2 import service_account
from google.cloud import bigquery

# ─── 設定 ───────────────────────────────────────────────
SPREADSHEET_ID = "1NHwYw9EeFApinVMfvEBrJm39WislzgL9h76U9I3RTuo"
WORKSHEET_NAME = "保有銘柄"
RANGE = "A1:I200"  # 余裕を持って200行まで読む

BQ_PROJECT = "onitsuka-app"
BQ_DATASET = "analytics"
BQ_TABLE = "holdings"
BQ_LOCATION = "asia-northeast1"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# sa-key.json はスクリプトの親ディレクトリ（プロジェクトルート）にある
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SA_KEY_PATH = os.path.join(SCRIPT_DIR, "..", "sa-key.json")


def _parse_amount(val: str) -> float | None:
    """'¥104,600' → 104600.0, 空文字/Noneはそのまま None を返す"""
    if not val or not str(val).strip():
        return None
    cleaned = re.sub(r"[¥,\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_quantity(val: str) -> float | None:
    """
    '100株' → 100.0
    '5,949口' → 5949.0
    '1株' → 1.0
    '10.5株' → 10.5
    """
    if not val or not str(val).strip():
        return None
    cleaned = re.sub(r"[株口,\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_security(val: str):
    """
    F列 '会社名\nコード' をパースして (company_name, code) を返す。
    国内株式: コードは4-5桁の数字
    米国株式: コードは英字ティッカー (TSLA, GOOGL など)
    投資信託: コード相当の文字列がない場合は company_name のみ
    """
    parts = [p.strip() for p in str(val).split("\n") if p.strip()]
    if len(parts) == 0:
        return None, None
    if len(parts) == 1:
        # コードなし（投資信託の一部）
        return parts[0], None

    company_name = parts[0]
    code_candidate = parts[1]

    # 日本株コード: 4-5桁の数字
    if re.match(r"^\d{4,5}$", code_candidate):
        return company_name, code_candidate

    # 米国株ティッカー: 大文字英字
    if re.match(r"^[A-Z]{1,5}$", code_candidate):
        return company_name, code_candidate

    # それ以外（"再投資型" など）はコードなし
    return company_name, None


def read_sheet_data() -> list[list[str]]:
    """Google Sheetsから生データを読み込む"""
    print(f"Google Sheets 読み込み中: {SPREADSHEET_ID} / {WORKSHEET_NAME}")
    creds = service_account.Credentials.from_service_account_file(SA_KEY_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(WORKSHEET_NAME)
    data = ws.get(RANGE)
    print(f"  {len(data)} 行取得完了（ヘッダー含む）")
    return data


def parse_transactions(raw_data: list[list[str]]) -> pd.DataFrame:
    """生データをトランザクション DataFrame に変換"""
    if not raw_data:
        return pd.DataFrame()

    header = raw_data[0]
    rows = raw_data[1:]

    records = []
    for i, row in enumerate(rows, start=2):
        # 空行スキップ
        if not row or all(not str(c).strip() for c in row):
            continue

        # 行を9列に揃える（末尾不足を空文字で補完）
        row = list(row) + [""] * (9 - len(row))

        trade_type = str(row[3]).strip()  # D列: 取引
        product_type = str(row[4]).strip()  # E列: 商品

        # 購入取引のみ処理（売却は除外）
        if trade_type != "購入":
            continue

        # 約定日をパース（B列）
        trade_date_str = str(row[1]).strip()
        try:
            trade_date = datetime.date.fromisoformat(trade_date_str.replace("/", "-"))
        except ValueError:
            # "2026/03/16" 形式の場合
            try:
                trade_date = datetime.datetime.strptime(trade_date_str, "%Y/%m/%d").date()
            except ValueError:
                print(f"  行{i}: 日付パース失敗 '{trade_date_str}'、スキップ")
                continue

        company_name, code = _parse_security(row[5])  # F列: 銘柄
        quantity = _parse_quantity(row[7])  # H列: 数量
        amount = _parse_amount(row[8])      # I列: 受渡金額

        if company_name is None:
            continue

        # 口座区分
        account_type = str(row[6]).strip()  # G列

        records.append({
            "trade_date": trade_date,
            "company_name": company_name,
            "code": code,
            "product_type": product_type,
            "account_type": account_type,
            "quantity": quantity,
            "amount": amount,
        })

    df = pd.DataFrame(records)
    print(f"  購入トランザクション: {len(df)} 件")
    return df


def aggregate_holdings(df_tx: pd.DataFrame) -> pd.DataFrame:
    """
    トランザクションを銘柄コードごとに集計して保有銘柄 DataFrame を生成。

    スキーマ:
        code             STRING     - 銘柄コード（国内株式・米国株式のみ）
        company_name     STRING     - 会社名
        product_type     STRING     - 商品区分（国内株式/米国株式/投資信託）
        account_type     STRING     - 口座区分（NISA成長投資枠/つみたて投資枠）
        shares           FLOAT64    - 保有株数（口数）
        purchase_price   FLOAT64    - 取得単価（VWAP: 合計金額/合計株数）
        purchase_date    DATE       - 最初の購入日
        latest_purchase_date DATE   - 最新の購入日
        purchase_amount  FLOAT64    - 取得金額合計（円）
        tx_count         INT64      - 購入回数
    """
    if df_tx.empty:
        return pd.DataFrame()

    # コードがない投資信託は company_name をキーにする
    df_tx = df_tx.copy()
    df_tx["key"] = df_tx.apply(
        lambda r: r["code"] if r["code"] else r["company_name"], axis=1
    )

    records = []
    for key, grp in df_tx.groupby("key"):
        grp = grp.sort_values("trade_date")
        code = grp["code"].iloc[0]
        company_name = grp["company_name"].iloc[0]
        product_type = grp["product_type"].iloc[0]
        account_type = grp["account_type"].iloc[0]

        total_qty = grp["quantity"].sum() if grp["quantity"].notna().any() else None
        total_amount = grp["amount"].sum() if grp["amount"].notna().any() else None

        # VWAP取得単価
        if total_qty and total_qty > 0 and total_amount:
            purchase_price = round(total_amount / total_qty, 2)
        else:
            purchase_price = None

        records.append({
            "code": code,
            "company_name": company_name,
            "product_type": product_type,
            "account_type": account_type,
            "shares": float(total_qty) if total_qty is not None else None,
            "purchase_price": purchase_price,
            "purchase_date": grp["trade_date"].min(),
            "latest_purchase_date": grp["trade_date"].max(),
            "purchase_amount": float(total_amount) if total_amount is not None else None,
            "tx_count": len(grp),
        })

    df_holdings = pd.DataFrame(records)
    print(f"  集計後の保有銘柄数: {len(df_holdings)} 件")
    return df_holdings


def load_to_bigquery(df: pd.DataFrame) -> None:
    """DataFrame を BigQuery テーブル onitsuka-app.analytics.holdings にロード"""
    print(f"\nBigQuery ロード中: {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

    creds = service_account.Credentials.from_service_account_file(SA_KEY_PATH)
    client = bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)

    # テーブルスキーマ
    schema = [
        bigquery.SchemaField("code", "STRING", description="銘柄コード（国内株式:4-5桁数字、米国株式:ティッカー、投資信託:None）"),
        bigquery.SchemaField("company_name", "STRING", description="会社名・ファンド名"),
        bigquery.SchemaField("product_type", "STRING", description="商品区分（国内株式/米国株式/投資信託）"),
        bigquery.SchemaField("account_type", "STRING", description="口座区分（NISA成長投資枠/NISAつみたて投資枠等）"),
        bigquery.SchemaField("shares", "FLOAT64", description="保有株数・口数"),
        bigquery.SchemaField("purchase_price", "FLOAT64", description="取得単価（VWAP、円換算）"),
        bigquery.SchemaField("purchase_date", "DATE", description="最初の購入日"),
        bigquery.SchemaField("latest_purchase_date", "DATE", description="最新の購入日"),
        bigquery.SchemaField("purchase_amount", "FLOAT64", description="取得金額合計（円換算）"),
        bigquery.SchemaField("tx_count", "INT64", description="購入回数"),
    ]

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # date 型カラムを datetime.date のまま維持（pandas は object 型で渡す）
    df = df.copy()
    for col in ["purchase_date", "latest_purchase_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # 完了待機

    table = client.get_table(table_id)
    print(f"  ロード完了: {table.num_rows} 行")


def verify_bq(client: bigquery.Client) -> None:
    """ロード後の検証クエリ"""
    print("\n検証クエリ実行...")
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` LIMIT 5"
    df = client.query(sql).to_dataframe(create_bqstorage_client=False)
    print(df.to_string())


def main():
    # 1. シートからデータ読み込み
    raw = read_sheet_data()

    # 2. トランザクションをパース
    df_tx = parse_transactions(raw)
    if df_tx.empty:
        print("エラー: 購入トランザクションが見つかりませんでした。")
        sys.exit(1)

    print("\nトランザクションサンプル:")
    print(df_tx.head(5).to_string())

    # 3. 銘柄ごとに集計
    df_holdings = aggregate_holdings(df_tx)
    print("\n保有銘柄サンプル:")
    print(df_holdings.to_string())

    # 4. BigQuery へロード
    load_to_bigquery(df_holdings)

    # 5. 検証
    creds = service_account.Credentials.from_service_account_file(SA_KEY_PATH)
    client = bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)
    verify_bq(client)

    print("\n完了！")


if __name__ == "__main__":
    main()
