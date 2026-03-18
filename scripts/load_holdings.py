"""
保有銘柄データをGoogle Sheetsから読み込み、BigQueryにロードするスクリプト。

Google Sheets「保有銘柄」タブの列構成（現行フォーマット）:
  A(0): 商品カテゴリ  (国内株/米国株/投資信託)
  B(1): 約定日        ("2026/1/23" 形式)
  C(2): 受渡日
  D(3): 銘柄・ファンド名
  E(4): 銘柄コード・ティッカー
  F(5): 口座          (NISA成長投資枠/NISAつみたて投資枠 等)
  G(6): 売買区分      (買付/売却)
  H(7): 数量          (数値のみ)
  I(8): 単価          (数値のみ、現地通貨)

処理:
  - 買付取引のみ対象（G列 == "買付"）
  - 商品カテゴリを BigQuery スキーマ用名称に変換
  - purchase_amount = 数量 × 単価（現地通貨。為替変換はしない）
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
import google.auth
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
# GitHub Actions では GOOGLE_APPLICATION_CREDENTIALS が設定されるため ADC を使用
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SA_KEY_PATH = os.path.join(SCRIPT_DIR, "..", "sa-key.json")


def _get_credentials(scopes=None):
    """認証情報を取得する。
    ローカル: sa-key.json が存在すればそれを使用
    CI (GitHub Actions): GOOGLE_APPLICATION_CREDENTIALS 経由の ADC を使用
    """
    if os.path.exists(SA_KEY_PATH):
        if scopes:
            return service_account.Credentials.from_service_account_file(SA_KEY_PATH, scopes=scopes)
        return service_account.Credentials.from_service_account_file(SA_KEY_PATH)
    # Application Default Credentials (GitHub Actions / gcloud auth)
    creds, _ = google.auth.default(scopes=scopes)
    return creds


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
    creds = _get_credentials(scopes=SCOPES)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(WORKSHEET_NAME)
    data = ws.get(RANGE)
    print(f"  {len(data)} 行取得完了（ヘッダー含む）")
    return data


def parse_transactions(raw_data: list[list[str]]) -> pd.DataFrame:
    """生データをトランザクション DataFrame に変換（現行フォーマット対応）

    列マッピング:
      A(0): 商品カテゴリ  B(1): 約定日  C(2): 受渡日
      D(3): 銘柄名        E(4): コード  F(5): 口座
      G(6): 売買区分      H(7): 数量    I(8): 単価
    """
    if not raw_data:
        return pd.DataFrame()

    header = raw_data[0]
    rows = raw_data[1:]

    # 商品カテゴリ → BigQuery スキーマ名への変換
    CATEGORY_MAP = {
        "国内株": "国内株式",
        "米国株": "米国株式",
        "投資信託": "投資信託",
    }

    records = []
    for i, row in enumerate(rows, start=2):
        # 空行スキップ
        if not row or all(not str(c).strip() for c in row):
            continue

        # 行を9列に揃える
        row = list(row) + [""] * (9 - len(row))

        trade_type = str(row[6]).strip()   # G列: 売買区分（買付/売却）

        # 買付取引のみ処理
        if trade_type != "買付":
            continue

        # 約定日をパース（B列: "2026/1/23" 形式）
        trade_date_str = str(row[1]).strip()
        try:
            trade_date = datetime.date.fromisoformat(trade_date_str.replace("/", "-"))
        except ValueError:
            try:
                trade_date = datetime.datetime.strptime(trade_date_str, "%Y/%m/%d").date()
            except ValueError:
                print(f"  行{i}: 日付パース失敗 '{trade_date_str}'、スキップ")
                continue

        raw_category = str(row[0]).strip()         # A列: 商品カテゴリ
        product_type = CATEGORY_MAP.get(raw_category, raw_category)
        company_name = str(row[3]).strip()         # D列: 銘柄・ファンド名
        code = str(row[4]).strip() or None         # E列: 銘柄コード・ティッカー
        account_type = str(row[5]).strip()         # F列: 口座

        if not company_name:
            continue

        quantity = _parse_quantity(row[7])         # H列: 数量（"10" など）
        unit_price = _parse_amount(row[8])         # I列: 単価（"1900" など）

        # purchase_amount = 数量 × 単価（現地通貨）
        amount = (quantity * unit_price) if (quantity and unit_price) else None

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
    print(f"  買付トランザクション: {len(df)} 件")
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

    creds = _get_credentials()
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
    creds = _get_credentials()
    client = bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)
    verify_bq(client)

    print("\n完了！")


if __name__ == "__main__":
    main()
