"""外部価格取得スクリプト（米国株・投資信託の時価を BigQuery に保存）

【対象資産】
  米国株:   GOOGL, TSLA  → yfinance → USD -> JPY 換算
  投資信託: 同指数の TSE 上場 ETF を参照してパフォーマンス比から NAV を推定

【投資信託 NAV 推定方式】
  投資信託は非上場のため直接価格を取得できない。
  代わりに「同じインデックスを追う TSE 上場 ETF」の価格を使い、
  購入日以降のパフォーマンス比（ETF現在価格 / 購入日ETF価格）を計算し、
  推定 NAV = 平均購入単価 × パフォーマンス比 として近似する。
  純粋なインデックスファンドでは±1-2% 程度の精度が得られる。

【SSL 問題の回避（Windows 日本語パス対策）】
  certifi cacert.pem が日本語パスに置かれる場合、curl_cffi がエラー（curl: 77）。
  → cacert.pem を ASCII パスの一時ディレクトリにコピーして環境変数を上書き。

実行:
  .venv\\Scripts\\python scripts/fetch_external_prices.py
"""

import os
import sys
import shutil
import tempfile
import datetime

# ────────────────────────────────────────────────────────────
# [1] SSL 証明書パス問題を最初に修正（他 import より前に実行）
# ────────────────────────────────────────────────────────────

def _fix_ssl_cert_path() -> str | None:
    """日本語パスの cacert.pem を ASCII パスの一時ディレクトリにコピーする。"""
    try:
        import certifi
        cert_src = certifi.where()
        try:
            cert_src.encode("ascii")
            return cert_src
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        temp_cert = os.path.join(tempfile.gettempdir(), "cacert_stock_monitor.pem")
        if not os.path.exists(temp_cert):
            shutil.copy2(cert_src, temp_cert)
            print(f"[SSL] Cert copied to: {temp_cert}")
        else:
            print(f"[SSL] Using temp cert: {temp_cert}")
        os.environ["CURL_CA_BUNDLE"]     = temp_cert
        os.environ["REQUESTS_CA_BUNDLE"] = temp_cert
        os.environ["SSL_CERT_FILE"]      = temp_cert
        return temp_cert
    except Exception as exc:
        print(f"[SSL] Cert fix skipped: {exc}")
        return None


_SSL_CERT_PATH = _fix_ssl_cert_path()

# ────────────────────────────────────────────────────────────
# [2] 通常 import
# ────────────────────────────────────────────────────────────

import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────
# 設定
# ────────────────────────────────────────────────────────────

BQ_PROJECT  = os.environ.get("BQ_PROJECT", "onitsuka-app")
BQ_DATASET  = "analytics"
BQ_TABLE    = "external_prices"
BQ_LOCATION = "asia-northeast1"

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
SA_KEY_PATH = os.path.join(SCRIPT_DIR, "..", "sa-key.json")

# 米国株ティッカー（yfinance）
US_TICKERS = ["GOOGL", "TSLA"]

# 為替レート（yfinance）
FX_TICKER = "USDJPY=X"

# 投資信託: holdings.company_name -> TSE 上場 ETF ティッカー（同インデックス追従）
# ETF プロキシ方式: 購入日以降の ETF パフォーマンス比で NAV を推定
# ※ キーは analytics.holdings テーブルの company_name と完全一致させること
FUND_ETF_PROXY_MAP = {
    # eMAXIS Slim シリーズ（三菱UFJアセットマネジメント）
    "eMAXIS Slim 米国株式(S&P500)":                                  "2558.T",  # MAXIS 米国株式(S&P500)上場投信
    "eMAXIS Slim 全世界株式(オール・カントリー)(オルカン)":              "2559.T",  # MAXIS 全世界株式(AC)上場投信
    "eMAXIS Slim 国内株式(TOPIX)":                                   "1306.T",  # NEXT FUNDS TOPIX ETF
    "eMAXIS Slim 新興国株式インデックス":                              "1681.T",  # iShares 新興国株 ETF (TSE)
    # iFree シリーズ（大和アセットマネジメント）
    "iFree 日経225インデックス":                                      "1321.T",  # NEXT FUNDS 日経225連動型
    # 楽天シリーズ（楽天投信投資顧問）
    "楽天・全米株式インデックス・ファンド(楽天・VTI)":                  "2554.T",  # MAXIS 全世界株式(含む日本)(AC)上場投信
    "楽天・全米株式インデックス・ファンド（楽天・バンガード・ファンド（全米株式））": "2554.T",
    # SBI アセットマネジメント
    "SBIインド＆ベトナム株ファンド":                                   "1678.T",  # NEXT FUNDS インド・ニフティ50
}

# ────────────────────────────────────────────────────────────
# BigQuery クライアント（holdings データ取得用）
# ────────────────────────────────────────────────────────────

def _get_bq_client() -> bigquery.Client:
    """BigQuery クライアントを返す（ローカル: SA キー / CI: ADC）。"""
    if os.path.exists(SA_KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(SA_KEY_PATH)
        return bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)
    return bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)


def _load_fund_holdings() -> dict[str, dict]:
    """analytics.holdings から投資信託の平均購入単価と最初の購入日を取得する。

    Returns:
        {fund_name: {"avg_cost": float, "first_buy_date": datetime.date}}
    """
    client = _get_bq_client()
    sql = f"""
        SELECT
            company_name,
            SAFE_DIVIDE(SUM(purchase_amount), SUM(shares)) AS avg_cost,
            MIN(order_date)                                 AS first_buy_date
        FROM `{BQ_PROJECT}.{BQ_DATASET}.holdings`
        WHERE trade_type = '買付'
          AND product_category = '投資信託'
        GROUP BY company_name
    """
    try:
        df = client.query(sql).to_dataframe(create_bqstorage_client=False)
        result = {}
        for _, row in df.iterrows():
            result[row["company_name"]] = {
                "avg_cost":       float(row["avg_cost"]) if pd.notna(row["avg_cost"]) else None,
                "first_buy_date": row["first_buy_date"],
            }
        return result
    except Exception as exc:
        print(f"  [WARN] holdings 取得失敗: {exc}")
        return {}

# ────────────────────────────────────────────────────────────
# yfinance 用セッション（curl_cffi + ASCII SSL 証明書パス）
# ────────────────────────────────────────────────────────────

def _make_curl_session():
    """SSL 証明書付きの curl_cffi セッションを生成する（yfinance v1.x 対応）。"""
    try:
        from curl_cffi.requests import Session as CurlSession
        verify = _SSL_CERT_PATH if (_SSL_CERT_PATH and os.path.exists(_SSL_CERT_PATH)) else True
        session = CurlSession(impersonate="chrome", verify=verify)
        print(f"[SSL] curl_cffi session ready (cert: {verify})")
        return session
    except Exception as exc:
        print(f"[SSL] curl_cffi session failed: {exc}")
        return None


_CURL_SESSION = _make_curl_session()

# ────────────────────────────────────────────────────────────
# 価格取得: 米国株・FX（yfinance）
# ────────────────────────────────────────────────────────────

def _get_yf_price(ticker_sym: str) -> float | None:
    """yfinance から最新終値を取得する（失敗時は None）。"""
    try:
        t = yf.Ticker(ticker_sym, session=_CURL_SESSION) if _CURL_SESSION else yf.Ticker(ticker_sym)
        hist = t.history(period="5d")
        if hist.empty:
            print(f"  [WARN] {ticker_sym}: no data (delisted?)")
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as exc:
        print(f"  [WARN] {ticker_sym}: fetch failed ({type(exc).__name__})")
        return None


def _get_yf_price_on_date(ticker_sym: str, target_date: datetime.date) -> float | None:
    """yfinance から指定日付付近の終値を取得する（±7 営業日以内）。"""
    try:
        t = yf.Ticker(ticker_sym, session=_CURL_SESSION) if _CURL_SESSION else yf.Ticker(ticker_sym)
        start = target_date - datetime.timedelta(days=10)
        end   = target_date + datetime.timedelta(days=5)
        hist = t.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        if hist.empty:
            return None
        # 指定日以降の最初の取引日の価格を返す
        hist.index = pd.to_datetime(hist.index).tz_localize(None)
        target_dt = pd.Timestamp(target_date)
        after = hist[hist.index >= target_dt]
        if after.empty:
            return float(hist["Close"].iloc[-1])  # 最後の利用可能な価格
        return float(after["Close"].iloc[0])
    except Exception as exc:
        print(f"  [WARN] {ticker_sym} on {target_date}: fetch failed ({type(exc).__name__})")
        return None

# ────────────────────────────────────────────────────────────
# 価格取得: 投資信託（ETF プロキシ方式）
# ────────────────────────────────────────────────────────────

def _estimate_fund_nav(
    fund_name: str,
    proxy_ticker: str,
    avg_cost: float,
    first_buy_date: datetime.date,
) -> float | None:
    """ETF プロキシを使って投資信託の現在 NAV を推定する。

    推定式: current_NAV ≈ avg_cost × (ETF_current / ETF_on_first_buy_date)
    - 同じインデックスを追う ETF の累積リターンを fund NAV に適用
    - 純粋なインデックスファンドでは精度 ±2% 程度

    Args:
        fund_name:      ファンド名（ログ出力用）
        proxy_ticker:   TSE 上場 ETF ティッカー（例: "2558.T"）
        avg_cost:       平均購入単価（JPY/unit）
        first_buy_date: 最初の購入日

    Returns:
        推定 NAV (float) または None
    """
    if avg_cost is None or avg_cost <= 0:
        return None

    # ETF の購入日付近の価格を取得
    etf_at_purchase = _get_yf_price_on_date(proxy_ticker, first_buy_date)
    if etf_at_purchase is None or etf_at_purchase == 0:
        print(f"  [WARN] {proxy_ticker}: no price data at {first_buy_date}")
        return None

    # ETF の現在価格を取得
    etf_current = _get_yf_price(proxy_ticker)
    if etf_current is None or etf_current == 0:
        print(f"  [WARN] {proxy_ticker}: no current price data")
        return None

    performance_ratio = etf_current / etf_at_purchase
    estimated_nav = round(avg_cost * performance_ratio, 1)

    display_name = fund_name[:25] if len(fund_name) > 25 else fund_name
    print(f"  {display_name}: avg_cost={avg_cost:,.0f} "
          f"* ETF({etf_at_purchase:,.0f}->{etf_current:,.0f} "
          f"x{performance_ratio:.3f}) = NAV {estimated_nav:,.0f}")

    return estimated_nav

# ────────────────────────────────────────────────────────────
# メイン取得ロジック
# ────────────────────────────────────────────────────────────

def fetch_prices() -> list[dict]:
    """米国株・投資信託の最新価格を取得してレコードリストを返す。"""
    today = datetime.date.today()
    records = []

    # ─── 1. USD/JPY レート ───────────────────────────────
    print("Getting USD/JPY rate...")
    usdjpy = _get_yf_price(FX_TICKER)
    if usdjpy is None:
        print("  [ERROR] USD/JPY fetch failed.")
    else:
        print(f"  USD/JPY = {usdjpy:.2f}")

    # ─── 2. 米国株（USD -> JPY 換算）────────────────────
    print("\nFetching US stocks...")
    for sym in US_TICKERS:
        price_usd = _get_yf_price(sym)
        if price_usd is None:
            continue
        price_jpy = round(price_usd * usdjpy, 0) if usdjpy else None
        if price_jpy:
            print(f"  {sym}: USD {price_usd:,.2f} -> JPY {price_jpy:,.0f}")
        else:
            print(f"  {sym}: USD {price_usd:,.2f} (no JPY rate)")
        records.append({
            "fetch_date":      today,
            "ticker":          sym,
            "asset_name":      sym,
            "asset_type":      "米国株",
            "price_original":  round(price_usd, 4),
            "currency":        "USD",
            "usdjpy_rate":     round(usdjpy, 4) if usdjpy else None,
            "price_jpy":       price_jpy,
        })

    # ─── 3. 投資信託（ETF プロキシ方式）────────────────
    print("\nFetching investment trust NAV (ETF proxy method)...")
    print("Loading fund holdings data from BigQuery...")
    fund_holdings = _load_fund_holdings()
    print(f"  {len(fund_holdings)} funds found in holdings")

    for fund_name, proxy_ticker in FUND_ETF_PROXY_MAP.items():
        if fund_name not in fund_holdings:
            print(f"  [SKIP] Not in holdings: {fund_name[:30]}")
            continue

        holding_data = fund_holdings[fund_name]
        avg_cost      = holding_data["avg_cost"]
        first_buy     = holding_data["first_buy_date"]

        if isinstance(first_buy, pd.Timestamp):
            first_buy = first_buy.date()
        elif hasattr(first_buy, 'date'):
            first_buy = first_buy.date()

        nav = _estimate_fund_nav(fund_name, proxy_ticker, avg_cost, first_buy)

        records.append({
            "fetch_date":      today,
            "ticker":          proxy_ticker,
            "asset_name":      fund_name,
            "asset_type":      "投資信託",
            "price_original":  nav,
            "currency":        "JPY",
            "usdjpy_rate":     None,
            "price_jpy":       nav,
        })

    print(f"\nTotal {len(records)} records generated.")
    return records

# ────────────────────────────────────────────────────────────
# BigQuery ロード
# ────────────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("fetch_date",     "DATE",    description="取得日"),
    bigquery.SchemaField("ticker",         "STRING",  description="ティッカー/ETFコード"),
    bigquery.SchemaField("asset_name",     "STRING",  description="資産名"),
    bigquery.SchemaField("asset_type",     "STRING",  description="資産種別（米国株/投資信託）"),
    bigquery.SchemaField("price_original", "FLOAT64", description="元通貨建て価格 / 推定NAV"),
    bigquery.SchemaField("currency",       "STRING",  description="元通貨（USD/JPY）"),
    bigquery.SchemaField("usdjpy_rate",    "FLOAT64", description="USD/JPY レート"),
    bigquery.SchemaField("price_jpy",      "FLOAT64", description="JPY 換算価格 / 推定NAV"),
]


def load_to_bq(records: list[dict]) -> None:
    """レコードを BigQuery の external_prices テーブルに WRITE_TRUNCATE でロード。"""
    client = _get_bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    df = pd.DataFrame(records)
    df["fetch_date"] = pd.to_datetime(df["fetch_date"]).dt.date

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"\nLoading to BigQuery: {table_id}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"  Loaded: {table.num_rows} rows")

    print("\n--- Load result ---")
    verify_df = client.query(
        f"SELECT fetch_date, asset_type, asset_name, price_jpy "
        f"FROM `{table_id}` ORDER BY asset_type, asset_name"
    ).to_dataframe(create_bqstorage_client=False)
    print(verify_df.to_string(index=False))


def main():
    print("=" * 60)
    print("External Price Fetch Script")
    print(f"Date: {datetime.date.today()}")
    print("=" * 60)

    records = fetch_prices()

    if not records:
        print("\n[ERROR] No price data retrieved.")
        sys.exit(1)

    load_to_bq(records)
    print("\nDone!")


if __name__ == "__main__":
    main()
