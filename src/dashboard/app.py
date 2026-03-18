"""株式モニタリングダッシュボード

BigQuery Analytics Layer のデータをリアルタイムで可視化する Streamlit アプリ。

実行方法:
  streamlit run src/dashboard/app.py

環境変数（.env から自動ロード）:
  GOOGLE_APPLICATION_CREDENTIALS : サービスアカウントキーのパス
  BQ_PROJECT                      : BigQuery プロジェクト ID（デフォルト: onitsuka-app）
"""
import os
import sys
from datetime import datetime, date

# .env 読み込み
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from google.cloud import bigquery
from google.oauth2 import service_account

# ─────────────────────────────────────────
# 定数
# ─────────────────────────────────────────
BQ_PROJECT  = os.environ.get("BQ_PROJECT", "onitsuka-app")
BQ_LOCATION = "asia-northeast1"
CACHE_TTL   = 1800  # 30分キャッシュ

# ─────────────────────────────────────────
# ページ設定（最初に呼ぶ）
# ─────────────────────────────────────────
st.set_page_config(
    page_title="株式モニタリング | FIRE目標 ¥1億円",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "窪田フレームワーク × 成長株分析 ダッシュボード",
    },
)

# ─────────────────────────────────────────
# グローバルCSS
# ─────────────────────────────────────────
st.markdown("""
<style>
/* ─── 全体 ─── */
[data-testid="stSidebar"] {background: #1a1b26;}
.block-container {padding-top: 1rem;}

/* ─── フェーズバッジ ─── */
.badge-bull    {background:#a6e3a1;color:#1e1e2e;padding:3px 14px;border-radius:20px;font-weight:700;font-size:1em;}
.badge-bear    {background:#f38ba8;color:#1e1e2e;padding:3px 14px;border-radius:20px;font-weight:700;font-size:1em;}
.badge-neutral {background:#fab387;color:#1e1e2e;padding:3px 14px;border-radius:20px;font-weight:700;font-size:1em;}

/* ─── シグナルバッジ ─── */
.sig-entry {background:#a6e3a1;color:#1e1e2e;padding:2px 8px;border-radius:4px;font-size:.85em;font-weight:700;}
.sig-watch {background:#f9e2af;color:#1e1e2e;padding:2px 8px;border-radius:4px;font-size:.85em;}

/* ─── KPI カード ─── */
.kpi-box {
    background:linear-gradient(135deg,#1e1e2e,#2a2b3d);
    border:1px solid #45475a;
    border-radius:12px;
    padding:16px 20px;
    text-align:center;
}
.kpi-label {font-size:.8em;color:#a6adc8;margin-bottom:4px;}
.kpi-value {font-size:1.6em;font-weight:700;color:#cdd6f4;}
.kpi-delta {font-size:.75em;margin-top:2px;}

/* ─── FIRE プログレスバー ─── */
.fire-track {
    background:#313244;border-radius:8px;height:18px;overflow:hidden;margin:6px 0;
}
.fire-fill {
    height:100%;border-radius:8px;
    background:linear-gradient(90deg,#f38ba8,#fab387,#a6e3a1);
    transition:width .5s ease;
}

/* ─── 判定バナー ─── */
.verdict-buy {
    background: linear-gradient(135deg, #1e3a1e, #2d5a2d);
    border: 2px solid #a6e3a1;
    border-radius: 16px;
    padding: 20px 24px;
    text-align: center;
    font-size: 1.4em;
    font-weight: 700;
    color: #a6e3a1;
    margin-bottom: 16px;
}
.verdict-watch {
    background: linear-gradient(135deg, #3a3a1e, #5a5a2d);
    border: 2px solid #f9e2af;
    border-radius: 16px;
    padding: 20px 24px;
    text-align: center;
    font-size: 1.4em;
    font-weight: 700;
    color: #f9e2af;
    margin-bottom: 16px;
}
.verdict-hold {
    background: linear-gradient(135deg, #2a2b3d, #313244);
    border: 2px solid #45475a;
    border-radius: 16px;
    padding: 20px 24px;
    text-align: center;
    font-size: 1.4em;
    font-weight: 700;
    color: #a6adc8;
    margin-bottom: 16px;
}
.entry-card {
    background: linear-gradient(135deg, #1e3a1e, #2d5a2d);
    border: 2px solid #a6e3a1;
    border-radius: 12px;
    padding: 14px;
    margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# BigQuery クライアント（キャッシュ）
# ─────────────────────────────────────────
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """BigQuery クライアントを初期化する。

    認証方法の優先順:
      1. Streamlit Cloud secrets (gcp_service_account セクション)
      2. GOOGLE_APPLICATION_CREDENTIALS 環境変数が指すキーファイル
      3. Application Default Credentials (Cloud Run / GitHub Actions / gcloud auth)
    """
    scopes = ["https://www.googleapis.com/auth/bigquery"]

    # ① Streamlit Cloud secrets 対応（secrets.toml が存在しない場合は無視）
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]),
                scopes=scopes,
            )
            return bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)
    except Exception:
        pass  # secrets.toml 未設定時（ローカル環境）はスキップ

    # ② ローカル: サービスアカウントキーファイル
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "sa-key.json")
    if os.path.exists(creds_path):
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=scopes,
        )
        return bigquery.Client(project=BQ_PROJECT, credentials=creds, location=BQ_LOCATION)

    # ③ ADC (Cloud Run / GitHub Actions)
    return bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)


def _bq(sql: str) -> pd.DataFrame:
    """BQ クエリを実行して DataFrame を返す（例外は呼び出し側で処理）"""
    client = get_bq_client()
    return client.query(sql).to_dataframe(create_bqstorage_client=False)


# ─────────────────────────────────────────
# データ取得関数（各関数で独立キャッシュ）
# ─────────────────────────────────────────
@st.cache_data(ttl=CACHE_TTL)
def load_market_env() -> pd.DataFrame:
    return _bq("""
        SELECT date, topix_close, topix_ma200, market_phase, environment_score
        FROM `onitsuka-app.analytics.market_environment`
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_screening() -> pd.DataFrame:
    return _bq("""
        SELECT
          code, company_name, sector33_name,
          latest_close, avg_turnover_20d_oku, liquidity_grade,
          volatility_score, chart_score, kubota_trade_score,
          sales_cagr_3y_pct, op_cagr_3y_pct, roe_pct, roic_pct,
          growth_invest_score,
          per, pbr,
          market_phase, kubota_signal, signal_confidence, screening_status,
          next_earnings_date, days_to_earnings,
          atr_pct, ma200_trend, price_vs_ma200, consolidation, volume_surge, volume_ratio,
          hv_contraction, price_strength_score
        FROM `onitsuka-app.analytics.integrated_score`
        WHERE screening_status = 'ACTIVE'
        ORDER BY kubota_trade_score DESC, growth_invest_score DESC
        LIMIT 300
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_backtest() -> pd.DataFrame:
    return _bq("""
        SELECT
          signal_date, code,
          atr_pct, hv_20d_pct, hv_60d_pct, range_contraction,
          return_5d_pct, return_10d_pct, return_20d_pct,
          win_5d, win_10d, win_20d
        FROM `onitsuka-app.analytics.backtest_signals`
        WHERE signal_date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 365 DAY)
        ORDER BY signal_date DESC
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_topix_history() -> pd.DataFrame:
    return _bq("""
        SELECT date, open, high, low, close
        FROM `onitsuka-app.stock_raw.topix_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 500 DAY)
        ORDER BY date ASC
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_price_history(code: str) -> pd.DataFrame:
    # daily_quotesは5桁コード形式（例: "16620"）
    # holdingsは4桁コード（例: "1662"）→ 末尾に"0"を付加して検索
    bq_code = code + "0" if len(code) == 4 and code.isdigit() else code
    return _bq(f"""
        SELECT date, open, high, low, close, volume, turnover_value
        FROM `onitsuka-app.stock_raw.daily_quotes`
        WHERE code = '{bq_code}'
          AND date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 400 DAY)
        ORDER BY date ASC
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_holdings() -> pd.DataFrame:
    """保有銘柄をBQから取得（holdings_summary ビュー使用）"""
    return _bq("""
        SELECT
          product_category,
          company_name,
          code,
          account,
          total_shares    AS shares,
          avg_cost_per_share AS purchase_price,
          total_cost      AS purchase_amount,
          first_buy_date,
          last_buy_date,
          latest_close,
          kubota_signal,
          kubota_trade_score,
          growth_invest_score,
          next_earnings_date,
          days_to_earnings,
          price_strength_score,
          current_value,
          unrealized_pnl,
          return_pct
        FROM `onitsuka-app.analytics.holdings_summary`
        ORDER BY product_category, code
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_score_history(code: str) -> pd.DataFrame:
    try:
        return _bq(f"""
            SELECT snapshot_date, kubota_trade_score, growth_invest_score, kubota_signal, latest_close
            FROM `onitsuka-app.analytics.score_history`
            WHERE code = '{code}'
            ORDER BY snapshot_date ASC
        """)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────
# ヘルパー関数
# ─────────────────────────────────────────
def _safe_num(val, default=None):
    """安全なfloat変換。変換失敗やNaNはdefaultを返す"""
    try:
        v = float(val)
        return v if pd.notna(v) else default
    except (TypeError, ValueError):
        return default


def phase_html(phase: str) -> str:
    cls = {"BULL": "badge-bull", "BEAR": "badge-bear"}.get(phase, "badge-neutral")
    label = {"BULL": "🐂 BULL（強気）", "BEAR": "🐻 BEAR（弱気）"}.get(phase, "🟡 NEUTRAL（中立）")
    return f'<span class="{cls}">{label}</span>'


def _score_color(val, max_val: float, hue: int = 120) -> str:
    """スコアを背景色 hsl に変換（テキストは常に暗色で視認性確保）"""
    pct = min(float(val) / max_val, 1.0) if max_val else 0
    sat = int(pct * 65)
    lig = int(90 - pct * 22)  # 68%〜90%（常に明るい背景）
    # 背景が明るい(lig>60%)ため文字は常にダーク。スコア0は無色扱い
    text_color = "#1e1e2e" if sat > 0 else "#6c7086"
    return (
        f"background-color: hsl({hue},{sat}%,{lig}%);"
        f" color: {text_color};"
        f" font-weight: {'700' if pct >= 0.6 else '400'};"
    )


def _style_screening(df: pd.DataFrame) -> pd.DataFrame:
    """スクリーニング結果テーブルのスタイリング"""
    styles = pd.DataFrame("", index=df.index, columns=df.columns)

    if "窪田S" in df.columns:
        styles["窪田S"] = df["窪田S"].apply(
            lambda v: _score_color(v, 10, 120) if pd.notna(v) and v != "" else ""
        )
    if "成長株S" in df.columns:
        styles["成長株S"] = df["成長株S"].apply(
            lambda v: _score_color(v, 29, 210) if pd.notna(v) and v != "" else ""
        )
    if "シグナル" in df.columns:
        styles["シグナル"] = df["シグナル"].apply(
            lambda v: "background-color:#a6e3a1;font-weight:700;color:#1e1e2e" if v == "買いシグナル"
            else ("background-color:#f9e2af;color:#1e1e2e" if "放れ待ち" in str(v) else "")
        )
    if "決算(日)" in df.columns:
        styles["決算(日)"] = df["決算(日)"].apply(
            lambda v: "background-color:#f38ba8;font-weight:700;color:#1e1e2e" if pd.notna(v) and str(v) not in ("", "nan") and float(v) <= 10
            else ("background-color:#fab387;color:#1e1e2e" if pd.notna(v) and str(v) not in ("", "nan") and float(v) <= 20 else "")
        )
    return styles


def _candlestick_fig(
    df: pd.DataFrame,
    title: str,
    show_ma: bool = True,
    purchase_px: float | None = None,
) -> go.Figure:
    """ローソク足チャートを生成"""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # ── 取得単価との差分を customdata に格納（Python側で整形済み文字列として渡す）──
    if purchase_px is not None:
        df["_diff"]     = df["close"] - purchase_px
        df["_diff_pct"] = (df["close"] - purchase_px) / purchase_px * 100
        # Plotly の Candlestick は customdata のフォーマット指定を無視するため
        # Python 側で先に文字列にフォーマットして渡す
        df["_diff_str"]     = df["_diff"].map(lambda v: f"{v:+,.0f}")
        df["_diff_pct_str"] = df["_diff_pct"].map(lambda v: f"{v:+.1f}")
        customdata  = df[["_diff_str", "_diff_pct_str"]].values
        diff_line   = (
            f"取得単価: ¥{purchase_px:,.0f}<br>"
            "差分: ¥%{customdata[0]}　(%{customdata[1]}%)"
        )
    else:
        customdata = None
        diff_line  = ""

    hover_tmpl = (
        "<b>%{x|%Y年%m月%d日}</b><br>"
        "始値: ¥%{open:,.0f}<br>"
        "高値: ¥%{high:,.0f}<br>"
        "安値: ¥%{low:,.0f}<br>"
        "終値: ¥%{close:,.0f}<br>"
        + diff_line
        + "<extra></extra>"
    )

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.75, 0.25],
        shared_xaxes=True,
        vertical_spacing=0.03,
    )

    # ローソク足
    candle = go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        name="株価",
        increasing_line_color="#a6e3a1",
        decreasing_line_color="#f38ba8",
        increasing_fillcolor="#a6e3a1",
        decreasing_fillcolor="#f38ba8",
        hovertemplate=hover_tmpl,
    )
    if customdata is not None:
        candle.customdata = customdata
    fig.add_trace(candle, row=1, col=1)

    # 移動平均線
    if show_ma and len(df) > 25:
        for n, color in [(25, "#74c7ec"), (75, "#fab387"), (200, "#f9e2af")]:
            ma = df["close"].rolling(n).mean()
            if ma.notna().any():
                fig.add_trace(go.Scatter(
                    x=df["date"], y=ma,
                    mode="lines", name=f"MA{n}",
                    line=dict(color=color, width=1.5),
                    opacity=0.85,
                    hovertemplate=f"MA{n}: ¥%{{y:,.0f}}<extra></extra>",
                ), row=1, col=1)

    # 出来高バー
    if "volume" in df.columns:
        bar_colors = [
            "#a6e3a1" if c >= o else "#f38ba8"
            for c, o in zip(df["close"], df["open"])
        ]
        fig.add_trace(go.Bar(
            x=df["date"], y=df["volume"],
            name="出来高", marker_color=bar_colors, opacity=0.7,
            hovertemplate="%{x|%Y年%m月%d日}<br>出来高: %{y:,.0f}株<extra></extra>",
        ), row=2, col=1)

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        height=520,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.02, bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=60, r=20, t=50, b=40),
    )
    fig.update_xaxes(
        gridcolor="#313244", showgrid=True,
        rangebreaks=[dict(bounds=["sat", "mon"])],
        tickformat="%Y年%m月%d日",
    )
    # 価格軸（row=1）: 円表示・小数なし
    fig.update_yaxes(gridcolor="#313244", showgrid=True)
    fig.update_yaxes(tickformat=",.0f", ticksuffix="円", row=1, col=1)
    return fig


# ─────────────────────────────────────────
# 用語解説・買い売り判定パネル
# ─────────────────────────────────────────

GLOSSARY = {
    "窪田スコア（0〜10点）": (
        "「銘柄選びの教科書」著者・窪田剛氏のフレームワークに基づくトレード適性スコア。"
        "ボラティリティ評価（5点）＋チャート評価（5点）の合計で0〜10点。"
        "【ボラティリティ 5点満点】ATR≥1.5%（値動き十分）:+2 / HV収縮（HV20<HV60）:+2 / 1ヶ月レンジ幅15〜80%:+1。"
        "【チャート 5点満点】MA200上向き:+1 / 株価がMA200上方:+1 / もみ合い収縮（10日値幅/30日値幅<50%）:+2 / 出来高急増（20日平均比1.5倍超）:+1。"
        "7点以上が買いを検討できる目安。"
    ),
    "成長株スコア（0〜29点）": (
        "企業の成長力・収益性・割安度を数値化した投資適性スコア。"
        "売上CAGR3年（最高8点）:≥15%→8/≥10%→6/≥5%→3。"
        "営業利益CAGR3年（最高8点）:≥20%→8/≥10%→6/≥5%→3。"
        "ROE（最高7点）:≥15%→7/≥10%→5/≥8%→3。"
        "ROIC（最高6点）:≥12%→6/≥8%→4/≥5%→2。"
        "PER割安度（最高5点）:セクター平均の80%未満→5/平均以下→3/1.3倍以下→1。"
        "PBR（最高5点）:PBR<1.0→5/PBR<1.5→3。"
        "15点以上が優良成長株の目安。"
    ),
    "買いシグナル（今すぐ買いを検討）": "5つの条件（相場上昇・ATR≥1.5%・HV収縮・レンジ収縮・出来高急増）がすべて揃った銘柄。積極的なエントリーを検討できる状態。",
    "放れ待ち": "コンソリデーション（株価が一定レンジに収まった煮詰まり状態）に入っている銘柄。ブレイクアウト（価格が放れるタイミング）を待つ状態。",
    "ATR（平均真値幅）": "過去14日間の平均的な1日の値動き幅。ATR%が1.5%以上あると「十分な値動き」と判定。小さすぎると利益が取りにくい。",
    "HV収縮（ボラティリティ収縮）": "過去20日のボラティリティ（HV20）が過去60日（HV60）より小さい状態。株価が静まり返っている時期で、ブレイクアウト前の特徴。",
    "MA200（200日移動平均線）": "過去200営業日の終値の平均値。長期トレンドの目安。株価がMA200の上方にあり、MA200が右肩上がりであれば長期上昇トレンド。",
    "売買代金（億円）": "1日に取引された金額の20日平均。10億円以上あると十分な流動性（売買のしやすさ）があると判断。",
    "ROE（自己資本利益率）": "会社が株主から預かったお金（自己資本）をどれだけ効率よく使って利益を出しているか。15%以上が優良。",
    "ROIC（投下資本利益率）": "事業に投下した資金全体（借金+自己資本）に対してどれだけ利益を生んでいるか。12%以上が目安。",
    "売上CAGR（3年平均成長率）": "3年間の年平均売上成長率。15%以上あると高成長企業と判断。",
    "PER（株価収益率）": "株価÷1株当たり利益。同業他社比で割安かどうかの目安。低いほど割安とされるが成長株は高めになりやすい。",
    "PBR（株価純資産倍率）": "株価÷1株純資産。1倍割れは「解散価値以下」とされ割安の目安。",
    "相場フェーズ（BULL／BEAR／NEUTRAL）": "BULL=強気相場（買いに有利）、BEAR=弱気相場（買い控えを推奨）、NEUTRAL=中立。TOPIXのMA200トレンドから判定。",
}


def _glossary_expander():
    with st.expander("📖 用語集・指標の見方（わからない言葉はここで確認）", expanded=False):
        cols = st.columns(2)
        items = list(GLOSSARY.items())
        half = (len(items) + 1) // 2
        for i, (term, desc) in enumerate(items):
            col = cols[0] if i < half else cols[1]
            with col:
                st.markdown(f"**{term}**")
                st.caption(desc)
                st.markdown("")


def _buy_sell_panel(stock: pd.Series, market_phase: str = ""):
    """銘柄の買い時・売り時判定パネル（リデザイン版）"""
    signal = str(stock.get("kubota_signal", "-"))
    days_to_earn = _safe_num(stock.get("days_to_earnings"))

    # ── a) LARGE VERDICT BANNER ──
    if signal == "買いシグナル":
        st.markdown("""
        <div class="verdict-buy">
            🟢 買いシグナル<br>
            <span style="font-size:.75em;font-weight:400;">今すぐ買いを検討できます</span>
        </div>""", unsafe_allow_html=True)
    elif "WATCH" in signal:
        st.markdown("""
        <div class="verdict-watch">
            🟡 放れ待ち<br>
            <span style="font-size:.75em;font-weight:400;">放れ待ち — もう少し様子を見ましょう</span>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="verdict-hold">
            ⚪ 見送り<br>
            <span style="font-size:.75em;font-weight:400;">まだ条件が揃っていません</span>
        </div>""", unsafe_allow_html=True)

    # ── b) EARNINGS ALERT ──
    if days_to_earn is not None and days_to_earn <= 20:
        color = "#f38ba8" if days_to_earn <= 10 else "#fab387"
        earn_int = int(days_to_earn)
        st.markdown(
            f'<div style="background:{color};color:#1e1e2e;border-radius:8px;padding:8px 14px;'
            f'font-weight:700;margin-bottom:10px;">⚠️ 決算発表まで約{earn_int}日 '
            f'― 決算またぎは値動きが大きくなるため注意</div>',
            unsafe_allow_html=True,
        )

    # ── c) 取引シミュレーション ──
    close = _safe_num(stock.get("latest_close"))
    atr_pct_val = _safe_num(stock.get("atr_pct"))
    stop_loss = None
    target = None
    atr_val = None
    if signal == "買いシグナル" and close is not None and atr_pct_val is not None:
        atr_val = close * (atr_pct_val / 100)
        stop_loss = round(close - 2 * atr_val)
        target = round(close + 3 * atr_val)

        st.markdown("#### 💡 取引シミュレーション")
        sim_c1, sim_c2, sim_c3 = st.columns(3)
        sim_c1.metric("エントリー目安", f"¥{close:,.0f}")
        sim_c2.metric("損切りライン", f"¥{stop_loss:,.0f}", delta="-ATR×2", delta_color="inverse")
        sim_c3.metric("利確目安", f"¥{target:,.0f}", delta="+ATR×3")
        st.caption("リスクリワード比 1:1.5（損切り額の1.5倍の利益を狙う設定）")

    # ── d) 売り時のサイン ──
    with st.expander("📉 売り時のサイン（エグジット戦略）", expanded=False):
        st.markdown("以下の状況が起きたら売りを検討してください")
        if stop_loss is not None:
            st.markdown(f"❗ 損切りライン（¥{stop_loss:,.0f}）を終値で下回った")
        else:
            st.markdown("❗ 損切りライン（ATR×2）を終値で下回った")
        st.markdown("❗ MA200（200日平均線）を終値で下回った")
        if target is not None:
            st.markdown(f"✨ 利確目安（¥{target:,.0f}）に到達した")
        else:
            st.markdown("✨ 利確目安（ATR×3）に到達した")
        st.markdown("✨ 出来高を伴わずに株価だけが急上昇した")

    # ── e) 買い条件チェックリスト ──
    st.markdown("#### 📋 買い条件チェックリスト")

    def check(cond: bool, ok_msg: str, ng_msg: str, warn: bool = False):
        if cond:
            st.markdown(f"✅ &nbsp; {ok_msg}", unsafe_allow_html=True)
        elif warn:
            st.markdown(f"⚠️ &nbsp; {ng_msg}", unsafe_allow_html=True)
        else:
            st.markdown(f"❌ &nbsp; {ng_msg}", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**相場・チャート条件**")
        check(market_phase == "BULL",
              "相場全体が上昇中（TOPIX の長期トレンドが右肩上がり）",
              f"相場全体が上昇中（現在: {market_phase if market_phase else '不明'}）")
        check(str(stock.get("ma200_trend", "")) == "UP",
              "この株の長期トレンドが上向き（MA200が右肩上がり）",
              "この株の長期トレンドが上向き（MA200が横ばい or 下向き）")
        check(str(stock.get("price_vs_ma200", "")) == "ABOVE",
              "株価が長期平均より上にある（MA200の上方）",
              "株価が長期平均より上にある（MA200の下方）")
        check(bool(stock.get("consolidation", False)),
              "株価が落ち着いた状態（煮詰まり・ブレイクアウト前）",
              "株価が落ち着いた状態（まだレンジ収縮していない）")
        check(bool(stock.get("volume_surge", False)),
              "出来高が急増している（多くの投資家が注目）",
              "出来高が急増している（まだ平均的な出来高）")

    with col2:
        st.markdown("**企業・財務条件**")
        liq = str(stock.get("liquidity_grade", ""))
        check(liq in ("PASS_A", "PASS_B", "PASS_C"),
              f"売買しやすい（1日の取引金額が十分 / {liq}）",
              "売買しやすい（流動性不足 — 売買しにくい可能性あり）")
        sales_cagr = _safe_num(stock.get("sales_cagr_3y_pct"))
        check(sales_cagr is not None and sales_cagr >= 5,
              f"売上が成長している（3年で年5%以上 / {sales_cagr:.1f}%）" if sales_cagr is not None else "売上が成長している（3年で年5%以上）",
              f"売上が成長している（{sales_cagr:.1f}%）" if sales_cagr is not None else "売上が成長している（データなし）")
        roe = _safe_num(stock.get("roe_pct"))
        check(roe is not None and roe >= 10,
              f"資本効率が良い（ROE 10%以上 / {roe:.1f}%）" if roe is not None else "資本効率が良い（ROE 10%以上）",
              f"資本効率が良い（ROE {roe:.1f}%）" if roe is not None else "資本効率が良い（データなし）")
        check(str(stock.get("financial_health", "")) == "PASS",
              "財務が健全（自己資本比率・営業利益が基準内）",
              "財務が健全（財務に注意あり）")
        earn_int_val = int(days_to_earn) if days_to_earn is not None else None
        check(
            earn_int_val is None or earn_int_val > 20,
            f"決算発表まで余裕がある（{earn_int_val}日以上）" if earn_int_val is not None else "決算発表まで余裕がある（決算日不明）",
            f"決算発表まで余裕がある（⚠ 決算が近い: {earn_int_val}日後）" if earn_int_val is not None else "決算発表まで余裕がある（決算日不明）",
            warn=earn_int_val is not None and earn_int_val <= 20,
        )

    # ── f) スコアサマリー ──
    st.markdown("#### 📊 スコアサマリー")
    ksc = _safe_num(stock.get("kubota_trade_score"))
    gsc = _safe_num(stock.get("growth_invest_score"))
    c1, c2 = st.columns(2)
    with c1:
        st.metric(
            "窪田スコア",
            f"{ksc:.0f} / 10" if ksc is not None else "N/A",
            help="チャート形状・ボラティリティの総合点。7点以上が狙い目。",
        )
        bar_k = int(ksc / 10 * 100) if ksc is not None else 0
        st.markdown(
            f'<div style="background:#313244;border-radius:6px;height:10px;">'
            f'<div style="width:{bar_k}%;height:100%;border-radius:6px;background:#a6e3a1;"></div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.metric(
            "成長株スコア",
            f"{gsc:.0f} / 29" if gsc is not None else "N/A",
            help="売上CAGR・ROE・ROIC・PER・PBRの総合点。15点以上が狙い目。",
        )
        bar_g = int(gsc / 29 * 100) if gsc is not None else 0
        st.markdown(
            f'<div style="background:#313244;border-radius:6px;height:10px;">'
            f'<div style="width:{bar_g}%;height:100%;border-radius:6px;background:#74c7ec;"></div></div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────
# 保有銘柄タブ
# ─────────────────────────────────────────

def _holding_action(row) -> str:
    """保有銘柄ごとのアクション提案を返す"""
    return_pct       = _safe_num(row.get("return_pct"))
    days_to_earnings = _safe_num(row.get("days_to_earnings"))
    kubota_signal    = str(row.get("kubota_signal", ""))

    if return_pct is not None and return_pct < -8:
        return "🛑 損切り検討"
    if days_to_earnings is not None and days_to_earnings <= 14:
        return "⚠️ 決算前注意"
    if return_pct is not None and return_pct >= 20:
        return "💰 利確検討"
    if kubota_signal == "買いシグナル":
        return "✅ 保有継続"
    return "📊 様子見"


def render_tab_holdings(df_holdings: pd.DataFrame):
    """Tab 0: 💼 保有銘柄管理"""
    st.subheader("💼 保有銘柄管理")

    if df_holdings.empty:
        st.info("保有銘柄データがありません。スプレッドシートの「保有銘柄」タブにデータを入力してください。")
        return

    # ─── None値の補完（米国株・投資信託はJ-Quants対象外）───
    def _fill_non_jp(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        cat_col = "product_category"
        if cat_col not in df.columns:
            return df
        non_jp_mask = ~df[cat_col].str.contains("国内", na=False)
        if "kubota_signal" in df.columns:
            df.loc[non_jp_mask, "kubota_signal"] = "ー（対象外）"
        return df
    df_holdings = _fill_non_jp(df_holdings)

    # 評価額が取得できた全銘柄で損益集計（国内株・米国株・投資信託すべて対象）
    df_jp = df_holdings[df_holdings["current_value"].notna()] \
        if "current_value" in df_holdings.columns else df_holdings

    # ─── ポートフォリオサマリー ───
    st.markdown("### ポートフォリオサマリー")
    st.caption("※ 評価額・損益は国内株・米国株・投資信託を合算。投資信託は ETF プロキシ方式による推定値（±2% 誤差）。")

    total_value = df_jp["current_value"].sum() if "current_value" in df_jp.columns else 0
    total_pnl   = df_jp["unrealized_pnl"].sum() if "unrealized_pnl" in df_jp.columns else 0
    total_cost  = df_jp["purchase_amount"].sum() if "purchase_amount" in df_jp.columns else 0
    avg_return  = (
        df_jp["return_pct"].mean()
        if "return_pct" in df_jp.columns and df_jp["return_pct"].notna().any()
        else None
    )
    holding_count = len(df_holdings)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        val_disp = f"¥{int(total_value):,}" if pd.notna(total_value) and total_value != 0 else "N/A"
        st.metric("評価総額", val_disp, help="国内株・米国株・投資信託すべての評価額合計")
    with c2:
        pnl_notna = pd.notna(total_pnl)
        pnl_disp  = f"¥{int(total_pnl):,}" if pnl_notna else "N/A"
        delta_str = f"{'+' if total_pnl >= 0 else ''}{int(total_pnl):,}円" if pnl_notna else None
        st.metric(
            "含み損益",
            pnl_disp,
            delta=delta_str,
            delta_color="normal" if (pnl_notna and total_pnl >= 0) else "inverse",
        )
    with c3:
        avg_ret_disp = f"{avg_return:+.1f}%" if avg_return is not None else "N/A"
        st.metric("平均リターン", avg_ret_disp, help="評価額がある全保有銘柄の平均含み損益率")
    with c4:
        st.metric("保有銘柄数", f"{holding_count} 件")

    # ─── カテゴリ別実績 ───
    st.markdown("#### カテゴリ別実績")
    CATEGORIES = [
        ("国内株",  "🇯🇵", "国内株",  "#4f9cf9"),
        ("米国株",  "🇺🇸", "米国株",  "#a78bfa"),
        ("投資信託", "📈", "投資信託", "#34d399"),
    ]

    # カテゴリ別集計
    cat_data = []
    for cat_key, cat_icon, cat_name, accent in CATEGORIES:
        if "product_category" in df_jp.columns:
            df_cat = df_jp[df_jp["product_category"].str.contains(cat_key, na=False)]
        else:
            df_cat = pd.DataFrame()

        cat_value = df_cat["current_value"].sum()  if not df_cat.empty and "current_value"  in df_cat.columns else 0
        cat_pnl   = df_cat["unrealized_pnl"].sum() if not df_cat.empty and "unrealized_pnl" in df_cat.columns else 0
        cat_cost  = df_cat["purchase_amount"].sum()  if not df_cat.empty and "purchase_amount" in df_cat.columns else 0
        cat_ret   = (
            round(df_cat["return_pct"].mean(), 2)
            if not df_cat.empty and "return_pct" in df_cat.columns and df_cat["return_pct"].notna().any()
            else None
        )
        cat_count = len(df_cat) if not df_cat.empty else 0
        if cat_ret is None and cat_cost > 0:
            cat_ret = round((cat_value - cat_cost) / cat_cost * 100, 2)
        cat_data.append((cat_icon, cat_name, accent, cat_value, cat_pnl, cat_ret, cat_count))

    cat_cols = st.columns(len(cat_data))
    for col_ui, (icon, name, accent, val, pnl, ret, count) in zip(cat_cols, cat_data):
        with col_ui:
            # 損益の色
            pnl_color  = "#4ade80" if pnl > 0 else ("#f87171" if pnl < 0 else "#94a3b8")
            ret_color  = "#4ade80" if (ret or 0) > 0 else ("#f87171" if (ret or 0) < 0 else "#94a3b8")
            pnl_sign   = "+" if pnl > 0 else ""
            ret_sign   = "+" if (ret or 0) > 0 else ""

            v_disp   = f"¥{int(val):,}"                           if val   else "—"
            pnl_disp = f"{pnl_sign}¥{int(abs(pnl)):,}"           if pnl   else "—"
            ret_disp = f"{ret_sign}{ret:.1f}%"                     if ret is not None else "—"

            st.markdown(f"""
<div style="
    border: 1px solid {accent}55;
    border-top: 3px solid {accent};
    border-radius: 10px;
    padding: 16px 18px 14px;
    background: {accent}0d;
">
  <div style="font-size:13px; font-weight:600; color:#94a3b8; letter-spacing:.05em; margin-bottom:12px;">
    {icon} {name} &nbsp;<span style="font-weight:400; font-size:11px;">({count} 銘柄)</span>
  </div>
  <div style="font-size:22px; font-weight:700; color:#e2e8f0; margin-bottom:6px; letter-spacing:-.02em;">
    {v_disp}
  </div>
  <div style="display:flex; align-items:center; gap:10px; margin-top:8px; flex-wrap:wrap;">
    <span style="font-size:14px; font-weight:600; color:{pnl_color};">{pnl_disp}</span>
    <span style="
        font-size:12px; font-weight:700; color:{ret_color};
        background:{ret_color}22; border-radius:4px; padding:2px 8px;
    ">{ret_disp}</span>
  </div>
  <div style="font-size:11px; color:#64748b; margin-top:8px;">含み損益 / 損益率</div>
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ─── アクション提案テーブル ───
    st.markdown("### アクション提案")
    df_h = df_holdings.copy()
    df_h["アクション"] = df_h.apply(_holding_action, axis=1)

    # アクション集計
    action_counts = df_h["アクション"].value_counts()
    action_cols = st.columns(min(len(action_counts), 5))
    for i, (action, cnt) in enumerate(action_counts.items()):
        if i < len(action_cols):
            action_cols[i].metric(action, f"{cnt} 件")

    st.divider()

    # ─── 保有銘柄テーブル ───
    st.markdown("### 保有銘柄一覧")

    TABLE_COLS = {
        "code": "コード",
        "company_name": "会社名",
        "product_category": "商品",
        "account": "口座",
        "shares": "保有株数",
        "purchase_price": "取得単価",
        "latest_close": "現在値",
        "return_pct": "損益%",
        "unrealized_pnl": "含み損益(円)",
        "current_value": "評価額",
        "kubota_signal": "シグナル",
        "days_to_earnings": "決算(日)",
        "アクション": "アクション",
    }

    avail = [c for c in TABLE_COLS if c in df_h.columns or c == "アクション"]
    df_table = df_h[avail].rename(columns=TABLE_COLS)

    def _style_holdings(df: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=df.index, columns=df.columns)
        if "損益%" in df.columns:
            for idx, val in df["損益%"].items():
                v = _safe_num(val)
                if v is not None and v > 0:
                    styles.loc[idx, :] = "background-color: rgba(166,227,161,0.12)"
                elif v is not None and v < -5:
                    styles.loc[idx, :] = "background-color: rgba(243,139,168,0.15)"
        if "シグナル" in df.columns:
            styles["シグナル"] = df["シグナル"].apply(
                lambda v: "background-color:#a6e3a1;font-weight:700;color:#1e1e2e"
                if v == "買いシグナル"
                else ("background-color:#f9e2af;color:#1e1e2e" if "放れ待ち" in str(v) else "")
            )
        return styles

    col_config = {}
    if "保有株数" in df_table.columns:
        col_config["保有株数"] = st.column_config.NumberColumn("保有株数", format="%d")
    if "取得単価" in df_table.columns:
        col_config["取得単価"] = st.column_config.NumberColumn("取得単価", format="¥%,.0f")
    if "現在値" in df_table.columns:
        col_config["現在値"] = st.column_config.NumberColumn("現在値", format="¥%,.0f")
    if "損益%" in df_table.columns:
        col_config["損益%"] = st.column_config.NumberColumn("損益%", format="%.1f")
    if "含み損益(円)" in df_table.columns:
        col_config["含み損益(円)"] = st.column_config.NumberColumn("含み損益(円)", format="¥%,.0f")
    if "評価額" in df_table.columns:
        col_config["評価額"] = st.column_config.NumberColumn("評価額", format="¥%,.0f")

    st.dataframe(
        df_table.style.apply(_style_holdings, axis=None),
        use_container_width=True,
        height=min(600, (len(df_table) + 3) * 38),
        column_config=col_config,
    )

    st.divider()

    # ─── 銘柄別チャート（国内株式のみ） ───
    st.markdown("### 銘柄別チャート")
    if "product_category" in df_holdings.columns:
        domestic = df_holdings[
            df_holdings["product_category"].str.contains("国内", na=False)
            & df_holdings["code"].notna()
            & (df_holdings["code"] != "")
        ]
    else:
        domestic = df_holdings[df_holdings["code"].notna() & (df_holdings["code"] != "")]
    if domestic.empty:
        st.info("国内株式の保有銘柄がありません。")
    else:
        chart_options = domestic.apply(
            lambda r: f"{r['code']}  {r['company_name']}", axis=1
        ).tolist()
        selected_stock = st.selectbox("銘柄を選択", chart_options, key="holdings_chart_sel")
        sel_code = selected_stock.split()[0].strip()

        with st.spinner("株価データ読込中..."):
            try:
                df_price = load_price_history(sel_code)
            except Exception as e:
                st.error(f"株価データ取得失敗: {e}")
                df_price = pd.DataFrame()

        if not df_price.empty:
            df_price["date"] = pd.to_datetime(df_price["date"])
            df_price = df_price.sort_values("date")

            # 取得単価ラインを追加
            holding_row = domestic[domestic["code"] == sel_code]
            purchase_px = None
            if not holding_row.empty:
                purchase_px = _safe_num(holding_row.iloc[0].get("purchase_price"))

            fig = _candlestick_fig(
                df_price.tail(180),
                title=f"{sel_code}  {selected_stock.split(None, 1)[1] if ' ' in selected_stock else ''}",
                show_ma=True,
                purchase_px=purchase_px,
            )

            # 取得単価の水平線
            if purchase_px is not None:
                fig.add_hline(
                    y=purchase_px,
                    line_dash="dash",
                    line_color="#fab387",
                    annotation_text=f"取得単価 ¥{purchase_px:,.0f}",
                    annotation_position="top left",
                    row=1, col=1,
                )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("株価データがありません。")

    # ─── 更新方法案内 ───
    with st.expander("📋 保有銘柄データ更新方法"):
        st.info(
            "スプレッドシートの「保有銘柄」タブを更新後、GitHub Actions を手動実行するか、"
            "以下のコマンドをローカルで実行してください:\n\n"
            "`.venv\\Scripts\\python scripts/load_holdings.py`"
        )


# ─────────────────────────────────────────
# サイドバー
# ─────────────────────────────────────────
def render_sidebar(df_screening: pd.DataFrame, current_assets_man: int = 0):
    with st.sidebar:
        st.markdown("## 📈 Stock Monitor")
        st.caption("FIRE 目標 ¥1億円 | 窪田フレームワーク")

        st.divider()

        # FIRE 進捗（保有銘柄評価額から自動計算）
        st.markdown("### 💰 FIRE 進捗")
        target = 10000  # 1億円（万円単位）
        current_assets = current_assets_man
        pct = min(current_assets / target * 100, 100) if target > 0 else 0
        assets_disp = f"¥{current_assets:,}万円" if current_assets > 0 else "データ取得中..."
        st.markdown(f"""
        <div class="kpi-label">現在の評価総額</div>
        <div style="font-size:1.3rem;font-weight:700;color:#cba6f7;margin-bottom:4px">{assets_disp}</div>
        <div class="kpi-label">達成率 {pct:.1f}% （目標 ¥{target:,}万円）</div>
        <div class="fire-track">
          <div class="fire-fill" style="width:{pct}%"></div>
        </div>
        <div class="kpi-label">残り ¥{max(target - current_assets, 0):,}万円</div>
        """, unsafe_allow_html=True)
        st.caption("※ 投資信託は ETF プロキシ方式による推定値。米国株は yfinance 取得値 × USD/JPY 換算。")

        st.divider()

        # フィルター
        st.markdown("### 🔎 銘柄絞り込み")
        sectors = ["すべて"] + sorted(
            df_screening["sector33_name"].dropna().unique().tolist()
        ) if not df_screening.empty else ["すべて"]
        sel_sector = st.selectbox("セクター", sectors,
            help="特定の業種に絞って表示します。")

        min_kubota = st.slider("窪田スコア（最低）", 0, 10, 0,
            help=(
                "ボラティリティ（5点）＋チャート（5点）の合計スコア（0〜10点）。\n"
                "【ボラティリティ】ATR≥1.5%: +2 / HV収縮: +2 / レンジ幅15〜80%: +1\n"
                "【チャート】MA200上向き: +1 / MA200上方: +1 / もみ合い収縮: +2 / 出来高急増: +1\n"
                "→ 7点以上が買いを検討できる目安"
            ))
        min_growth = st.slider("成長株スコア（最低）", 0, 29, 0,
            help=(
                "成長力・収益性・割安度を数値化したスコア（0〜29点＋PER/PBR加点）。\n"
                "売上CAGR3年（8点）＋営業利益CAGR3年（8点）＋ROE（7点）＋ROIC（6点）"
                "＋PER割安度（5点）＋PBR（5点）の合計。\n"
                "→ 15点以上が優良成長株の目安"
            ))
        with st.expander("📐 スコア算出の内訳"):
            st.markdown("""
**🏆 窪田スコア（0〜10点）**

*ボラティリティ評価（5点満点）*
| 条件 | 点数 |
|------|------|
| ATR ≥ 1.5%（1日の値動きが十分） | ＋2 |
| HV収縮（直近20日 < 過去60日の変動率） | ＋2 |
| 1ヶ月レンジ幅が 15〜80% | ＋1 |

*チャート評価（5点満点）*
| 条件 | 点数 |
|------|------|
| MA200 が上向き（20日前比 +0.5%超） | ＋1 |
| 株価が MA200 の上方 | ＋1 |
| もみ合い収縮（10日値幅 / 30日値幅 < 50%） | ＋2 |
| 出来高急増（20日平均の 1.5 倍超） | ＋1 |

▶ **7点以上**が買い検討の目安

---

**📈 成長株スコア（0〜29点 ＋α）**

| 指標 | 最高 | 高評価の基準 |
|------|------|-------------|
| 売上 CAGR（3年） | 8点 | ≥15%→8 / ≥10%→6 / ≥5%→3 |
| 営業利益 CAGR（3年） | 8点 | ≥20%→8 / ≥10%→6 / ≥5%→3 |
| ROE（自己資本利益率） | 7点 | ≥15%→7 / ≥10%→5 / ≥8%→3 |
| ROIC（投下資本利益率） | 6点 | ≥12%→6 / ≥8%→4 / ≥5%→2 |
| PER（セクター比割安度） | ＋5点 | 平均の80%未満→5 / 平均以下→3 |
| PBR（純資産倍率） | ＋5点 | PBR < 1.0 →5 / < 1.5 →3 |

▶ **15点以上**が優良成長株の目安
""")
        sig_opts = ["すべて", "買いシグナル", "放れ待ち"]
        sel_signal = st.selectbox("シグナル", sig_opts,
            help="「買いシグナル」＝今すぐ買い検討できる銘柄。「放れ待ち」＝ブレイクアウト待ちの銘柄。")

        st.divider()

        # 更新ボタン
        if st.button("🔄 データ再読込", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    return sel_sector, min_kubota, min_growth, sel_signal


# ─────────────────────────────────────────
# 各タブレンダラー
# ─────────────────────────────────────────

def render_market_header(df_env: pd.DataFrame):
    """相場環境バナー（全タブ共通）"""
    if df_env.empty:
        return
    env = df_env.iloc[0]
    phase = str(env.get("market_phase", "N/A"))
    topix_close = env.get("topix_close", 0)
    topix_ma200 = env.get("topix_ma200", 0)
    env_score = env.get("environment_score", 0)
    env_label = "🟢 良好" if env_score >= 3 else "🟡 中立" if env_score >= 2 else "🔴 注意"
    diff = topix_close - topix_ma200

    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
    with c1:
        st.markdown(
            f"<div style='padding:8px 0'><b>相場フェーズ</b><br>{phase_html(phase)}</div>",
            unsafe_allow_html=True
        )
    with c2:
        st.metric("TOPIX（東証全体の指数）", f"{topix_close:,.1f}",
                  delta=f"{diff:+.1f} vs MA200（200日平均）",
                  help="東証プライム全銘柄の加重平均株価指数。相場全体の動きを示す。")
    with c3:
        st.metric("TOPIX MA200（長期平均）", f"{topix_ma200:,.1f}",
                  help="過去200営業日のTOPIX平均値。これが右肩上がりで株価がMA200の上にあれば強気相場。")
    with c4:
        st.metric("環境スコア", f"{env_score}/3", delta=env_label,
                  help="3/3=強気（積極的に買える）、2/3=中立、1/3=弱気（買い控えを推奨）")
    with c5:
        above = "MA200 上方 ✅" if topix_close > topix_ma200 else "MA200 下方 ⚠️"
        st.metric("現在位置", above,
                  help="株価がMA200の上にあれば上昇トレンド継続。下にあれば注意が必要。")


def render_tab_candidates(df: pd.DataFrame, market_phase: str = ""):
    """Tab 1: 今日の注目銘柄（カードレイアウト）"""
    st.subheader("🏆 今日の注目銘柄")
    st.info(
        "「買いシグナル」＝今すぐ買いを検討できる銘柄 / 「放れ待ち」＝近いうちにチャンスが来る可能性がある銘柄"
    )

    # 相場フェーズバナー
    if market_phase == "BULL":
        st.success("✅ 現在は買いやすい相場（BULL）です。エントリーシグナルが出ている銘柄を積極的に検討できます。")
    elif market_phase == "BEAR":
        st.error("⚠️ 現在は弱気相場（BEAR）です。新規の買いは慎重に。シグナルが出ていても様子見を推奨します。")
    else:
        st.warning("🟡 現在は中立相場です。特に好条件の銘柄のみ検討してください。")

    if df.empty:
        st.info("スクリーニングデータがありません。")
        return

    df_entry = df[df["kubota_signal"] == "買いシグナル"].copy()
    df_watch = df[df["kubota_signal"].str.contains("放れ待ち", na=False)].copy()

    # メトリクス行
    c1, c2, c3 = st.columns(3)
    c1.metric("🟢 今すぐ買い検討", f"{len(df_entry)} 銘柄")
    c2.metric("🟡 放れ待ち", f"{len(df_watch)} 銘柄")
    c3.metric("相場フェーズ", market_phase if market_phase else "N/A")

    # ENTRY SIGNAL カード（最大9件、3列グリッド）
    st.markdown("#### 🟢 買いシグナル — 今すぐ買いを検討できる銘柄")
    if df_entry.empty:
        st.info("現在 ENTRY SIGNAL 銘柄はありません。")
    else:
        for i in range(0, min(9, len(df_entry)), 3):
            cols = st.columns(3)
            for j, col in enumerate(cols):
                if i + j < len(df_entry):
                    try:
                        row = df_entry.iloc[i + j]
                        close_val = _safe_num(row.get("latest_close"))
                        atr_pct_v = _safe_num(row.get("atr_pct"))
                        atr_val_c = close_val * atr_pct_v / 100 if close_val is not None and atr_pct_v is not None else None
                        stop_loss_c = round(close_val - 2 * atr_val_c) if atr_val_c is not None else None
                        ksc_v = _safe_num(row.get("kubota_trade_score"))
                        earn_raw = _safe_num(row.get("days_to_earnings"), 999)
                        earn_int = int(earn_raw)
                        close_str = f"¥{close_val:,.0f}" if close_val is not None else "N/A"
                        stop_str = f"🛑 損切り目安 ¥{stop_loss_c:,.0f}" if stop_loss_c is not None else ""
                        ksc_str = str(int(ksc_v)) if ksc_v is not None else "N/A"
                        earn_html = (
                            f"<div style='color:#f38ba8;font-size:.8em;font-weight:700;'>⚠️ 決算{earn_int}日後</div>"
                            if earn_int <= 20 else ""
                        )
                        with col:
                            st.markdown(f"""
                            <div style="background:linear-gradient(135deg,#1e3a1e,#2d5a2d);
                                        border:2px solid #a6e3a1;border-radius:12px;padding:14px;
                                        margin-bottom:8px;">
                              <div style="font-size:1.1em;font-weight:700;color:#a6e3a1;">🟢 買いシグナル</div>
                              <div style="font-size:1.3em;font-weight:700;color:#cdd6f4;margin:4px 0;">
                                {row.get('code', '')} {row.get('company_name', '')}</div>
                              <div style="color:#a6adc8;font-size:.85em;">{row.get('sector33_name', '')}</div>
                              <div style="margin-top:8px;font-size:1.4em;font-weight:700;color:#cdd6f4;">
                                {close_str}</div>
                              {earn_html}
                              <div style="margin-top:6px;font-size:.85em;color:#a6adc8;">
                                {stop_str}
                              </div>
                              <div style="font-size:.8em;color:#a6adc8;margin-top:4px;">
                                窪田スコア {ksc_str}/10
                              </div>
                              {'<div style="margin-top:6px;"><span style="background:#a6e3a1;color:#1e1e2e;border-radius:4px;padding:2px 7px;font-size:.82em;font-weight:700;">確度 ' + str(int(row["signal_confidence"])) + '%</span></div>' if pd.notna(row.get("signal_confidence")) else ''}
                            </div>
                            """, unsafe_allow_html=True)
                    except Exception:
                        pass

    # WATCH テーブル
    st.markdown("#### 🟡 放れ待ち — 近いうちにチャンスが来る可能性がある銘柄")
    if df_watch.empty:
        st.info("現在 放れ待ち 銘柄はありません。")
    else:
        WATCH_COLS = {
            "code": "コード",
            "company_name": "会社名",
            "sector33_name": "セクター",
            "latest_close": "終値",
            "kubota_trade_score": "窪田S",
            "growth_invest_score": "成長株S",
            "signal_confidence": "確度(%)",
            "days_to_earnings": "決算(日)",
        }
        avail = [c for c in WATCH_COLS if c in df_watch.columns]
        st.dataframe(
            df_watch[avail].rename(columns=WATCH_COLS),
            use_container_width=True,
            height=min(400, (len(df_watch) + 3) * 38),
        )


def render_tab_screening(df: pd.DataFrame, sel_sector, min_kubota, min_growth, sel_signal):
    st.subheader("🔍 スクリーニング結果")

    st.info(
        "**見方のポイント**　"
        "🟢 シグナル列が「買いシグナル」の銘柄は買いの5条件が揃っています。"
        "「窪田S」は7点以上、「成長株S」は15点以上を目安に絞り込むと有望銘柄に絞れます。"
        "「決算(日)」が赤い銘柄は決算発表が近く値動きが荒れやすいため注意してください。",
        icon="💡",
    )
    _glossary_expander()

    # フィルター適用
    df_f = df.copy()
    if sel_sector != "すべて":
        df_f = df_f[df_f["sector33_name"] == sel_sector]
    if min_kubota > 0:
        df_f = df_f[df_f["kubota_trade_score"] >= min_kubota]
    if min_growth > 0:
        df_f = df_f[df_f["growth_invest_score"] >= min_growth]
    if sel_signal != "すべて":
        df_f = df_f[df_f["kubota_signal"] == sel_signal]

    # サマリー指標
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("表示銘柄数", f"{len(df_f)} 件")
    c2.metric("買いシグナル", f"{(df_f['kubota_signal'] == '買いシグナル').sum()} 件")
    c3.metric("放れ待ち", f"{df_f['kubota_signal'].str.contains('放れ待ち', na=False).sum()} 件")
    c4.metric(
        "平均窪田スコア",
        f"{df_f['kubota_trade_score'].mean():.1f}" if not df_f.empty else "N/A"
    )

    # ── セクター別シグナルマップ ──────────────────────────────
    st.markdown("#### 📊 セクター別シグナルマップ")

    if not df_f.empty and "sector33_name" in df_f.columns and "kubota_signal" in df_f.columns:
        # クロス集計
        _sig_map = (
            df_f.groupby("sector33_name")["kubota_signal"]
            .value_counts()
            .unstack(fill_value=0)
            .reset_index()
        )
        # 必要列を補完
        for _col in ["買いシグナル", "放れ待ち"]:
            if _col not in _sig_map.columns:
                _sig_map[_col] = 0
        _sig_map["その他"] = _sig_map.drop(columns=["sector33_name", "買いシグナル", "放れ待ち"], errors="ignore").sum(axis=1)
        _sig_map["合計"] = _sig_map["買いシグナル"] + _sig_map["放れ待ち"] + _sig_map["その他"]
        _sig_map["買いシグナル率"] = _sig_map["買いシグナル"] / _sig_map["合計"].replace(0, 1) * 100
        _sig_map = _sig_map.sort_values("買いシグナル率", ascending=True)

        # 横積み棒グラフ
        _fig_sec = go.Figure()
        _fig_sec.add_trace(go.Bar(
            y=_sig_map["sector33_name"],
            x=_sig_map["買いシグナル"],
            name="買いシグナル",
            orientation="h",
            marker_color="#a6e3a1",
            customdata=_sig_map[["買いシグナル率"]].values,
            hovertemplate="%{y}<br>買いシグナル: %{x}件 (%{customdata[0]:.1f}%)<extra></extra>",
        ))
        _fig_sec.add_trace(go.Bar(
            y=_sig_map["sector33_name"],
            x=_sig_map["放れ待ち"],
            name="放れ待ち",
            orientation="h",
            marker_color="#f9e2af",
            hovertemplate="%{y}<br>放れ待ち: %{x}件<extra></extra>",
        ))
        _fig_sec.add_trace(go.Bar(
            y=_sig_map["sector33_name"],
            x=_sig_map["その他"],
            name="その他",
            orientation="h",
            marker_color="#45475a",
            hovertemplate="%{y}<br>その他: %{x}件<extra></extra>",
        ))
        _fig_sec.update_layout(
            barmode="stack",
            height=max(320, len(_sig_map) * 26),
            margin=dict(l=170, r=40, t=20, b=30),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#313244", title="銘柄数"),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
        )
        st.plotly_chart(_fig_sec, use_container_width=True)

        # 集計テーブル
        with st.expander("📋 セクター別集計テーブル"):
            _tbl = _sig_map[["sector33_name", "買いシグナル", "放れ待ち", "その他", "合計", "買いシグナル率"]].rename(
                columns={"sector33_name": "セクター", "買いシグナル率": "買いシグナル率(%)"}
            )
            _tbl = _tbl.sort_values("買いシグナル率(%)", ascending=False).reset_index(drop=True)
            st.dataframe(
                _tbl.style.format({"買いシグナル率(%)": "{:.1f}"}),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("表示できるデータがありません。")

    # 表示列
    COLS = {
        "code": "コード",
        "company_name": "会社名",
        "sector33_name": "セクター",
        "latest_close": "終値",
        "avg_turnover_20d_oku": "売買代金(億)",
        "kubota_trade_score": "窪田S",
        "growth_invest_score": "成長株S",
        "sales_cagr_3y_pct": "売上CAGR%",
        "roe_pct": "ROE%",
        "per": "PER",
        "pbr": "PBR",
        "kubota_signal": "シグナル",
        "days_to_earnings": "決算(日)",
    }
    existing = [c for c in COLS.keys() if c in df_f.columns]
    df_show = df_f[existing].rename(columns=COLS)

    st.dataframe(
        df_show.style.apply(_style_screening, axis=None),
        use_container_width=True,
        height=580,
        column_config={
            "終値": st.column_config.NumberColumn("終値", format="¥%,.0f"),
            "売買代金(億)": st.column_config.NumberColumn("売買代金(億)", format="%.1f"),
            "PER": st.column_config.NumberColumn("PER", format="%.1f"),
            "PBR": st.column_config.NumberColumn("PBR", format="%.1f"),
            "売上CAGR%": st.column_config.NumberColumn("売上CAGR%", format="%.1f"),
            "ROE%": st.column_config.NumberColumn("ROE%", format="%.1f"),
            "窪田S": st.column_config.NumberColumn("窪田S", format="%d"),
            "成長株S": st.column_config.NumberColumn("成長株S", format="%d"),
        },
    )

    # セクター分布
    with st.expander("📊 セクター別 スコア分布"):
        df_sec = (
            df_f.groupby("sector33_name")
            .agg(
                銘柄数=("code", "count"),
                avg_kubota=("kubota_trade_score", "mean"),
                avg_growth=("growth_invest_score", "mean"),
                entry_cnt=("kubota_signal", lambda x: (x == "買いシグナル").sum()),
            )
            .reset_index()
            .sort_values("avg_kubota", ascending=True)
        )
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=df_sec["sector33_name"],
            x=df_sec["avg_kubota"],
            orientation="h",
            name="平均窪田スコア",
            text=df_sec.apply(
                lambda r: f"{r['銘柄数']}銘柄 / ENTRY:{r['entry_cnt']}件", axis=1
            ),
            textposition="outside",
            marker_color="#74c7ec",
        ))
        fig.update_layout(
            title="セクター別 平均窪田スコア",
            height=max(300, len(df_sec) * 28),
            margin=dict(l=160, r=60, t=40, b=40),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#313244"),
        )
        st.plotly_chart(fig, use_container_width=True)


def render_tab_chart(df_screening: pd.DataFrame):
    st.subheader("📌 銘柄詳細・売買タイミング判定")

    if df_screening.empty:
        st.warning("スクリーニングデータがありません。")
        return

    st.info(
        "**使い方** ① 下のボックスで銘柄を検索 → ② 買い時・売り時チェックリストを確認 → ③ チャートで最終確認 → ④ 条件が揃っているか判断",
        icon="📌",
    )

    # ── 銘柄検索（より目立つデザイン）
    col_search, col_priority = st.columns([3, 1])
    with col_search:
        search = st.text_input(
            "🔍 銘柄コード or 会社名で検索（例: 7203、トヨタ）",
            value="", key="chart_search",
            placeholder="コードまたは会社名を入力してください",
        )
    with col_priority:
        priority_entry = st.checkbox("🟢 買いシグナル銘柄を優先表示", value=False, key="chart_priority")

    if search:
        mask = (
            df_screening["code"].str.contains(search, case=False, na=False) |
            df_screening["company_name"].str.contains(search, case=False, na=False)
        )
        df_sel = df_screening[mask]
    else:
        df_sel = df_screening.copy()

    # ENTRY SIGNAL 優先表示
    if priority_entry:
        df_entry_first = df_sel[df_sel["kubota_signal"] == "買いシグナル"]
        df_rest = df_sel[df_sel["kubota_signal"] != "買いシグナル"]
        df_sel = pd.concat([df_entry_first, df_rest], ignore_index=True)

    if df_sel.empty:
        st.warning("該当する銘柄が見つかりません。")
        return

    options = df_sel.apply(
        lambda r: f"{r['code']}  {r['company_name']}  ({r['sector33_name']})", axis=1
    ).tolist()
    selected = st.selectbox("銘柄を選択", options, key="chart_sel")
    sel_code = selected.split()[0].strip()

    _matched = df_screening[df_screening["code"] == sel_code]
    if _matched.empty:
        st.warning(f"銘柄 {sel_code} のデータが見つかりません。")
        return
    stock = _matched.iloc[0]
    market_phase = str(stock.get("market_phase", ""))

    # ── 買い時・売り時判定パネル（チャートより先に表示）
    st.divider()
    _buy_sell_panel(stock, market_phase)
    st.divider()

    # 銘柄情報カード
    close_disp = _safe_num(stock.get("latest_close"))
    ksc_disp = _safe_num(stock.get("kubota_trade_score"))
    gsc_disp = _safe_num(stock.get("growth_invest_score"))
    per_disp = _safe_num(stock.get("per"))
    dte_disp = _safe_num(stock.get("days_to_earnings"))

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("現在値", f"¥{close_disp:,.0f}" if close_disp is not None else "N/A",
              help="直近取引日の終値")
    c2.metric("窪田スコア", f"{int(ksc_disp)}/10" if ksc_disp is not None else "N/A",
              help="7点以上が買いの目安")
    c3.metric("成長株スコア", f"{int(gsc_disp)}/29" if gsc_disp is not None else "N/A",
              help="15点以上が優良成長株の目安")
    c4.metric("PER", f"{per_disp:.1f}x" if per_disp is not None else "N/A")
    c5.metric(
        "シグナル",
        "🟢 買いシグナル" if stock.get("kubota_signal") == "買いシグナル"
        else ("🟡 放れ待ち" if stock.get("kubota_signal") == "放れ待ち" else "➖"),
    )
    c6.metric(
        "次回決算",
        f"{int(dte_disp)}日後" if dte_disp is not None else "N/A"
    )

    # 期間選択
    period_map = {"60日": 60, "90日": 90, "180日": 180, "全期間": 9999}
    period_label = st.radio("表示期間", list(period_map.keys()), horizontal=True, index=1)
    period_days = period_map[period_label]

    # 株価データ取得
    with st.spinner("株価データ読込中..."):
        try:
            df_price = load_price_history(sel_code)
        except Exception as e:
            st.error(f"株価データ取得失敗: {e}")
            return

    if df_price.empty:
        st.warning("株価データがありません。")
        return

    df_price["date"] = pd.to_datetime(df_price["date"])
    df_price = df_price.sort_values("date").tail(period_days)

    # ローソク足チャート
    company_name = stock.get("company_name", "")
    sector_name = stock.get("sector33_name", "")
    fig = _candlestick_fig(
        df_price,
        title=f"{sel_code}  {company_name}  [{sector_name}]",
        show_ma=True,
    )

    # 窪田スコア注釈
    fig.add_annotation(
        xref="paper", yref="paper",
        x=0.01, y=0.97,
        text=f"窪田スコア: <b>{int(ksc_disp) if ksc_disp is not None else 'N/A'}/10</b>  成長株スコア: <b>{int(gsc_disp) if gsc_disp is not None else 'N/A'}/29</b>",
        showarrow=False,
        bgcolor="rgba(30,30,46,0.8)",
        bordercolor="#45475a",
        font=dict(color="#cdd6f4", size=13),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ファンダメンタル情報
    with st.expander("📋 ファンダメンタル詳細"):
        cols = [
            ("売上CAGR(3年)", "sales_cagr_3y_pct", "{:.1f}%"),
            ("営業利益CAGR(3年)", "op_cagr_3y_pct", "{:.1f}%"),
            ("ROE", "roe_pct", "{:.1f}%"),
            ("ROIC", "roic_pct", "{:.1f}%"),
            ("PER", "per", "{:.1f}x"),
            ("PBR", "pbr", "{:.1f}x"),
            ("売買代金(平均20日)", "avg_turnover_20d_oku", "{:.1f}億円"),
            ("流動性グレード", "liquidity_grade", "{}"),
        ]
        col_widgets = st.columns(4)
        for i, (label, key, fmt) in enumerate(cols):
            val = stock.get(key)
            try:
                display = fmt.format(val) if pd.notna(val) else "N/A"
            except Exception:
                display = "N/A"
            col_widgets[i % 4].metric(label, display)

    # スコア推移
    with st.spinner("スコア推移読込中..."):
        df_hist = load_score_history(sel_code)

    if not df_hist.empty:
        df_hist["snapshot_date"] = pd.to_datetime(df_hist["snapshot_date"])
        df_hist = df_hist.sort_values("snapshot_date")

        fig_score = make_subplots(specs=[[{"secondary_y": True}]])
        fig_score.add_trace(
            go.Scatter(
                x=df_hist["snapshot_date"], y=df_hist["kubota_trade_score"],
                mode="lines+markers", name="窪田スコア",
                line=dict(color="#a6e3a1", width=2),
                marker=dict(size=5),
            ),
            secondary_y=False,
        )
        fig_score.add_trace(
            go.Scatter(
                x=df_hist["snapshot_date"], y=df_hist["growth_invest_score"],
                mode="lines+markers", name="成長株スコア",
                line=dict(color="#74c7ec", width=2),
                marker=dict(size=5),
            ),
            secondary_y=True,
        )
        fig_score.update_layout(
            title="スコア推移",
            height=260,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", bgcolor="rgba(0,0,0,0)"),
            margin=dict(l=60, r=60, t=40, b=40),
        )
        fig_score.update_yaxes(title_text="窪田スコア", range=[0, 12], secondary_y=False,
                                gridcolor="#313244")
        fig_score.update_yaxes(title_text="成長株スコア", range=[0, 32], secondary_y=True)
        st.plotly_chart(fig_score, use_container_width=True)
    else:
        st.caption("スコア履歴データなし（--mode snapshot を実行すると蓄積されます）")


def render_tab_backtest(df_bt: pd.DataFrame):
    st.subheader("📉 バックテスト結果（過去1年）")

    if df_bt.empty:
        st.info("バックテストデータがありません。")
        return

    # 数値型変換
    for col in ["return_5d_pct", "return_10d_pct", "return_20d_pct",
                "win_5d", "win_10d", "win_20d"]:
        df_bt[col] = pd.to_numeric(df_bt[col], errors="coerce")

    # KPI
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総シグナル数", f"{len(df_bt)} 件")
    c2.metric(
        "勝率（5日後）",
        f"{df_bt['win_5d'].mean() * 100:.1f}%",
        delta=f"平均 {df_bt['return_5d_pct'].mean():.1f}%",
    )
    c3.metric(
        "勝率（10日後）",
        f"{df_bt['win_10d'].mean() * 100:.1f}%",
        delta=f"平均 {df_bt['return_10d_pct'].mean():.1f}%",
    )
    c4.metric(
        "勝率（20日後）",
        f"{df_bt['win_20d'].mean() * 100:.1f}%",
        delta=f"平均 {df_bt['return_20d_pct'].mean():.1f}%",
    )

    c5, c6, c7, c8 = st.columns(4)
    _max = df_bt['return_20d_pct'].max()
    _min = df_bt['return_20d_pct'].min()
    _std = df_bt['return_20d_pct'].std()
    c5.metric("最大利益（20日）", f"{_max:.1f}%" if pd.notna(_max) else "N/A")
    c6.metric("最大損失（20日）", f"{_min:.1f}%" if pd.notna(_min) else "N/A")
    c7.metric("標準偏差（20日）", f"{_std:.1f}%" if pd.notna(_std) else "N/A")
    _loss_sum = abs(df_bt[df_bt['return_20d_pct'] < 0]['return_20d_pct'].sum())
    _gain_sum = df_bt[df_bt['return_20d_pct'] > 0]['return_20d_pct'].sum()
    _pf = f"{_gain_sum / _loss_sum:.1f}" if _loss_sum > 0 else "∞"
    c8.metric("プロフィットファクター", _pf)

    # チャート列
    col_left, col_right = st.columns(2)

    # 20日リターン分布
    with col_left:
        fig_hist = px.histogram(
            df_bt.dropna(subset=["return_20d_pct"]),
            x="return_20d_pct", nbins=40,
            title="20日後リターン分布",
            color_discrete_sequence=["#74c7ec"],
            labels={"return_20d_pct": "リターン（%）"},
        )
        avg_ret = df_bt["return_20d_pct"].mean()
        fig_hist.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
        fig_hist.add_vline(
            x=avg_ret,
            line_dash="solid", line_color="#a6e3a1",
            annotation_text=f"平均 {avg_ret:.1f}%",
            annotation_font_color="#a6e3a1",
        )
        fig_hist.update_layout(
            height=360, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#313244"), yaxis=dict(gridcolor="#313244"),
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    # 5/10/20日別 箱ひげ図
    with col_right:
        df_melt = pd.melt(
            df_bt.dropna(subset=["return_5d_pct", "return_10d_pct", "return_20d_pct"]),
            value_vars=["return_5d_pct", "return_10d_pct", "return_20d_pct"],
            var_name="期間", value_name="リターン(%)",
        ).replace({"return_5d_pct": "5日後", "return_10d_pct": "10日後", "return_20d_pct": "20日後"})
        fig_box = px.box(
            df_melt, x="期間", y="リターン(%)",
            title="保有期間別 リターン分布",
            color="期間",
            color_discrete_sequence=["#a6e3a1", "#74c7ec", "#fab387"],
        )
        fig_box.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
        fig_box.update_layout(
            height=360, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            xaxis=dict(gridcolor="#313244"), yaxis=dict(gridcolor="#313244"),
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig_box, use_container_width=True)

    # 時系列散布図
    df_bt["signal_date"] = pd.to_datetime(df_bt["signal_date"])
    fig_scatter = go.Figure()
    fig_scatter.add_trace(go.Scatter(
        x=df_bt["signal_date"],
        y=df_bt["return_20d_pct"],
        mode="markers",
        text=df_bt["code"],
        hovertemplate="<b>%{text}</b><br>シグナル日: %{x|%Y-%m-%d}<br>20日後: %{y:.1f}%<extra></extra>",
        marker=dict(
            color=df_bt["return_20d_pct"],
            colorscale="RdYlGn",
            cmin=-20, cmax=20,
            size=8,
            opacity=0.75,
            colorbar=dict(title="リターン%"),
        ),
    ))
    fig_scatter.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.3)
    fig_scatter.update_layout(
        title="シグナル日別 20日後リターン推移",
        height=360,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="シグナル日", gridcolor="#313244"),
        yaxis=dict(title="20日後リターン（%）", gridcolor="#313244"),
        margin=dict(t=40, b=40),
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # 詳細テーブル
    with st.expander("📋 バックテスト詳細データ"):
        st.dataframe(
            df_bt[["signal_date", "code",
                   "return_5d_pct", "return_10d_pct", "return_20d_pct",
                   "win_5d", "win_10d", "win_20d",
                   "atr_pct", "hv_20d_pct"]].rename(columns={
                "signal_date": "シグナル日",
                "code": "コード",
                "return_5d_pct": "5日後%",
                "return_10d_pct": "10日後%",
                "return_20d_pct": "20日後%",
                "win_5d": "勝(5日)",
                "win_10d": "勝(10日)",
                "win_20d": "勝(20日)",
                "atr_pct": "ATR%",
                "hv_20d_pct": "HV20日",
            }),
            use_container_width=True,
            column_config={
                "5日後%": st.column_config.NumberColumn(format="%.1f"),
                "10日後%": st.column_config.NumberColumn(format="%.1f"),
                "20日後%": st.column_config.NumberColumn(format="%.1f"),
            },
        )


def render_tab_topix(df_topix: pd.DataFrame):
    st.subheader("🌏 TOPIX 推移")

    if df_topix.empty:
        st.warning("TOPIXデータがありません。")
        return

    df_topix["date"] = pd.to_datetime(df_topix["date"])
    df_topix = df_topix.sort_values("date")

    # 期間選択
    period_map = {"90日": 90, "180日": 180, "1年": 252, "全期間": 9999}
    period_label = st.radio(
        "表示期間", list(period_map.keys()), horizontal=True, index=1, key="topix_period"
    )
    # MA計算
    df_full = df_topix.copy()
    for n in [25, 75, 200]:
        df_full[f"ma{n}"] = df_full["close"].rolling(n).mean()
    df_t = df_full.tail(period_map[period_label]).copy()

    # ローソク足
    fig = _candlestick_fig(df_t, "TOPIX 日足チャート", show_ma=False)

    # MA追加（TOPIX に volume が無いので手動で追加）
    for n, color in [(25, "#74c7ec"), (75, "#fab387"), (200, "#f9e2af")]:
        col = f"ma{n}"
        if col in df_t.columns and df_t[col].notna().any():
            fig.add_trace(go.Scatter(
                x=df_t["date"], y=df_t[col],
                mode="lines", name=f"MA{n}",
                line=dict(color=color, width=1.5),
                opacity=0.85,
            ), row=1, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # 月次リターン
    with st.expander("📊 月次リターン"):
        df_monthly = (
            df_topix.set_index("date")["close"]
            .resample("ME").last()
            .pct_change()
            .dropna()
            .tail(24)
            .reset_index()
        )
        df_monthly.columns = ["date", "monthly_return"]
        df_monthly["color"] = df_monthly["monthly_return"].apply(
            lambda v: "#a6e3a1" if v >= 0 else "#f38ba8"
        )
        fig_monthly = go.Figure(go.Bar(
            x=df_monthly["date"],
            y=df_monthly["monthly_return"] * 100,
            marker_color=df_monthly["color"],
            text=df_monthly["monthly_return"].apply(lambda v: f"{v*100:.1f}%"),
            textposition="outside",
        ))
        fig_monthly.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
        fig_monthly.update_layout(
            title="TOPIX 月次リターン（直近2年）",
            yaxis_title="月次リターン（%）",
            height=320,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#313244"),
            yaxis=dict(gridcolor="#313244"),
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig_monthly, use_container_width=True)


# ─────────────────────────────────────────
# アクションボード（ヘルパー + メイン関数）
# ─────────────────────────────────────────

def _lerp_color(intensity: float, dark=(49, 50, 68), light=(166, 227, 161)) -> str:
    """0.0〜1.0 の強度を dark→light のグラデーション色に変換"""
    r = int(dark[0] + intensity * (light[0] - dark[0]))
    g = int(dark[1] + intensity * (light[1] - dark[1]))
    b = int(dark[2] + intensity * (light[2] - dark[2]))
    return f"rgb({r},{g},{b})"


def _mini_candlestick(
    df_price: pd.DataFrame,
    code: str,
    company: str,
    pnl_pct: float | None = None,
    purchase_px: float | None = None,
    signal: str = "",
) -> go.Figure:
    """保有銘柄用ミニローソク足（チャートボード向け 直近60日）"""
    df = df_price.tail(60).copy()
    df["date"] = pd.to_datetime(df["date"])

    # 損益に応じた背景色
    if pnl_pct is not None and pnl_pct >= 10:
        bg = "rgba(74,222,128,0.10)"
    elif pnl_pct is not None and pnl_pct >= 0:
        bg = "rgba(74,222,128,0.04)"
    elif pnl_pct is not None and pnl_pct >= -5:
        bg = "rgba(248,113,113,0.04)"
    elif pnl_pct is not None:
        bg = "rgba(248,113,113,0.12)"
    else:
        bg = "rgba(30,30,46,0.6)"

    pnl_color = "#4ade80" if (pnl_pct or 0) >= 0 else "#f87171"
    pnl_str = f"{pnl_pct:+.1f}%" if pnl_pct is not None else ""

    sig_icon = "🟢 " if signal == "買いシグナル" else ("🟡 " if "放れ待ち" in str(signal) else "")
    title_text = (
        f"{sig_icon}<b>{code}</b> "
        f"<span style='color:#a6adc8;font-size:11px'>{company[:10]}</span>"
        + (f"  <b style='color:{pnl_color}'>{pnl_str}</b>" if pnl_str else "")
    )

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color="#a6e3a1", decreasing_line_color="#f38ba8",
        increasing_fillcolor="#a6e3a1", decreasing_fillcolor="#f38ba8",
        showlegend=False, hoverinfo="skip",
    ))
    ma25 = df["close"].rolling(25).mean()
    if ma25.notna().any():
        fig.add_trace(go.Scatter(
            x=df["date"], y=ma25, mode="lines",
            line=dict(color="#74c7ec", width=1),
            showlegend=False, hoverinfo="skip",
        ))
    if purchase_px is not None:
        fig.add_hline(y=purchase_px, line_dash="dot", line_color="#fab387", line_width=1.2)

    fig.update_layout(
        title=dict(text=title_text, font=dict(size=12), x=0.04, xanchor="left"),
        height=185,
        paper_bgcolor=bg,
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=4, r=4, t=34, b=4),
        xaxis=dict(visible=False, rangeslider_visible=False),
        yaxis=dict(visible=False),
    )
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig


def render_tab_actionboard(
    df_env: pd.DataFrame,
    df_screening: pd.DataFrame,
    df_holdings: pd.DataFrame,
):
    """📡 アクションボード — 今日やるべきことを30秒で把握する"""

    # ═══════════════════════════════════════════════════
    # SECTION 1: 相場サマリーバナー
    # ═══════════════════════════════════════════════════
    if not df_env.empty:
        env = df_env.iloc[0]
        phase      = str(env.get("market_phase", "NEUTRAL"))
        env_score  = int(_safe_num(env.get("environment_score")) or 0)
        topix_close = _safe_num(env.get("topix_close")) or 0
        topix_ma200 = _safe_num(env.get("topix_ma200")) or 0
        topix_diff_pct = (topix_close - topix_ma200) / topix_ma200 * 100 if topix_ma200 else 0
    else:
        phase, env_score, topix_close, topix_diff_pct = "NEUTRAL", 0, 0, 0

    entry_cnt = int((df_screening["kubota_signal"] == "買いシグナル").sum()) if not df_screening.empty else 0
    watch_cnt = int(df_screening["kubota_signal"].str.contains("放れ待ち", na=False).sum()) if not df_screening.empty else 0

    if phase == "BULL" and env_score >= 3:
        b_bg, b_bd, b_icon = "linear-gradient(135deg,#1e3a1e,#2d5a2d)", "#a6e3a1", "🐂 BULL"
        action_txt = "積極的なエントリーを検討できます"
    elif phase == "BEAR" or env_score <= 1:
        b_bg, b_bd, b_icon = "linear-gradient(135deg,#3a1e1e,#5a2d2d)", "#f38ba8", "🐻 BEAR"
        action_txt = "新規の買いは慎重に。シグナルが出ていても様子見推奨"
    else:
        b_bg, b_bd, b_icon = "linear-gradient(135deg,#2a2b3d,#313244)", "#fab387", "🟡 NEUTRAL"
        action_txt = "特に好条件の銘柄のみ、慎重に検討してください"

    diff_color = "#a6e3a1" if topix_diff_pct >= 0 else "#f38ba8"
    st.markdown(f"""
<div style="background:{b_bg};border:2px solid {b_bd};border-radius:12px;
            padding:14px 22px;display:flex;align-items:center;gap:24px;
            flex-wrap:wrap;margin-bottom:16px;">
  <div style="font-size:1.3em;font-weight:700;color:{b_bd};">{b_icon} 相場</div>
  <div style="color:#cdd6f4;font-size:.95em;">
    TOPIX <b>{topix_close:,.1f}</b>
    <span style="color:{diff_color};font-size:.88em;">&nbsp;(MA200比 {topix_diff_pct:+.1f}%)</span>
  </div>
  <div style="color:#a6adc8;font-size:.88em;">
    🟢 買いシグナル <b style="color:#a6e3a1;">{entry_cnt} 件</b>
    &nbsp;｜&nbsp;
    🟡 放れ待ち <b style="color:#f9e2af;">{watch_cnt} 件</b>
  </div>
  <div style="margin-left:auto;color:#cdd6f4;font-size:.88em;font-style:italic;">
    {action_txt}
  </div>
</div>
""", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════
    # SECTION 2: 今すぐアクション（2カラム）
    # ═══════════════════════════════════════════════════
    st.markdown("### 🚨 今すぐ判断が必要な銘柄")
    col_buy, col_alert = st.columns(2)

    # ── 左: 買い候補 ──
    with col_buy:
        st.markdown("#### 📈 買い候補（新規エントリー検討）")
        if df_screening.empty:
            st.info("スクリーニングデータがありません。")
        else:
            df_entry = df_screening[df_screening["kubota_signal"] == "買いシグナル"].copy()
            df_watch_hi = (
                df_screening[
                    df_screening["kubota_signal"].str.contains("放れ待ち", na=False)
                    & (df_screening.get("kubota_trade_score", pd.Series(dtype=float)) >= 7)
                ].copy()
                if "kubota_trade_score" in df_screening.columns else pd.DataFrame()
            )

            if df_entry.empty and df_watch_hi.empty:
                st.success("✅ 現在、新規エントリー候補はありません。\n\n次のシグナルが出るまで待ちましょう。")
            else:
                # 買いシグナル（上位5件）
                for _, row in df_entry.head(5).iterrows():
                    ksc   = _safe_num(row.get("kubota_trade_score")) or 0
                    close = _safe_num(row.get("latest_close"))
                    atr_p = _safe_num(row.get("atr_pct"))
                    earn  = _safe_num(row.get("days_to_earnings"))
                    conf  = _safe_num(row.get("signal_confidence"))

                    stop_str = target_str = ""
                    if close and atr_p:
                        atr_v = close * atr_p / 100
                        stop_str   = f"🛑 損切り: ¥{round(close - 2 * atr_v):,}"
                        target_str = f"🎯 利確目安: ¥{round(close + 3 * atr_v):,}"

                    star = "⭐⭐⭐" if ksc >= 9 else ("⭐⭐" if ksc >= 7 else "⭐")
                    p_color = "#a6e3a1" if ksc >= 9 else ("#74c7ec" if ksc >= 7 else "#f9e2af")
                    earn_badge = (
                        f" <span style='background:#f38ba8;color:#1e1e2e;border-radius:3px;"
                        f"padding:1px 5px;font-size:.75em;font-weight:700;'>⚠️ 決算{int(earn)}日後</span>"
                        if earn is not None and earn <= 20 else ""
                    )
                    conf_badge = (
                        f" <span style='background:#a6e3a155;color:#a6e3a1;border-radius:3px;"
                        f"padding:1px 5px;font-size:.75em;'>確度 {int(conf)}%</span>"
                        if conf is not None else ""
                    )
                    st.markdown(f"""
<div style="border:1px solid #a6e3a155;border-left:4px solid {p_color};
            border-radius:8px;padding:10px 14px;margin-bottom:8px;
            background:rgba(166,227,161,0.04);">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
    <div>
      <span style="font-weight:700;color:#cdd6f4;">{row.get('code','')} {row.get('company_name','')[:14]}</span>
      {earn_badge}{conf_badge}
      <div style="color:#a6adc8;font-size:.82em;margin-top:2px;">{row.get('sector33_name','')}</div>
    </div>
    <span style="font-size:.85em;color:{p_color};font-weight:700;">{star}</span>
  </div>
  <div style="margin-top:6px;display:flex;gap:16px;flex-wrap:wrap;font-size:.88em;">
    <span style="color:#cdd6f4;">¥{int(close):,}</span>
    <span style="color:#a6adc8;">窪田S <b style="color:#a6e3a1;">{int(ksc)}/10</b></span>
  </div>
  <div style="margin-top:4px;font-size:.82em;color:#6c7086;">{stop_str}&nbsp;&nbsp;{target_str}</div>
</div>
""", unsafe_allow_html=True)

                # 放れ待ち高スコア（上位3件）
                if not df_watch_hi.empty:
                    st.markdown(
                        "<div style='color:#f9e2af;font-size:.88em;font-weight:600;margin:6px 0 4px;'>"
                        "🟡 放れ直前候補（窪田スコア7点以上）</div>",
                        unsafe_allow_html=True,
                    )
                    for _, row in df_watch_hi.head(3).iterrows():
                        ksc   = _safe_num(row.get("kubota_trade_score")) or 0
                        close = _safe_num(row.get("latest_close"))
                        st.markdown(f"""
<div style="border:1px solid #f9e2af44;border-left:3px solid #f9e2af;
            border-radius:6px;padding:8px 12px;margin-bottom:5px;
            background:rgba(249,226,175,0.04);">
  <span style="font-weight:600;color:#cdd6f4;">{row.get('code','')} {row.get('company_name','')[:14]}</span>
  <span style="float:right;color:#a6adc8;font-size:.85em;">¥{int(close):,}  S:{int(ksc)}/10</span>
  <div style="color:#a6adc8;font-size:.8em;margin-top:2px;">{row.get('sector33_name','')}</div>
</div>""", unsafe_allow_html=True)

    # ── 右: 保有銘柄アラート ──
    with col_alert:
        st.markdown("#### ⚠️ 保有銘柄のアラート")
        if df_holdings.empty:
            st.info("保有銘柄データがありません。")
        else:
            alerts = []
            for _, row in df_holdings.iterrows():
                ret    = _safe_num(row.get("return_pct"))
                earn   = _safe_num(row.get("days_to_earnings"))
                code   = str(row.get("code", ""))
                name   = str(row.get("company_name", ""))[:16]
                close  = _safe_num(row.get("latest_close"))
                pnl    = _safe_num(row.get("unrealized_pnl"))
                cat    = str(row.get("product_category", ""))

                price_disp = f"¥{int(close):,}" if close else ""
                if ret is not None and ret <= -8:
                    alerts.append(("🛑", "損切り検討", "#f38ba8", code, name, cat,
                                   f"含み損 {ret:+.1f}% — 損切りラインを超過しています", price_disp, pnl, 0))
                elif ret is not None and ret >= 20:
                    alerts.append(("💰", "利確検討", "#a6e3a1", code, name, cat,
                                   f"含み益 {ret:+.1f}% — 目標利益に到達しています", price_disp, pnl, 1))
                elif earn is not None and earn <= 7:
                    alerts.append(("📅", "決算直前", "#f38ba8", code, name, cat,
                                   f"決算まで {int(earn)} 日 — 急激な値動きに注意", price_disp, pnl, 2))
                elif earn is not None and earn <= 14:
                    alerts.append(("📅", "決算前注意", "#fab387", code, name, cat,
                                   f"決算まで {int(earn)} 日 — ポジション縮小を検討", price_disp, pnl, 3))
                elif ret is not None and ret <= -5:
                    alerts.append(("⚠️", "含み損注意", "#fab387", code, name, cat,
                                   f"含み損 {ret:+.1f}% — 損切りライン接近", price_disp, pnl, 4))

            # 優先度順にソート
            alerts.sort(key=lambda x: x[9])

            if not alerts:
                st.success("✅ 今日は緊急アクションが必要な保有銘柄はありません。")
                st.caption("全ての保有銘柄が正常な範囲内にあります。ゆっくり様子を見ましょう。")
            else:
                for icon, label, color, c, n, cat, reason, price_disp, pnl_val, _ in alerts:
                    pnl_color = "#4ade80" if (pnl_val or 0) >= 0 else "#f87171"
                    pnl_disp = f"含み損益: ¥{int(pnl_val):,}" if pnl_val is not None else ""
                    st.markdown(f"""
<div style="border:1px solid {color}55;border-left:4px solid {color};
            border-radius:8px;padding:10px 14px;margin-bottom:8px;background:{color}0d;">
  <div style="display:flex;justify-content:space-between;">
    <span style="font-weight:700;color:{color};">{icon} {label}</span>
    <span style="font-size:.85em;color:#a6adc8;">{price_disp}</span>
  </div>
  <div style="color:#cdd6f4;font-weight:600;margin-top:3px;">{c} {n}
    <span style="font-size:.8em;color:#6c7086;font-weight:400;"> {cat}</span>
  </div>
  <div style="color:#a6adc8;font-size:.85em;margin-top:2px;">{reason}</div>
  {f'<div style="color:{pnl_color};font-size:.85em;font-weight:700;margin-top:3px;">{pnl_disp}</div>' if pnl_disp else ''}
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ═══════════════════════════════════════════════════
    # SECTION 3: 保有銘柄チャートボード
    # ═══════════════════════════════════════════════════
    st.markdown("### 📊 保有銘柄チャートボード")
    st.caption("直近60日のローソク足 ＋ MA25 ＋ 取得単価ライン（橙破線）。背景色：緑=含み益 / 赤=含み損")

    if df_holdings.empty:
        st.info("保有銘柄データがありません。")
    else:
        if "product_category" in df_holdings.columns:
            domestic_h = df_holdings[
                df_holdings["product_category"].str.contains("国内", na=False)
                & df_holdings["code"].notna()
                & (df_holdings["code"].astype(str).str.strip() != "")
            ].copy()
        else:
            domestic_h = df_holdings[df_holdings["code"].notna()].copy()

        if domestic_h.empty:
            st.info("国内株式の保有銘柄がありません（米国株・投資信託は株価データ非対応）。")
        else:
            n_show = min(9, len(domestic_h))
            with st.spinner("チャートデータ読込中..."):
                for chunk_start in range(0, n_show, 3):
                    chunk = domestic_h.iloc[chunk_start:chunk_start + 3]
                    chart_cols = st.columns(3)
                    for col_c, (_, hrow) in zip(chart_cols, chunk.iterrows()):
                        code_h     = str(hrow.get("code", ""))
                        name_h     = str(hrow.get("company_name", ""))
                        pnl_pct_h  = _safe_num(hrow.get("return_pct"))
                        purchase_h = _safe_num(hrow.get("purchase_price"))
                        signal_h   = str(hrow.get("kubota_signal", ""))
                        try:
                            df_ph = load_price_history(code_h)
                        except Exception:
                            df_ph = pd.DataFrame()
                        with col_c:
                            if df_ph.empty:
                                st.warning(f"{code_h} データなし")
                            else:
                                df_ph["date"] = pd.to_datetime(df_ph["date"])
                                df_ph = df_ph.sort_values("date")
                                st.plotly_chart(
                                    _mini_candlestick(
                                        df_ph, code_h, name_h,
                                        pnl_pct=pnl_pct_h,
                                        purchase_px=purchase_h,
                                        signal=signal_h,
                                    ),
                                    use_container_width=True,
                                    config={"displayModeBar": False},
                                )
                                # 取得単価 / 現在値 / 差分バー
                                if purchase_h is not None:
                                    cur_h = float(df_ph.iloc[-1]["close"])
                                    dv    = cur_h - purchase_h
                                    dp    = dv / purchase_h * 100
                                    dc    = "#4ade80" if dv >= 0 else "#f87171"
                                    ds    = "+" if dv >= 0 else ""
                                    st.markdown(f"""
<div style="display:flex;border:1px solid #313244;border-top:none;
            border-radius:0 0 6px 6px;font-size:.75em;text-align:center;overflow:hidden;
            margin-top:-6px;">
  <div style="flex:1;padding:3px 2px;background:#1e1e2e;">
    <div style="color:#45475a;font-size:.85em;">取得単価</div>
    <div style="color:#a6adc8;font-weight:600;">¥{int(purchase_h):,}</div>
  </div>
  <div style="flex:1;padding:3px 2px;background:#1e1e2e;border-left:1px solid #313244;">
    <div style="color:#45475a;font-size:.85em;">現在値</div>
    <div style="color:#cdd6f4;font-weight:600;">¥{int(cur_h):,}</div>
  </div>
  <div style="flex:1;padding:3px 2px;background:{dc}18;border-left:1px solid #313244;">
    <div style="color:#45475a;font-size:.85em;">差分(円)</div>
    <div style="color:{dc};font-weight:700;">{ds}¥{int(abs(dv)):,}</div>
  </div>
  <div style="flex:1;padding:3px 2px;background:{dc}18;border-left:1px solid #313244;">
    <div style="color:#45475a;font-size:.85em;">差分(%)</div>
    <div style="color:{dc};font-weight:700;">{ds}{dp:.1f}%</div>
  </div>
</div>
""", unsafe_allow_html=True)
            if len(domestic_h) > 9:
                st.caption(
                    f"※ 保有銘柄 {len(domestic_h)} 件中、上位 9 件を表示。"
                    "全件は「💼 保有銘柄管理」タブをご確認ください。"
                )

    st.divider()

    # ═══════════════════════════════════════════════════
    # SECTION 4: 全銘柄ヒートマップ（セクター別ツリーマップ）
    # ═══════════════════════════════════════════════════
    st.markdown("### 🌡️ 全銘柄ヒートマップ")
    st.caption(
        "セクター別ツリーマップ。**セルサイズ＝売買代金**（大きいほど流動性が高い）、"
        "**色でシグナル状態・スコア**を確認できます。セルをクリックするとセクター内を拡大表示。"
    )

    if df_screening.empty:
        st.info("スクリーニングデータがありません。")
    else:
        hmap_mode = st.radio(
            "色の基準",
            ["シグナル状態", "窪田スコア", "成長株スコア"],
            horizontal=True,
            key="hmap_mode",
        )

        df_hm = df_screening.copy()
        # 売買代金が欠損の場合は最小値（0.1億円）で補完
        df_hm["_size"] = df_hm["avg_turnover_20d_oku"].fillna(0.1).clip(lower=0.1)

        # モード別に色の基準値を設定
        if hmap_mode == "シグナル状態":
            # 買いシグナル=10, 放れ待ち=4, その他=-3
            def _sig_num(sig: str) -> float:
                if sig == "買いシグナル":
                    return 10.0
                if "放れ待ち" in str(sig):
                    return 4.0
                return -3.0
            df_hm["_color"] = df_hm["kubota_signal"].apply(_sig_num)
            color_min, color_max = -3.0, 10.0
            # -3→赤, 4→黄(約0.54), 10→緑
            colorscale = [
                [0.00, "#f38ba8"],
                [0.54, "#f9e2af"],
                [1.00, "#a6e3a1"],
            ]
        elif hmap_mode == "窪田スコア":
            df_hm["_color"] = df_hm["kubota_trade_score"].fillna(0).astype(float)
            color_min, color_max = 0.0, 10.0
            colorscale = [[0.0, "#313244"], [1.0, "#a6e3a1"]]
        else:
            df_hm["_color"] = df_hm["growth_invest_score"].fillna(0).astype(float)
            color_min, color_max = 0.0, 29.0
            colorscale = [[0.0, "#313244"], [1.0, "#74c7ec"]]

        # ツリーマップ用データ（セクターノード + 銘柄ノード）
        df_hm["_sector"] = df_hm["sector33_name"].fillna("その他")
        sectors = df_hm["_sector"].unique().tolist()

        # セクター別の加重平均色を事前計算（売買代金加重）
        sec_avg_color: dict[str, float] = {}
        for _s in sectors:
            _sg = df_hm[df_hm["_sector"] == _s]
            _tw = _sg["_size"].sum()
            sec_avg_color[_s] = (
                (_sg["_color"] * _sg["_size"]).sum() / _tw
                if _tw > 0 else _sg["_color"].mean()
            )

        ids, labels, parents, values, node_colors, customdata, font_sizes = \
            [], [], [], [], [], [], []

        # セクターノード（親）
        for sec in sectors:
            ids.append(f"__sec__{sec}")
            labels.append(sec)
            parents.append("")
            values.append(0.0)
            node_colors.append(sec_avg_color.get(sec, 0.0))
            customdata.append(["", "-", 0, 0, "-"])
            font_sizes.append(11)

        # 銘柄ノード（子）
        for _, row in df_hm.iterrows():
            code  = str(row.get("code", ""))
            name  = str(row.get("company_name", ""))
            sig   = str(row.get("kubota_signal", "-"))
            ksc   = int(_safe_num(row.get("kubota_trade_score")) or 0)
            gsc   = int(_safe_num(row.get("growth_invest_score")) or 0)
            close = _safe_num(row.get("latest_close"))
            sec   = str(row.get("_sector", "その他"))
            size  = float(row.get("_size", 0.1))

            # ラベル: 銘柄コード + 会社名（短縮）を2行で表示
            short_name = name[:9] if len(name) > 9 else name
            ids.append(code)
            labels.append(f"{code}<br>{short_name}")
            parents.append(f"__sec__{sec}")
            values.append(size)
            node_colors.append(float(row.get("_color", 0.0)))
            customdata.append([
                name, sig, ksc, gsc,
                f"¥{int(close):,}" if close else "-",
            ])
            # フォントサイズ: 売買代金が大きいセルほど大きく
            fs = 12 if size >= 50 else (10 if size >= 15 else (9 if size >= 4 else 8))
            font_sizes.append(fs)

        fig_hm = go.Figure(go.Treemap(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="remainder",
            marker=dict(
                colors=node_colors,
                colorscale=colorscale,
                cmin=color_min,
                cmax=color_max,
                showscale=False,
                line=dict(width=1, color="#1e1e2e"),
            ),
            texttemplate="<b>%{label}</b>",
            hovertemplate=(
                "<b>%{customdata[0]}</b>  %{label}<br>"
                "シグナル: %{customdata[1]}<br>"
                "窪田スコア: %{customdata[2]}/10　　成長株: %{customdata[3]}/29<br>"
                "現在値: %{customdata[4]}<br>"
                "売買代金(20日均): %{value:.1f}億円"
                "<extra></extra>"
            ),
            customdata=customdata,
            tiling=dict(packing="squarify", pad=2),
            textfont=dict(color="#cdd6f4", size=font_sizes),
            pathbar=dict(
                visible=True,
                thickness=18,
                textfont=dict(color="#cdd6f4", size=10),
            ),
        ))
        fig_hm.update_layout(
            height=600,
            paper_bgcolor="#1e1e2e",
            plot_bgcolor="#1e1e2e",
            margin=dict(l=0, r=0, t=10, b=0),
        )

        # 凡例
        if hmap_mode == "シグナル状態":
            st.markdown(
                "<span style='background:#a6e3a1;color:#1e1e2e;padding:2px 10px;border-radius:4px;"
                "font-size:.82em;font-weight:700;margin-right:6px;'>■ 買いシグナル</span>"
                "<span style='background:#f9e2af;color:#1e1e2e;padding:2px 10px;border-radius:4px;"
                "font-size:.82em;margin-right:6px;'>■ 放れ待ち</span>"
                "<span style='background:#585b70;color:#cdd6f4;padding:2px 10px;border-radius:4px;"
                "font-size:.82em;'>■ その他</span>",
                unsafe_allow_html=True,
            )
        elif hmap_mode == "窪田スコア":
            st.caption("暗色 = スコア低（0点）→ 明緑 = スコア高（10点）")
        else:
            st.caption("暗色 = スコア低（0点）→ 明青緑 = スコア高（29点）")

        st.plotly_chart(fig_hm, use_container_width=True)


# ─────────────────────────────────────────
# メイン
# ─────────────────────────────────────────
def main():
    # ヘッダー
    st.markdown(
        "# 📈 株式モニタリング ダッシュボード",
        help="窪田フレームワーク × 成長株分析 | FIRE目標 ¥1億円"
    )

    with st.expander("🗺️ このダッシュボードの使い方（初めての方はここから）", expanded=False):
        st.markdown("""
**このダッシュボードでできること**
- 東証プライム全銘柄を毎日自動でスクリーニングし、買い時の銘柄を絞り込む
- 窪田剛「銘柄選びの教科書」の3条件（流動性・ボラティリティ・チャートパターン）をAIが自動判定
- 成長株の財務指標（売上成長率・ROE・ROICなど）を一覧で確認できる

**推奨の使い方フロー**
1. **相場フェーズを確認**（上部バナー） → BULL（強気）の時期だけ積極的に買いを検討
2. **「今日の注目銘柄」タブ**で今すぐ検討できる銘柄を確認
3. **「銘柄詳細・売買判定」タブ**で気になる銘柄を検索 → チェックリストで条件を確認
4. 条件が揃っていたら実際の株価チャートで最終確認してエントリー判断

**投資の注意事項** このシステムはあくまで投資の参考情報です。最終的な投資判断は自己責任でお願いします。
        """)

    # データ読み込み（失敗時は空 DataFrame）
    with st.spinner("BigQuery からデータを取得中..."):
        try:
            df_env = load_market_env()
        except Exception as e:
            st.error(f"相場環境データ取得エラー: {e}")
            df_env = pd.DataFrame()

        try:
            df_screening = load_screening()
        except Exception as e:
            st.error(f"スクリーニングデータ取得エラー: {e}")
            df_screening = pd.DataFrame(
                columns=["code", "company_name", "sector33_name",
                         "kubota_signal", "kubota_trade_score", "growth_invest_score"]
            )

        # 保有銘柄は評価額をサイドバーに渡すため先読み
        try:
            df_holdings = load_holdings()
        except Exception:
            df_holdings = pd.DataFrame()

    # 評価額合計（円→万円）をサイドバー用に計算
    total_current_value_man = 0
    if not df_holdings.empty and "current_value" in df_holdings.columns:
        total_current_value_man = int(df_holdings["current_value"].sum(skipna=True) / 10000)

    # サイドバー（フィルター）
    sel_sector, min_kubota, min_growth, sel_signal = render_sidebar(
        df_screening, total_current_value_man
    )

    # 相場環境バナー
    render_market_header(df_env)
    st.divider()

    # タブ
    tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📡 アクションボード",
        "💼 保有銘柄管理",
        "🏆 今日の注目銘柄",
        "🔍 銘柄詳細・売買判定",
        "📊 全銘柄スクリーニング",
        "📉 過去シグナル実績",
        "🌏 相場環境（TOPIX）",
    ])

    with tab0:
        render_tab_actionboard(df_env, df_screening, df_holdings)

    with tab1:
        render_tab_holdings(df_holdings)

    with tab2:
        market_phase = str(df_env.iloc[0].get("market_phase", "")) if not df_env.empty else ""
        render_tab_candidates(df_screening, market_phase)

    with tab3:
        render_tab_chart(df_screening)

    with tab4:
        render_tab_screening(df_screening, sel_sector, min_kubota, min_growth, sel_signal)

    with tab5:
        try:
            df_bt = load_backtest()
        except Exception as e:
            st.error(f"バックテストデータ取得エラー: {e}")
            df_bt = pd.DataFrame()
        render_tab_backtest(df_bt)

    with tab6:
        try:
            df_topix = load_topix_history()
        except Exception as e:
            st.error(f"TOPIXデータ取得エラー: {e}")
            df_topix = pd.DataFrame()
        render_tab_topix(df_topix)

    # フッター
    st.divider()
    st.caption(
        "📌 データソース: J-Quants API v2 (Standardプラン) / Google Cloud BigQuery  |  "
        "窪田剛「銘柄選びの教科書」フレームワーク  |  "
        f"© {datetime.now().year} 鬼塚 雄太"
    )


if __name__ == "__main__":
    main()
