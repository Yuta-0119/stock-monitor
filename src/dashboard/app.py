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
          market_phase, kubota_signal, screening_status,
          next_earnings_date, days_to_earnings
        FROM `onitsuka-app.analytics.integrated_score`
        WHERE screening_status = 'ACTIVE'
        ORDER BY kubota_trade_score DESC, growth_invest_score DESC
        LIMIT 300
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_backtest() -> pd.DataFrame:
    return _bq("""
        SELECT
          signal_date, code, company_name,
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
    return _bq(f"""
        SELECT date, open, high, low, close, volume, turnover_value
        FROM `onitsuka-app.stock_raw.daily_quotes`
        WHERE code = '{code}'
          AND date >= DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 400 DAY)
        ORDER BY date ASC
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
def phase_html(phase: str) -> str:
    cls = {"BULL": "badge-bull", "BEAR": "badge-bear"}.get(phase, "badge-neutral")
    label = {"BULL": "🐂 BULL（強気）", "BEAR": "🐻 BEAR（弱気）"}.get(phase, "🟡 NEUTRAL（中立）")
    return f'<span class="{cls}">{label}</span>'


def _score_color(val, max_val: float, hue: int = 120) -> str:
    """スコアを背景色 hsl に変換"""
    pct = min(float(val) / max_val, 1.0) if max_val else 0
    sat = int(pct * 65)
    lig = int(90 - pct * 22)
    return f"background-color: hsl({hue},{sat}%,{lig}%); font-weight: 700"


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
            lambda v: "background-color:#a6e3a1;font-weight:700;color:#1e1e2e" if v == "ENTRY SIGNAL"
            else ("background-color:#f9e2af;color:#1e1e2e" if "WATCH" in str(v) else "")
        )
    if "決算(日)" in df.columns:
        styles["決算(日)"] = df["決算(日)"].apply(
            lambda v: "background-color:#f38ba8;font-weight:700;color:#1e1e2e" if pd.notna(v) and str(v) not in ("", "nan") and float(v) <= 10
            else ("background-color:#fab387;color:#1e1e2e" if pd.notna(v) and str(v) not in ("", "nan") and float(v) <= 20 else "")
        )
    return styles


def _candlestick_fig(df: pd.DataFrame, title: str, show_ma: bool = True) -> go.Figure:
    """ローソク足チャートを生成"""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.75, 0.25],
        shared_xaxes=True,
        vertical_spacing=0.03,
    )
    # ローソク足
    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        name="株価",
        increasing_line_color="#a6e3a1",
        decreasing_line_color="#f38ba8",
        increasing_fillcolor="#a6e3a1",
        decreasing_fillcolor="#f38ba8",
    ), row=1, col=1)

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
    )
    fig.update_yaxes(gridcolor="#313244", showgrid=True)
    return fig


# ─────────────────────────────────────────
# 用語解説・買い売り判定パネル
# ─────────────────────────────────────────

GLOSSARY = {
    "窪田スコア（0〜10点）": "「銘柄選びの教科書」著者・窪田剛氏のフレームワークに基づくトレード適性スコア。ボラティリティ（値動きの激しさ）とチャートの形状から算出。10点に近いほどトレードチャンス。",
    "成長株スコア（0〜29点）": "売上・利益の成長率（CAGR）、ROE（自己資本利益率）、ROIC（投下資本利益率）、PER・PBRのバリュエーションを総合したスコア。29点に近いほど優良成長株。",
    "ENTRY SIGNAL（買いシグナル）": "5つの条件（相場上昇・ATR≥1.5%・HV収縮・レンジ収縮・出来高急増）がすべて揃った銘柄。積極的なエントリーを検討できる状態。",
    "WATCH（放れ待ち）": "コンソリデーション（株価が一定レンジに収まった煮詰まり状態）に入っている銘柄。ブレイクアウト（価格が放れるタイミング）を待つ状態。",
    "ATR（平均真値幅）": "過去14日間の平均的な1日の値動き幅。ATR%が1.5%以上あると「十分な値動き」と判定。小さすぎると利益が取りにくい。",
    "HV収縮": "過去20日のボラティリティ（HV20）が過去60日（HV60）より小さい状態。株価が静まり返っている時期で、ブレイクアウト前の特徴。",
    "MA200（200日移動平均線）": "過去200営業日の終値の平均値。長期トレンドの目安。株価がMA200の上方にあり、MA200が右肩上がりであれば長期上昇トレンド。",
    "売買代金（億円）": "1日に取引された金額の20日平均。10億円以上あると十分な流動性（売買のしやすさ）があると判断。",
    "ROE（自己資本利益率）": "会社が株主から預かったお金（自己資本）をどれだけ効率よく使って利益を出しているか。15%以上が優良。",
    "ROIC（投下資本利益率）": "事業に投下した資金全体（借金+自己資本）に対してどれだけ利益を生んでいるか。12%以上が目安。",
    "売上CAGR（3年成長率）": "3年間の年平均売上成長率。15%以上あると高成長企業と判断。",
    "PER（株価収益率）": "株価÷1株当たり利益。同業他社比で割安かどうかの目安。低いほど割安とされるが成長株は高めになりやすい。",
    "PBR（株価純資産倍率）": "株価÷1株純資産。1倍割れは「解散価値以下」とされ割安の目安。",
    "相場フェーズ（BULL/BEAR/NEUTRAL）": "BULL=強気相場（買いに有利）、BEAR=弱気相場（買い控えを推奨）、NEUTRAL=中立。TOPIXのMA200トレンドから判定。",
}


def _glossary_expander():
    with st.expander("📖 用語集・指標の見方（わからない言葉はここで確認）"):
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
    """銘柄の買い時・売り時判定パネル"""
    signal = str(stock.get("kubota_signal", "-"))
    days_to_earn = pd.to_numeric(stock.get("days_to_earnings"), errors="coerce")

    # ── 総合判定バッジ ──
    if signal == "ENTRY SIGNAL":
        st.markdown("""
        <div style="background:#a6e3a1;color:#1e1e2e;border-radius:12px;padding:14px 20px;
                    text-align:center;font-size:1.3em;font-weight:700;margin-bottom:12px;">
            🟢 今すぐ買い検討できる状態（ENTRY SIGNAL）
        </div>""", unsafe_allow_html=True)
        st.caption("5つの条件がすべて揃っています。損切りラインを設定した上でエントリーを検討してください。")
    elif "WATCH" in signal:
        st.markdown("""
        <div style="background:#f9e2af;color:#1e1e2e;border-radius:12px;padding:14px 20px;
                    text-align:center;font-size:1.3em;font-weight:700;margin-bottom:12px;">
            🟡 放れ待ち（WATCH）― もう少し待つタイミング
        </div>""", unsafe_allow_html=True)
        st.caption("株価が煮詰まっています。出来高を伴ったブレイクアウト（価格が上放れ）を確認してからエントリーを検討。")
    else:
        st.markdown("""
        <div style="background:#45475a;color:#cdd6f4;border-radius:12px;padding:14px 20px;
                    text-align:center;font-size:1.3em;font-weight:700;margin-bottom:12px;">
            ⚪ まだ条件未達 ― 監視継続
        </div>""", unsafe_allow_html=True)
        st.caption("買いの条件が揃っていません。スコアが改善するまで監視を続けてください。")

    # ── 決算アラート ──
    if pd.notna(days_to_earn) and days_to_earn <= 20:
        color = "#f38ba8" if days_to_earn <= 10 else "#fab387"
        st.markdown(
            f'<div style="background:{color};color:#1e1e2e;border-radius:8px;padding:8px 14px;'
            f'font-weight:700;margin-bottom:10px;">⚠️ 決算発表まで約{int(days_to_earn)}日 '
            f'― 決算またぎは値動きが大きくなるため注意</div>',
            unsafe_allow_html=True,
        )

    # ── 条件チェックリスト ──
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
        st.markdown("**📈 相場・チャート条件**")
        check(market_phase == "BULL",
              "相場は上昇トレンド（BULL）",
              f"相場は{'下落' if market_phase == 'BEAR' else '中立'}（BULL でない）")
        check(str(stock.get("ma200_trend", "")) == "UP",
              "MA200 が右肩上がり（長期上昇トレンド）",
              "MA200 が横ばい or 下向き")
        check(str(stock.get("price_vs_ma200", "")) == "ABOVE",
              "株価が MA200 の上にある",
              "株価が MA200 を下回っている")
        check(bool(stock.get("consolidation", False)),
              "レンジ収縮中（煮詰まり状態）",
              "まだレンジ収縮していない")
        check(bool(stock.get("volume_surge", False)),
              "出来高急増（平均の1.5倍以上）",
              "出来高はまだ平均的")

    with col2:
        st.markdown("**💰 ファンダメンタル条件**")
        liq = str(stock.get("liquidity_grade", ""))
        check(liq in ("PASS_A", "PASS_B", "PASS_C"),
              f"流動性OK（{liq}）― 売買しやすい",
              "流動性不足 ― 売買しにくい可能性あり")
        sales_cagr = pd.to_numeric(stock.get("sales_cagr_3y_pct"), errors="coerce")
        check(pd.notna(sales_cagr) and sales_cagr >= 5,
              f"売上成長中（{sales_cagr:.1f}%/年）",
              f"売上成長が低い（{sales_cagr:.1f}%）" if pd.notna(sales_cagr) else "売上データなし")
        roe = pd.to_numeric(stock.get("roe_pct"), errors="coerce")
        check(pd.notna(roe) and roe >= 10,
              f"ROE 良好（{roe:.1f}%）",
              f"ROE が低い（{roe:.1f}%）" if pd.notna(roe) else "ROEデータなし")
        check(str(stock.get("financial_health", "")) == "PASS",
              "財務健全",
              "財務に注意あり")
        check(pd.notna(days_to_earn) and days_to_earn > 20 if pd.notna(days_to_earn) else True,
              f"決算まで余裕あり（{int(days_to_earn)}日後）" if pd.notna(days_to_earn) else "決算日不明",
              f"⚠ 決算が近い（{int(days_to_earn)}日後）― リスク注意",
              warn=pd.notna(days_to_earn) and days_to_earn <= 20)

    # ── スコアサマリー ──
    st.markdown("#### 📊 スコアサマリー")
    ksc = pd.to_numeric(stock.get("kubota_trade_score"), errors="coerce")
    gsc = pd.to_numeric(stock.get("growth_invest_score"), errors="coerce")
    c1, c2 = st.columns(2)
    with c1:
        st.metric(
            "窪田スコア",
            f"{ksc:.0f} / 10" if pd.notna(ksc) else "N/A",
            help="チャート形状・ボラティリティの総合点。7点以上が狙い目。",
        )
        bar_k = int(ksc / 10 * 100) if pd.notna(ksc) else 0
        st.markdown(
            f'<div style="background:#313244;border-radius:6px;height:10px;">'
            f'<div style="width:{bar_k}%;height:100%;border-radius:6px;background:#a6e3a1;"></div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.metric(
            "成長株スコア",
            f"{gsc:.0f} / 29" if pd.notna(gsc) else "N/A",
            help="売上CAGR・ROE・ROIC・PER・PBRの総合点。15点以上が狙い目。",
        )
        bar_g = int(gsc / 29 * 100) if pd.notna(gsc) else 0
        st.markdown(
            f'<div style="background:#313244;border-radius:6px;height:10px;">'
            f'<div style="width:{bar_g}%;height:100%;border-radius:6px;background:#74c7ec;"></div></div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────
# サイドバー
# ─────────────────────────────────────────
def render_sidebar(df_screening: pd.DataFrame):
    with st.sidebar:
        st.markdown("## 📈 Stock Monitor")
        st.caption("FIRE 目標 ¥1億円 | 窪田フレームワーク")

        st.divider()

        # FIRE 進捗（手動入力）
        st.markdown("### 💰 FIRE 進捗")
        current_assets = st.number_input(
            "現在の総資産（万円）", min_value=0, max_value=20000,
            value=st.session_state.get("fire_assets", 1000),
            step=50, key="fire_assets"
        )
        target = 10000  # 1億円
        pct = min(current_assets / target * 100, 100)
        st.markdown(f"""
        <div class="kpi-label">達成率 {pct:.1f}% （目標 ¥{target:,}万円）</div>
        <div class="fire-track">
          <div class="fire-fill" style="width:{pct}%"></div>
        </div>
        <div class="kpi-label">残り ¥{max(target - current_assets, 0):,}万円</div>
        """, unsafe_allow_html=True)

        st.divider()

        # フィルター
        st.markdown("### 🔎 銘柄絞り込み")
        sectors = ["すべて"] + sorted(
            df_screening["sector33_name"].dropna().unique().tolist()
        ) if not df_screening.empty else ["すべて"]
        sel_sector = st.selectbox("セクター", sectors,
            help="特定の業種に絞って表示します。")

        min_kubota = st.slider("窪田スコア（最低）", 0, 10, 0,
            help="チャート・ボラティリティのスコア。7以上に絞ると有望銘柄のみ表示。")
        min_growth = st.slider("成長株スコア（最低）", 0, 29, 0,
            help="売上成長・ROE・PERなどのスコア。15以上に絞ると優良成長株のみ表示。")
        sig_opts = ["すべて", "ENTRY SIGNAL", "WATCH（放れ待ち）"]
        sel_signal = st.selectbox("シグナル", sig_opts,
            help="「ENTRY SIGNAL」＝今すぐ買い検討できる銘柄。「WATCH」＝ブレイクアウト待ちの銘柄。")

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
        st.metric("TOPIX（東証全体の指数）", f"{topix_close:,.2f}",
                  delta=f"{diff:+.2f} vs MA200（200日平均）",
                  help="東証プライム全銘柄の加重平均株価指数。相場全体の動きを示す。")
    with c3:
        st.metric("TOPIX MA200（長期平均）", f"{topix_ma200:,.2f}",
                  help="過去200営業日のTOPIX平均値。これが右肩上がりで株価がMA200の上にあれば強気相場。")
    with c4:
        st.metric("環境スコア", f"{env_score}/3", delta=env_label,
                  help="3/3=強気（積極的に買える）、2/3=中立、1/3=弱気（買い控えを推奨）")
    with c5:
        above = "MA200 上方 ✅" if topix_close > topix_ma200 else "MA200 下方 ⚠️"
        st.metric("現在位置", above,
                  help="株価がMA200の上にあれば上昇トレンド継続。下にあれば注意が必要。")


def render_tab_screening(df: pd.DataFrame, sel_sector, min_kubota, min_growth, sel_signal):
    st.subheader("🔍 スクリーニング結果")

    st.info(
        "**見方のポイント**　"
        "🟢 シグナル列が「ENTRY SIGNAL」の銘柄は買いの5条件が揃っています。"
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
    c2.metric("ENTRY SIGNAL", f"{(df_f['kubota_signal'] == 'ENTRY SIGNAL').sum()} 件")
    c3.metric("WATCH", f"{df_f['kubota_signal'].str.contains('WATCH', na=False).sum()} 件")
    c4.metric(
        "平均窪田スコア",
        f"{df_f['kubota_trade_score'].mean():.1f}" if not df_f.empty else "N/A"
    )

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
            "PBR": st.column_config.NumberColumn("PBR", format="%.2f"),
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
                entry_cnt=("kubota_signal", lambda x: (x == "ENTRY SIGNAL").sum()),
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


def render_tab_signals(df: pd.DataFrame):
    st.subheader("🚀 エントリーシグナル銘柄")

    st.info(
        "**ENTRY SIGNAL** ＝ 買いの5条件が揃っている銘柄。損切りラインを決めてからエントリーを検討してください。\n\n"
        "**WATCH（放れ待ち）** ＝ 株価が一定レンジに収まっている銘柄。出来高を伴ってレンジを上抜けたタイミングで買いを検討。",
        icon="🚀",
    )

    df_entry = df[df["kubota_signal"] == "ENTRY SIGNAL"].copy()
    df_watch = df[df["kubota_signal"].str.contains("WATCH", na=False)].copy()

    c1, c2 = st.columns(2)
    c1.metric("🟢 今すぐ買い検討（ENTRY SIGNAL）", f"{len(df_entry)} 銘柄",
              help="5条件すべて揃った銘柄数")
    c2.metric("🟡 ブレイクアウト待ち（WATCH）", f"{len(df_watch)} 銘柄",
              help="煮詰まり状態で放れ待ちの銘柄数")

    SIGNAL_COLS = {
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
        "days_to_earnings": "決算(日)",
        "next_earnings_date": "次回決算",
    }

    col_cfg = {
        "終値": st.column_config.NumberColumn(format="¥%,.0f"),
        "PER": st.column_config.NumberColumn(format="%.1f"),
        "売上CAGR%": st.column_config.NumberColumn(format="%.1f"),
        "ROE%": st.column_config.NumberColumn(format="%.1f"),
    }

    if not df_entry.empty:
        st.markdown("#### 🟢 ENTRY SIGNAL")
        avail = [c for c in SIGNAL_COLS if c in df_entry.columns]
        st.dataframe(
            df_entry[avail].rename(columns=SIGNAL_COLS),
            use_container_width=True,
            column_config=col_cfg,
            height=min(400, (len(df_entry) + 3) * 38),
        )
    else:
        st.info("現在 ENTRY SIGNAL 銘柄はありません。")

    if not df_watch.empty:
        st.markdown("#### 🟡 WATCH（放れ待ち）")
        avail = [c for c in SIGNAL_COLS if c in df_watch.columns]
        st.dataframe(
            df_watch[avail].rename(columns=SIGNAL_COLS),
            use_container_width=True,
            column_config=col_cfg,
            height=min(400, (len(df_watch) + 3) * 38),
        )
    else:
        st.info("現在 WATCH 銘柄はありません。")

    # 決算接近アラート
    df_alert = df[
        df["days_to_earnings"].notna() &
        (df["days_to_earnings"].astype(str) != "nan") &
        (pd.to_numeric(df["days_to_earnings"], errors="coerce") <= 20)
    ].copy()
    if not df_alert.empty:
        st.divider()
        st.markdown("#### ⚠️ 決算20日以内（要注意）")
        df_alert["days_to_earnings"] = pd.to_numeric(df_alert["days_to_earnings"], errors="coerce")
        df_alert = df_alert.sort_values("days_to_earnings")
        st.dataframe(
            df_alert[["code", "company_name", "sector33_name",
                       "kubota_signal", "days_to_earnings", "next_earnings_date",
                       "latest_close"]].rename(columns={
                "code": "コード", "company_name": "会社名",
                "sector33_name": "セクター", "kubota_signal": "シグナル",
                "days_to_earnings": "決算まで(日)", "next_earnings_date": "次回決算",
                "latest_close": "終値",
            }),
            use_container_width=True,
        )


def render_tab_chart(df_screening: pd.DataFrame):
    st.subheader("📌 銘柄詳細・売買タイミング判定")

    if df_screening.empty:
        st.warning("スクリーニングデータがありません。")
        return

    st.info(
        "**使い方** ① 下のボックスで銘柄を選択 → ② チャートと買い時・売り時チェックリストを確認 → ③ 条件が揃っているか判断",
        icon="📌",
    )

    # ── 銘柄検索（コード or 会社名で絞れるよう選択肢を工夫）
    search = st.text_input("🔍 銘柄コード or 会社名で絞り込み（例: 7203、トヨタ）", value="", key="chart_search")
    if search:
        mask = (
            df_screening["code"].str.contains(search, case=False, na=False) |
            df_screening["company_name"].str.contains(search, case=False, na=False)
        )
        df_sel = df_screening[mask]
    else:
        df_sel = df_screening

    if df_sel.empty:
        st.warning("該当する銘柄が見つかりません。")
        return

    options = df_sel.apply(
        lambda r: f"{r['code']}  {r['company_name']}  ({r['sector33_name']})", axis=1
    ).tolist()
    selected = st.selectbox("銘柄を選択", options, key="chart_sel")
    sel_code = selected.split()[0].strip()

    stock = df_screening[df_screening["code"] == sel_code].iloc[0]
    market_phase = str(stock.get("market_phase", ""))

    # ── 買い時・売り時判定パネル ──
    st.divider()
    _buy_sell_panel(stock, market_phase)
    st.divider()

    # 銘柄情報カード
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("現在値", f"¥{stock['latest_close']:,.0f}", help="直近取引日の終値")
    c2.metric("窪田スコア", f"{stock['kubota_trade_score']}/10", help="7点以上が買いの目安")
    c3.metric("成長株スコア", f"{stock['growth_invest_score']}/29", help="15点以上が優良成長株の目安")
    c4.metric("PER", f"{stock['per']:.1f}x" if pd.notna(stock['per']) else "N/A")
    c5.metric(
        "シグナル",
        "🟢 ENTRY" if stock["kubota_signal"] == "ENTRY SIGNAL"
        else ("🟡 WATCH" if "WATCH" in str(stock["kubota_signal"]) else "➖"),
    )
    c6.metric(
        "次回決算",
        f"{stock.get('days_to_earnings', 'N/A')}日後"
        if pd.notna(stock.get("days_to_earnings")) else "N/A"
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
    fig = _candlestick_fig(
        df_price,
        title=f"{sel_code}  {stock['company_name']}  [{stock['sector33_name']}]",
        show_ma=True,
    )

    # 窪田スコア注釈
    kubota_s = stock["kubota_trade_score"]
    growth_s = stock["growth_invest_score"]
    fig.add_annotation(
        xref="paper", yref="paper",
        x=0.01, y=0.97,
        text=f"窪田スコア: <b>{kubota_s}/10</b>  成長株スコア: <b>{growth_s}/29</b>",
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
            ("PBR", "pbr", "{:.2f}x"),
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
        delta=f"平均 {df_bt['return_5d_pct'].mean():.2f}%",
    )
    c3.metric(
        "勝率（10日後）",
        f"{df_bt['win_10d'].mean() * 100:.1f}%",
        delta=f"平均 {df_bt['return_10d_pct'].mean():.2f}%",
    )
    c4.metric(
        "勝率（20日後）",
        f"{df_bt['win_20d'].mean() * 100:.1f}%",
        delta=f"平均 {df_bt['return_20d_pct'].mean():.2f}%",
    )

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("最大利益（20日）", f"{df_bt['return_20d_pct'].max():.2f}%")
    c6.metric("最大損失（20日）", f"{df_bt['return_20d_pct'].min():.2f}%")
    c7.metric("標準偏差（20日）", f"{df_bt['return_20d_pct'].std():.2f}%")
    c8.metric(
        "プロフィットファクター",
        f"{df_bt[df_bt['return_20d_pct'] > 0]['return_20d_pct'].sum() / abs(df_bt[df_bt['return_20d_pct'] < 0]['return_20d_pct'].sum()):.2f}"
        if df_bt['return_20d_pct'].min() < 0 else "∞",
    )

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
        text=df_bt["company_name"],
        hovertemplate="<b>%{text}</b><br>シグナル日: %{x|%Y-%m-%d}<br>20日後: %{y:.2f}%<extra></extra>",
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
            df_bt[["signal_date", "code", "company_name",
                   "return_5d_pct", "return_10d_pct", "return_20d_pct",
                   "win_5d", "win_10d", "win_20d",
                   "atr_pct", "hv_20d_pct"]].rename(columns={
                "signal_date": "シグナル日",
                "code": "コード",
                "company_name": "会社名",
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
                "5日後%": st.column_config.NumberColumn(format="%.2f"),
                "10日後%": st.column_config.NumberColumn(format="%.2f"),
                "20日後%": st.column_config.NumberColumn(format="%.2f"),
            },
        )


def render_tab_topix(df_topix: pd.DataFrame):
    st.subheader("📈 TOPIX 推移")

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
    df_t = df_topix.tail(period_map[period_label]).copy()

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
2. **「エントリーシグナル」タブ**で今すぐ検討できる銘柄を確認
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

    # サイドバー（フィルター）
    sel_sector, min_kubota, min_growth, sel_signal = render_sidebar(df_screening)

    # 相場環境バナー
    render_market_header(df_env)
    st.divider()

    # タブ
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔍 銘柄スクリーニング",
        "🚀 買いシグナル銘柄",
        "📌 銘柄詳細・売買判定",
        "📉 過去シグナル検証",
        "📈 相場全体（TOPIX）",
    ])

    with tab1:
        render_tab_screening(df_screening, sel_sector, min_kubota, min_growth, sel_signal)

    with tab2:
        render_tab_signals(df_screening)

    with tab3:
        render_tab_chart(df_screening)

    with tab4:
        try:
            df_bt = load_backtest()
        except Exception as e:
            st.error(f"バックテストデータ取得エラー: {e}")
            df_bt = pd.DataFrame()
        render_tab_backtest(df_bt)

    with tab5:
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
