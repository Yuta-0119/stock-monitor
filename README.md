# stock-monitor

国内株式モニタリング環境 — J-Quants API (Standard) + BigQuery + Looker Studio

## 前提条件

- [x] J-Quants Standardプラン契約済み・APIキー発行済み
- [x] Google Cloud プロジェクト作成済み
- [ ] GCPサービスアカウント作成
- [ ] BigQueryデータセット・テーブル作成
- [ ] GitHub リポジトリ作成・Secrets設定

---

## セットアップ手順

### Step 1: GCPサービスアカウント作成

BigQueryへの書込み権限を持つサービスアカウントを作成します。

```bash
# GCP CLIでプロジェクトを設定
gcloud config set project YOUR_PROJECT_ID

# サービスアカウント作成
gcloud iam service-accounts create stock-monitor \
  --display-name="Stock Monitor Pipeline"

# 必要な権限を付与
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:stock-monitor@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:stock-monitor@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# キーファイル生成（ローカル実行用）
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=stock-monitor@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

> **注意**: `sa-key.json` は `.gitignore` に含まれているのでGitにコミットされません。

### Step 2: ローカル環境セットアップ

```bash
# リポジトリをクローン
git clone https://github.com/YOUR_USERNAME/stock-monitor.git
cd stock-monitor

# Python仮想環境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 依存パッケージ
pip install -r requirements.txt

# 環境変数ファイル
cp .env.example .env
# .env を編集して実際の値を設定
```

`.env` に以下を設定:
```
JQUANTS_API_KEY=あなたのAPIキー
BQ_PROJECT=あなたのGCPプロジェクトID
GOOGLE_APPLICATION_CREDENTIALS=./sa-key.json
```

### Step 3: API疎通確認

```bash
# 環境変数を読み込んで実行
export $(cat .env | xargs)
python -m src.main --mode check
```

`✅ J-Quants API接続OK` が表示されれば成功。

### Step 4: BigQueryテーブル作成

```bash
python -m src.main --mode setup
```

以下の4データセット・全テーブルが作成されます:
- `stock_raw` (9テーブル)
- `stock_master` (2テーブル)
- `stock_analytics` (ビュー — Phase 3で追加)
- `portfolio` (4テーブル)

### Step 5: 初期データロード

```bash
python -m src.main --mode init
```

Standard プランの過去10年分のデータを一括取込みします。
所要時間: 約30〜60分（CSV一括ダウンロード利用時）。

### Step 6: GitHubリポジトリ設定

```bash
# リポジトリ作成（GitHub CLIの場合）
gh repo create stock-monitor --private

# Secrets設定
gh secret set JQUANTS_API_KEY --body "あなたのAPIキー"
gh secret set BQ_PROJECT --body "あなたのGCPプロジェクトID"
gh secret set GCP_SA_KEY < sa-key.json

# プッシュ
git add -A
git commit -m "initial commit"
git push -u origin main
```

### Step 7: 動作確認

GitHub Actions の `Daily Stock Data Ingestion` ワークフローを手動実行:

1. GitHub リポジトリ → Actions タブ
2. `Daily Stock Data Ingestion` を選択
3. `Run workflow` → mode: `daily` → 実行

---

## 使い方

### 日次バッチ（自動）
- **平日 18:00 JST** に自動実行（GitHub Actions）
- 株価・財務・指数・空売りデータを更新

### 週次バッチ（自動）
- **毎週土曜 10:00 JST** に自動実行
- 信用取引残高・投資部門別売買動向を更新

### 手動実行
```bash
# 日次
python -m src.main --mode daily

# 週次
python -m src.main --mode weekly

# バックフィル（期間指定）
python -m src.main --mode backfill --from 20240101 --to 20240630
```

---

## プロジェクト構成

```
stock-monitor/
├── .github/workflows/
│   ├── daily_ingest.yml       # 日次バッチ
│   └── weekly_ingest.yml      # 週次バッチ
├── src/
│   ├── config.py              # 設定管理
│   ├── jquants_client.py      # J-Quants API v2 クライアント
│   ├── bq_loader.py           # BigQuery書込み
│   ├── ingest/
│   │   ├── equity_master.py   # 銘柄マスター
│   │   ├── daily_quotes.py    # 株価OHLC
│   │   ├── financial_summary.py # 財務サマリー
│   │   ├── index_data.py      # TOPIX・指数
│   │   └── market_data.py     # 信用取引・空売り・投資部門別
│   └── main.py                # CLIエントリポイント
├── sql/
│   ├── ddl/                   # テーブル定義
│   └── views/                 # 分析ビュー（Phase 3で追加）
├── requirements.txt
└── .env.example
```

---

## 今後の開発ロードマップ

| Phase | 内容 | 状態 |
|-------|------|------|
| 0 | 環境構築・API疎通 | ← 今ここ |
| 1 | DDL作成・初期ロード | |
| 2 | 日次パイプライン稼働 | |
| 3 | 分析ビュー構築（スクリーニング・スコアリング） | |
| 4 | Looker Studio ダッシュボード | |
| 5 | ポートフォリオ連携 | |
| 6 | アラート機能 | |
