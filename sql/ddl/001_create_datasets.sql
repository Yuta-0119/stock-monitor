-- ============================================================
-- BigQuery データセット作成
-- 実行方法: bq コマンドまたは BigQuery Console から実行
-- ============================================================

-- 1. 生データ格納用
CREATE SCHEMA IF NOT EXISTS stock_raw
  OPTIONS (
    description = 'J-Quants APIから取得した生データ',
    location = 'asia-northeast1'
  );

-- 2. 分析用ビュー・中間テーブル
CREATE SCHEMA IF NOT EXISTS stock_analytics
  OPTIONS (
    description = '分析用ビュー・スコアリング結果',
    location = 'asia-northeast1'
  );

-- 3. マスターデータ
CREATE SCHEMA IF NOT EXISTS stock_master
  OPTIONS (
    description = '銘柄マスター・手動補完データ',
    location = 'asia-northeast1'
  );

-- 4. ポートフォリオ管理
CREATE SCHEMA IF NOT EXISTS portfolio
  OPTIONS (
    description = '保有銘柄・ウォッチリスト・資産推移',
    location = 'asia-northeast1'
  );
