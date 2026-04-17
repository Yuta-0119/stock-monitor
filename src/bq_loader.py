"""BigQuery データローダー

データの書込み・MERGE・テーブル管理を担当。
"""
import logging
from typing import Any

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQLoader:
    """BigQuery書込みモジュール"""

    def __init__(self, project: str, location: str = "asia-northeast1"):
        self.project = project
        self.location = location
        self.client = bigquery.Client(project=project, location=location)

    def ensure_dataset(self, dataset_name: str) -> None:
        """データセットが存在しなければ作成する"""
        dataset_id = f"{self.project}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        self.client.create_dataset(dataset, exists_ok=True)
        logger.debug(f"Dataset ready: {dataset_id}")

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        schema: list[bigquery.SchemaField] | None = None,
    ) -> int:
        """DataFrameをBigQueryテーブルにロード

        Args:
            df: 書込むデータ
            table_id: テーブルID (dataset.table)
            write_disposition: WRITE_APPEND / WRITE_TRUNCATE
            schema: スキーマ定義（省略時は自動検出）

        Returns:
            書込み行数
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping load to {table_id}")
            return 0

        # データセットを自動作成（存在しない場合）
        parts = table_id.split(".")
        if len(parts) >= 2:
            self.ensure_dataset(parts[-2])

        full_table_id = f"{self.project}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
        )
        if schema:
            job_config.schema = schema

        logger.info(f"Loading {len(df)} rows to {full_table_id}")
        job = self.client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )
        job.result()  # Wait for completion

        logger.info(f"Loaded {job.output_rows} rows to {full_table_id}")
        return job.output_rows or len(df)

    def merge_dataframe(
        self,
        df: pd.DataFrame,
        target_table: str,
        merge_keys: list[str],
        staging_table: str | None = None,
        match_target_schema: bool = False,
    ) -> int:
        """MERGE方式でのUPSERT（重複回避）

        Args:
            df: 書込むデータ
            target_table: ターゲットテーブルID (dataset.table)
            merge_keys: MERGE条件のカラム名リスト
            staging_table: 一時テーブル名（省略時は自動生成）
            match_target_schema: True なら staging を target と同じカラム型で作成。
                pandas auto-detect だと float64 → BQ FLOAT64 になり、target が
                NUMERIC のとき MERGE で型衝突する。これを回避するため target
                スキーマを取得して staging に強制コピーする。
                既存呼び出し側に影響を与えないようデフォルトは False。

        Returns:
            処理行数
        """
        if df.empty:
            return 0

        # テーブルが存在しない場合は単純挿入（テーブルを自動作成）
        if not self.table_exists(target_table):
            logger.info(f"Table {target_table} not found, creating via WRITE_APPEND")
            return self.load_dataframe(df, target_table, write_disposition="WRITE_APPEND")

        full_target = f"{self.project}.{target_table}"
        staging = staging_table or f"{target_table}_staging"
        full_staging = f"{self.project}.{staging}"

        # 1. ステージングテーブルに書込み
        if match_target_schema:
            try:
                target_schema_full = self.client.get_table(full_target).schema
                df_cols = set(df.columns)
                # Restrict to columns actually present in df, preserving target column order
                matched_schema = [s for s in target_schema_full if s.name in df_cols]
                self.load_dataframe(
                    df, staging, write_disposition="WRITE_TRUNCATE", schema=matched_schema,
                )
            except Exception as e:
                logger.warning(
                    f"match_target_schema failed for {target_table}: {e} -- falling back to autodetect"
                )
                self.load_dataframe(df, staging, write_disposition="WRITE_TRUNCATE")
        else:
            self.load_dataframe(df, staging, write_disposition="WRITE_TRUNCATE")

        # 2. MERGE実行
        on_clause = " AND ".join(
            [f"T.`{k}` = S.`{k}`" for k in merge_keys]
        )
        update_cols = [c for c in df.columns if c not in merge_keys]
        insert_cols = ", ".join([f"`{c}`" for c in df.columns])
        insert_vals = ", ".join([f"S.`{c}`" for c in df.columns])

        if update_cols:
            update_clause = ", ".join([f"T.`{c}` = S.`{c}`" for c in update_cols])
            when_matched = f"UPDATE SET {update_clause}"
        else:
            # 全カラムがmerge_keyの場合: 重複行は無視（自己代入で実質no-op）
            when_matched = f"UPDATE SET T.`{merge_keys[0]}` = T.`{merge_keys[0]}`"

        merge_sql = f"""
        MERGE `{full_target}` T
        USING `{full_staging}` S
        ON {on_clause}
        WHEN MATCHED THEN
          {when_matched}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """

        logger.info(f"Executing MERGE into {full_target}")
        query_job = self.client.query(merge_sql)
        result = query_job.result()
        affected = query_job.num_dml_affected_rows or 0
        logger.info(f"MERGE completed: {affected} rows affected")

        # 3. ステージングテーブル削除
        self.client.delete_table(full_staging, not_found_ok=True)

        return affected

    def execute_sql(self, sql: str) -> pd.DataFrame:
        """SQLを実行して結果をDataFrameで返す"""
        logger.info(f"Executing SQL: {sql[:100]}...")
        return self.client.query(sql).to_dataframe()

    def execute_sql_file(self, filepath: str) -> None:
        """SQLファイルを実行"""
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()

        # セミコロンで分割して個別実行
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            logger.info(f"Executing: {stmt[:80]}...")
            job = self.client.query(stmt)
            job.result()
            logger.info("Done")

    def get_latest_date(self, table_id: str, date_column: str = "date") -> str | None:
        """Return MAX(date_column) as 'YYYY-MM-DD' or None if empty / errored.

        Uses the row iterator (not .to_dataframe()) to avoid the pandas-gbq /
        db-dtypes dependency chain that silently fails inside GitHub Actions
        runners. Failures are still logged so partial-progress is diagnosable
        instead of vanishing into a swallowed exception.
        """
        import logging as _lg
        _log = _lg.getLogger(__name__)
        full_table = f"{self.project}.{table_id}"
        try:
            sql = f"SELECT MAX({date_column}) AS max_date FROM `{full_table}`"
            rows = list(self.client.query(sql).result())
            if not rows:
                return None
            max_date = rows[0][0]  # value of MAX(...)
            if max_date is None:
                return None
            return str(max_date)[:10]  # 'YYYY-MM-DD' (handles date / datetime / Timestamp)
        except Exception as e:
            _log.warning("get_latest_date(%s, %s) failed: %s", table_id, date_column, e)
            return None

    def table_exists(self, table_id: str) -> bool:
        """テーブルの存在確認"""
        full_table = f"{self.project}.{table_id}"
        try:
            self.client.get_table(full_table)
            return True
        except Exception:
            return False

    def get_row_count(self, table_id: str) -> int:
        """テーブルの行数を取得"""
        full_table = f"{self.project}.{table_id}"
        sql = f"SELECT COUNT(*) as cnt FROM `{full_table}`"
        result = self.client.query(sql).to_dataframe()
        return int(result["cnt"].iloc[0])
