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
            table_id: 完全テーブルID (project.dataset.table)
            write_disposition: WRITE_APPEND / WRITE_TRUNCATE
            schema: スキーマ定義（省略時は自動検出）

        Returns:
            書込み行数
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping load to {table_id}")
            return 0

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
    ) -> int:
        """MERGE方式でのUPSERT（重複回避）

        一時テーブルにデータを書込んでからMERGEする。

        Args:
            df: 書込むデータ
            target_table: ターゲットテーブルID (dataset.table)
            merge_keys: MERGE条件のカラム名リスト
            staging_table: 一時テーブル名（省略時は自動生成）

        Returns:
            処理行数
        """
        if df.empty:
            return 0

        full_target = f"{self.project}.{target_table}"
        staging = staging_table or f"{target_table}_staging"
        full_staging = f"{self.project}.{staging}"

        # 1. ステージングテーブルに書込み
        self.load_dataframe(df, staging, write_disposition="WRITE_TRUNCATE")

        # 2. MERGE実行
        on_clause = " AND ".join(
            [f"T.`{k}` = S.`{k}`" for k in merge_keys]
        )
        update_cols = [c for c in df.columns if c not in merge_keys and c != "_ingested_at"]
        update_clause = ", ".join([f"T.`{c}` = S.`{c}`" for c in update_cols])
        if update_clause:
            when_matched = f"UPDATE SET {update_clause}, T._ingested_at = CURRENT_TIMESTAMP()"
        else:
            when_matched = "UPDATE SET T._ingested_at = CURRENT_TIMESTAMP()"
        insert_cols = ", ".join([f"`{c}`" for c in df.columns])
        insert_vals = ", ".join([f"S.`{c}`" for c in df.columns])

        merge_sql = f"""
        MERGE `{full_target}` T
        USING `{full_staging}` S
        ON {on_clause}
        WHEN MATCHED THEN
          {when_matched}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}, _ingested_at)
          VALUES ({insert_vals}, CURRENT_TIMESTAMP())
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
        """テーブルの最新日付を取得"""
        full_table = f"{self.project}.{table_id}"
        try:
            sql = f"SELECT MAX({date_column}) as max_date FROM `{full_table}`"
            result = self.client.query(sql).to_dataframe()
            max_date = result["max_date"].iloc[0]
            if pd.isna(max_date):
                return None
            return str(max_date)
        except Exception:
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

