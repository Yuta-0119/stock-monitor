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

        # 1. ステージングテーブルに書込み (autodetect)
        # pandas float64 -> BQ FLOAT64 etc. The MERGE step below SAFE_CASTs back
        # to the target column types, which avoids the pyarrow limitation that
        # cannot directly convert pandas float64 -> BQ NUMERIC during load.
        self.load_dataframe(df, staging, write_disposition="WRITE_TRUNCATE")

        # If match_target_schema requested, fetch target column types so the
        # MERGE SQL can SAFE_CAST FLOAT staging values into NUMERIC target slots.
        col_types: dict[str, str] = {}
        if match_target_schema:
            try:
                target_schema_full = self.client.get_table(full_target).schema
                col_types = {s.name: s.field_type for s in target_schema_full}
            except Exception as e:
                logger.warning(
                    f"match_target_schema schema fetch failed for {target_table}: {e}"
                )

        # 2. MERGE実行
        # Helper: wrap S.<col> in SAFE_CAST when target type needs it.
        # Common float64 -> NUMERIC / BIGNUMERIC conversion happens here.
        _CAST_TYPES = {"NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"}
        def _src_ref(col: str) -> str:
            t = col_types.get(col, "")
            if t in _CAST_TYPES:
                return f"SAFE_CAST(S.`{col}` AS {t})"
            return f"S.`{col}`"

        on_clause = " AND ".join(
            [f"T.`{k}` = {_src_ref(k)}" for k in merge_keys]
        )

        # Partition-filter hint for tables with require_partition_filter=true.
        # BigQuery cannot infer partition elimination from `T.date = S.date`
        # alone (S.date is unknown at planning time), so for DATE-partitioned
        # targets we inject a static BETWEEN range derived from the staged df.
        # Harmless when target is not partition-filter-enforced — just gives BQ
        # a partition-prune hint that also slightly speeds up the MERGE.
        # Observed failure (2026-04-20 GitHub Actions run 24660120082):
        #   BadRequest: Cannot query over table
        #   'onitsuka-app.raw.stock_prices_daily' without a filter over
        #   column(s) 'date' that can be used for partition elimination
        partition_cols_candidates = [
            "date", "trade_date", "disclosed_date",
            "snapshot_time", "pub_date", "published_date",
        ]
        for pc in partition_cols_candidates:
            if pc in merge_keys and pc in df.columns and not df[pc].isna().all():
                try:
                    series = pd.to_datetime(df[pc], errors="coerce").dropna()
                    if series.empty:
                        continue
                    dmin = series.min().strftime("%Y-%m-%d")
                    dmax = series.max().strftime("%Y-%m-%d")
                    on_clause += (
                        f" AND T.`{pc}` BETWEEN DATE '{dmin}' AND DATE '{dmax}'"
                    )
                    logger.info(
                        f"Added partition filter: T.{pc} BETWEEN "
                        f"{dmin} AND {dmax}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not derive partition filter for {pc}: {e}"
                    )
                break  # only one partition column per table

        update_cols = [c for c in df.columns if c not in merge_keys]
        insert_cols = ", ".join([f"`{c}`" for c in df.columns])
        insert_vals = ", ".join([_src_ref(c) for c in df.columns])

        if update_cols:
            update_clause = ", ".join([f"T.`{c}` = {_src_ref(c)}" for c in update_cols])
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

        Strategy (2026-04-20 update):
          1. First try INFORMATION_SCHEMA.PARTITIONS — this does NOT scan data
             and is not affected by require_partition_filter=true. Works for
             any DATE/DAY-partitioned table including stock_prices_daily,
             stock_prices_minute, financial_summary.
          2. Fall back to `SELECT MAX(date_column)` for non-partitioned tables
             or when the partition introspection fails.

        Failures are logged so partial-progress is diagnosable instead of
        vanishing into a swallowed exception.
        """
        import logging as _lg
        _log = _lg.getLogger(__name__)

        # Split table_id into dataset.table — INFORMATION_SCHEMA needs these
        parts = table_id.split(".")
        if len(parts) != 2:
            _log.warning("get_latest_date: expected 'dataset.table', got %r", table_id)
            return None
        dataset, table_name = parts

        # Path 1: INFORMATION_SCHEMA.PARTITIONS (metadata-only, no data scan)
        try:
            sql = f"""
            SELECT MAX(PARSE_DATE('%Y%m%d', partition_id)) AS max_date
            FROM `{self.project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = @table_name
              AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                ]
            )
            rows = list(self.client.query(sql, job_config=job_config).result())
            if rows and rows[0][0] is not None:
                return str(rows[0][0])[:10]
        except Exception as e:
            _log.debug(
                "INFORMATION_SCHEMA.PARTITIONS lookup failed for %s (%s) — "
                "falling back to SELECT MAX()", table_id, e,
            )

        # Path 2: fallback for non-partitioned tables or metadata-unavailable
        full_table = f"{self.project}.{table_id}"
        try:
            sql = f"SELECT MAX({date_column}) AS max_date FROM `{full_table}`"
            rows = list(self.client.query(sql).result())
            if not rows:
                return None
            max_date = rows[0][0]
            if max_date is None:
                return None
            return str(max_date)[:10]
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
