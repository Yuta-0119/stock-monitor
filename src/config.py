"""設定管理モジュール"""
import os
from dataclasses import dataclass


@dataclass
class Config:
    """アプリケーション設定"""
    # J-Quants API
    jquants_api_key: str
    jquants_base_url: str = "https://api.jquants.com/v2"

    # BigQuery
    bq_project: str = ""
    bq_location: str = "asia-northeast1"

    # データセット名（raw.* テーブルに統一）
    ds_raw: str = "raw"
    ds_analytics: str = "analytics"
    ds_master: str = "raw"       # 銘柄マスターも raw データセットへ
    ds_portfolio: str = "portfolio"

    # バッチ設定
    rate_limit_per_min: int = 120  # Standard プラン用
    retry_count: int = 3
    retry_backoff: float = 2.0

    @classmethod
    def from_env(cls) -> "Config":
        """環境変数から設定を読み込む"""
        api_key = os.environ.get("JQUANTS_API_KEY", "")
        if not api_key:
            raise ValueError("JQUANTS_API_KEY 環境変数が設定されていません")

        bq_project = os.environ.get("BQ_PROJECT", "")
        if not bq_project:
            raise ValueError("BQ_PROJECT 環境変数が設定されていません")

        return cls(
            jquants_api_key=api_key,
            bq_project=bq_project,
        )
