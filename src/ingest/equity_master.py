"""銘柄マスター取込み"""
import logging
import pandas as pd
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)

# J-Quants V2 APIレスポンスのカラムマッピング
# 新フォーマット（CoName等）と旧フォーマット（CompanyName等）の両方をサポート
COLUMN_MAP = {
    # 新フォーマット（2024年以降の短縮名）
    "Code": "code",
    "CoName": "company_name",
    "CoNameEn": "company_name_english",
    "S17": "sector17_code",
    "S17Nm": "sector17_name",
    "S33": "sector33_code",
    "S33Nm": "sector33_name",
    "ScaleCat": "scale_category",
    "Mkt": "market_code",
    "MktNm": "market_name",
    # 旧フォーマット（互換性維持）
    "CompanyName": "company_name",
    "CompanyNameEnglish": "company_name_english",
    "Sector17Code": "sector17_code",
    "Sector17CodeName": "sector17_name",
    "Sector33Code": "sector33_code",
    "Sector33CodeName": "sector33_name",
    "ScaleCategory": "scale_category",
    "MarketCode": "market_code",
    "MarketCodeName": "market_name",
}


def ingest(client: JQuantsClient, loader: BQLoader, config) -> int:
    """銘柄マスターを取得してBigQueryにロード（全件洗い替え）"""
    data = client.get_equity_master()
    if not data:
        logger.warning("No equity master data returned")
        return 0

    df = pd.DataFrame(data)

    # カラム名変換（存在するもののみ）
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # 必要カラムだけ抽出（重複除去してから選択）
    keep_cols = list(dict.fromkeys(v for v in COLUMN_MAP.values() if v in df.columns))
    df = df[keep_cols]

    # コードを5桁文字列に統一
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)

    logger.info(f"Equity master: {len(df)} records")

    # 全件洗い替え（WRITE_TRUNCATE）
    return loader.load_dataframe(
        df,
        f"{config.ds_master}.equities_master",
        write_disposition="WRITE_TRUNCATE",
    )
