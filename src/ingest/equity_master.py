"""銘柄マスター取込み"""
import logging
import pandas as pd
from src.jquants_client import JQuantsClient
from src.bq_loader import BQLoader

logger = logging.getLogger(__name__)

# J-Quants V2 APIレスポンスのカラムマッピング
COLUMN_MAP = {
    "Code": "code",
    "CompanyName": "company_name",
    "CompanyNameEnglish": "company_name_english",
    "Sector17Code": "sector17_code",
    "Sector17CodeName": "sector17_name",
    "Sector33Code": "sector33_code",
    "Sector33CodeName": "sector33_name",
    "ScaleCategory": "scale_category",
    "MarketCode": "market_code",
    "MarketCodeName": "market_segment",
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

    # 必要カラムだけ抽出
    keep_cols = [v for v in COLUMN_MAP.values() if v in df.columns]
    df = df[keep_cols]

    # コードを5桁文字列に統一
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(5)

    logger.info(f"Equity master: {len(df)} records")

    # 全件洗い替え（WRITE_TRUNCATE）
    return loader.load_dataframe(
        df,
        f"{config.ds_master}.equity_master",
        write_disposition="WRITE_TRUNCATE",
    )
