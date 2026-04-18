"""J-Quants API v2 クライアント

Standardプランに対応したAPIクライアント。
- APIキー認証（V2方式）
- ページネーション自動処理
- CSV一括ダウンロード対応
- レートリミット制御（120件/分）
"""
import io
import time
import logging
from typing import Any

import requests
import pandas as pd

logger = logging.getLogger(__name__)


class JQuantsClient:
    """J-Quants API v2 クライアント"""

    def __init__(self, api_key: str, base_url: str = "https://api.jquants.com/v2"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "Accept-Encoding": "gzip",
        })
        self._last_request_time = 0.0
        self._min_interval = 0.5  # 120件/分 = 0.5秒/件

    def _throttle(self):
        """レートリミット制御"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: dict | None = None,
             retry_count: int = 3) -> dict:
        """GETリクエスト（リトライ付き）"""
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retry_count):
            try:
                self._throttle()
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP {resp.status_code}: {e}")
                raise
            except requests.exceptions.RequestException as e:
                if attempt < retry_count - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Request failed ({e}). Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        return {}

    def _get_all_pages(self, endpoint: str, params: dict | None = None) -> list[dict]:
        """ページネーション対応の全データ取得"""
        params = params or {}
        all_data = []
        page = 0

        while True:
            page += 1
            resp = self._get(endpoint, params)
            data = resp.get("data", [])
            all_data.extend(data)
            logger.info(f"  Page {page}: {len(data)} records (total: {len(all_data)})")

            pagination_key = resp.get("pagination_key")
            if not pagination_key:
                break
            params["pagination_key"] = pagination_key

        return all_data

    # ─── Bulk CSV Download ───────────────────────────────────

    def bulk_list(self, endpoint: str) -> list[dict]:
        """CSVダウンロード対象ファイル一覧を取得"""
        resp = self._get("/bulk/list", params={"endpoint": endpoint})
        return resp.get("data", [])

    def bulk_download(self, file_key: str) -> pd.DataFrame:
        """CSVファイルをダウンロードしてDataFrameに変換"""
        resp = self._get("/bulk/get", params={"key": file_key})
        download_url = resp.get("url", "")
        if not download_url:
            raise ValueError(f"Download URL not found for key: {file_key}")

        logger.info(f"Downloading CSV: {file_key}")
        csv_resp = requests.get(download_url, timeout=300)
        csv_resp.raise_for_status()
        return pd.read_csv(io.BytesIO(csv_resp.content), compression='gzip')

    # ─── Equity Master ───────────────────────────────────────

    def get_equity_master(self, date: str | None = None) -> list[dict]:
        """上場銘柄マスターを取得"""
        params = {}
        if date:
            params["date"] = date
        logger.info(f"Fetching equity master (date={date or 'latest'})")
        return self._get_all_pages("/equities/master", params)

    # ─── Daily Quotes ────────────────────────────────────────

    def get_daily_quotes(self, date: str | None = None,
                         code: str | None = None,
                         from_date: str | None = None,
                         to_date: str | None = None) -> list[dict]:
        """日次株価OHLCを取得"""
        params = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        logger.info(f"Fetching daily quotes (date={date}, code={code})")
        return self._get_all_pages("/equities/bars/daily", params)

    # ─── Financial Summary ───────────────────────────────────

    def get_financial_summary(self, date: str | None = None,
                              code: str | None = None) -> list[dict]:
        """財務サマリーを取得"""
        params = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        logger.info(f"Fetching financial summary (date={date}, code={code})")
        return self._get_all_pages("/fins/summary", params)

    # ─── TOPIX ───────────────────────────────────────────────

    def get_topix(self, from_date: str | None = None,
                  to_date: str | None = None) -> list[dict]:
        """TOPIX四本値を取得"""
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        logger.info(f"Fetching TOPIX (from={from_date}, to={to_date})")
        return self._get_all_pages("/indices/bars/daily/topix", params)

    # ─── Index ───────────────────────────────────────────────

    def get_indices(self, date: str | None = None,
                    from_date: str | None = None,
                    to_date: str | None = None,
                    code: str | None = None) -> list[dict]:
        """指数四本値を取得（Standard以上）。

        J-Quants API spec (v2): `/indices/bars/daily` requires AT LEAST ONE of
        `date` or `code`. Supplying `from`/`to` alone returns 400.

        Call patterns:
          - date="YYYYMMDD"                   → all ~79 indices for that date
          - code="XXXX" + from/to             → one index over a range
          - code="XXXX" (no range)            → one index, full history
        """
        params: dict = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if not (date or code):
            raise ValueError(
                "get_indices requires at least one of 'date' or 'code' — "
                "the J-Quants API rejects queries without either parameter."
            )
        logger.info(
            f"Fetching indices (date={date}, code={code}, "
            f"from={from_date}, to={to_date})"
        )
        return self._get_all_pages("/indices/bars/daily", params)

    # ─── Margin Interest ─────────────────────────────────────

    def get_margin_interest(self, date: str | None = None,
                            code: str | None = None) -> list[dict]:
        """信用取引週末残高を取得（Standard以上）"""
        params = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        logger.info(f"Fetching margin interest (date={date})")
        return self._get_all_pages("/markets/margin-interest", params)

    # ─── Short Selling Ratio ─────────────────────────────────

    def get_short_selling_ratio(self, date: str | None = None,
                                sector33_code: str | None = None) -> list[dict]:
        """業種別空売り比率を取得（Standard以上）"""
        params = {}
        if date:
            params["date"] = date
        if sector33_code:
            params["sector33code"] = sector33_code
        logger.info(f"Fetching short selling ratio (date={date})")
        return self._get_all_pages("/markets/short-ratio", params)

    # ─── Investor Types ──────────────────────────────────────

    def get_investor_types(self, from_date: str | None = None,
                           to_date: str | None = None,
                           section: str | None = None) -> list[dict]:
        """投資部門別売買動向を取得"""
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if section:
            params["section"] = section
        logger.info(f"Fetching investor types (from={from_date})")
        return self._get_all_pages("/equities/investor-types", params)

    # ─── Earnings Calendar ───────────────────────────────────

    def get_earnings_calendar(self, date: str | None = None,
                              code: str | None = None) -> list[dict]:
        """決算発表予定日を取得"""
        params = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        logger.info(f"Fetching earnings calendar (date={date})")
        return self._get_all_pages("/equities/earnings-calendar", params)

    # ─── Trading Calendar ────────────────────────────────────

    def get_trading_calendar(self, from_date: str | None = None,
                             to_date: str | None = None) -> list[dict]:
        """取引カレンダーを取得"""
        params = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        logger.info(f"Fetching trading calendar")
        return self._get_all_pages("/markets/calendar", params)

    # ─── Utility ─────────────────────────────────────────────

    def health_check(self) -> bool:
        """API接続確認"""
        try:
            data = self.get_equity_master()
            logger.info(f"Health check OK: {len(data)} equities found")
            return len(data) > 0
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def get_minute_quotes(self, date: str) -> list[dict]:
        """分足OHLCを取得

        Args:
            date: 取得日（YYYYMMDD形式）
        """
        params = {"date": date}
        logger.info(f"Fetching minute quotes for date={date}")
        return self._get_all_pages("/equities/bars/minute", params)
