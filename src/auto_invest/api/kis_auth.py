"""한국투자증권 API 인증 모듈

토큰 발급, 캐싱, 인증 헤더 생성을 담당한다.
"""

import json
import logging
from datetime import datetime, timedelta

import requests

from auto_invest.config import BASE_DIR, settings

logger = logging.getLogger(__name__)

TOKEN_CACHE_PATH = BASE_DIR / ".cache" / "token.json"


class KISAuth:
    """KIS Open API 인증 관리"""

    def __init__(self) -> None:
        self._access_token: str | None = None
        self._token_expired_at: datetime | None = None
        self._load_cached_token()

    # ── 토큰 발급 ──────────────────────────────────────────

    def get_access_token(self) -> str:
        """유효한 access_token을 반환한다. 만료 시 자동 재발급."""
        if (
            self._access_token
            and self._token_expired_at
            and (datetime.now() < self._token_expired_at)
        ):
            return self._access_token

        logger.info("토큰 발급 요청")
        url = f"{settings.base_url}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
        }

        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        # expires_in은 초 단위, 여유분 1시간 차감
        expires_in = int(data.get("expires_in", 86400))
        self._token_expired_at = datetime.now() + timedelta(seconds=expires_in - 3600)

        self._save_cached_token()
        logger.info("토큰 발급 완료 (만료: %s)", self._token_expired_at)
        return self._access_token

    # ── 인증 헤더 ──────────────────────────────────────────

    def get_headers(self, tr_id: str = "") -> dict[str, str]:
        """API 호출에 필요한 공통 헤더를 반환한다.

        Args:
            tr_id: 거래ID (API별로 다름, 예: VTTC0802U)
        """
        token = self.get_access_token()
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
        }
        if tr_id:
            headers["tr_id"] = tr_id
        return headers

    # ── 토큰 캐시 ──────────────────────────────────────────

    def _save_cached_token(self) -> None:
        TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        cache = {
            "access_token": self._access_token,
            "expired_at": self._token_expired_at.isoformat() if self._token_expired_at else None,
        }
        TOKEN_CACHE_PATH.write_text(json.dumps(cache), encoding="utf-8")

    def _load_cached_token(self) -> None:
        if not TOKEN_CACHE_PATH.exists():
            return
        try:
            cache = json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
            expired_at = datetime.fromisoformat(cache["expired_at"])
            if datetime.now() < expired_at:
                self._access_token = cache["access_token"]
                self._token_expired_at = expired_at
                logger.info("캐시된 토큰 로드 (만료: %s)", expired_at)
        except (json.JSONDecodeError, KeyError, ValueError):
            logger.warning("토큰 캐시 파일 손상, 무시")


# 싱글톤 인스턴스
auth = KISAuth()
