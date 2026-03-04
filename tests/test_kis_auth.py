"""KIS 인증 모듈 테스트"""

import json
from datetime import datetime, timedelta
from unittest.mock import patch

from auto_invest.api.kis_auth import KISAuth
from auto_invest.config import Settings


class TestSettings:
    def test_paper_mode_url(self):
        s = Settings(
            kis_app_key="test", kis_app_secret="test", kis_account_no="12345678",
            kis_mode="paper",
        )
        assert "openapivts" in s.base_url

    def test_real_mode_url(self):
        s = Settings(
            kis_app_key="test", kis_app_secret="test", kis_account_no="12345678",
            kis_mode="real",
        )
        assert "openapivts" not in s.base_url
        assert "openapi.koreainvestment.com" in s.base_url

    def test_is_paper(self):
        s = Settings(
            kis_app_key="test", kis_app_secret="test", kis_account_no="12345678",
            kis_mode="paper",
        )
        assert s.is_paper is True


class TestKISAuth:
    def test_cached_token_loaded(self, tmp_path):
        """캐시 파일이 있으면 토큰을 로드한다."""
        cache_path = tmp_path / "token.json"
        future = (datetime.now() + timedelta(hours=12)).isoformat()
        cache_path.write_text(json.dumps({
            "access_token": "cached_token_123",
            "expired_at": future,
        }))

        with patch("auto_invest.api.kis_auth.TOKEN_CACHE_PATH", cache_path):
            auth = KISAuth()
            assert auth._access_token == "cached_token_123"

    def test_expired_cache_ignored(self, tmp_path):
        """만료된 캐시 토큰은 무시한다."""
        cache_path = tmp_path / "token.json"
        past = (datetime.now() - timedelta(hours=1)).isoformat()
        cache_path.write_text(json.dumps({
            "access_token": "old_token",
            "expired_at": past,
        }))

        with patch("auto_invest.api.kis_auth.TOKEN_CACHE_PATH", cache_path):
            auth = KISAuth()
            assert auth._access_token is None

    @patch("auto_invest.api.kis_auth.requests.post")
    def test_get_access_token_calls_api(self, mock_post, tmp_path):
        """토큰이 없으면 API를 호출하여 발급받는다."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {
            "access_token": "new_token_abc",
            "token_type": "Bearer",
            "expires_in": 86400,
        }
        mock_post.return_value.raise_for_status = lambda: None

        cache_path = tmp_path / "token.json"
        with patch("auto_invest.api.kis_auth.TOKEN_CACHE_PATH", cache_path):
            auth = KISAuth()
            token = auth.get_access_token()

        assert token == "new_token_abc"
        mock_post.assert_called_once()

    def test_get_headers_includes_required_fields(self, tmp_path):
        """헤더에 필수 필드가 포함되어야 한다."""
        cache_path = tmp_path / "token.json"
        future = (datetime.now() + timedelta(hours=12)).isoformat()
        cache_path.write_text(json.dumps({
            "access_token": "test_token",
            "expired_at": future,
        }))

        with patch("auto_invest.api.kis_auth.TOKEN_CACHE_PATH", cache_path):
            auth = KISAuth()
            headers = auth.get_headers(tr_id="VTTC0802U")

        assert "authorization" in headers
        assert headers["authorization"].startswith("Bearer ")
        assert "appkey" in headers
        assert "appsecret" in headers
        assert headers["tr_id"] == "VTTC0802U"
