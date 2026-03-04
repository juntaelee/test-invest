from pathlib import Path

from pydantic_settings import BaseSettings

# 프로젝트 루트 디렉토리
BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """한국투자증권 API 설정"""

    kis_app_key: str
    kis_app_secret: str
    kis_account_no: str  # 계좌번호 (8자리)
    kis_acnt_prdt_cd: str = "01"  # 계좌상품코드
    kis_user_id: str = ""
    kis_mode: str = "paper"  # real / paper

    model_config = {"env_file": BASE_DIR / ".env", "env_file_encoding": "utf-8"}

    @property
    def base_url(self) -> str:
        """실전/모의투자 URL 자동 분기"""
        if self.kis_mode == "real":
            return "https://openapi.koreainvestment.com:9443"
        return "https://openapivts.koreainvestment.com:29443"

    @property
    def is_paper(self) -> bool:
        return self.kis_mode != "real"


# 싱글톤 설정 인스턴스
settings = Settings()
