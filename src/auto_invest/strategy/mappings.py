"""미국 섹터/키워드 → 한국 ETF 매핑 데이터.

미국 섹터 ETF나 뉴스 키워드에 대응하는 한국 ETF 코드를 정의한다.
한국 ETF의 구성종목은 kr_etf.py에서 자동으로 조회한다.
대형주 편중을 줄이기 위해 테마 ETF를 적극 활용한다.
확장 시 이 파일만 수정하면 됨.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class KrStock:
    """한국 종목 정보."""

    code: str  # 종목코드 (예: "005930")
    name: str  # 종목명 (예: "삼성전자")
    weight: float = 1.0  # 매핑 가중치 (기본 1.0)


@dataclass(frozen=True, slots=True)
class KrEtfMapping:
    """한국 ETF 매핑 정보."""

    code: str  # ETF 종목코드 (예: "091160")
    name: str  # ETF명 (예: "KODEX 반도체")


# ── 미국 섹터 ETF → 한국 ETF 매핑 ──────────────────────────────────
# 한국 ETF의 구성종목은 kr_etf.py에서 자동으로 조회됨
# 섹터 ETF + 테마 ETF를 조합하여 중소형주 커버리지 확대
SECTOR_ETF_MAP: dict[str, list[KrEtfMapping]] = {
    # 기술 (Technology)
    "XLK": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("363580", "KODEX K-테크TOP10"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
        KrEtfMapping("456600", "KODEX K-로봇"),
    ],
    # 반도체 (Semiconductor)
    "SOXX": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
        KrEtfMapping("466940", "KODEX AI반도체핵심장비"),
        KrEtfMapping("396500", "TIGER 소부장"),
    ],
    # 헬스케어 (Healthcare)
    "XLV": [
        KrEtfMapping("266420", "KODEX 헬스케어"),
        KrEtfMapping("371460", "TIGER 바이오TOP10"),
    ],
    # 에너지 (Energy)
    "XLE": [
        KrEtfMapping("117460", "KODEX 에너지화학"),
    ],
    # 산업재 (Industrials)
    "XLI": [
        KrEtfMapping("140710", "KODEX 운송"),
        KrEtfMapping("455850", "KODEX K-조선"),
        KrEtfMapping("465330", "TIGER 조선TOP10"),
        KrEtfMapping("456600", "KODEX K-로봇"),
    ],
    # 소재 (Materials)
    "XLB": [
        KrEtfMapping("117680", "KODEX 철강"),
    ],
    # 금융 (Financials)
    "XLF": [
        KrEtfMapping("091170", "KODEX 은행"),
        KrEtfMapping("140700", "KODEX 보험"),
    ],
    # 경기소비재 (Consumer Discretionary)
    "XLY": [
        KrEtfMapping("091180", "KODEX 자동차"),
        KrEtfMapping("396520", "TIGER K게임"),
    ],
    # 통신 (Communication Services)
    "XLC": [
        KrEtfMapping("363580", "KODEX K-테크TOP10"),
        KrEtfMapping("396520", "TIGER K게임"),
    ],
}

# ── 뉴스 키워드 → 한국 ETF 매핑 ──────────────────────────────────
KEYWORD_MAP: dict[str, list[KrEtfMapping]] = {
    "semiconductor": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
        KrEtfMapping("466940", "KODEX AI반도체핵심장비"),
    ],
    "chip": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
    ],
    "ai": [
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
        KrEtfMapping("363580", "KODEX K-테크TOP10"),
        KrEtfMapping("456600", "KODEX K-로봇"),
    ],
    "battery": [
        KrEtfMapping("305720", "KODEX 2차전지산업"),
        KrEtfMapping("394670", "TIGER 2차전지TOP10"),
    ],
    "ev": [
        KrEtfMapping("091180", "KODEX 자동차"),
        KrEtfMapping("305720", "KODEX 2차전지산업"),
        KrEtfMapping("394670", "TIGER 2차전지TOP10"),
    ],
    "electric vehicle": [
        KrEtfMapping("091180", "KODEX 자동차"),
        KrEtfMapping("394670", "TIGER 2차전지TOP10"),
    ],
    "oil": [
        KrEtfMapping("117460", "KODEX 에너지화학"),
    ],
    "shipbuilding": [
        KrEtfMapping("455850", "KODEX K-조선"),
        KrEtfMapping("465330", "TIGER 조선TOP10"),
    ],
    "steel": [
        KrEtfMapping("117680", "KODEX 철강"),
    ],
    "pharmaceutical": [
        KrEtfMapping("266420", "KODEX 헬스케어"),
        KrEtfMapping("371460", "TIGER 바이오TOP10"),
    ],
    "biotech": [
        KrEtfMapping("266420", "KODEX 헬스케어"),
        KrEtfMapping("371460", "TIGER 바이오TOP10"),
    ],
    "display": [
        KrEtfMapping("363580", "KODEX K-테크TOP10"),
    ],
    "memory": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
    ],
    "hbm": [
        KrEtfMapping("091160", "KODEX 반도체"),
        KrEtfMapping("462330", "TIGER AI반도체핵심공정"),
    ],
    "robot": [
        KrEtfMapping("456600", "KODEX K-로봇"),
    ],
    "game": [
        KrEtfMapping("396520", "TIGER K게임"),
    ],
}
