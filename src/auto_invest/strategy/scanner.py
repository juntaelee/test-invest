"""중소형주 발굴 스캐너.

KIS API의 시장 데이터(거래량 순위, 시간외 잔량, 외국인 거래량)를 조합하여
중소형주 매수 시그널을 발굴한다.
결과는 SQLite에 캐싱한다 (1시간 TTL).
"""

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from auto_invest.api.kis_market import (
    get_after_hour_balance_rank,
    get_fluctuation_rank,
    get_volume_rank,
)
from auto_invest.utils import cache

logger = logging.getLogger(__name__)

# 캐시 TTL (초): 1시간
CACHE_TTL_SECONDS = 3600
_CACHE_KEY = "scanner:discover"

# 시총 상위 대형주 제외 기준 (종목코드 기반, 대표 대형주)
# TODO: 동적으로 시총 순위를 조회하여 필터링하는 것이 이상적
LARGE_CAP_CODES = {
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "373220",  # LG에너지솔루션
    "207940",  # 삼성바이오로직스
    "005380",  # 현대차
    "000270",  # 기아
    "005490",  # POSCO홀딩스
    "055550",  # 신한지주
    "035420",  # NAVER
    "035720",  # 카카오
    "006400",  # 삼성SDI
    "051910",  # LG화학
    "003670",  # 포스코퓨처엠
    "105560",  # KB금융
    "028260",  # 삼성물산
    "012330",  # 현대모비스
    "066570",  # LG전자
    "003490",  # 대한항공
    "032830",  # 삼성생명
    "086790",  # 하나금융지주
    "316140",  # 우리금융지주
    "009150",  # 삼성전기
    "034730",  # SK
    "000810",  # 삼성화재
    "018260",  # 삼성에스디에스
    "015760",  # 한국전력
    "259960",  # 크래프톤
    "402340",  # SK스퀘어
    "017670",  # SK텔레콤
    "030200",  # KT
    "047050",  # 포스코인터내셔널
    "010130",  # 고려아연
    "034020",  # 두산에너빌리티
    "011200",  # HMM
    "010950",  # S-Oil
    "033780",  # KT&G
    "068270",  # 셀트리온
    "036570",  # 엔씨소프트
    "096770",  # SK이노베이션
    "326030",  # SK바이오팜
}

# 스코어 가중치
VOLUME_WEIGHT = 0.35
AFTER_HOUR_WEIGHT = 0.35
FLUCTUATION_WEIGHT = 0.30

SCORE_MAX = 10.0
MIN_SCORE = 1.0


@dataclass
class DiscoverItem:
    """발굴 종목."""

    stock_code: str
    stock_name: str
    total_score: float
    volume_score: float
    after_hour_score: float
    fluctuation_score: float
    # 원본 데이터
    volume: int = 0
    volume_rate: float = 0.0  # 거래량증가율 (%)
    after_hour_buy_qty: int = 0
    current_price: int = 0
    change_rate: float = 0.0


@dataclass
class DiscoverReport:
    """발굴 보고서."""

    timestamp: str
    volume_top: list[DiscoverItem] = field(default_factory=list)
    after_hour_top: list[DiscoverItem] = field(default_factory=list)
    fluctuation_top: list[DiscoverItem] = field(default_factory=list)
    combined: list[DiscoverItem] = field(default_factory=list)
    cached_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "volume_top": [asdict(i) for i in self.volume_top],
            "after_hour_top": [asdict(i) for i in self.after_hour_top],
            "fluctuation_top": [asdict(i) for i in self.fluctuation_top],
            "combined": [asdict(i) for i in self.combined],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DiscoverReport":
        return cls(
            timestamp=data["timestamp"],
            volume_top=[DiscoverItem(**i) for i in data.get("volume_top", [])],
            after_hour_top=[DiscoverItem(**i) for i in data.get("after_hour_top", [])],
            fluctuation_top=[DiscoverItem(**i) for i in data.get("fluctuation_top", [])],
            combined=[DiscoverItem(**i) for i in data.get("combined", [])],
        )


def _is_large_cap(code: str) -> bool:
    """대형주 여부."""
    return code in LARGE_CAP_CODES


def _normalize_scores(raw: dict[str, float]) -> dict[str, float]:
    """0~SCORE_MAX 범위로 정규화."""
    if not raw:
        return {}
    peak = max(raw.values())
    if peak <= 0:
        return {k: 0.0 for k in raw}
    factor = SCORE_MAX / peak
    return {k: round(v * factor, 2) for k, v in raw.items()}


def run_scanner(
    top_n: int = 30,
    force_refresh: bool = False,
) -> DiscoverReport:
    """발굴 스캐너 실행.

    Args:
        top_n: 종합 발굴 종목 수
        force_refresh: True이면 캐시 무시

    Returns:
        DiscoverReport
    """
    if not force_refresh:
        cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            logger.info("발굴 스캐너 캐시 사용")
            report = DiscoverReport.from_dict(cached)  # type: ignore[arg-type]
            report.cached_at = cached_at
            return report

    logger.info("발굴 스캐너 시작 (top_n=%d)", top_n)

    # 1. 데이터 수집
    volume_data = get_volume_rank(max_items=50)
    after_hour_data = get_after_hour_balance_rank(max_items=50)
    fluctuation_data = get_fluctuation_rank(max_items=50)

    # 2. 대형주 필터링 + 종목 정보 수집
    stock_info: dict[str, dict] = {}  # code -> {name, current_price, change_rate}

    # 거래량 점수 (거래량증가율 기반)
    volume_raw: dict[str, float] = {}
    for item in volume_data:
        code = item["stock_code"]
        if _is_large_cap(code):
            continue
        volume_raw[code] = max(item["volume_rate"], 0)
        stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item["volume"],
            "volume_rate": item["volume_rate"],
        })

    # 시간외 점수 (매수잔량 기반)
    after_hour_raw: dict[str, float] = {}
    for item in after_hour_data:
        code = item["stock_code"]
        if _is_large_cap(code):
            continue
        buy_qty = item["after_hour_buy_qty"]
        after_hour_raw[code] = float(max(buy_qty, 0))
        info = stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": 0,
            "volume_rate": 0,
        })
        info["after_hour_buy_qty"] = buy_qty

    # 등락률 점수 (상승률 기반)
    fluctuation_raw: dict[str, float] = {}
    for item in fluctuation_data:
        code = item["stock_code"]
        if _is_large_cap(code):
            continue
        fluctuation_raw[code] = item["change_rate"]
        stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item.get("volume", 0),
            "volume_rate": 0,
        })

    # 3. 정규화
    volume_scores = _normalize_scores(volume_raw)
    after_hour_scores = _normalize_scores(after_hour_raw)
    fluctuation_scores = _normalize_scores(fluctuation_raw)

    # 4. 종합 점수 산출
    all_codes = set(volume_scores) | set(after_hour_scores) | set(fluctuation_scores)
    items: list[DiscoverItem] = []

    for code in all_codes:
        info = stock_info.get(code)
        if not info:
            continue

        v_score = volume_scores.get(code, 0.0)
        a_score = after_hour_scores.get(code, 0.0)
        f_score = fluctuation_scores.get(code, 0.0)
        total = round(
            v_score * VOLUME_WEIGHT + a_score * AFTER_HOUR_WEIGHT + f_score * FLUCTUATION_WEIGHT,
            2,
        )

        if total < MIN_SCORE:
            continue

        items.append(DiscoverItem(
            stock_code=code,
            stock_name=info["name"],
            total_score=total,
            volume_score=v_score,
            after_hour_score=a_score,
            fluctuation_score=f_score,
            volume=info.get("volume", 0),
            volume_rate=info.get("volume_rate", 0),
            after_hour_buy_qty=info.get("after_hour_buy_qty", 0),
            current_price=info.get("current_price", 0),
            change_rate=info.get("change_rate", 0),
        ))

    items.sort(key=lambda x: x.total_score, reverse=True)

    # 5. 카테고리별 TOP 리스트
    volume_top = sorted(
        [i for i in items if i.volume_score > 0],
        key=lambda x: x.volume_score,
        reverse=True,
    )[:10]

    after_hour_top = sorted(
        [i for i in items if i.after_hour_score > 0],
        key=lambda x: x.after_hour_score,
        reverse=True,
    )[:10]

    fluctuation_top = sorted(
        [i for i in items if i.fluctuation_score > 0],
        key=lambda x: x.fluctuation_score,
        reverse=True,
    )[:10]

    report = DiscoverReport(
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        volume_top=volume_top,
        after_hour_top=after_hour_top,
        fluctuation_top=fluctuation_top,
        combined=items[:top_n],
    )

    report.cached_at = cache.put(_CACHE_KEY, report.to_dict())
    logger.info("발굴 완료: %d개 종목", len(items))
    return report
