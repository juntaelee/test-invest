"""중소형주 발굴 스캐너.

KIS API의 시장 데이터(거래량/거래대금/회전율 순위, 등락률, 외국인·기관 순매수)를
조합하여 중소형주 매수 시그널을 발굴한다.
결과는 SQLite에 캐싱한다 (1시간 TTL).
"""

import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from auto_invest.api.kis_market import (
    get_fluctuation_rank,
    get_investor_trend,
    get_trade_strength,
    get_trading_value_rank,
    get_volume_rank,
)
from auto_invest.utils import cache

logger = logging.getLogger(__name__)

# 캐시 TTL (초): 1시간
CACHE_TTL_SECONDS = 3600
_CACHE_KEY = "scanner:discover"

# 스캐너 실행 상태 추적
_scanning = False
_scanning_lock = threading.Lock()

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

# 스코어 가중치 (합 = 1.0, 등락률은 카드 표시만, 종합점수 제외)
VOLUME_WEIGHT = 0.20
TRADING_VALUE_WEIGHT = 0.20
FLUCTUATION_WEIGHT = 0.0
TURNOVER_WEIGHT = 0.15
FOREIGN_WEIGHT = 0.15
INSTITUTION_WEIGHT = 0.15
STRENGTH_WEIGHT = 0.15

SCORE_MAX = 10.0
MIN_SCORE = 1.0
CARD_TOP_N = 30


@dataclass
class DiscoverItem:
    """발굴 종목."""

    stock_code: str
    stock_name: str
    total_score: float
    volume_score: float
    trading_value_score: float
    fluctuation_score: float
    turnover_score: float = 0.0
    foreign_score: float = 0.0
    institution_score: float = 0.0
    strength_score: float = 0.0
    # 원본 데이터
    volume: int = 0
    volume_rate: float = 0.0
    trading_value: int = 0
    turnover_rate: float = 0.0
    foreign_net_qty: int = 0
    institution_net_qty: int = 0
    trade_strength: float = 0.0
    current_price: int = 0
    change_rate: float = 0.0


@dataclass
class DiscoverReport:
    """발굴 보고서."""

    timestamp: str
    volume_top: list[DiscoverItem] = field(default_factory=list)
    trading_value_top: list[DiscoverItem] = field(default_factory=list)
    fluctuation_top: list[DiscoverItem] = field(default_factory=list)
    turnover_top: list[DiscoverItem] = field(default_factory=list)
    foreign_top: list[DiscoverItem] = field(default_factory=list)
    institution_top: list[DiscoverItem] = field(default_factory=list)
    strength_top: list[DiscoverItem] = field(default_factory=list)
    combined: list[DiscoverItem] = field(default_factory=list)
    cached_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "volume_top": [asdict(i) for i in self.volume_top],
            "trading_value_top": [asdict(i) for i in self.trading_value_top],
            "fluctuation_top": [asdict(i) for i in self.fluctuation_top],
            "turnover_top": [asdict(i) for i in self.turnover_top],
            "foreign_top": [asdict(i) for i in self.foreign_top],
            "institution_top": [asdict(i) for i in self.institution_top],
            "strength_top": [asdict(i) for i in self.strength_top],
            "combined": [asdict(i) for i in self.combined],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DiscoverReport":
        return cls(
            timestamp=data["timestamp"],
            volume_top=[DiscoverItem(**i) for i in data.get("volume_top", [])],
            trading_value_top=[DiscoverItem(**i) for i in data.get("trading_value_top", [])],
            fluctuation_top=[DiscoverItem(**i) for i in data.get("fluctuation_top", [])],
            turnover_top=[DiscoverItem(**i) for i in data.get("turnover_top", [])],
            foreign_top=[DiscoverItem(**i) for i in data.get("foreign_top", [])],
            institution_top=[DiscoverItem(**i) for i in data.get("institution_top", [])],
            strength_top=[DiscoverItem(**i) for i in data.get("strength_top", [])],
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
    cache_only: bool = False,
) -> DiscoverReport | None:
    """발굴 스캐너 실행.

    Args:
        top_n: 종합 발굴 종목 수
        force_refresh: True이면 캐시 무시
        cache_only: True이면 캐시만 조회, 없으면 None 반환

    Returns:
        DiscoverReport 또는 cache_only일 때 캐시 없으면 None
    """
    if not force_refresh:
        cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            logger.info("발굴 스캐너 캐시 사용")
            report = DiscoverReport.from_dict(cached)  # type: ignore[arg-type]
            report.cached_at = cached_at
            return report

    if cache_only:
        return None

    global _scanning
    with _scanning_lock:
        if _scanning:
            logger.info("발굴 스캐너가 이미 실행 중 — 캐시 반환 시도")
            cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=None)
            if cached is not None:
                report = DiscoverReport.from_dict(cached)  # type: ignore[arg-type]
                report.cached_at = cached_at
                return report
            return None
        _scanning = True

    logger.info("발굴 스캐너 시작 (top_n=%d)", top_n)
    try:
        return _run_scanner_impl(top_n)
    finally:
        with _scanning_lock:
            _scanning = False


def is_scanning() -> bool:
    """스캐너가 현재 실행 중인지 반환."""
    return _scanning


def _run_scanner_impl(top_n: int) -> DiscoverReport:
    """스캐너 본체 (내부 전용)."""
    # 1. 데이터 수집
    volume_data = get_volume_rank(max_items=50)
    trading_value_data = get_trading_value_rank(max_items=50)
    fluctuation_data = get_fluctuation_rank(max_items=50)

    # 2. 종목 정보 수집
    stock_info: dict[str, dict] = {}

    # 거래량/회전율 점수 (volume rank API)
    volume_raw: dict[str, float] = {}
    turnover_raw: dict[str, float] = {}
    for item in volume_data:
        code = item["stock_code"]
        volume_raw[code] = max(item["volume_rate"], 0)
        tr = item.get("turnover_rate", 0.0)
        if tr > 0:
            turnover_raw[code] = tr
        stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item["volume"],
            "volume_rate": item["volume_rate"],
            "trading_value": item.get("trading_value", 0),
            "turnover_rate": tr,
        })

    # 거래대금 점수 (trading value rank API - 별도 호출)
    trading_value_raw: dict[str, float] = {}
    for item in trading_value_data:
        code = item["stock_code"]
        tv = item.get("trading_value", 0)
        if tv > 0:
            trading_value_raw[code] = float(tv)
        stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item["volume"],
            "volume_rate": item["volume_rate"],
            "trading_value": tv,
            "turnover_rate": item.get("turnover_rate", 0),
        })

    # 등락률 점수 (상승률 기반)
    fluctuation_raw: dict[str, float] = {}
    for item in fluctuation_data:
        code = item["stock_code"]
        fluctuation_raw[code] = item["change_rate"]
        stock_info.setdefault(code, {
            "name": item["stock_name"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item.get("volume", 0),
            "volume_rate": 0,
            "trading_value": 0,
            "turnover_rate": 0,
        })

    # 외국인/기관 순매수 (종목별 개별 조회)
    all_candidate_codes = set(volume_raw) | set(trading_value_raw) | set(fluctuation_raw)
    foreign_raw: dict[str, float] = {}
    institution_raw: dict[str, float] = {}
    logger.info("투자자 매매동향 조회 중 (%d종목)...", len(all_candidate_codes))
    for code in all_candidate_codes:
        trend = get_investor_trend(code)
        if not trend:
            continue
        fq = trend["foreign_net_qty"]
        iq = trend["institution_net_qty"]
        info = stock_info.get(code)
        if info:
            info["foreign_net_qty"] = fq
            info["institution_net_qty"] = iq
        if fq > 0:
            foreign_raw[code] = float(fq)
        if iq > 0:
            institution_raw[code] = float(iq)
    logger.info("투자자 매매동향 조회 완료 (외국인 순매수 %d, 기관 순매수 %d)",
                len(foreign_raw), len(institution_raw))

    # 체결강도 조회 (종목별 개별 조회)
    strength_raw: dict[str, float] = {}
    logger.info("체결강도 조회 중 (%d종목)...", len(all_candidate_codes))
    for code in all_candidate_codes:
        val = get_trade_strength(code)
        if val is None:
            continue
        info = stock_info.get(code)
        if info:
            info["trade_strength"] = val
        # 100 이상(매수 우위)만 양의 점수 부여
        if val > 100:
            strength_raw[code] = val - 100
    logger.info("체결강도 조회 완료 (매수우위 %d종목)", len(strength_raw))

    # 3. 정규화
    volume_scores = _normalize_scores(volume_raw)
    trading_value_scores = _normalize_scores(trading_value_raw)
    fluctuation_scores = _normalize_scores(fluctuation_raw)
    turnover_scores = _normalize_scores(turnover_raw)
    foreign_scores = _normalize_scores(foreign_raw)
    institution_scores = _normalize_scores(institution_raw)
    strength_scores = _normalize_scores(strength_raw)

    # 4. 종합 점수 산출
    all_codes = (
        set(volume_scores)
        | set(trading_value_scores)
        | set(fluctuation_scores)
        | set(turnover_scores)
        | set(foreign_scores)
        | set(institution_scores)
        | set(strength_scores)
    )
    items: list[DiscoverItem] = []

    for code in all_codes:
        info = stock_info.get(code)
        if not info:
            continue

        v_score = volume_scores.get(code, 0.0)
        tv_score = trading_value_scores.get(code, 0.0)
        f_score = fluctuation_scores.get(code, 0.0)
        t_score = turnover_scores.get(code, 0.0)
        fg_score = foreign_scores.get(code, 0.0)
        ig_score = institution_scores.get(code, 0.0)
        s_score = strength_scores.get(code, 0.0)
        total = round(
            v_score * VOLUME_WEIGHT
            + tv_score * TRADING_VALUE_WEIGHT
            + f_score * FLUCTUATION_WEIGHT
            + t_score * TURNOVER_WEIGHT
            + fg_score * FOREIGN_WEIGHT
            + ig_score * INSTITUTION_WEIGHT
            + s_score * STRENGTH_WEIGHT,
            2,
        )

        items.append(DiscoverItem(
            stock_code=code,
            stock_name=info["name"],
            total_score=total,
            volume_score=v_score,
            trading_value_score=tv_score,
            fluctuation_score=f_score,
            turnover_score=t_score,
            foreign_score=fg_score,
            institution_score=ig_score,
            strength_score=s_score,
            volume=info.get("volume", 0),
            volume_rate=info.get("volume_rate", 0),
            trading_value=info.get("trading_value", 0),
            turnover_rate=info.get("turnover_rate", 0),
            foreign_net_qty=info.get("foreign_net_qty", 0),
            institution_net_qty=info.get("institution_net_qty", 0),
            trade_strength=info.get("trade_strength", 0),
            current_price=info.get("current_price", 0),
            change_rate=info.get("change_rate", 0),
        ))

    items.sort(key=lambda x: x.total_score, reverse=True)

    # 5. 카테고리별 TOP 리스트 (전체 종목에서 추출)
    # combined만 MIN_SCORE 필터 적용
    volume_top = sorted(
        [i for i in items if i.volume_score > 0],
        key=lambda x: x.volume_score, reverse=True,
    )[:CARD_TOP_N]

    trading_value_top = sorted(
        [i for i in items if i.trading_value_score > 0],
        key=lambda x: x.trading_value_score, reverse=True,
    )[:CARD_TOP_N]

    fluctuation_top = sorted(
        [i for i in items if i.fluctuation_score > 0],
        key=lambda x: x.fluctuation_score, reverse=True,
    )[:CARD_TOP_N]

    turnover_top = sorted(
        [i for i in items if i.turnover_score > 0],
        key=lambda x: x.turnover_score, reverse=True,
    )[:CARD_TOP_N]

    foreign_top = sorted(
        [i for i in items if i.foreign_score > 0],
        key=lambda x: x.foreign_score, reverse=True,
    )[:CARD_TOP_N]

    institution_top = sorted(
        [i for i in items if i.institution_score > 0],
        key=lambda x: x.institution_score, reverse=True,
    )[:CARD_TOP_N]

    strength_top = sorted(
        [i for i in items if i.strength_score > 0],
        key=lambda x: x.strength_score, reverse=True,
    )[:CARD_TOP_N]

    report = DiscoverReport(
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        volume_top=volume_top,
        trading_value_top=trading_value_top,
        fluctuation_top=fluctuation_top,
        turnover_top=turnover_top,
        foreign_top=foreign_top,
        institution_top=institution_top,
        strength_top=strength_top,
        combined=[i for i in items if i.total_score >= MIN_SCORE][:top_n],
    )

    report.cached_at = cache.put(_CACHE_KEY, report.to_dict())
    logger.info("발굴 완료: %d개 종목", len(items))
    return report
