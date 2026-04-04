"""중소형주 발굴 스캐너.

KIS API의 시장 데이터(거래량/회전율 순위, 체결강도)를 조합하여
중소형주 매수 시그널을 발굴한다.
결과는 SQLite에 캐싱한다 (1시간 TTL).

듀얼 스케줄:
- 스캔 (3분): 전체 순위 조회 + 체결강도 병렬 조회 + 발굴 목록 갱신
- 추적 (30초): 발굴/이탈 종목의 현재가/체결강도만 업데이트
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from auto_invest.api.kis_market import (
    get_stock_price,
    get_trade_strength,
    get_turnover_rank,
    get_volume_rank,
)
from auto_invest.utils import cache, timeseries

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# 캐시 TTL (초): 1시간
CACHE_TTL_SECONDS = 3600
_CACHE_KEY = "scanner:discover2"

# 스캐너 실행 상태 추적
_scanning = False
_scanning_lock = threading.Lock()

# 가격 필터 (원)
MIN_PRICE = 2000

# 등락률 상한 (%) — 이 이상은 "제외" 처리
MAX_CHANGE_RATE = 20.0

# 이탈 종목 추적 시간 (초)
DEPARTURE_TRACK_SECONDS = 30 * 60  # 30분

# 시총 상위 대형주 제외 기준 (종목코드 기반, 대표 대형주)
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


def _is_large_cap(code: str) -> bool:
    """대형주 여부."""
    return code in LARGE_CAP_CODES


# ── 종목 상태 관리 (인메모리) ────────────────────────────────

@dataclass
class StockState:
    """발굴 종목의 상태."""
    stock_code: str
    stock_name: str
    status: str  # active, departed, excluded
    turnover_rate: float = 0.0
    trade_strength: float = 0.0
    current_price: int = 0
    change_rate: float = 0.0
    volume: int = 0
    volume_rate: float = 0.0
    trading_value: int = 0
    first_seen: float = 0.0      # 최초 발견 시각 (unix)
    departed_at: float | None = None  # 이탈 시각


# 발굴 종목 상태 맵 (stock_code → StockState)
_discovered: dict[str, StockState] = {}
_discovered_lock = threading.Lock()


def get_discovered_stocks() -> list[dict]:
    """현재 발굴 목록 반환 (active + departed + excluded, expired 제외)."""
    now = time.time()
    result: list[dict] = []
    with _discovered_lock:
        expired_codes: list[str] = []
        for code, state in _discovered.items():
            # expired 판정
            if (
                state.status == "departed"
                and state.departed_at
                and now - state.departed_at > DEPARTURE_TRACK_SECONDS
            ):
                expired_codes.append(code)
                continue
            result.append({
                "stock_code": state.stock_code,
                "stock_name": state.stock_name,
                "status": state.status,
                "turnover_rate": state.turnover_rate,
                "trade_strength": state.trade_strength,
                "current_price": state.current_price,
                "change_rate": state.change_rate,
                "volume": state.volume,
                "volume_rate": state.volume_rate,
                "trading_value": state.trading_value,
            })
        for code in expired_codes:
            del _discovered[code]
    return result


def get_tracking_codes() -> list[str]:
    """추적 대상 종목코드 리스트 (active + departed)."""
    now = time.time()
    codes: list[str] = []
    with _discovered_lock:
        for code, state in _discovered.items():
            if state.status == "excluded":
                continue
            if (
                state.status == "departed"
                and state.departed_at
                and now - state.departed_at > DEPARTURE_TRACK_SECONDS
            ):
                continue
            codes.append(code)
    return codes


def clear_discovered() -> None:
    """발굴 목록 초기화 (장 시작 시)."""
    with _discovered_lock:
        _discovered.clear()
    logger.info("[스캐너] 발굴 목록 초기화")


# ── 발굴 종목 데이터 ────────────────────────────────────────

@dataclass
class DiscoverItem:
    """발굴 종목."""
    stock_code: str
    stock_name: str
    turnover_rate: float
    trade_strength: float
    current_price: int = 0
    change_rate: float = 0.0
    volume: int = 0
    volume_rate: float = 0.0
    trading_value: int = 0
    status: str = "active"


@dataclass
class DiscoverReport:
    """발굴 보고서."""
    timestamp: str
    items: list[DiscoverItem] = field(default_factory=list)
    cached_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "items": [asdict(i) for i in self.items],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DiscoverReport":
        return cls(
            timestamp=data["timestamp"],
            items=[DiscoverItem(**i) for i in data.get("items", [])],
        )


def is_scanning() -> bool:
    """스캐너가 현재 실행 중인지 반환."""
    return _scanning


# 하위호환 별칭
is_scanning2 = is_scanning


def run_scanner2(
    force_refresh: bool = False,
    cache_only: bool = False,
) -> DiscoverReport | None:
    """발굴 스캐너: 거래량 30 + 회전율 70 합집합 → 체결강도 교집합."""
    if not force_refresh:
        cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            logger.info("[스캔] 캐시 사용")
            report = DiscoverReport.from_dict(cached)
            report.cached_at = cached_at
            return report

    if cache_only:
        return None

    global _scanning  # noqa: PLW0603
    with _scanning_lock:
        if _scanning:
            logger.info("[스캔] 이미 실행 중 — 캐시 반환 시도")
            cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=None)
            if cached is not None:
                report = DiscoverReport.from_dict(cached)
                report.cached_at = cached_at
                return report
            return None
        _scanning = True

    logger.info("[스캔] 스캐너 시작")
    try:
        return _run_scan_impl()
    finally:
        with _scanning_lock:
            _scanning = False


def _run_scan_impl() -> DiscoverReport:
    """스캔 본체: 거래량 30 + 회전율 70 → 필터 → 체결강도 → 발굴 목록 갱신."""
    # 1. 거래량 순위 30개 + 회전율 순위 70개 병렬 조회
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_volume = pool.submit(get_volume_rank, max_items=30)
        f_turnover = pool.submit(get_turnover_rank, max_items=70)
    volume_data = f_volume.result()
    turnover_data = f_turnover.result()

    # 2. 합집합 구성
    stock_info: dict[str, dict] = {}
    for item in volume_data + turnover_data:
        code = item["stock_code"]
        if code in stock_info:
            if item["turnover_rate"] > stock_info[code]["turnover_rate"]:
                stock_info[code].update(item)
            continue
        stock_info[code] = {
            "name": item["stock_name"],
            "turnover_rate": item["turnover_rate"],
            "current_price": item["current_price"],
            "change_rate": item["change_rate"],
            "volume": item["volume"],
            "volume_rate": item["volume_rate"],
            "trading_value": item.get("trading_value", 0),
        }

    # 3. 필터: 가격 ≥ 2000, 대형주 제외
    candidates: dict[str, dict] = {}
    excluded: dict[str, dict] = {}  # 등락률 20%↑

    for code, info in stock_info.items():
        if _is_large_cap(code):
            continue
        if info["current_price"] < MIN_PRICE:
            continue
        if info["change_rate"] >= MAX_CHANGE_RATE:
            excluded[code] = info
            continue
        if info["turnover_rate"] > 0:
            candidates[code] = info

    logger.info(
        "[스캔] 후보: %d종목 (제외: %d종목, 등락률≥%.0f%%)",
        len(candidates), len(excluded), MAX_CHANGE_RATE,
    )

    # 4. 후보 종목의 체결강도 병렬 조회 (제외 종목은 스킵)
    strength_map: dict[str, float] = {}

    def _fetch_strength(code: str) -> tuple[str, float | None]:
        return code, get_trade_strength(code)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = pool.map(_fetch_strength, candidates.keys())

    for code, val in results:
        if val is not None:
            strength_map[code] = val

    logger.info("[스캔] 체결강도 조회 완료: %d종목", len(strength_map))

    # 5. 교집합: 회전율 > 0 AND 체결강도 > 100
    now_ts = time.time()
    active_codes: set[str] = set()
    items: list[DiscoverItem] = []

    for code, info in candidates.items():
        strength_val = strength_map.get(code)
        if strength_val is None or strength_val <= 100:
            continue
        active_codes.add(code)
        items.append(DiscoverItem(
            stock_code=code,
            stock_name=info["name"],
            turnover_rate=info["turnover_rate"],
            trade_strength=strength_val,
            current_price=info["current_price"],
            change_rate=info["change_rate"],
            volume=info["volume"],
            volume_rate=info["volume_rate"],
            trading_value=info["trading_value"],
            status="active",
        ))

    # 체결강도 내림차순 정렬
    items.sort(key=lambda x: x.trade_strength, reverse=True)

    # 6. 이탈/제외 종목 상태 갱신
    with _discovered_lock:
        prev_codes = {
            c for c, s in _discovered.items() if s.status == "active"
        }
        departed_codes = prev_codes - active_codes

        # 기존 active → departed
        for code in departed_codes:
            state = _discovered[code]
            state.status = "departed"
            state.departed_at = now_ts
            logger.info("[스캔] 이탈: %s (%s)", state.stock_name, code)

        # 신규/유지 active 갱신
        for item in items:
            code = item.stock_code
            if code in _discovered:
                state = _discovered[code]
                state.status = "active"
                state.departed_at = None
            else:
                state = StockState(
                    stock_code=code,
                    stock_name=item.stock_name,
                    status="active",
                    first_seen=now_ts,
                )
                logger.info("[스캔] 신규: %s (%s)", item.stock_name, code)
            state.turnover_rate = item.turnover_rate
            state.trade_strength = item.trade_strength
            state.current_price = item.current_price
            state.change_rate = item.change_rate
            state.volume = item.volume
            state.volume_rate = item.volume_rate
            state.trading_value = item.trading_value
            _discovered[code] = state

        # 제외 종목 등록
        for code, info in excluded.items():
            if code not in _discovered:
                _discovered[code] = StockState(
                    stock_code=code,
                    stock_name=info["name"],
                    status="excluded",
                    current_price=info["current_price"],
                    change_rate=info["change_rate"],
                    turnover_rate=info["turnover_rate"],
                    volume=info["volume"],
                    volume_rate=info["volume_rate"],
                    trading_value=info["trading_value"],
                    first_seen=now_ts,
                )
            else:
                _discovered[code].status = "excluded"
                _discovered[code].change_rate = info["change_rate"]

    # 7. 시계열 적재 (active 종목)
    ts_items = [
        {
            "stock_code": item.stock_code,
            "price": item.current_price,
            "change_rate": item.change_rate,
            "trade_strength": item.trade_strength,
            "turnover_rate": item.turnover_rate,
            "trading_value": item.trading_value,
        }
        for item in items
    ]
    if ts_items:
        timeseries.record_batch(ts_items)

    # 8. 캐시 저장
    report = DiscoverReport(
        timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        items=items,
    )
    report.cached_at = cache.put(_CACHE_KEY, report.to_dict())
    logger.info("[스캔] 완료: %d개 종목 (교집합)", len(items))
    return report


# ── 추적 (가벼운 업데이트) ──────────────────────────────────


def track_stocks() -> int:
    """발굴/이탈 종목의 현재가와 체결강도를 업데이트하고 시계열에 적재한다.

    Returns:
        추적한 종목 수
    """
    codes = get_tracking_codes()
    if not codes:
        return 0

    # 현재가 + 체결강도 병렬 조회
    def _fetch(code: str) -> tuple[str, dict | None, float | None]:
        price_info = get_stock_price(code)
        strength = get_trade_strength(code)
        return code, price_info, strength

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_fetch, codes))

    ts_items: list[dict] = []
    with _discovered_lock:
        for code, price_info, strength in results:
            state = _discovered.get(code)
            if not state:
                continue
            if price_info:
                state.current_price = price_info["current_price"]
                state.change_rate = price_info["change_rate"]
            if strength is not None:
                state.trade_strength = strength

            ts_items.append({
                "stock_code": code,
                "price": state.current_price,
                "change_rate": state.change_rate,
                "trade_strength": state.trade_strength,
                "turnover_rate": state.turnover_rate,
                "trading_value": state.trading_value,
            })

    if ts_items:
        timeseries.record_batch(ts_items)

    logger.debug("[추적] %d종목 업데이트 완료", len(ts_items))
    return len(ts_items)
