"""종가배팅 스크리너.

장 마감 직전(15:20) 종가배팅 후보를 자동 스크리닝한다.
1차 필터: 거래량 급증률 상위 + 거래대금 50억↑
2차 스코어링: 6개 지표 100점 만점
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

from auto_invest.api.kis_market import (
    get_daily_prices,
    get_investor_trend,
    get_stock_price,
    get_trade_strength,
    get_volume_rank,
)
from auto_invest.utils import cache

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# 캐시 설정
CACHE_TTL_SECONDS = 3600  # 1시간
_CACHE_KEY = "screener:closing"

# 스크리너 실행 상태
_screening = False
_screening_lock = threading.Lock()

# ── 1차 필터 기준 ────────────────────────────────────────
MIN_TRADING_VALUE = 5_000_000_000  # 거래대금 50억 원
MIN_VOLUME_RATE = 200.0  # 전일 대비 거래량 200%↑
MIN_PRICE = 2000  # 최소 가격 2,000원
MAX_CHANGE_RATE = 15.0  # 등락률 15% 초과 제외 (과열)
MIN_CHANGE_RATE = 1.0  # 등락률 1% 미만 제외 (모멘텀 부족)


@dataclass
class ClosingCandidate:
    """종가배팅 후보 종목."""

    stock_code: str
    stock_name: str
    score: float = 0.0
    grade: str = ""  # "강력추천", "추천", "관심"

    # 가격 데이터
    current_price: int = 0
    open_price: int = 0
    high_price: int = 0
    low_price: int = 0
    change_rate: float = 0.0
    volume: int = 0
    volume_rate: float = 0.0
    trading_value: int = 0

    # 스코어링 세부
    score_volume: float = 0.0  # 거래량 급증률 (25점)
    score_change: float = 0.0  # 당일 등락률 (20점)
    score_strength: float = 0.0  # 체결강도 (15점)
    score_candle: float = 0.0  # 양봉 품질 (15점)
    score_ma: float = 0.0  # 이동평균선 (15점)
    score_investor: float = 0.0  # 외국인/기관 (10점)

    # 원시 지표
    trade_strength: float = 0.0
    foreign_net: int = 0
    institution_net: int = 0
    above_ma5: bool = False
    above_ma20: bool = False
    above_ma60: bool = False


@dataclass
class ClosingReport:
    """종가배팅 스크리닝 리포트."""

    timestamp: str
    candidates: list[ClosingCandidate] = field(default_factory=list)
    total_scanned: int = 0
    cached_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "candidates": [asdict(c) for c in self.candidates],
            "total_scanned": self.total_scanned,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ClosingReport":
        return cls(
            timestamp=data["timestamp"],
            candidates=[ClosingCandidate(**c) for c in data.get("candidates", [])],
            total_scanned=data.get("total_scanned", 0),
        )


def is_screening() -> bool:
    """스크리너가 현재 실행 중인지 반환."""
    return _screening


# ── 스코어링 함수 ────────────────────────────────────────


def _score_volume_rate(volume_rate: float) -> float:
    """거래량 급증률 점수 (25점 만점).

    200% → 15점, 500%+ → 25점 (선형 보간)
    """
    if volume_rate < 200:
        return 0.0
    if volume_rate >= 500:
        return 25.0
    return 15.0 + (volume_rate - 200) / 300 * 10.0


def _score_change_rate(change_rate: float) -> float:
    """당일 등락률 점수 (20점 만점).

    +2~5% → 20점 (이상적), +5~8% → 15점, +8~10% → 10점,
    +10~15% → 5점, +1~2% → 10점
    """
    if 2.0 <= change_rate < 5.0:
        return 20.0
    if 5.0 <= change_rate < 8.0:
        return 15.0
    if 8.0 <= change_rate < 10.0:
        return 10.0
    if 10.0 <= change_rate <= 15.0:
        return 5.0
    if 1.0 <= change_rate < 2.0:
        return 10.0
    return 0.0


def _score_trade_strength(strength: float) -> float:
    """체결강도 점수 (15점 만점).

    100~120% → 8~15점 (선형), 120%+ → 15점
    """
    if strength < 100:
        return 0.0
    if strength >= 120:
        return 15.0
    return 8.0 + (strength - 100) / 20 * 7.0


def _score_candle_quality(
    open_price: int, high_price: int, low_price: int, close: int,
) -> float:
    """양봉 품질 점수 (15점 만점).

    (종가-시가) / (고가-저가) 비율. 음봉이면 0점, 윗꼬리 짧을수록 고점수.
    """
    if close <= open_price:
        return 0.0
    candle_range = high_price - low_price
    if candle_range <= 0:
        return 0.0
    body_ratio = (close - open_price) / candle_range
    return round(body_ratio * 15.0, 1)


def _score_moving_averages(
    current_price: int, daily_prices: list[dict],
) -> tuple[float, bool, bool, bool]:
    """이동평균선 점수 (15점 만점).

    5일선 위 +5, 20일선 위 +5, 60일선 위 +5.
    """
    if not daily_prices:
        return 0.0, False, False, False

    closes = [d["close"] for d in daily_prices]
    score = 0.0
    above_5 = False
    above_20 = False
    above_60 = False

    if len(closes) >= 5:
        ma5 = sum(closes[:5]) / 5
        above_5 = current_price > ma5
        if above_5:
            score += 5.0

    if len(closes) >= 20:
        ma20 = sum(closes[:20]) / 20
        above_20 = current_price > ma20
        if above_20:
            score += 5.0

    if len(closes) >= 60:
        ma60 = sum(closes[:60]) / 60
        above_60 = current_price > ma60
        if above_60:
            score += 5.0

    return score, above_5, above_20, above_60


def _score_investor(foreign_net: int, institution_net: int) -> float:
    """외국인/기관 순매수 점수 (10점 만점).

    둘 다 순매수 → 10점, 하나만 → 5점.
    """
    score = 0.0
    if foreign_net > 0:
        score += 5.0
    if institution_net > 0:
        score += 5.0
    return score


def _assign_grade(score: float) -> str:
    """점수 기반 등급 부여."""
    if score >= 80:
        return "강력추천"
    if score >= 60:
        return "추천"
    if score >= 40:
        return "관심"
    return ""


# ── 메인 스크리닝 ────────────────────────────────────────


def run_closing_screener(force_refresh: bool = False) -> ClosingReport | None:
    """종가배팅 스크리너 실행.

    1차: 거래량 급증률 상위 종목 추출 (거래대금/가격 필터)
    2차: 6개 지표 스코어링
    """
    if not force_refresh:
        cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            logger.info("[종가배팅] 캐시 사용")
            report = ClosingReport.from_dict(cached)
            report.cached_at = cached_at
            return report

    global _screening  # noqa: PLW0603
    with _screening_lock:
        if _screening:
            logger.info("[종가배팅] 이미 실행 중")
            cached, cached_at = cache.get(_CACHE_KEY, ttl_seconds=None)
            if cached is not None:
                report = ClosingReport.from_dict(cached)
                report.cached_at = cached_at
                return report
            return None
        _screening = True

    logger.info("[종가배팅] 스크리닝 시작")
    try:
        return _run_screening_impl()
    finally:
        with _screening_lock:
            _screening = False


def _run_screening_impl() -> ClosingReport:
    """스크리닝 본체."""
    # 1차: 거래량 순위 100개 조회
    volume_data = get_volume_rank(max_items=110)

    # 1차 필터링
    candidates: list[dict] = []
    for item in volume_data:
        if "ETF" in item["stock_name"].upper():
            continue
        if item["volume_rate"] < MIN_VOLUME_RATE:
            continue
        if item["trading_value"] < MIN_TRADING_VALUE:
            continue
        if item["current_price"] < MIN_PRICE:
            continue
        if item["change_rate"] > MAX_CHANGE_RATE or item["change_rate"] < MIN_CHANGE_RATE:
            continue
        candidates.append(item)

    total_scanned = len(volume_data)
    logger.info("[종가배팅] 1차 필터: %d → %d종목", total_scanned, len(candidates))

    if not candidates:
        report = ClosingReport(
            timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
            total_scanned=total_scanned,
        )
        cache.put(_CACHE_KEY, report.to_dict())
        return report

    # 2차: 세부 데이터 병렬 조회 + 스코어링
    def _fetch_details(item: dict) -> ClosingCandidate | None:
        code = item["stock_code"]

        price_info = get_stock_price(code)
        if not price_info:
            return None

        strength = get_trade_strength(code)
        if strength is None:
            strength = 0.0

        investor = get_investor_trend(code)
        foreign_net = investor.get("foreign_net_qty", 0)
        institution_net = investor.get("institution_net_qty", 0)

        daily = get_daily_prices(code, days=60)

        # 스코어링
        s_volume = _score_volume_rate(item["volume_rate"])
        s_change = _score_change_rate(item["change_rate"])
        s_strength = _score_trade_strength(strength)
        s_candle = _score_candle_quality(
            price_info["open_price"],
            price_info["high_price"],
            price_info["low_price"],
            price_info["current_price"],
        )
        s_ma, above_5, above_20, above_60 = _score_moving_averages(
            price_info["current_price"], daily,
        )
        s_investor = _score_investor(foreign_net, institution_net)

        total_score = s_volume + s_change + s_strength + s_candle + s_ma + s_investor

        return ClosingCandidate(
            stock_code=code,
            stock_name=item["stock_name"],
            score=round(total_score, 1),
            grade=_assign_grade(total_score),
            current_price=price_info["current_price"],
            open_price=price_info["open_price"],
            high_price=price_info["high_price"],
            low_price=price_info["low_price"],
            change_rate=item["change_rate"],
            volume=item["volume"],
            volume_rate=item["volume_rate"],
            trading_value=item["trading_value"],
            score_volume=s_volume,
            score_change=s_change,
            score_strength=s_strength,
            score_candle=s_candle,
            score_ma=s_ma,
            score_investor=s_investor,
            trade_strength=strength,
            foreign_net=foreign_net,
            institution_net=institution_net,
            above_ma5=above_5,
            above_ma20=above_20,
            above_ma60=above_60,
        )

    results: list[ClosingCandidate] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch_details, item) for item in candidates]
        for future in futures:
            try:
                candidate = future.result()
                if candidate and candidate.score >= 40:
                    results.append(candidate)
            except Exception:
                logger.warning("[종가배팅] 상세 조회 실패", exc_info=True)

    results.sort(key=lambda x: x.score, reverse=True)

    report = ClosingReport(
        timestamp=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        candidates=results,
        total_scanned=total_scanned,
    )
    report.cached_at = cache.put(_CACHE_KEY, report.to_dict())
    logger.info("[종가배팅] 스크리닝 완료: %d종목 (40점↑)", len(results))
    return report


def score_single_stock(stock_code: str, stock_name: str) -> ClosingCandidate | None:
    """단일 종목의 종가배팅 점수를 계산한다."""
    price_info = get_stock_price(stock_code)
    if not price_info:
        return None

    current_price = price_info["current_price"]
    open_price = price_info["open_price"]
    high_price = price_info["high_price"]
    low_price = price_info["low_price"]
    change_rate = price_info["change_rate"]
    volume = price_info.get("volume", 0)
    volume_rate = price_info.get("volume_rate", 0.0)
    trading_value = price_info.get("trading_value", 0)

    strength = get_trade_strength(stock_code)
    if strength is None:
        strength = 0.0

    investor = get_investor_trend(stock_code)
    foreign_net = investor.get("foreign_net_qty", 0)
    institution_net = investor.get("institution_net_qty", 0)

    daily = get_daily_prices(stock_code, days=60)

    s_volume = _score_volume_rate(volume_rate)
    s_change = _score_change_rate(change_rate)
    s_strength = _score_trade_strength(strength)
    s_candle = _score_candle_quality(open_price, high_price, low_price, current_price)
    s_ma, above_5, above_20, above_60 = _score_moving_averages(current_price, daily)
    s_investor = _score_investor(foreign_net, institution_net)

    total_score = s_volume + s_change + s_strength + s_candle + s_ma + s_investor

    return ClosingCandidate(
        stock_code=stock_code,
        stock_name=stock_name,
        score=round(total_score, 1),
        grade=_assign_grade(total_score),
        current_price=current_price,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        change_rate=change_rate,
        volume=volume,
        volume_rate=volume_rate,
        trading_value=trading_value,
        score_volume=s_volume,
        score_change=s_change,
        score_strength=s_strength,
        score_candle=s_candle,
        score_ma=s_ma,
        score_investor=s_investor,
        trade_strength=strength,
        foreign_net=foreign_net,
        institution_net=institution_net,
        above_ma5=above_5,
        above_ma20=above_20,
        above_ma60=above_60,
    )
