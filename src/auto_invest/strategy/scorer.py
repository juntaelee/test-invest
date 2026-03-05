"""스코어링 엔진.

섹터 등락률 + 테마 ETF 등락률 + 뉴스 키워드 기반으로 한국 종목 점수를 산출한다.
- 섹터 점수: 35% 가중치
- 테마 점수: 35% 가중치
- 뉴스 점수: 30% 가중치
- 최소 1.0점 이상만 추천

한국 ETF 구성종목을 자동으로 조회하여 동적으로 종목을 발굴한다.
"""

import logging
from dataclasses import dataclass

from .kr_etf import fetch_etf_holdings
from .mappings import KEYWORD_MAP, SECTOR_ETF_MAP, THEME_ETF_MAP, KrEtfMapping, KrStock

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Recommendation:
    """종목 추천 결과."""

    code: str
    name: str
    total_score: float
    sector_score: float
    theme_score: float
    news_score: float


SECTOR_WEIGHT = 0.35
THEME_WEIGHT = 0.35
NEWS_WEIGHT = 0.30
MIN_SCORE = 0.5
SCORE_MAX = 10.0
MAX_WEIGHT_PCT = 10.0  # 종목당 최대 가중치 (%)
MILD_DROP_THRESHOLD = -1.0  # 이 범위 내 하락은 감소된 가중치로 반영
MILD_DROP_FACTOR = 0.3  # 약보합 하락 시 적용 비율 (30%)


def _resolve_etf_to_stocks(etf_mappings: list[KrEtfMapping]) -> list[KrStock]:
    """한국 ETF 매핑 → 구성종목 KrStock 리스트로 변환.

    ETF 구성종목의 비중(%)을 가중치로 사용하되,
    MAX_WEIGHT_PCT로 상한을 제한하여 대형주 독식을 방지한다.
    """
    stocks: list[KrStock] = []
    for etf_mapping in etf_mappings:
        holdings = fetch_etf_holdings(etf_mapping.code)
        for constituent in holdings.constituents:
            # 비중 상한 제한 후 0~1 범위로 변환
            capped_pct = min(constituent.weight, MAX_WEIGHT_PCT)
            weight = capped_pct / 100.0
            stocks.append(
                KrStock(
                    code=constituent.code,
                    name=constituent.name,
                    weight=weight,
                )
            )
    return stocks


def _accumulate_scores(
    stock_scores: dict[str, float],
    stock_info: dict[str, KrStock],
    stocks: list[KrStock],
    value: float,
) -> None:
    """종목 리스트에 value × weight 를 누적."""
    for stock in stocks:
        key = stock.code
        stock_scores[key] = stock_scores.get(key, 0.0) + value * stock.weight
        # 종목 정보는 가장 높은 weight 기준으로 유지
        if key not in stock_info or stock.weight > stock_info[key].weight:
            stock_info[key] = stock


def _normalize(scores: dict[str, float], max_val: float = SCORE_MAX) -> dict[str, float]:
    """점수를 0~max_val 범위로 정규화."""
    if not scores:
        return {}
    peak = max(scores.values())
    if peak <= 0:
        return {k: 0.0 for k in scores}
    factor = max_val / peak
    return {k: round(v * factor, 2) for k, v in scores.items()}


def _effective_change(pct_change: float) -> float | None:
    """등락률을 유효 점수로 변환. 상승은 그대로, 약보합 하락은 감소 반영, 큰 하락은 제외."""
    if pct_change > 0:
        return pct_change
    if pct_change >= MILD_DROP_THRESHOLD:
        # -1% ~ 0%: 절대값의 30%를 점수로 부여 (약보합도 관심 유지)
        return abs(pct_change) * MILD_DROP_FACTOR
    return None


def calc_sector_scores(
    sector_changes: dict[str, float],
) -> tuple[dict[str, float], dict[str, KrStock]]:
    """섹터 ETF 등락률 기반 종목 점수 계산 (0~10 정규화)."""
    raw: dict[str, float] = {}
    info: dict[str, KrStock] = {}

    for etf, pct_change in sector_changes.items():
        effective = _effective_change(pct_change)
        if effective is None:
            continue
        etf_mappings = SECTOR_ETF_MAP.get(etf, [])
        stocks = _resolve_etf_to_stocks(etf_mappings)
        _accumulate_scores(raw, info, stocks, effective)

    return _normalize(raw), info


def calc_theme_scores(
    theme_changes: dict[str, float],
) -> tuple[dict[str, float], dict[str, KrStock]]:
    """테마 ETF 등락률 기반 종목 점수 계산 (0~10 정규화)."""
    raw: dict[str, float] = {}
    info: dict[str, KrStock] = {}

    for etf, pct_change in theme_changes.items():
        effective = _effective_change(pct_change)
        if effective is None:
            continue
        etf_mappings = THEME_ETF_MAP.get(etf, [])
        stocks = _resolve_etf_to_stocks(etf_mappings)
        _accumulate_scores(raw, info, stocks, effective)

    return _normalize(raw), info


def calc_news_scores(
    keyword_counts: dict[str, int],
) -> tuple[dict[str, float], dict[str, KrStock]]:
    """뉴스 키워드 출현 빈도 기반 종목 점수 계산 (0~10 정규화)."""
    raw: dict[str, float] = {}
    info: dict[str, KrStock] = {}

    for keyword, count in keyword_counts.items():
        etf_mappings = KEYWORD_MAP.get(keyword, [])
        stocks = _resolve_etf_to_stocks(etf_mappings)
        _accumulate_scores(raw, info, stocks, count)

    return _normalize(raw), info


def score_stocks(
    sector_changes: dict[str, float],
    keyword_counts: dict[str, int],
    top_n: int = 10,
    theme_changes: dict[str, float] | None = None,
) -> list[Recommendation]:
    """종합 점수를 산출하여 추천 종목 리스트를 반환.

    Args:
        sector_changes: 섹터 ETF 등락률 (%)
        keyword_counts: 뉴스 키워드 출현 횟수
        top_n: 추천 종목 수
        theme_changes: 테마 ETF 등락률 (%)

    Returns:
        점수순으로 정렬된 Recommendation 리스트
    """
    sector_scores, sector_info = calc_sector_scores(sector_changes)
    theme_scores, theme_info = calc_theme_scores(theme_changes or {})
    news_scores, news_info = calc_news_scores(keyword_counts)

    # 종목 정보 통합 (섹터 → 테마 → 뉴스 순 우선)
    all_codes = set(sector_scores) | set(theme_scores) | set(news_scores)
    stock_info: dict[str, KrStock] = {}
    for code in all_codes:
        stock_info[code] = (
            sector_info.get(code) or theme_info.get(code) or news_info.get(code)  # type: ignore[assignment]
        )

    # 종합 점수 계산
    results: list[Recommendation] = []
    for code in all_codes:
        s_score = sector_scores.get(code, 0.0)
        t_score = theme_scores.get(code, 0.0)
        n_score = news_scores.get(code, 0.0)
        total = round(
            s_score * SECTOR_WEIGHT + t_score * THEME_WEIGHT + n_score * NEWS_WEIGHT,
            2,
        )

        if total < MIN_SCORE:
            continue

        info = stock_info[code]
        results.append(
            Recommendation(
                code=info.code,
                name=info.name,
                total_score=total,
                sector_score=s_score,
                theme_score=t_score,
                news_score=n_score,
            )
        )

    results.sort(key=lambda r: r.total_score, reverse=True)
    return results[:top_n]
