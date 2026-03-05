"""추천 오케스트레이터.

미국 시장 데이터 수집 → 뉴스 수집 → 스코어링 → 보고서 생성 파이프라인.
결과는 SQLite에 캐싱한다 (1시간 TTL).
"""

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from auto_invest.utils import cache

from .scorer import Recommendation, score_stocks
from .us_market_data import MarketSnapshot, fetch_market_snapshot
from .us_news import NewsResult, fetch_news

logger = logging.getLogger(__name__)

# 캐시 TTL (초): 24시간 (수동 새로고침으로 갱신)
CACHE_TTL_SECONDS = 86400
_CACHE_KEY_PREFIX = "recommendation:"


@dataclass
class RecommendationReport:
    """추천 보고서."""

    timestamp: str
    market_snapshot: MarketSnapshot
    news_result: NewsResult
    recommendations: list[Recommendation] = field(default_factory=list)
    cached_at: float | None = None  # 캐시 저장 시각 (unix timestamp)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "market_snapshot": asdict(self.market_snapshot),
            "news_result": asdict(self.news_result),
            "recommendations": [asdict(r) for r in self.recommendations],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RecommendationReport":
        return cls(
            timestamp=data["timestamp"],
            market_snapshot=MarketSnapshot(**data["market_snapshot"]),
            news_result=NewsResult(**data["news_result"]),
            recommendations=[Recommendation(**r) for r in data.get("recommendations", [])],
        )

    def summary(self) -> str:
        """사람이 읽을 수 있는 요약 텍스트."""
        lines = [
            f"=== 한국 종목 추천 보고서 ({self.timestamp}) ===",
            "",
            "[ 미국 지수 등락률 ]",
        ]
        for name, pct in self.market_snapshot.index_changes.items():
            lines.append(f"  {name}: {pct:+.2f}%")

        lines.append("")
        lines.append("[ 미국 섹터 ETF 등락률 ]")
        for etf, pct in sorted(self.market_snapshot.sector_changes.items(), key=lambda x: -x[1]):
            lines.append(f"  {etf}: {pct:+.2f}%")

        if self.market_snapshot.theme_changes:
            lines.append("")
            lines.append("[ 미국 테마 ETF 등락률 ]")
            for etf, pct in sorted(self.market_snapshot.theme_changes.items(), key=lambda x: -x[1]):
                lines.append(f"  {etf}: {pct:+.2f}%")

        lines.append("")
        lines.append(f"[ 뉴스 헤드라인 {len(self.news_result.headlines)}건 ]")
        if self.news_result.keyword_counts:
            lines.append("  주요 키워드:")
            for kw, cnt in sorted(self.news_result.keyword_counts.items(), key=lambda x: -x[1]):
                lines.append(f"    {kw}: {cnt}건")

        lines.append("")
        lines.append(f"[ 추천 종목 TOP {len(self.recommendations)} ]")
        for i, rec in enumerate(self.recommendations, 1):
            lines.append(
                f"  {i}. {rec.name}({rec.code}) "
                f"종합={rec.total_score:.1f} "
                f"(섹터={rec.sector_score:.1f}, 테마={rec.theme_score:.1f}, "
                f"뉴스={rec.news_score:.1f})"
            )

        return "\n".join(lines)


def run_recommendation(
    top_n: int = 10,
    force_refresh: bool = False,
) -> RecommendationReport:
    """추천 파이프라인 실행.

    캐시 TTL(기본 1시간) 내 재호출 시 이전 결과를 반환한다.

    Args:
        top_n: 추천 종목 수 (기본 10)
        force_refresh: True이면 캐시 무시하고 새로 조회

    Returns:
        RecommendationReport
    """
    cache_key = f"{_CACHE_KEY_PREFIX}{top_n}"

    # SQLite 캐시 히트 확인
    if not force_refresh:
        cached, cached_at = cache.get(cache_key, ttl_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            logger.info("캐시 사용 (저장 시각: %s)", _format_timestamp(cached_at))
            report = RecommendationReport.from_dict(cached)  # type: ignore[arg-type]
            report.cached_at = cached_at
            return report

    logger.info("추천 파이프라인 시작 (top_n=%d)", top_n)

    # 1. 미국 시장 데이터 수집
    snapshot = fetch_market_snapshot()

    # 2. 미국 뉴스 수집
    news = fetch_news()

    # 3. 스코어링
    recommendations = score_stocks(
        sector_changes=snapshot.sector_changes,
        keyword_counts=news.keyword_counts,
        top_n=top_n,
        theme_changes=snapshot.theme_changes,
    )

    # 4. 보고서 생성
    report = RecommendationReport(
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        market_snapshot=snapshot,
        news_result=news,
        recommendations=recommendations,
    )

    # SQLite 캐시 저장
    report.cached_at = cache.put(cache_key, report.to_dict())
    logger.info("추천 완료: %d개 종목 (캐시 갱신)", len(recommendations))
    return report


def _format_timestamp(ts: float | None) -> str:
    if ts is None:
        return "알 수 없음"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
