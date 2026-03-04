"""미국 뉴스 수집 (Google News RSS 기반).

Google News RSS 피드에서 헤드라인을 수집하고 키워드 출현 빈도를 계산한다.
"""

import logging
from dataclasses import dataclass

import feedparser

from .mappings import KEYWORD_MAP

logger = logging.getLogger(__name__)

# Google News RSS 피드 URL 목록
NEWS_FEEDS = [
    # Business
    "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
    # Technology
    "https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRqTVhZU0FtVnVHZ0pWVXlnQVAB",
    "https://news.google.com/rss/search?q=stock+market&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=semiconductor&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=EV+battery&hl=en-US&gl=US&ceid=US:en",
]


@dataclass
class NewsResult:
    """뉴스 수집 결과."""

    headlines: list[str]
    keyword_counts: dict[str, int]  # 키워드 → 출현 횟수


def _fetch_headlines() -> list[str]:
    """모든 RSS 피드에서 헤드라인을 수집하고 중복 제거."""
    seen: set[str] = set()
    headlines: list[str] = []

    for url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title = entry.get("title", "").strip()
                if title and title not in seen:
                    seen.add(title)
                    headlines.append(title)
        except Exception:
            logger.warning("RSS 피드 파싱 실패: %s", url, exc_info=True)

    logger.info("뉴스 헤드라인 %d건 수집", len(headlines))
    return headlines


def count_keywords(headlines: list[str]) -> dict[str, int]:
    """헤드라인에서 키워드 출현 빈도를 계산."""
    counts: dict[str, int] = {}
    lower_headlines = [h.lower() for h in headlines]

    for keyword in KEYWORD_MAP:
        kw_lower = keyword.lower()
        count = sum(1 for h in lower_headlines if kw_lower in h)
        if count > 0:
            counts[keyword] = count

    return counts


def fetch_news() -> NewsResult:
    """뉴스를 수집하고 키워드 분석 결과를 반환."""
    headlines = _fetch_headlines()
    keyword_counts = count_keywords(headlines)

    for kw, cnt in sorted(keyword_counts.items(), key=lambda x: -x[1]):
        logger.info("  키워드 '%s': %d건", kw, cnt)

    return NewsResult(headlines=headlines, keyword_counts=keyword_counts)
