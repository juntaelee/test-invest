"""미국 시장 데이터 수집 (yfinance 기반).

S&P500, NASDAQ, Dow 주요 지수 + 섹터 ETF 등락률을 조회한다.
"""

import logging
from dataclasses import dataclass, field

import yfinance as yf

from .mappings import SECTOR_ETF_MAP

logger = logging.getLogger(__name__)

# 주요 미국 지수
INDEX_TICKERS = {
    "S&P500": "^GSPC",
    "NASDAQ": "^IXIC",
    "DOW": "^DJI",
}


@dataclass
class MarketSnapshot:
    """미국 시장 스냅샷 (등락률 %)."""

    index_changes: dict[str, float] = field(default_factory=dict)  # 지수명 → 등락률
    sector_changes: dict[str, float] = field(default_factory=dict)  # ETF 티커 → 등락률


def _calc_change_pct(ticker_data) -> float | None:
    """최근 2거래일 종가 기준 등락률(%) 계산."""
    close = ticker_data.get("Close")
    if close is None or len(close) < 2:
        return None
    values = close.dropna()
    if len(values) < 2:
        return None
    prev, last = values.iloc[-2], values.iloc[-1]
    if prev == 0:
        return None
    return round(((last - prev) / prev) * 100, 2)


def fetch_market_snapshot() -> MarketSnapshot:
    """미국 지수 + 섹터 ETF 등락률을 일괄 조회하여 반환."""
    all_tickers = list(INDEX_TICKERS.values()) + list(SECTOR_ETF_MAP.keys())
    ticker_str = " ".join(all_tickers)

    logger.info("yfinance 데이터 조회: %s", ticker_str)
    data = yf.download(ticker_str, period="5d", group_by="ticker", progress=False)

    snapshot = MarketSnapshot()

    # 지수 등락률
    for name, ticker in INDEX_TICKERS.items():
        ticker_data = data[ticker] if ticker in data.columns.get_level_values(0) else None
        if ticker_data is not None:
            pct = _calc_change_pct(ticker_data)
            if pct is not None:
                snapshot.index_changes[name] = pct
                logger.info("  %s: %+.2f%%", name, pct)

    # 섹터 ETF 등락률
    for etf in SECTOR_ETF_MAP:
        ticker_data = data[etf] if etf in data.columns.get_level_values(0) else None
        if ticker_data is not None:
            pct = _calc_change_pct(ticker_data)
            if pct is not None:
                snapshot.sector_changes[etf] = pct
                logger.info("  %s: %+.2f%%", etf, pct)

    return snapshot
