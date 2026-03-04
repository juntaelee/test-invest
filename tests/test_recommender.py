"""추천 파이프라인 통합 테스트.

외부 API (yfinance, feedparser, wisereport)를 mock하여 테스트한다.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from auto_invest.strategy.kr_etf import EtfConstituent, EtfHoldings
from auto_invest.strategy.recommender import RecommendationReport, run_recommendation
from auto_invest.strategy.us_market_data import MarketSnapshot
from auto_invest.strategy.us_news import NewsResult


def _mock_fetch_etf_holdings(etf_code: str) -> EtfHoldings:
    """ETF 구성종목 mock 데이터."""
    data = {
        "091160": EtfHoldings(
            etf_code="091160",
            constituents=[
                EtfConstituent("000660", "SK하이닉스", 26.42),
                EtfConstituent("005930", "삼성전자", 22.77),
                EtfConstituent("042700", "한미반도체", 10.15),
            ],
        ),
        "363580": EtfHoldings(
            etf_code="363580",
            constituents=[
                EtfConstituent("005930", "삼성전자", 20.0),
                EtfConstituent("035420", "NAVER", 15.0),
                EtfConstituent("035720", "카카오", 10.0),
            ],
        ),
        "305720": EtfHoldings(
            etf_code="305720",
            constituents=[
                EtfConstituent("373220", "LG에너지솔루션", 30.0),
                EtfConstituent("006400", "삼성SDI", 25.0),
            ],
        ),
        "091180": EtfHoldings(
            etf_code="091180",
            constituents=[
                EtfConstituent("005380", "현대차", 30.0),
                EtfConstituent("000270", "기아", 25.0),
            ],
        ),
    }
    return data.get(etf_code, EtfHoldings(etf_code=etf_code))


def _make_mock_download(*args, **kwargs):
    """yf.download mock: 간단한 5일치 데이터 생성."""
    tickers = args[0].split() if args else []
    dates = pd.date_range("2026-02-26", periods=5, freq="B")

    prices = {
        "^GSPC": [5000, 5010, 5020, 5050, 5075],
        "^IXIC": [16000, 16050, 16100, 16200, 16350],
        "^DJI": [39000, 39050, 39100, 39080, 39150],
        "XLK": [200, 201, 202, 204, 208],
        "SOXX": [500, 502, 505, 510, 520],
        "XLV": [140, 140, 139, 138, 137],
        "XLE": [85, 85, 86, 87, 86],
        "XLI": [110, 111, 112, 113, 115],
        "XLB": [80, 80, 81, 81, 80],
        "XLF": [40, 40, 41, 41, 42],
        "XLY": [170, 171, 172, 173, 175],
        "XLC": [75, 76, 77, 78, 80],
    }

    df_data = {}
    for ticker in tickers:
        close_data = prices.get(ticker, [100, 100, 101, 102, 103])
        df_data[(ticker, "Close")] = close_data
        df_data[(ticker, "Open")] = close_data
        df_data[(ticker, "High")] = close_data
        df_data[(ticker, "Low")] = close_data
        df_data[(ticker, "Volume")] = [1000000] * 5

    return pd.DataFrame(df_data, index=dates)


def _make_mock_feed(url: str):
    """feedparser.parse mock."""
    mock = MagicMock()
    headlines = [
        "NVIDIA Beats Earnings, AI Chip Demand Surges",
        "Samsung Semiconductor Expansion Plans Announced",
        "Electric Vehicle Sales Hit New Record",
        "Battery Technology Breakthrough Reported",
        "Oil Prices Drop Amid Supply Concerns",
    ]
    mock.entries = [
        MagicMock(
            title=h,
            get=lambda key, default="", _h=h: _h if key == "title" else default,
        )
        for h in headlines
    ]
    return mock


@patch("auto_invest.strategy.scorer.fetch_etf_holdings", side_effect=_mock_fetch_etf_holdings)
@patch("auto_invest.strategy.us_news.feedparser.parse", side_effect=_make_mock_feed)
@patch("auto_invest.strategy.us_market_data.yf.download", side_effect=_make_mock_download)
class TestRunRecommendation:
    """전체 추천 파이프라인 통합 테스트."""

    def test_full_pipeline(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=5, force_refresh=True)

        assert isinstance(report, RecommendationReport)
        assert report.timestamp
        assert isinstance(report.market_snapshot, MarketSnapshot)
        assert isinstance(report.news_result, NewsResult)

    def test_recommendations_not_empty(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=10, force_refresh=True)
        assert len(report.recommendations) > 0

    def test_recommendations_sorted_by_score(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=10, force_refresh=True)

        for i in range(len(report.recommendations) - 1):
            assert (
                report.recommendations[i].total_score
                >= report.recommendations[i + 1].total_score
            )

    def test_top_n_respected(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=3, force_refresh=True)
        assert len(report.recommendations) <= 3

    def test_summary_format(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=5, force_refresh=True)
        summary = report.summary()

        assert "한국 종목 추천 보고서" in summary
        assert "미국 지수 등락률" in summary
        assert "추천 종목" in summary

    def test_market_snapshot_has_data(self, mock_download, mock_parse, mock_etf):
        report = run_recommendation(top_n=5, force_refresh=True)

        assert len(report.market_snapshot.index_changes) > 0
        assert len(report.market_snapshot.sector_changes) > 0
