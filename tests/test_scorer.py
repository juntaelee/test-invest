"""스코어링 로직 단위 테스트.

kr_etf 조회를 mock하여 외부 API 호출 없이 테스트한다.
"""

from unittest.mock import patch

from auto_invest.strategy.kr_etf import EtfConstituent, EtfHoldings
from auto_invest.strategy.scorer import (
    Recommendation,
    calc_news_scores,
    calc_sector_scores,
    score_stocks,
)


def _mock_fetch_etf_holdings(etf_code: str) -> EtfHoldings:
    """ETF 구성종목 mock 데이터."""
    data = {
        "091160": EtfHoldings(  # KODEX 반도체
            etf_code="091160",
            constituents=[
                EtfConstituent("000660", "SK하이닉스", 26.42),
                EtfConstituent("005930", "삼성전자", 22.77),
                EtfConstituent("042700", "한미반도체", 10.15),
            ],
        ),
        "363580": EtfHoldings(  # KODEX K-테크TOP10
            etf_code="363580",
            constituents=[
                EtfConstituent("005930", "삼성전자", 20.0),
                EtfConstituent("035420", "NAVER", 15.0),
                EtfConstituent("035720", "카카오", 10.0),
            ],
        ),
        "305720": EtfHoldings(  # KODEX 2차전지산업
            etf_code="305720",
            constituents=[
                EtfConstituent("373220", "LG에너지솔루션", 30.0),
                EtfConstituent("006400", "삼성SDI", 25.0),
                EtfConstituent("051910", "LG화학", 15.0),
            ],
        ),
        "091180": EtfHoldings(  # KODEX 자동차
            etf_code="091180",
            constituents=[
                EtfConstituent("005380", "현대차", 30.0),
                EtfConstituent("000270", "기아", 25.0),
            ],
        ),
        "266420": EtfHoldings(  # KODEX 헬스케어
            etf_code="266420",
            constituents=[
                EtfConstituent("207940", "삼성바이오로직스", 25.0),
                EtfConstituent("068270", "셀트리온", 20.0),
            ],
        ),
        "117460": EtfHoldings(  # KODEX 에너지화학
            etf_code="117460",
            constituents=[
                EtfConstituent("096770", "SK이노베이션", 20.0),
                EtfConstituent("010950", "S-Oil", 15.0),
            ],
        ),
        "117680": EtfHoldings(  # KODEX 철강
            etf_code="117680",
            constituents=[
                EtfConstituent("005490", "POSCO홀딩스", 40.0),
            ],
        ),
        "091170": EtfHoldings(  # KODEX 은행
            etf_code="091170",
            constituents=[
                EtfConstituent("055550", "신한지주", 20.0),
                EtfConstituent("105560", "KB금융", 18.0),
            ],
        ),
        "140700": EtfHoldings(  # KODEX 보험
            etf_code="140700",
            constituents=[
                EtfConstituent("032830", "삼성생명", 30.0),
            ],
        ),
        "140710": EtfHoldings(  # KODEX 운송
            etf_code="140710",
            constituents=[
                EtfConstituent("003490", "대한항공", 25.0),
            ],
        ),
        "455850": EtfHoldings(  # KODEX K-조선
            etf_code="455850",
            constituents=[
                EtfConstituent("009540", "HD한국조선해양", 30.0),
                EtfConstituent("042660", "한화오션", 20.0),
            ],
        ),
    }
    return data.get(etf_code, EtfHoldings(etf_code=etf_code))


@patch("auto_invest.strategy.scorer.fetch_etf_holdings", side_effect=_mock_fetch_etf_holdings)
class TestCalcSectorScores:
    """섹터 점수 계산 테스트."""

    def test_rising_sector_gives_positive_score(self, mock_fetch):
        sector_changes = {"XLK": 2.5, "SOXX": 1.0}
        scores, info = calc_sector_scores(sector_changes)

        # 삼성전자, SK하이닉스가 점수를 받아야 함
        assert "005930" in scores
        assert "000660" in scores
        assert scores["005930"] > 0

    def test_falling_sector_gives_no_score(self, mock_fetch):
        sector_changes = {"XLK": -1.5, "SOXX": -2.0}
        scores, _ = calc_sector_scores(sector_changes)
        assert len(scores) == 0

    def test_mixed_sectors(self, mock_fetch):
        sector_changes = {"XLK": 3.0, "XLE": -1.0}
        scores, _ = calc_sector_scores(sector_changes)

        # XLK 관련주만 점수가 있어야 함
        assert "005930" in scores  # 삼성전자 (XLK → KODEX 반도체)
        assert "096770" not in scores  # SK이노베이션 (XLE, 하락)

    def test_normalization_max_is_10(self, mock_fetch):
        sector_changes = {"XLK": 5.0, "SOXX": 3.0}
        scores, _ = calc_sector_scores(sector_changes)
        assert max(scores.values()) == 10.0

    def test_empty_input(self, mock_fetch):
        scores, _ = calc_sector_scores({})
        assert scores == {}


@patch("auto_invest.strategy.scorer.fetch_etf_holdings", side_effect=_mock_fetch_etf_holdings)
class TestCalcNewsScores:
    """뉴스 점수 계산 테스트."""

    def test_keyword_hits_give_scores(self, mock_fetch):
        keyword_counts = {"semiconductor": 5, "battery": 3}
        scores, info = calc_news_scores(keyword_counts)

        assert "000660" in scores  # SK하이닉스 (반도체 ETF)
        assert "005930" in scores  # 삼성전자

    def test_battery_keywords_map_to_battery_stocks(self, mock_fetch):
        keyword_counts = {"battery": 10}
        scores, info = calc_news_scores(keyword_counts)

        assert "373220" in scores  # LG에너지솔루션
        assert "006400" in scores  # 삼성SDI

    def test_empty_keywords(self, mock_fetch):
        scores, _ = calc_news_scores({})
        assert scores == {}


@patch("auto_invest.strategy.scorer.fetch_etf_holdings", side_effect=_mock_fetch_etf_holdings)
class TestScoreStocks:
    """종합 스코어링 테스트."""

    def test_combined_scoring(self, mock_fetch):
        sector_changes = {"XLK": 2.0, "SOXX": 1.5}
        keyword_counts = {"semiconductor": 5, "ai": 3}

        results = score_stocks(sector_changes, keyword_counts, top_n=5)

        assert len(results) > 0
        assert isinstance(results[0], Recommendation)
        for i in range(len(results) - 1):
            assert results[i].total_score >= results[i + 1].total_score

    def test_minimum_score_filter(self, mock_fetch):
        sector_changes = {"XLF": 0.01}
        keyword_counts = {}

        results = score_stocks(sector_changes, keyword_counts, top_n=10)

        for rec in results:
            assert rec.total_score >= 1.0

    def test_top_n_limit(self, mock_fetch):
        sector_changes = {"XLK": 5.0, "SOXX": 4.0, "XLV": 3.0, "XLI": 2.0}
        keyword_counts = {"semiconductor": 10, "ai": 8, "battery": 5}

        results = score_stocks(sector_changes, keyword_counts, top_n=3)
        assert len(results) <= 3

    def test_recommendation_fields(self, mock_fetch):
        sector_changes = {"XLK": 3.0}
        keyword_counts = {"semiconductor": 5}

        results = score_stocks(sector_changes, keyword_counts)

        if results:
            rec = results[0]
            assert rec.code
            assert rec.name
            assert rec.total_score > 0
            assert rec.sector_score >= 0
            assert rec.news_score >= 0

    def test_both_empty(self, mock_fetch):
        results = score_stocks({}, {})
        assert results == []

    def test_dynamic_stocks_from_etf(self, mock_fetch):
        """ETF 구성종목이 동적으로 반영되는지 확인."""
        sector_changes = {"XLK": 3.0}
        keyword_counts = {}

        results = score_stocks(sector_changes, keyword_counts, top_n=10)

        # KODEX 반도체 구성종목인 한미반도체도 추천에 포함되어야 함
        codes = [r.code for r in results]
        assert "042700" in codes  # 한미반도체
