"""매매 전략 모듈.

미국 시장 기반 한국 종목 추천 기능을 제공한다.
"""

from .recommender import RecommendationReport, run_recommendation
from .scorer import Recommendation

__all__ = [
    "Recommendation",
    "RecommendationReport",
    "run_recommendation",
]
