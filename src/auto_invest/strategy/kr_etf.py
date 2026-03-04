"""한국 ETF 구성종목 조회 (네이버 금융 기반).

네이버 WiseReport에서 ETF 구성종목(PDF)과 종목코드를 가져온다.
결과는 SQLite에 캐싱한다 (24시간 TTL).
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field

import requests

from auto_invest.utils import cache

logger = logging.getLogger(__name__)

_WISEREPORT_ETF_URL = "https://navercomp.wisereport.co.kr/v2/ETF/ETF.aspx"
_WISEREPORT_AC_URL = "https://navercomp.wisereport.co.kr/v2/company/autocomplete.aspx"
_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
_REQUEST_TIMEOUT = 10
_REQUEST_DELAY = 0.3  # 요청 간 대기 (초)

# 캐시 TTL: 24시간 (ETF 구성종목은 하루 1회 변경)
CACHE_TTL_SECONDS = 86400

# 종목명 → 종목코드 캐시 키 접두어
_NAME_CACHE_PREFIX = "stock_code:"
_ETF_CACHE_PREFIX = "etf_holdings:"


@dataclass(frozen=True, slots=True)
class EtfConstituent:
    """ETF 구성종목."""

    code: str  # 종목코드 (예: "005930")
    name: str  # 종목명 (예: "삼성전자")
    weight: float  # 비중 (%) → 가중치로 사용


@dataclass
class EtfHoldings:
    """ETF 구성종목 목록."""

    etf_code: str
    constituents: list[EtfConstituent] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "etf_code": self.etf_code,
            "constituents": [
                {"code": c.code, "name": c.name, "weight": c.weight} for c in self.constituents
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EtfHoldings":
        return cls(
            etf_code=data["etf_code"],
            constituents=[EtfConstituent(**c) for c in data.get("constituents", [])],
        )


def lookup_stock_name(code: str) -> str | None:
    """종목코드로 종목명을 조회 (WiseReport autocomplete)."""
    cache_key = f"stock_name:{code}"
    cached, _ = cache.get(cache_key, ttl_seconds=CACHE_TTL_SECONDS * 30)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        time.sleep(_REQUEST_DELAY)
        resp = requests.get(
            _WISEREPORT_AC_URL,
            params={"searchVal": code},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json()
        if results and isinstance(results, list):
            for item in results:
                if item.get("item_cd", "") == code:
                    name = item.get("item_nm", "")
                    if name:
                        cache.put(cache_key, name)
                        return name
    except Exception:
        logger.warning("종목명 조회 실패: %s", code, exc_info=True)

    return None


def _lookup_stock_code(name: str) -> str | None:
    """종목명으로 종목코드를 조회 (WiseReport autocomplete)."""
    # SQLite 캐시 확인 (종목코드는 잘 바뀌지 않으므로 긴 TTL)
    cache_key = f"{_NAME_CACHE_PREFIX}{name}"
    cached, _ = cache.get(cache_key, ttl_seconds=CACHE_TTL_SECONDS * 30)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        time.sleep(_REQUEST_DELAY)
        resp = requests.get(
            _WISEREPORT_AC_URL,
            params={"searchVal": name},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json()
        if results and isinstance(results, list):
            code = results[0].get("item_cd", "")
            if code:
                cache.put(cache_key, code)
                return code
    except Exception:
        logger.warning("종목코드 조회 실패: %s", name, exc_info=True)

    return None


def fetch_etf_holdings(etf_code: str) -> EtfHoldings:
    """ETF 구성종목을 조회하여 반환.

    Args:
        etf_code: 한국 ETF 종목코드 (예: "091160")

    Returns:
        EtfHoldings (구성종목 비중순 정렬)
    """
    cache_key = f"{_ETF_CACHE_PREFIX}{etf_code}"

    # SQLite 캐시 확인
    cached, _ = cache.get(cache_key, ttl_seconds=CACHE_TTL_SECONDS)
    if cached is not None:
        logger.debug("ETF %s 캐시 사용", etf_code)
        return EtfHoldings.from_dict(cached)  # type: ignore[arg-type]

    logger.info("ETF %s 구성종목 조회 중...", etf_code)
    holdings = EtfHoldings(etf_code=etf_code)

    try:
        resp = requests.get(
            _WISEREPORT_ETF_URL,
            params={"cmp_cd": etf_code, "cn": ""},
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        # 페이지 내 JSON 데이터 추출 (CU_data 변수)
        match = re.search(r'"grid_data"\s*:\s*(\[.*?\])', resp.text)
        if not match:
            logger.warning("ETF %s 구성종목 데이터 없음", etf_code)
            cache.put(cache_key, holdings.to_dict())
            return holdings

        items = json.loads(match.group(1))

        for item in items:
            name = item.get("STK_NM_KOR", "").strip()
            raw_weight = item.get("ETF_WEIGHT")
            weight = float(raw_weight) if raw_weight is not None else 0.0
            if not name or weight <= 0:
                continue

            code = _lookup_stock_code(name)
            if not code:
                logger.debug("종목코드 미확인: %s (건너뜀)", name)
                continue

            holdings.constituents.append(EtfConstituent(code=code, name=name, weight=weight))

        # 비중 내림차순 정렬
        holdings.constituents.sort(key=lambda c: c.weight, reverse=True)
        logger.info("ETF %s: %d개 종목 조회 완료", etf_code, len(holdings.constituents))

    except Exception:
        logger.warning("ETF %s 조회 실패", etf_code, exc_info=True)

    cache.put(cache_key, holdings.to_dict())
    return holdings


def fetch_multiple_etf_holdings(etf_codes: list[str]) -> dict[str, EtfHoldings]:
    """여러 ETF의 구성종목을 일괄 조회."""
    results: dict[str, EtfHoldings] = {}
    for code in etf_codes:
        results[code] = fetch_etf_holdings(code)
    return results
