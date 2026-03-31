"""한국투자증권 시장 데이터 API 래퍼.

거래량 순위, 시간외 잔량 순위, 시간외 거래량 순위, 등락률 순위를 조회한다.
"""

import logging

import requests

from auto_invest.api.kis_auth import auth
from auto_invest.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT = 10


def _get_data(
    path: str,
    tr_id: str,
    params: dict,
    max_items: int = 30,
) -> list[dict]:
    """순위 API 공통 호출 로직."""
    url = f"{settings.base_url}/uapi/domestic-stock/v1/{path}"
    headers = auth.get_headers(tr_id=tr_id)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "알 수 없는 오류")
            logger.error("순위 조회 실패 [%s]: %s", tr_id, msg)
            return []

        items = data.get("output", [])
        if not items:
            items = data.get("output1", [])
        return items[:max_items]

    except requests.RequestException as e:
        logger.error("순위 API 요청 실패 [%s]: %s", tr_id, e)
        return []


def _filter_stocks(raw: list[dict]) -> list[dict]:
    """ETF/ETN 제외, 6자리 숫자 종목코드만 반환."""
    filtered = []
    for item in raw:
        code = item.get("mksc_shrn_iscd", "").strip()
        if not code:
            code = item.get("stck_shrn_iscd", "").strip()
        name = item.get("hts_kor_isnm", "").strip()
        if not code or not name:
            continue
        if len(code) != 6 or not code.isdigit():
            continue
        item["_code"] = code
        item["_name"] = name
        filtered.append(item)
    return filtered


def _volume_rank_common(
    blng_cls_code: str = "0",
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """거래량 순위 API 공통 호출.

    Args:
        blng_cls_code: 정렬 기준 ("0"=거래량, "3"=거래대금)
        market: "ALL" | "KOSPI" | "KOSDAQ"
        max_items: 최대 조회 건수
    """
    market_code = {"ALL": "0", "KOSPI": "1", "KOSDAQ": "2"}.get(market, "0")

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20171",
        "FID_INPUT_ISCD": "",
        "FID_DIV_CLS_CODE": market_code,
        "FID_BLNG_CLS_CODE": blng_cls_code,
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
        "FID_INPUT_PRICE_1": "0",
        "FID_INPUT_PRICE_2": "0",
        "FID_VOL_CNT": "0",
        "FID_INPUT_DATE_1": "",
    }

    raw = _get_data("quotations/volume-rank", "FHPST01710000", params, max_items)
    results = []
    for item in _filter_stocks(raw):
        results.append({
            "stock_code": item["_code"],
            "stock_name": item["_name"],
            "volume": int(item.get("acml_vol", "0")),
            "volume_rate": float(item.get("vol_inrt", "0")),
            "trading_value": int(item.get("acml_tr_pbmn", "0")),
            "turnover_rate": float(item.get("vol_tnrt", "0")),
            "current_price": int(item.get("stck_prpr", "0")),
            "change_rate": float(item.get("prdy_ctrt", "0")),
        })

    return results


def get_volume_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """거래량 순위 조회 (절대 거래량 기준)."""
    return _volume_rank_common("0", market, max_items)


def get_trading_value_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """거래대금 순위 조회."""
    return _volume_rank_common("3", market, max_items)


def get_turnover_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """회전율 순위 조회 (평균거래회전율 기준)."""
    return _volume_rank_common("2", market, max_items)


def get_investor_trend(stock_code: str) -> dict:
    """종목별 투자자 매매동향 조회 (당일).

    Returns:
        {"foreign_net_qty": int, "institution_net_qty": int,
         "foreign_net_value": int, "institution_net_value": int}
        오류 시 빈 dict.
    """
    url = f"{settings.base_url}/uapi/domestic-stock/v1/quotations/inquire-investor"
    headers = auth.get_headers(tr_id="FHKST01010900")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") != "0":
            return {}

        items = data.get("output", [])
        if not items:
            return {}

        # 첫 번째 항목이 당일 데이터
        today = items[0]
        return {
            "foreign_net_qty": int(today.get("frgn_ntby_qty") or "0"),
            "institution_net_qty": int(today.get("orgn_ntby_qty") or "0"),
            "foreign_net_value": int(today.get("frgn_ntby_tr_pbmn") or "0"),
            "institution_net_value": int(today.get("orgn_ntby_tr_pbmn") or "0"),
        }

    except requests.RequestException as e:
        logger.error("투자자 매매동향 조회 실패 [%s]: %s", stock_code, e)
        return {}


def get_after_hour_balance_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """시간외 잔량 순위 조회.

    장 마감 후 시간외 거래에서 매수잔량이 많은 종목 순위.

    Returns:
        [{"stock_code", "stock_name", "after_hour_buy_qty",
          "after_hour_sell_qty", "current_price", "change_rate"}, ...]
    """
    market_code = {"ALL": "0", "KOSPI": "1", "KOSDAQ": "2"}.get(market, "0")

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20176",
        "FID_INPUT_ISCD": "",
        "FID_DIV_CLS_CODE": market_code,
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
        "FID_INPUT_PRICE_1": "0",
        "FID_INPUT_PRICE_2": "0",
        "FID_VOL_CNT": "0",
        "FID_INPUT_DATE_1": "",
        "FID_RANK_SORT_CLS_CODE": "0",
    }

    raw = _get_data("ranking/after-hour-balance", "FHPST01760000", params, max_items)
    results = []
    for item in _filter_stocks(raw):
        results.append({
            "stock_code": item["_code"],
            "stock_name": item["_name"],
            "after_hour_buy_qty": int(item.get("ovtm_total_askp_rsqn", "0")),
            "after_hour_sell_qty": int(item.get("ovtm_total_bidp_rsqn", "0")),
            "current_price": int(item.get("stck_prpr", "0")),
            "change_rate": float(item.get("prdy_ctrt", "0")),
        })

    return results


def get_overtime_volume_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """시간외 거래량 순위 조회.

    시간외 거래에서 거래량이 많은 종목 순위.
    장전(08:30~08:40) 또는 장후(15:40~16:00) 시간대에만 데이터가 있음.

    Returns:
        [{"stock_code", "stock_name", "overtime_volume",
          "current_price", "change_rate"}, ...]
    """
    market_code = {"ALL": "0", "KOSPI": "1", "KOSDAQ": "2"}.get(market, "0")

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20175",
        "FID_INPUT_ISCD": "",
        "FID_DIV_CLS_CODE": market_code,
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
        "FID_INPUT_PRICE_1": "0",
        "FID_INPUT_PRICE_2": "0",
        "FID_VOL_CNT": "0",
        "FID_INPUT_DATE_1": "",
        "FID_RANK_SORT_CLS_CODE": "0",
        "FID_INPUT_OPTION_1": "0",
        "FID_INPUT_OPTION_2": "0",
    }

    raw = _get_data("ranking/overtime-volume", "FHPST01750000", params, max_items)
    results = []
    for item in _filter_stocks(raw):
        results.append({
            "stock_code": item["_code"],
            "stock_name": item["_name"],
            "overtime_volume": int(item.get("ovtm_vol", item.get("acml_vol", "0"))),
            "current_price": int(item.get("stck_prpr", "0")),
            "change_rate": float(item.get("prdy_ctrt", "0")),
        })

    return results


def get_fluctuation_rank(
    market: str = "ALL",
    max_items: int = 30,
) -> list[dict]:
    """등락률 순위 조회.

    상승률 상위 종목만 필터링하여 반환.

    Returns:
        [{"stock_code", "stock_name", "change_rate",
          "change_amount", "current_price", "volume"}, ...]
    """
    market_code = {"ALL": "0", "KOSPI": "1", "KOSDAQ": "2"}.get(market, "0")

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20170",
        "FID_INPUT_ISCD": "",
        "FID_DIV_CLS_CODE": market_code,
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
        "FID_INPUT_PRICE_1": "0",
        "FID_INPUT_PRICE_2": "0",
        "FID_VOL_CNT": "0",
        "FID_INPUT_DATE_1": "",
        "FID_RANK_SORT_CLS_CODE": "0",
    }

    raw = _get_data("ranking/fluctuation", "FHPST01770000", params, max_items * 2)
    results = []
    for item in _filter_stocks(raw):
        change_rate = float(item.get("prdy_ctrt", "0"))
        # 상승 종목만 (양수 등락률)
        if change_rate <= 0:
            continue
        results.append({
            "stock_code": item["_code"],
            "stock_name": item["_name"],
            "change_rate": change_rate,
            "change_amount": int(item.get("prdy_vrss", "0")),
            "current_price": int(item.get("stck_prpr", "0")),
            "volume": int(item.get("acml_vol", "0")),
        })

    return results[:max_items]


def get_trade_strength(stock_code: str) -> float | None:
    """종목의 체결강도를 조회한다 (당일 매수체결량/매도체결량 × 100).

    Returns:
        체결강도 (float). 100 이상이면 매수 우위. 실패 시 None.
    """
    url = f"{settings.base_url}/uapi/domestic-stock/v1/quotations/inquire-ccnl"
    headers = auth.get_headers(tr_id="FHKST01010300")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            return None
        items = data.get("output", [])
        if not items:
            return None
        return float(items[0].get("tday_rltv", "0"))
    except Exception:
        logger.exception("체결강도 조회 중 오류 (종목: %s)", stock_code)
        return None


def get_stock_price(stock_code: str) -> dict | None:
    """종목의 현재가를 조회한다.

    Returns:
        {"current_price": int, "change_rate": float, "change_amount": int,
         "prev_close": int, "volume": int} 또는 None
    """
    url = f"{settings.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = auth.get_headers(tr_id="FHKST01010100")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            logger.warning("현재가 조회 실패: %s", data.get("msg1"))
            return None
        output = data.get("output", {})
        return {
            "current_price": int(output.get("stck_prpr", "0")),
            "change_rate": float(output.get("prdy_ctrt", "0")),
            "change_amount": int(output.get("prdy_vrss", "0")),
            "prev_close": int(output.get("stck_sdpr", "0")),
            "volume": int(output.get("acml_vol", "0")),
        }
    except Exception:
        logger.exception("현재가 조회 중 오류 (종목: %s)", stock_code)
        return None
