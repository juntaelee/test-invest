"""한국투자증권 매매 API 래퍼.

잔고 조회, 시장가 매수/매도 주문을 처리한다.
"""

import logging

import requests

from auto_invest.api.kis_auth import auth
from auto_invest.config import settings

logger = logging.getLogger(__name__)


class KISTradingError(Exception):
    """KIS 매매 API 호출 실패."""


def get_balance() -> list[dict]:
    """보유 종목 잔고를 조회한다.

    Returns:
        종목별 잔고 딕셔너리 리스트.
        각 항목: stock_code, stock_name, quantity, avg_price, current_price,
                 profit_amount, profit_rate

    Raises:
        KISTradingError: API 호출 실패 시
    """
    tr_id = "VTTC8434R" if settings.is_paper else "TTTC8434R"
    url = f"{settings.base_url}/uapi/domestic-stock/v1/trading/inquire-balance"
    params = {
        "CANO": settings.kis_account_no,
        "ACNT_PRDT_CD": settings.kis_acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }

    holdings: list[dict] = []
    tr_cont = ""

    while True:
        headers = auth.get_headers(tr_id=tr_id)
        if tr_cont:
            headers["tr_cont"] = "N"

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise KISTradingError(f"잔고 조회 요청 실패: {e}") from e

        data = resp.json()

        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "알 수 없는 오류")
            logger.error("잔고 조회 실패: %s", msg)
            raise KISTradingError(f"잔고 조회 실패: {msg}")

        for raw in data.get("output1", []):
            # 실전/모의 API 응답 키 대소문자 통일
            item = {k.upper(): v for k, v in raw.items()}
            qty = int(item.get("HLDG_QTY", "0"))
            if qty <= 0:
                continue
            holdings.append({
                "stock_code": item["PDNO"],
                "stock_name": item["PRDT_NAME"],
                "quantity": qty,
                "avg_price": int(float(item.get("PCHS_AVG_PRIC", "0"))),
                "current_price": int(item.get("PRPR", "0")),
                "profit_amount": int(item.get("EVLU_PFLS_AMT", "0")),
                "profit_rate": float(item.get("EVLU_PFLS_RT", "0")),
            })

        # 페이지네이션
        tr_cont = data.get("tr_cont", "")
        if tr_cont == "M":
            ctx_fk = data.get("ctx_area_fk100", "")
            ctx_nk = data.get("ctx_area_nk100", "")
            params["CTX_AREA_FK100"] = ctx_fk
            params["CTX_AREA_NK100"] = ctx_nk
        else:
            break

    return holdings


def get_buying_power() -> int:
    """매수가능금액을 조회한다.

    Returns:
        매수가능금액 (원). 실패 시 0.
    """
    tr_id = "VTTC8908R" if settings.is_paper else "TTTC8908R"
    url = f"{settings.base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    headers = auth.get_headers(tr_id=tr_id)
    params = {
        "CANO": settings.kis_account_no,
        "ACNT_PRDT_CD": settings.kis_acnt_prdt_cd,
        "PDNO": "005930",  # 아무 종목 (매수가능금액 조회용)
        "ORD_UNPR": "0",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "N",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            logger.error("매수가능금액 조회 실패: %s", data.get("msg1"))
            return 0
        output = data.get("output", {})
        return int(output.get("nrcvb_buy_amt", "0"))
    except Exception:
        logger.exception("매수가능금액 조회 중 오류")
        return 0


def buy_order(stock_code: str, quantity: int) -> dict:
    """시장가 매수 주문.

    Returns:
        {success: bool, order_no: str | None, message: str}
    """
    tr_id = "VTTC0802U" if settings.is_paper else "TTTC0802U"
    return _place_order(tr_id, stock_code, quantity)


def sell_order(stock_code: str, quantity: int) -> dict:
    """시장가 매도 주문.

    Returns:
        {success: bool, order_no: str | None, message: str}
    """
    tr_id = "VTTC0801U" if settings.is_paper else "TTTC0801U"
    return _place_order(tr_id, stock_code, quantity)


def pre_market_buy_order(stock_code: str, quantity: int) -> dict:
    """장전 시간외 종가 매수 주문 (ORD_DVSN="05", 전일 종가 체결).

    Returns:
        {success: bool, order_no: str | None, message: str}
    """
    tr_id = "VTTC0802U" if settings.is_paper else "TTTC0802U"
    return _place_pre_market_order(tr_id, stock_code, quantity)


def _place_pre_market_order(tr_id: str, stock_code: str, quantity: int) -> dict:
    """장전 시간외 주문 공통 로직."""
    url = f"{settings.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    headers = auth.get_headers(tr_id=tr_id)
    body = {
        "CANO": settings.kis_account_no,
        "ACNT_PRDT_CD": settings.kis_acnt_prdt_cd,
        "PDNO": stock_code,
        "ORD_DVSN": "05",  # 장전 시간외 종가
        "ORD_QTY": str(quantity),
        "ORD_UNPR": "0",  # 종가 자동
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") == "0":
            order_no = data.get("output", {}).get("ODNO", "")
            return {"success": True, "order_no": order_no, "message": "장전 시간외 주문 성공"}

        msg = data.get("msg1", "알 수 없는 오류")
        logger.error("장전 시간외 주문 실패 [%s]: %s", tr_id, msg)
        return {"success": False, "order_no": None, "message": msg}
    except requests.RequestException as e:
        logger.error("장전 시간외 주문 요청 오류: %s", e)
        return {"success": False, "order_no": None, "message": str(e)}


def _place_order(tr_id: str, stock_code: str, quantity: int) -> dict:
    """주문 공통 로직."""
    url = f"{settings.base_url}/uapi/domestic-stock/v1/trading/order-cash"
    headers = auth.get_headers(tr_id=tr_id)
    body = {
        "CANO": settings.kis_account_no,
        "ACNT_PRDT_CD": settings.kis_acnt_prdt_cd,
        "PDNO": stock_code,
        "ORD_DVSN": "01",  # 시장가
        "ORD_QTY": str(quantity),
        "ORD_UNPR": "0",  # 시장가이므로 0
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("rt_cd") == "0":
            order_no = data.get("output", {}).get("ODNO", "")
            return {"success": True, "order_no": order_no, "message": "주문 성공"}

        msg = data.get("msg1", "알 수 없는 오류")
        logger.error("주문 실패 [%s]: %s", tr_id, msg)
        return {"success": False, "order_no": None, "message": msg}
    except requests.RequestException as e:
        logger.error("주문 요청 오류: %s", e)
        return {"success": False, "order_no": None, "message": str(e)}
