"""TP/SL 자동 체크 & 매도 스케줄러.

1분 간격으로 포트폴리오를 조회하여 익절/손절 조건 도달 시
시장가 전량 매도를 실행한다. 장 운영시간(09:00~15:30 KST, 평일)에만 동작.
"""

import logging
import threading
from datetime import datetime, timedelta, timezone

import schedule

from auto_invest.core.trading import (
    execute_pre_market_reservation,
    execute_sell,
    get_pending_pre_market_reservations,
    get_portfolio,
)

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def is_market_open() -> bool:
    """현재 KST 시각이 장 운영시간(평일 09:00~15:30)인지 확인."""
    now = datetime.now(tz=KST)
    # 월(0)~금(4)만 허용
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


_pre_market_executed_today: bool = False
_pre_market_executed_date: str = ""
_market_open_executed_today: bool = False
_market_open_executed_date: str = ""


def _execute_reservations(
    reservation_type: str,
    label: str,
) -> None:
    """지정 타입의 pending 예약을 일괄 실행한다."""
    reservations = get_pending_pre_market_reservations(
        reservation_type=reservation_type,
    )
    if not reservations:
        logger.info("%s 예약 없음 — 스킵", label)
        return

    logger.info("%s 예약 %d건 실행 시작", label, len(reservations))
    for res in reservations:
        try:
            result = execute_pre_market_reservation(res["id"])
            status = "성공" if result.get("success") else "실패"
            logger.info(
                "  [%s] %s(%s) %d주: %s",
                status, res["stock_name"], res["stock_code"],
                res["quantity"], result.get("message", ""),
            )
        except Exception:
            logger.error(
                "  예약 실행 오류: %s(%s)",
                res["stock_name"], res["stock_code"],
                exc_info=True,
            )


def check_pre_market_reservations() -> None:
    """08:30 KST에 장전 시간외 예약을 일괄 실행한다."""
    global _pre_market_executed_today, _pre_market_executed_date  # noqa: PLW0603

    now = datetime.now(tz=KST)
    today = now.strftime("%Y-%m-%d")

    if _pre_market_executed_date != today:
        _pre_market_executed_today = False
        _pre_market_executed_date = today

    if _pre_market_executed_today:
        return
    if now.weekday() >= 5:
        return

    target = now.replace(hour=8, minute=30, second=0, microsecond=0)
    if now < target:
        return

    _pre_market_executed_today = True
    _execute_reservations("pre_market", "장전 시간외")


def check_market_open_reservations() -> None:
    """09:00 KST에 장개시 시장가 매수 예약을 일괄 실행한다."""
    global _market_open_executed_today, _market_open_executed_date  # noqa: PLW0603

    now = datetime.now(tz=KST)
    today = now.strftime("%Y-%m-%d")

    if _market_open_executed_date != today:
        _market_open_executed_today = False
        _market_open_executed_date = today

    if _market_open_executed_today:
        return
    if now.weekday() >= 5:
        return

    target = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < target:
        return

    _market_open_executed_today = True
    _execute_reservations("market_open", "장개시 매수")


def check_tp_sl() -> None:
    """포트폴리오 조회 → TP/SL 도달 종목 자동 매도."""
    if not is_market_open():
        logger.debug("장 시간 외 — TP/SL 체크 스킵")
        return

    try:
        portfolio = get_portfolio()
    except Exception:
        logger.warning("TP/SL 체크: 포트폴리오 조회 실패", exc_info=True)
        return

    for item in portfolio:
        code = item["stock_code"]
        name = item.get("stock_name", code)
        quantity = item.get("quantity", 0)

        if quantity <= 0:
            continue

        if item.get("tp_reached"):
            logger.info(
                "TP 도달 매도 실행: %s(%s) 수익률=%.2f%% TP=%.2f%%",
                name, code, item.get("profit_rate", 0), item.get("tp_pct", 0),
            )
            result = execute_sell(code, quantity)
            if result["success"]:
                logger.info("TP 매도 성공: %s 주문번호=%s", name, result.get("order_no"))
            else:
                logger.error("TP 매도 실패: %s %s", name, result.get("message"))

        elif item.get("sl_reached"):
            logger.info(
                "SL 도달 매도 실행: %s(%s) 수익률=%.2f%% SL=%.2f%%",
                name, code, item.get("profit_rate", 0), item.get("sl_pct", 0),
            )
            result = execute_sell(code, quantity)
            if result["success"]:
                logger.info("SL 매도 성공: %s 주문번호=%s", name, result.get("order_no"))
            else:
                logger.error("SL 매도 실패: %s %s", name, result.get("message"))


def start_scheduler(stop_event: threading.Event) -> None:
    """schedule 루프 실행 (스레드용).

    stop_event가 set되면 루프를 종료한다.
    """
    schedule.every(1).minutes.do(check_tp_sl)
    schedule.every(10).seconds.do(check_pre_market_reservations)
    schedule.every(10).seconds.do(check_market_open_reservations)
    logger.info("스케줄러 시작 (TP/SL 1분, 시간외 08:30, 장개시 09:00)")

    while not stop_event.is_set():
        schedule.run_pending()
        stop_event.wait(timeout=1)

    schedule.clear()
    logger.info("TP/SL 모니터 종료")
