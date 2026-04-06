"""스케줄러: TP/SL 모니터 + 듀얼 스캔/추적 + 자동매매.

- TP/SL 체크: 1분 간격
- 스캔 (무거운): 3분 간격 — 전체 순위 조회 + 발굴 목록 갱신
- 추적 (가벼운): 30초 간격 — 발굴/이탈 종목 현재가/체결강도 업데이트
- 자동매수: 스캔 후 조건 충족 종목 자동 매수
- 장 종료(15:30) 시 자동매수 OFF
- 장 시작 시 전일 시계열/발굴 목록 클리어
"""

import logging
import threading
from datetime import datetime, timedelta, timezone

import schedule

from auto_invest.api.kis_trading import get_buying_power
from auto_invest.core.trading import (
    execute_buy,
    execute_pre_market_reservation,
    execute_sell,
    get_auto_trade_config,
    get_pending_pre_market_reservations,
    get_portfolio,
    is_auto_buy_enabled,
    set_auto_trade_config,
)
from auto_invest.strategy.scanner import (
    clear_discovered,
    get_discovered_stocks,
    run_scanner2,
    track_stocks,
)
from auto_invest.utils.timeseries import clear_old_data

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# 자동매수로 이미 매수한 종목 (당일, 중복 매수 방지)
_auto_bought_today: set[str] = set()
_auto_bought_date: str = ""


def is_market_open() -> bool:
    """현재 KST 시각이 장 운영시간(평일 09:00~15:30)인지 확인."""
    now = datetime.now(tz=KST)
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


# ── 장전/장개시 예약 ──────────────────────────────────────

_pre_market_executed_today: bool = False
_pre_market_executed_date: str = ""
_market_open_executed_today: bool = False
_market_open_executed_date: str = ""


def _execute_reservations(reservation_type: str, label: str) -> None:
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


# ── TP/SL 체크 ──────────────────────────────────────────

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


# ── 스캔 + 자동매수 ──────────────────────────────────────

def auto_scan() -> None:
    """장중 3분마다 발굴 스캐너 실행 + 자동매수."""
    if not is_market_open():
        return

    logger.info("[자동스캔] 스캐너 시작")
    try:
        run_scanner2(force_refresh=True)
        logger.info("[자동스캔] 스캐너 완료")
    except Exception:
        logger.warning("[자동스캔] 스캐너 실패", exc_info=True)
        return

    # 자동매수 체크
    if is_auto_buy_enabled():
        _execute_auto_buy()


def _execute_auto_buy() -> None:
    """조건 충족 종목 자동 매수."""
    global _auto_bought_today, _auto_bought_date  # noqa: PLW0603

    now = datetime.now(tz=KST)
    today = now.strftime("%Y-%m-%d")
    if _auto_bought_date != today:
        _auto_bought_today = set()
        _auto_bought_date = today

    config = get_auto_trade_config()
    strength_min = float(config.get("auto_buy_strength_min", "120"))
    change_max = float(config.get("auto_buy_change_max", "20"))
    fixed_amount = int(float(config.get("auto_buy_fixed_amount", "0")))
    tp_pct = float(config.get("auto_sell_tp", "5"))
    sl_pct = float(config.get("auto_sell_sl", "-5"))

    if fixed_amount <= 0:
        logger.warning("[자동매수] 1회 매수금액이 0 — 스킵")
        return

    stocks = get_discovered_stocks()
    for stock in stocks:
        code = stock["stock_code"]
        name = stock["stock_name"]

        if stock["status"] != "active":
            continue
        if code in _auto_bought_today:
            continue
        if stock["trade_strength"] < strength_min:
            continue
        if stock["change_rate"] >= change_max:
            continue
        if stock["current_price"] <= 0:
            continue

        # 매수 수량 계산
        quantity = fixed_amount // stock["current_price"]
        if quantity <= 0:
            continue

        # 매수가능금액 확인
        buying_power = get_buying_power()
        cost = stock["current_price"] * quantity
        if buying_power < cost:
            logger.info("[자동매수] 잔액 부족: %s (필요=%d, 가용=%d)", name, cost, buying_power)
            continue

        logger.info(
            "[자동매수] 매수 실행: %s(%s) %d주 × %d원 = %d원",
            name, code, quantity, stock["current_price"], cost,
        )
        result = execute_buy(
            stock_code=code,
            stock_name=name,
            quantity=quantity,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
        )
        if result["success"]:
            _auto_bought_today.add(code)
            logger.info("[자동매수] 성공: %s 주문번호=%s", name, result.get("order_no"))
        else:
            logger.error("[자동매수] 실패: %s %s", name, result.get("message"))


# ── 추적 (가벼운 업데이트) ──────────────────────────────

def auto_track() -> None:
    """장중 30초마다 발굴/이탈 종목 추적."""
    if not is_market_open():
        return
    try:
        count = track_stocks()
        if count > 0:
            logger.debug("[추적] %d종목 업데이트", count)
    except Exception:
        logger.warning("[추적] 실패", exc_info=True)


# ── 장 시작/종료 처리 ────────────────────────────────────

_day_init_done_date: str = ""
_day_close_done_date: str = ""


def check_day_init() -> None:
    """장 시작 시 (08:50) 전일 데이터 클리어."""
    global _day_init_done_date  # noqa: PLW0603

    now = datetime.now(tz=KST)
    today = now.strftime("%Y-%m-%d")
    if _day_init_done_date == today:
        return
    if now.weekday() >= 5:
        return

    target = now.replace(hour=8, minute=50, second=0, microsecond=0)
    if now < target:
        return

    _day_init_done_date = today
    clear_old_data()
    clear_discovered()
    logger.info("[장시작] 전일 데이터 클리어 완료")


def check_day_close() -> None:
    """장 종료 시 (15:30) 자동매수 OFF."""
    global _day_close_done_date  # noqa: PLW0603

    now = datetime.now(tz=KST)
    today = now.strftime("%Y-%m-%d")
    if _day_close_done_date == today:
        return
    if now.weekday() >= 5:
        return

    target = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < target:
        return

    _day_close_done_date = today
    if is_auto_buy_enabled():
        set_auto_trade_config("auto_buy_enabled", "false")
        set_auto_trade_config("auto_buy_fixed_amount", "0")
        logger.info("[장종료] 자동매수 OFF")


# ── 스케줄러 실행 ────────────────────────────────────────

def start_scheduler(stop_event: threading.Event) -> None:
    """schedule 루프 실행 (스레드용)."""
    # TP/SL 모니터
    schedule.every(1).minutes.do(check_tp_sl)
    # 스캔 (무거운) — 3분
    schedule.every(3).minutes.do(auto_scan)
    # 추적 (가벼운) — 30초
    schedule.every(30).seconds.do(auto_track)
    # 예약 처리
    schedule.every(10).seconds.do(check_pre_market_reservations)
    schedule.every(10).seconds.do(check_market_open_reservations)
    # 장 시작/종료
    schedule.every(30).seconds.do(check_day_init)
    schedule.every(30).seconds.do(check_day_close)

    logger.info(
        "스케줄러 시작 (TP/SL 1분, 스캔 3분, 추적 30초, "
        "시간외 08:30, 장개시 09:00, 장종료 15:30)"
    )

    # 서버 시작 시 장중이면 즉시 1회 스캔 (3분 대기 방지)
    if is_market_open():
        auto_scan()

    while not stop_event.is_set():
        schedule.run_pending()
        stop_event.wait(timeout=1)

    schedule.clear()
    logger.info("스케줄러 종료")
