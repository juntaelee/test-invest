"""매매 비즈니스 로직.

포지션 관리(TP/SL), 매수/매도 실행, 포트폴리오 조회를 담당한다.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from auto_invest.api import kis_trading

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

_DB_DIR = Path.home() / ".auto_invest"
_DB_PATH = _DB_DIR / "cache.db"

_CREATE_AUTO_TRADE_CONFIG = """
CREATE TABLE IF NOT EXISTS auto_trade_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_AUTO_TRADE_DEFAULTS = {
    "auto_buy_enabled": "false",
    "auto_buy_strength_min": "120",
    "auto_buy_change_max": "20",
    "auto_buy_ratio": "30",
    "auto_buy_fixed_amount": "0",
    "auto_sell_tp": "5",
    "auto_sell_sl": "-5",
}

_CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    stock_code TEXT PRIMARY KEY,
    stock_name TEXT NOT NULL,
    take_profit_pct REAL NOT NULL DEFAULT 4.0,
    stop_loss_pct REAL NOT NULL DEFAULT -3.0,
    bought_at TEXT NOT NULL,
    order_no TEXT
)
"""

_CREATE_PRE_MARKET_RESERVATIONS = """
CREATE TABLE IF NOT EXISTS pre_market_reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    tp_pct REAL NOT NULL DEFAULT 4.0,
    sl_pct REAL NOT NULL DEFAULT -3.0,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    executed_at TEXT,
    order_no TEXT,
    error_msg TEXT,
    reservation_type TEXT NOT NULL DEFAULT 'pre_market'
)
"""

# 기존 DB 마이그레이션: reservation_type 컬럼 추가
_MIGRATE_ADD_RESERVATION_TYPE = """
ALTER TABLE pre_market_reservations
ADD COLUMN reservation_type TEXT NOT NULL DEFAULT 'pre_market'
"""


def _get_conn() -> sqlite3.Connection:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=5)
    conn.execute(_CREATE_POSITIONS)
    conn.execute(_CREATE_PRE_MARKET_RESERVATIONS)
    conn.execute(_CREATE_AUTO_TRADE_CONFIG)
    # 기존 테이블에 reservation_type 컬럼이 없으면 추가
    try:
        conn.execute(_MIGRATE_ADD_RESERVATION_TYPE)
    except sqlite3.OperationalError:
        pass  # 이미 컬럼이 존재
    return conn


def execute_buy(
    stock_code: str,
    stock_name: str,
    quantity: int,
    tp_pct: float = 4.0,
    sl_pct: float = -3.0,
) -> dict:
    """매수 주문 실행 후 포지션 기록.

    Returns:
        {success: bool, order_no: str | None, message: str}
    """
    result = kis_trading.buy_order(stock_code, quantity)

    if result["success"]:
        now = datetime.now(tz=KST).isoformat()
        try:
            with _get_conn() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO positions
                       (stock_code, stock_name, take_profit_pct, stop_loss_pct, bought_at, order_no)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (stock_code, stock_name, tp_pct, sl_pct, now, result["order_no"]),
                )
        except sqlite3.Error:
            logger.warning("포지션 기록 실패: %s", stock_code, exc_info=True)

    return result


def execute_sell(stock_code: str, quantity: int) -> dict:
    """매도 주문 실행 후 포지션 삭제.

    Returns:
        {success: bool, order_no: str | None, message: str}
    """
    result = kis_trading.sell_order(stock_code, quantity)

    if result["success"]:
        try:
            with _get_conn() as conn:
                conn.execute("DELETE FROM positions WHERE stock_code = ?", (stock_code,))
        except sqlite3.Error:
            logger.warning("포지션 삭제 실패: %s", stock_code, exc_info=True)

    return result


def update_position(stock_code: str, tp_pct: float, sl_pct: float) -> bool:
    """포지션의 TP/SL 값을 수정한다."""
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "UPDATE positions SET take_profit_pct = ?, stop_loss_pct = ? WHERE stock_code = ?",
                (tp_pct, sl_pct, stock_code),
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        logger.warning("포지션 수정 실패: %s", stock_code, exc_info=True)
        return False


def delete_position(stock_code: str) -> bool:
    """포지션 기록을 삭제한다 (매도 없이 기록만 삭제)."""
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "DELETE FROM positions WHERE stock_code = ?", (stock_code,)
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        logger.warning("포지션 삭제 실패: %s", stock_code, exc_info=True)
        return False


def create_position(
    stock_code: str,
    stock_name: str,
    tp_pct: float = 4.0,
    sl_pct: float = -3.0,
) -> bool:
    """포지션을 신규 등록한다 (매수 없이 TP/SL만 설정)."""
    now = datetime.now(tz=KST).isoformat()
    try:
        with _get_conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO positions
                   (stock_code, stock_name, take_profit_pct, stop_loss_pct, bought_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (stock_code, stock_name, tp_pct, sl_pct, now),
            )
            return True
    except sqlite3.Error:
        logger.warning("포지션 등록 실패: %s", stock_code, exc_info=True)
        return False


def get_orphan_positions(holdings: list[dict] | None = None) -> list[dict]:
    """잔고에 없지만 positions 테이블에 남아 있는 고아 포지션을 반환한다."""
    if holdings is None:
        holdings = kis_trading.get_balance()

    holding_codes = {h["stock_code"] for h in holdings}

    orphans: list[dict] = []
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT stock_code, stock_name, take_profit_pct,"
                " stop_loss_pct, bought_at FROM positions"
            ).fetchall()
        for row in rows:
            if row[0] not in holding_codes:
                orphans.append({
                    "stock_code": row[0],
                    "stock_name": row[1],
                    "take_profit_pct": row[2],
                    "stop_loss_pct": row[3],
                    "bought_at": row[4],
                })
    except sqlite3.Error:
        logger.warning("고아 포지션 조회 실패", exc_info=True)

    return orphans


_RESERVATION_TYPE_LABELS = {
    "pre_market": "시간외",
    "market_open": "09시",
}


def create_pre_market_reservation(
    stock_code: str,
    stock_name: str,
    quantity: int,
    tp_pct: float = 4.0,
    sl_pct: float = -3.0,
    reservation_type: str = "pre_market",
) -> dict:
    """매수 예약을 등록한다 (pre_market: 08:30 시간외, market_open: 09:00 시장가)."""
    label = _RESERVATION_TYPE_LABELS.get(reservation_type, reservation_type)
    now = datetime.now(tz=KST).isoformat()
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """INSERT INTO pre_market_reservations
                   (stock_code, stock_name, quantity, tp_pct, sl_pct,
                    status, created_at, reservation_type)
                   VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)""",
                (stock_code, stock_name, quantity, tp_pct, sl_pct,
                 now, reservation_type),
            )
            return {
                "success": True,
                "id": cur.lastrowid,
                "message": f"{stock_name} {quantity}주 {label} 예약 등록",
            }
    except sqlite3.Error:
        logger.warning("예약 등록 실패: %s (%s)", stock_code, reservation_type, exc_info=True)
        return {"success": False, "id": None, "message": "예약 등록 실패"}


def get_pending_pre_market_reservations(
    reservation_type: str | None = None,
) -> list[dict]:
    """대기 중(pending)인 예약 목록을 반환한다.

    Args:
        reservation_type: 필터. None이면 전체, 'pre_market' 또는 'market_open'.
    """
    try:
        with _get_conn() as conn:
            if reservation_type:
                rows = conn.execute(
                    """SELECT id, stock_code, stock_name, quantity,
                              tp_pct, sl_pct, status, created_at,
                              executed_at, order_no, error_msg,
                              reservation_type
                       FROM pre_market_reservations
                       WHERE status = 'pending'
                         AND reservation_type = ?
                       ORDER BY created_at DESC""",
                    (reservation_type,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, stock_code, stock_name, quantity,
                              tp_pct, sl_pct, status, created_at,
                              executed_at, order_no, error_msg,
                              reservation_type
                       FROM pre_market_reservations
                       WHERE status = 'pending'
                       ORDER BY created_at DESC"""
                ).fetchall()
        return [
            {
                "id": r[0],
                "stock_code": r[1],
                "stock_name": r[2],
                "quantity": r[3],
                "tp_pct": r[4],
                "sl_pct": r[5],
                "status": r[6],
                "created_at": r[7],
                "executed_at": r[8],
                "order_no": r[9],
                "error_msg": r[10],
                "reservation_type": r[11],
            }
            for r in rows
        ]
    except sqlite3.Error:
        logger.warning("예약 조회 실패", exc_info=True)
        return []


def cancel_pre_market_reservation(reservation_id: int) -> bool:
    """장전 시간외 예약을 취소한다 (pending → cancelled)."""
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "UPDATE pre_market_reservations SET status = 'cancelled'"
                " WHERE id = ? AND status = 'pending'",
                (reservation_id,),
            )
            return cur.rowcount > 0
    except sqlite3.Error:
        logger.warning("시간외 예약 취소 실패: id=%s", reservation_id, exc_info=True)
        return False


def execute_pre_market_reservation(reservation_id: int) -> dict:
    """예약을 실행한다 (API 호출 + 포지션 등록).

    reservation_type에 따라 주문 방식이 달라진다:
    - pre_market: 장전 시간외 종가 주문 (ORD_DVSN=05)
    - market_open: 시장가 주문 (ORD_DVSN=01)
    """
    now = datetime.now(tz=KST).isoformat()
    try:
        with _get_conn() as conn:
            row = conn.execute(
                """SELECT id, stock_code, stock_name, quantity,
                          tp_pct, sl_pct, reservation_type
                   FROM pre_market_reservations
                   WHERE id = ? AND status = 'pending'""",
                (reservation_id,),
            ).fetchone()
    except sqlite3.Error:
        logger.error("예약 조회 실패: id=%s", reservation_id, exc_info=True)
        return {"success": False, "message": "예약 조회 실패"}

    if not row:
        return {"success": False, "message": "해당 예약이 없거나 이미 처리됨"}

    rid, code, name, qty, tp_pct, sl_pct, res_type = row

    # 예약 타입에 따라 주문 API 분기
    if res_type == "market_open":
        result = kis_trading.buy_order(code, qty)
    else:
        result = kis_trading.pre_market_buy_order(code, qty)

    label = _RESERVATION_TYPE_LABELS.get(res_type, res_type)

    try:
        with _get_conn() as conn:
            if result["success"]:
                conn.execute(
                    """UPDATE pre_market_reservations
                       SET status = 'executed', executed_at = ?,
                           order_no = ?
                       WHERE id = ?""",
                    (now, result["order_no"], rid),
                )
                # 포지션 자동 등록 (TP/SL)
                conn.execute(
                    """INSERT OR REPLACE INTO positions
                       (stock_code, stock_name, take_profit_pct,
                        stop_loss_pct, bought_at, order_no)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (code, name, tp_pct, sl_pct, now, result["order_no"]),
                )
            else:
                conn.execute(
                    """UPDATE pre_market_reservations
                       SET status = 'failed', executed_at = ?,
                           error_msg = ?
                       WHERE id = ?""",
                    (now, result["message"], rid),
                )
    except sqlite3.Error:
        logger.error("예약 상태 갱신 실패: id=%s", rid, exc_info=True)

    logger.info(
        "%s 예약 실행 [%s]: %s(%s) %d주 → %s",
        label,
        "성공" if result["success"] else "실패",
        name, code, qty, result.get("message", ""),
    )
    return result


def get_portfolio() -> list[dict]:
    """잔고 + 포지션 정보를 결합하여 포트폴리오를 반환한다.

    Returns:
        종목별 딕셔너리 리스트. 각 항목에는 잔고 정보 + tp_pct, sl_pct,
        tp_reached, sl_reached 가 포함된다.
    """
    holdings = kis_trading.get_balance()

    # positions 테이블에서 모든 포지션 로드
    positions: dict[str, dict] = {}
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT stock_code, stock_name, take_profit_pct,"
                " stop_loss_pct, bought_at, order_no FROM positions"
            ).fetchall()
        for row in rows:
            positions[row[0]] = {
                "stock_name": row[1],
                "take_profit_pct": row[2],
                "stop_loss_pct": row[3],
                "bought_at": row[4],
                "order_no": row[5],
            }
    except sqlite3.Error:
        logger.warning("포지션 조회 실패", exc_info=True)

    portfolio: list[dict] = []
    for h in holdings:
        code = h["stock_code"]
        pos = positions.get(code)

        tp_pct = pos["take_profit_pct"] if pos else None
        sl_pct = pos["stop_loss_pct"] if pos else None
        profit_rate = h.get("profit_rate", 0)

        tp_reached = (profit_rate >= tp_pct) if tp_pct is not None else False
        sl_reached = (profit_rate <= sl_pct) if sl_pct is not None else False

        portfolio.append({
            **h,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "tp_reached": tp_reached,
            "sl_reached": sl_reached,
        })

    return portfolio


# ── 자동매매 설정 ──────────────────────────────────────────


def get_auto_trade_config() -> dict[str, str]:
    """자동매매 설정 전체를 조회한다. 미설정 항목은 기본값 반환."""
    result = dict(_AUTO_TRADE_DEFAULTS)
    try:
        with _get_conn() as conn:
            rows = conn.execute("SELECT key, value FROM auto_trade_config").fetchall()
        for key, value in rows:
            result[key] = value
    except sqlite3.Error:
        logger.warning("자동매매 설정 조회 실패", exc_info=True)
    return result


def set_auto_trade_config(key: str, value: str) -> bool:
    """자동매매 설정 값을 저장한다."""
    try:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO auto_trade_config (key, value) VALUES (?, ?)",
                (key, value),
            )
            return True
    except sqlite3.Error:
        logger.warning("자동매매 설정 저장 실패: %s", key, exc_info=True)
        return False


def update_auto_trade_config(updates: dict[str, str]) -> bool:
    """자동매매 설정을 일괄 업데이트한다."""
    try:
        with _get_conn() as conn:
            for key, value in updates.items():
                conn.execute(
                    "INSERT OR REPLACE INTO auto_trade_config (key, value) VALUES (?, ?)",
                    (key, value),
                )
            return True
    except sqlite3.Error:
        logger.warning("자동매매 설정 일괄 저장 실패", exc_info=True)
        return False


def is_auto_buy_enabled() -> bool:
    """자동매수 활성화 여부."""
    config = get_auto_trade_config()
    return config.get("auto_buy_enabled", "false") == "true"


def toggle_auto_buy(enabled: bool, buying_power: int = 0) -> dict:
    """자동매수 ON/OFF 토글.

    ON 시 매수가능금액 × 비율로 1회 매수금액을 고정한다.
    """
    config = get_auto_trade_config()

    if enabled:
        ratio = int(float(config.get("auto_buy_ratio", "30")))
        fixed_amount = int(buying_power * ratio / 100)
        set_auto_trade_config("auto_buy_enabled", "true")
        set_auto_trade_config("auto_buy_fixed_amount", str(fixed_amount))
        logger.info(
            "자동매수 ON: 매수가능=%d, 비율=%d%%, 1회금액=%d",
            buying_power, ratio, fixed_amount,
        )
        return {
            "enabled": True,
            "fixed_amount": fixed_amount,
            "ratio": ratio,
            "buying_power": buying_power,
        }
    else:
        set_auto_trade_config("auto_buy_enabled", "false")
        set_auto_trade_config("auto_buy_fixed_amount", "0")
        logger.info("자동매수 OFF")
        return {"enabled": False, "fixed_amount": 0}
