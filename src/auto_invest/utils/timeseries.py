"""시계열 데이터 저장소.

당일 종목별 가격/체결강도/회전율 등의 시계열을 SQLite에 축적한다.
장 시작 시 전일 데이터를 클리어한다.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

_DB_DIR = Path.home() / ".auto_invest"
_DB_PATH = _DB_DIR / "cache.db"

_CREATE_TIMESERIES = """
CREATE TABLE IF NOT EXISTS stock_timeseries (
    stock_code TEXT NOT NULL,
    timestamp  TEXT NOT NULL,
    price      INTEGER,
    change_rate REAL,
    trade_strength REAL,
    turnover_rate REAL,
    trading_value INTEGER,
    PRIMARY KEY (stock_code, timestamp)
)
"""


def _get_conn() -> sqlite3.Connection:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=5)
    conn.execute(_CREATE_TIMESERIES)
    return conn


def record(
    stock_code: str,
    price: int | None = None,
    change_rate: float | None = None,
    trade_strength: float | None = None,
    turnover_rate: float | None = None,
    trading_value: int | None = None,
) -> None:
    """시계열 데이터 1건 적재."""
    now = datetime.now(tz=KST).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        with _get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO stock_timeseries
                   (stock_code, timestamp, price, change_rate,
                    trade_strength, turnover_rate, trading_value)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (stock_code, now, price, change_rate,
                 trade_strength, turnover_rate, trading_value),
            )
    except sqlite3.Error:
        logger.warning("시계열 적재 실패: %s", stock_code, exc_info=True)


def record_batch(items: list[dict]) -> None:
    """시계열 데이터 일괄 적재.

    Args:
        items: [{"stock_code", "price", "change_rate",
                 "trade_strength", "turnover_rate", "trading_value"}, ...]
    """
    now = datetime.now(tz=KST).strftime("%Y-%m-%dT%H:%M:%S")
    rows = [
        (
            item["stock_code"],
            now,
            item.get("price"),
            item.get("change_rate"),
            item.get("trade_strength"),
            item.get("turnover_rate"),
            item.get("trading_value"),
        )
        for item in items
    ]
    try:
        with _get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO stock_timeseries
                   (stock_code, timestamp, price, change_rate,
                    trade_strength, turnover_rate, trading_value)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
    except sqlite3.Error:
        logger.warning("시계열 일괄 적재 실패 (%d건)", len(rows), exc_info=True)


def get_series(stock_code: str, today_only: bool = True) -> list[dict]:
    """종목의 시계열 데이터를 조회한다.

    Args:
        stock_code: 종목코드
        today_only: True이면 당일 데이터만

    Returns:
        [{"timestamp", "price", "change_rate", "trade_strength",
          "turnover_rate", "trading_value"}, ...] (시간순)
    """
    try:
        with _get_conn() as conn:
            if today_only:
                today = datetime.now(tz=KST).strftime("%Y-%m-%d")
                rows = conn.execute(
                    """SELECT timestamp, price, change_rate,
                              trade_strength, turnover_rate, trading_value
                       FROM stock_timeseries
                       WHERE stock_code = ? AND timestamp >= ?
                       ORDER BY timestamp ASC""",
                    (stock_code, today),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT timestamp, price, change_rate,
                              trade_strength, turnover_rate, trading_value
                       FROM stock_timeseries
                       WHERE stock_code = ?
                       ORDER BY timestamp ASC""",
                    (stock_code,),
                ).fetchall()
    except sqlite3.Error:
        logger.warning("시계열 조회 실패: %s", stock_code, exc_info=True)
        return []

    return [
        {
            "timestamp": r[0],
            "price": r[1],
            "change_rate": r[2],
            "trade_strength": r[3],
            "turnover_rate": r[4],
            "trading_value": r[5],
        }
        for r in rows
    ]


def get_latest(stock_codes: list[str]) -> dict[str, dict]:
    """여러 종목의 최신 시계열 데이터를 한번에 조회.

    Returns:
        {stock_code: {"timestamp", "price", "change_rate",
                      "trade_strength", "turnover_rate", "trading_value"}}
    """
    if not stock_codes:
        return {}

    today = datetime.now(tz=KST).strftime("%Y-%m-%d")
    placeholders = ",".join("?" for _ in stock_codes)
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                f"""SELECT stock_code, timestamp, price, change_rate,
                           trade_strength, turnover_rate, trading_value
                    FROM stock_timeseries
                    WHERE stock_code IN ({placeholders})
                      AND timestamp >= ?
                    ORDER BY timestamp DESC""",
                [*stock_codes, today],
            ).fetchall()
    except sqlite3.Error:
        logger.warning("시계열 최신 조회 실패", exc_info=True)
        return {}

    result: dict[str, dict] = {}
    for r in rows:
        code = r[0]
        if code not in result:  # DESC 정렬이므로 첫 번째가 최신
            result[code] = {
                "timestamp": r[1],
                "price": r[2],
                "change_rate": r[3],
                "trade_strength": r[4],
                "turnover_rate": r[5],
                "trading_value": r[6],
            }
    return result


def clear_old_data() -> int:
    """전일 이전 데이터를 삭제한다.

    Returns:
        삭제된 행 수
    """
    today = datetime.now(tz=KST).strftime("%Y-%m-%d")
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                "DELETE FROM stock_timeseries WHERE timestamp < ?",
                (today,),
            )
            deleted = cur.rowcount
            if deleted > 0:
                logger.info("시계열 전일 데이터 삭제: %d건", deleted)
            return deleted
    except sqlite3.Error:
        logger.warning("시계열 클리어 실패", exc_info=True)
        return 0
