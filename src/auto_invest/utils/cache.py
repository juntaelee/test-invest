"""SQLite 기반 캐시 모듈.

서버 재시작 후에도 캐시가 유지된다.
"""

import json
import logging
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_DIR = Path.home() / ".auto_invest"
_DB_PATH = _DB_DIR / "cache.db"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at REAL NOT NULL
)
"""


def _get_conn() -> sqlite3.Connection:
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=5)
    conn.execute(_CREATE_TABLE)
    return conn


def get(key: str, ttl_seconds: float) -> tuple[object | None, float | None]:
    """캐시에서 값을 조회.

    Returns:
        (값, 저장 시각) 또는 만료/미존재 시 (None, None)
    """
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT value, created_at FROM cache WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return None, None
        value_json, created_at = row
        if time.time() - created_at > ttl_seconds:
            return None, None
        return json.loads(value_json), created_at
    except Exception:
        logger.warning("캐시 조회 실패: %s", key, exc_info=True)
        return None, None


def put(key: str, value: object) -> float:
    """캐시에 값을 저장.

    Returns:
        저장 시각 (unix timestamp)
    """
    now = time.time()
    try:
        value_json = json.dumps(value, ensure_ascii=False)
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cache (key, value, created_at) VALUES (?, ?, ?)",
                (key, value_json, now),
            )
    except Exception:
        logger.warning("캐시 저장 실패: %s", key, exc_info=True)
    return now


def get_created_at(key: str) -> float | None:
    """캐시 항목의 저장 시각만 조회."""
    try:
        with _get_conn() as conn:
            row = conn.execute("SELECT created_at FROM cache WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def get_latest_created_at(key_prefix: str) -> float | None:
    """특정 접두어를 가진 캐시 항목 중 가장 최근 저장 시각을 조회."""
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT MAX(created_at) FROM cache WHERE key LIKE ?",
                (key_prefix + "%",),
            ).fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def clear() -> None:
    """캐시 전체 삭제."""
    try:
        with _get_conn() as conn:
            conn.execute("DELETE FROM cache")
    except Exception:
        logger.warning("캐시 삭제 실패", exc_info=True)
