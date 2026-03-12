"""웹 대시보드 앱 (FastAPI + Jinja2)."""

import logging
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from auto_invest.api.kis_market import get_stock_price
from auto_invest.api.kis_trading import KISTradingError
from auto_invest.core.monitor import start_scheduler
from auto_invest.core.trading import (
    cancel_pre_market_reservation,
    create_position,
    create_pre_market_reservation,
    delete_position,
    execute_buy,
    execute_sell,
    get_orphan_positions,
    get_pending_pre_market_reservations,
    get_portfolio,
    update_position,
)
from auto_invest.strategy.kr_etf import lookup_stock_name
from auto_invest.strategy.recommender import RecommendationReport, run_recommendation
from auto_invest.strategy.scanner import run_scanner
from auto_invest.utils import cache

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """앱 시작/종료 시 TP/SL 모니터 스레드 관리."""
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=start_scheduler, args=(stop_event,), daemon=True, name="tp-sl-monitor"
    )
    monitor_thread.start()
    yield
    stop_event.set()
    monitor_thread.join(timeout=5)


app = FastAPI(title="Auto Invest Dashboard", lifespan=lifespan)

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ── 요청 모델 ─────────────────────────────────────────────

class BuyRequest(BaseModel):
    stock_code: str
    stock_name: str
    quantity: int
    tp_pct: float = 4.0
    sl_pct: float = -3.0


class SellRequest(BaseModel):
    stock_code: str
    quantity: int


class CreatePositionRequest(BaseModel):
    stock_code: str
    stock_name: str
    tp_pct: float = 4.0
    sl_pct: float = -3.0


class UpdatePositionRequest(BaseModel):
    stock_code: str
    tp_pct: float
    sl_pct: float


class ReservationRequest(BaseModel):
    stock_code: str
    stock_name: str
    quantity: int
    tp_pct: float = 4.0
    sl_pct: float = -3.0
    reservation_type: str = "pre_market"  # pre_market | market_open


# ── 헬퍼 ─────────────────────────────────────────────────

_SECTOR_KR = {
    "XLK": "IT/기술",
    "SOXX": "반도체",
    "XLV": "헬스케어",
    "XLE": "에너지",
    "XLI": "산업재",
    "XLB": "소재",
    "XLF": "금융",
    "XLY": "소비재",
    "XLC": "커뮤니케이션",
}

_THEME_KR = {
    "BOTZ": "AI/로봇",
    "LIT": "2차전지",
    "XBI": "바이오테크",
    "ESPO": "게임/e스포츠",
    "CIBR": "사이버보안",
    "SKYY": "클라우드",
    "ICLN": "클린에너지",
    "ITA": "방산/항공",
}

_KEYWORD_KR = {
    "semiconductor": "반도체",
    "chip": "칩",
    "ai": "AI/인공지능",
    "battery": "배터리",
    "ev": "전기차",
    "electric vehicle": "전기차",
    "oil": "석유/에너지",
    "shipbuilding": "조선",
    "steel": "철강",
    "pharmaceutical": "제약",
    "biotech": "바이오",
    "display": "디스플레이",
    "memory": "메모리",
    "hbm": "HBM",
    "robot": "로봇",
    "game": "게임",
}


def _build_context(report: RecommendationReport) -> dict:
    """템플릿에 전달할 컨텍스트 데이터 구성."""
    # 섹터 등락률 정렬 (내림차순), 한국어 변환
    sorted_sectors = sorted(
        report.market_snapshot.sector_changes.items(),
        key=lambda x: x[1],
        reverse=True,
    )
    sorted_sectors = [
        (_SECTOR_KR.get(etf, etf), pct) for etf, pct in sorted_sectors
    ]

    # 테마 등락률 정렬 (내림차순), 한국어 변환
    sorted_themes = sorted(
        report.market_snapshot.theme_changes.items(),
        key=lambda x: x[1],
        reverse=True,
    )
    sorted_themes = [
        (_THEME_KR.get(etf, etf), pct) for etf, pct in sorted_themes
    ]

    # 키워드 빈도 정렬 (내림차순), 한국어 변환
    sorted_keywords = sorted(
        report.news_result.keyword_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )
    sorted_keywords = [
        (_KEYWORD_KR.get(kw, kw), cnt) for kw, cnt in sorted_keywords
    ]

    # 캐시 시각 포맷팅 (3종류, KST)
    kst = timezone(timedelta(hours=9))

    def _fmt_ts(ts: float | None) -> str:
        if not ts:
            return ""
        return datetime.fromtimestamp(ts, tz=kst).strftime("%Y-%m-%d %H:%M:%S KST")

    cache_info = {
        "recommendation": {"time": _fmt_ts(report.cached_at), "ttl": "24시간"},
        "etf_holdings": {
            "time": _fmt_ts(cache.get_latest_created_at("etf_holdings:")),
            "ttl": "24시간",
        },
        "stock_code": {
            "time": _fmt_ts(cache.get_latest_created_at("stock_code:")),
            "ttl": "30일",
        },
    }

    return {
        "timestamp": report.timestamp,
        "cache_info": cache_info,
        "index_changes": report.market_snapshot.index_changes,
        "sector_changes": sorted_sectors,
        "theme_changes": sorted_themes,
        "recommendations": report.recommendations,
        "keyword_counts": sorted_keywords,
        "headline_count": len(report.news_result.headlines),
    }


# ── 기존 엔드포인트 ──────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """대시보드 메인 페이지 (스켈레톤)."""
    return templates.TemplateResponse(request, "dashboard.html", {})


@app.get("/api/dashboard-data")
def api_dashboard_data():
    """대시보드 데이터 JSON API."""
    report = run_recommendation(top_n=100)
    ctx = _build_context(report)
    return {
        "timestamp": ctx["timestamp"],
        "cache_info": ctx["cache_info"],
        "index_changes": ctx["index_changes"],
        "sector_changes": ctx["sector_changes"],
        "theme_changes": ctx["theme_changes"],
        "keyword_counts": ctx["keyword_counts"],
        "headline_count": ctx["headline_count"],
        "recommendations": [
            {
                "code": r.code,
                "name": r.name,
                "total_score": r.total_score,
                "sector_score": r.sector_score,
                "theme_score": r.theme_score,
                "news_score": r.news_score,
            }
            for r in report.recommendations
        ],
    }


@app.post("/refresh")
def refresh():
    """캐시 무시하고 새로 조회."""
    run_recommendation(top_n=100, force_refresh=True)
    return {"success": True}


@app.get("/api/recommend")
async def api_recommend(top_n: int = 10, force_refresh: bool = False):
    """JSON API 엔드포인트."""
    report = run_recommendation(top_n=top_n, force_refresh=force_refresh)
    return {
        "timestamp": report.timestamp,
        "index_changes": report.market_snapshot.index_changes,
        "sector_changes": report.market_snapshot.sector_changes,
        "theme_changes": report.market_snapshot.theme_changes,
        "keyword_counts": report.news_result.keyword_counts,
        "headline_count": len(report.news_result.headlines),
        "recommendations": [
            {
                "rank": i,
                "code": r.code,
                "name": r.name,
                "total_score": r.total_score,
                "sector_score": r.sector_score,
                "theme_score": r.theme_score,
                "news_score": r.news_score,
            }
            for i, r in enumerate(report.recommendations, 1)
        ],
    }


# ── 발굴 엔드포인트 ──────────────────────────────────────


@app.get("/discover", response_class=HTMLResponse)
async def discover_page(request: Request):
    """중소형주 발굴 페이지 (스켈레톤)."""
    return templates.TemplateResponse(request, "discover.html", {})


@app.get("/api/discover-data")
def api_discover_data():
    """발굴 데이터 JSON API."""
    report = run_scanner(top_n=30)

    kst = timezone(timedelta(hours=9))
    cached_at_str = ""
    if report.cached_at:
        cached_at_str = datetime.fromtimestamp(report.cached_at, tz=kst).strftime(
            "%Y-%m-%d %H:%M:%S KST"
        )

    result = report.to_dict()
    result["cached_at"] = cached_at_str
    return result


@app.post("/discover/refresh")
def discover_refresh():
    """발굴 캐시 무시하고 새로 조회."""
    run_scanner(top_n=30, force_refresh=True)
    return {"success": True}


# ── 매매 엔드포인트 ──────────────────────────────────────

@app.get("/portfolio", response_class=HTMLResponse)
async def portfolio_page(request: Request):
    """포트폴리오 페이지 (스켈레톤)."""
    return templates.TemplateResponse(request, "portfolio.html", {})


@app.get("/api/portfolio-data")
def api_portfolio_data():
    """포트폴리오 데이터 JSON API."""
    error = None
    portfolio = []
    orphan_positions = []
    pre_market_reservations = []
    try:
        portfolio = get_portfolio()
        orphan_positions = get_orphan_positions(
            [{"stock_code": p["stock_code"]} for p in portfolio]
        )
    except KISTradingError as e:
        error = str(e)
        logger.error("포트폴리오 조회 실패: %s", e)
    pre_market_reservations = get_pending_pre_market_reservations()
    return {
        "portfolio": portfolio,
        "orphan_positions": orphan_positions,
        "pre_market_reservations": pre_market_reservations,
        "error": error,
    }


@app.post("/api/buy")
def api_buy(req: BuyRequest):
    """매수 주문 API."""
    result = execute_buy(
        stock_code=req.stock_code,
        stock_name=req.stock_name,
        quantity=req.quantity,
        tp_pct=req.tp_pct,
        sl_pct=req.sl_pct,
    )
    return result


@app.post("/api/sell")
def api_sell(req: SellRequest):
    """매도 주문 API."""
    result = execute_sell(stock_code=req.stock_code, quantity=req.quantity)
    return result


@app.post("/api/position")
def api_create_position(req: CreatePositionRequest):
    """포지션 신규 등록 API (TP/SL만 설정, 매수 없음)."""
    ok = create_position(
        stock_code=req.stock_code,
        stock_name=req.stock_name,
        tp_pct=req.tp_pct,
        sl_pct=req.sl_pct,
    )
    if ok:
        return {"success": True, "message": "포지션 등록 완료"}
    return {"success": False, "message": "포지션 등록 실패"}


@app.put("/api/position")
def api_update_position(req: UpdatePositionRequest):
    """포지션 TP/SL 수정 API."""
    ok = update_position(stock_code=req.stock_code, tp_pct=req.tp_pct, sl_pct=req.sl_pct)
    if ok:
        return {"success": True, "message": "포지션 수정 완료"}
    return {"success": False, "message": "해당 포지션을 찾을 수 없습니다"}


@app.delete("/api/position/{stock_code}")
def api_delete_position(stock_code: str):
    """포지션 삭제 API (매도 없이 기록만 삭제)."""
    ok = delete_position(stock_code=stock_code)
    if ok:
        return {"success": True, "message": "포지션 삭제 완료"}
    return {"success": False, "message": "해당 포지션을 찾을 수 없습니다"}


# ── 종목명 조회 엔드포인트 ────────────────────────────────


@app.get("/api/stock-name/{stock_code}")
def api_stock_name(stock_code: str):
    """종목코드로 종목명과 현재가를 조회한다."""
    name = lookup_stock_name(stock_code)
    if not name:
        return {"success": False, "stock_name": None, "message": "종목명 조회 실패"}
    result: dict = {"success": True, "stock_name": name}
    price_info = get_stock_price(stock_code)
    if price_info:
        result["current_price"] = price_info["current_price"]
        result["change_rate"] = price_info["change_rate"]
        result["change_amount"] = price_info["change_amount"]
    return result


# ── 장전 시간외 예약 엔드포인트 ──────────────────────────


@app.post("/api/pre-market-reservation")
def api_create_reservation(req: ReservationRequest):
    """매수 예약 등록 API (시간외/장개시)."""
    if req.reservation_type not in ("pre_market", "market_open"):
        return {"success": False, "message": "잘못된 예약 타입"}
    result = create_pre_market_reservation(
        stock_code=req.stock_code,
        stock_name=req.stock_name,
        quantity=req.quantity,
        tp_pct=req.tp_pct,
        sl_pct=req.sl_pct,
        reservation_type=req.reservation_type,
    )
    return result


@app.delete("/api/pre-market-reservation/{reservation_id}")
def api_cancel_pre_market_reservation(reservation_id: int):
    """장전 시간외 예약 취소 API."""
    ok = cancel_pre_market_reservation(reservation_id)
    if ok:
        return {"success": True, "message": "예약 취소 완료"}
    return {"success": False, "message": "해당 예약을 찾을 수 없거나 이미 처리됨"}


def main():
    """서버 실행."""
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=5001)


if __name__ == "__main__":
    main()
