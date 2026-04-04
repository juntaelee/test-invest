"""허브 라우터: 올인원 트레이딩 허브 페이지 + 발굴/시계열/자동매매 API."""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from auto_invest.api.kis_market import get_stock_price, lookup_stock_name
from auto_invest.api.kis_trading import KISTradingError, get_buying_power
from auto_invest.core.trading import (
    get_auto_trade_config,
    get_portfolio,
    toggle_auto_buy,
    update_auto_trade_config,
)
from auto_invest.strategy.scanner import get_discovered_stocks, is_scanning, run_scanner2
from auto_invest.utils import timeseries

logger = logging.getLogger(__name__)

router = APIRouter()

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

KST = timezone(timedelta(hours=9))


# ── 페이지 ───────────────────────────────────────────────


@router.get("/", response_class=HTMLResponse)
async def hub_page(request: Request):
    """올인원 트레이딩 허브."""
    return templates.TemplateResponse(request, "hub.html", {})


@router.get("/discover2")
async def discover2_redirect():
    """기존 discover2 → 허브로 리다이렉트."""
    return RedirectResponse(url="/", status_code=301)


# ── 허브 데이터 API ──────────────────────────────────────


@router.get("/api/hub-data")
def api_hub_data():
    """발굴 종목 + 상태 + 스캔 상태."""
    stocks = get_discovered_stocks()
    return {
        "stocks": stocks,
        "scanning": is_scanning(),
        "timestamp": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
    }


@router.get("/api/timeseries/{stock_code}")
def api_timeseries(stock_code: str):
    """종목별 당일 시계열 전체."""
    series = timeseries.get_series(stock_code)
    return {"stock_code": stock_code, "series": series}


@router.get("/api/buying-power")
def api_buying_power():
    """매수가능 금액 조회."""
    amount = get_buying_power()
    return {"buying_power": amount}


@router.get("/api/holdings")
def api_holdings():
    """보유종목 조회 (수량, 평균단가, 현재가, 수익률)."""
    error = None
    portfolio = []
    try:
        portfolio = get_portfolio()
    except KISTradingError as e:
        error = str(e)
        logger.error("보유종목 조회 실패: %s", e)
    return {"holdings": portfolio, "error": error}


@router.get("/api/discover2-status")
def api_discover2_status():
    """스캐너 실행 상태 조회."""
    return {"scanning": is_scanning()}


@router.post("/discover2/refresh")
def discover2_refresh():
    """스캐너 강제 실행."""
    run_scanner2(force_refresh=True)
    return {"success": True}


@router.get("/api/discover2-data")
def api_discover2_data(cache_only: bool = False):
    """발굴 데이터 JSON API (하위 호환)."""
    report = run_scanner2(cache_only=cache_only)
    if report is None:
        return {"empty": True}

    cached_at_str = ""
    if report.cached_at:
        cached_at_str = datetime.fromtimestamp(report.cached_at, tz=KST).strftime(
            "%Y-%m-%d %H:%M:%S KST"
        )

    result = report.to_dict()
    result["cached_at"] = cached_at_str
    return result


# ── 종목명 조회 ──────────────────────────────────────────


@router.get("/api/stock-name/{stock_code}")
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


# ── 자동매매 설정 API ────────────────────────────────────


class AutoTradeConfigUpdate(BaseModel):
    auto_buy_strength_min: float | None = None
    auto_buy_change_max: float | None = None
    auto_buy_ratio: float | None = None
    auto_sell_tp: float | None = None
    auto_sell_sl: float | None = None


@router.get("/api/auto-trade/config")
def api_get_auto_trade_config():
    """자동매매 설정 조회."""
    config = get_auto_trade_config()
    return {"config": config}


@router.put("/api/auto-trade/config")
def api_update_auto_trade_config(req: AutoTradeConfigUpdate):
    """자동매매 설정 변경."""
    updates: dict[str, str] = {}
    if req.auto_buy_strength_min is not None:
        updates["auto_buy_strength_min"] = str(req.auto_buy_strength_min)
    if req.auto_buy_change_max is not None:
        updates["auto_buy_change_max"] = str(req.auto_buy_change_max)
    if req.auto_buy_ratio is not None:
        updates["auto_buy_ratio"] = str(req.auto_buy_ratio)
    if req.auto_sell_tp is not None:
        updates["auto_sell_tp"] = str(req.auto_sell_tp)
    if req.auto_sell_sl is not None:
        updates["auto_sell_sl"] = str(req.auto_sell_sl)

    if not updates:
        return {"success": False, "message": "변경할 항목 없음"}

    ok = update_auto_trade_config(updates)
    return {"success": ok, "config": get_auto_trade_config()}


class AutoTradeToggle(BaseModel):
    enabled: bool


@router.post("/api/auto-trade/toggle")
def api_toggle_auto_trade(req: AutoTradeToggle):
    """자동매수 ON/OFF 토글."""
    buying_power = 0
    if req.enabled:
        buying_power = get_buying_power()
    result = toggle_auto_buy(enabled=req.enabled, buying_power=buying_power)
    return {"success": True, **result}
