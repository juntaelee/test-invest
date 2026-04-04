"""매매 라우터: 매수/매도, 포지션 관리, 예약 API."""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from auto_invest.api.kis_trading import KISTradingError
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

logger = logging.getLogger(__name__)

router = APIRouter()


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


# ── 포트폴리오 ──────────────────────────────────────────


@router.get("/api/portfolio-data")
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


# ── 매수/매도 ────────────────────────────────────────────


@router.post("/api/buy")
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


@router.post("/api/sell")
def api_sell(req: SellRequest):
    """매도 주문 API."""
    result = execute_sell(stock_code=req.stock_code, quantity=req.quantity)
    return result


# ── 포지션 관리 ──────────────────────────────────────────


@router.post("/api/position")
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


@router.put("/api/position")
def api_update_position(req: UpdatePositionRequest):
    """포지션 TP/SL 수정 API."""
    ok = update_position(stock_code=req.stock_code, tp_pct=req.tp_pct, sl_pct=req.sl_pct)
    if ok:
        return {"success": True, "message": "포지션 수정 완료"}
    return {"success": False, "message": "해당 포지션을 찾을 수 없습니다"}


@router.delete("/api/position/{stock_code}")
def api_delete_position(stock_code: str):
    """포지션 삭제 API (매도 없이 기록만 삭제)."""
    ok = delete_position(stock_code=stock_code)
    if ok:
        return {"success": True, "message": "포지션 삭제 완료"}
    return {"success": False, "message": "해당 포지션을 찾을 수 없습니다"}


# ── 예약 ─────────────────────────────────────────────────


@router.post("/api/pre-market-reservation")
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


@router.delete("/api/pre-market-reservation/{reservation_id}")
def api_cancel_pre_market_reservation(reservation_id: int):
    """장전 시간외 예약 취소 API."""
    ok = cancel_pre_market_reservation(reservation_id)
    if ok:
        return {"success": True, "message": "예약 취소 완료"}
    return {"success": False, "message": "해당 예약을 찾을 수 없거나 이미 처리됨"}
