"""웹 앱 설정 및 라이프사이클."""

import logging
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from auto_invest.core.monitor import start_scheduler
from auto_invest.web.routers import hub, trade

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """앱 시작/종료 시 스케줄러 스레드 관리."""
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=start_scheduler, args=(stop_event,), daemon=True, name="scheduler"
    )
    monitor_thread.start()
    yield
    stop_event.set()
    monitor_thread.join(timeout=5)


app = FastAPI(title="Auto Invest", lifespan=lifespan)

# 라우터 등록
app.include_router(hub.router)
app.include_router(trade.router)


def main():
    """서버 실행."""
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=5001)


if __name__ == "__main__":
    main()
