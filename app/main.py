from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.api.routes.sf_callback import router as sf_callback_router
from app.api.routes.sf_dashboard import router as sf_dashboard_router
from app.api.routes.sf_outbound import router as sf_outbound_router
from app.clients.kingdee import kingdee_client, kingdee_test_client
from app.config import settings
from app.db.pool import close_pool, init_pool
from app.logging_config import setup_logging

setup_logging(log_level=settings.log_level, log_format=settings.log_format)
logger = logging.getLogger("union.sf_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_pool()
        logger.info("SF service asyncpg pool initialized")
    except Exception as exc:
        logger.error("SF service asyncpg pool init failed: %s", exc, exc_info=True)
        raise

    yield

    try:
        await kingdee_client.aclose()
        await kingdee_test_client.aclose()
    except Exception:
        logger.warning("Kingdee client close skipped", exc_info=True)
    await close_pool()
    logger.info("SF service shutdown complete")


app = FastAPI(
    title="Union SF Service",
    version="0.1.0",
    description="Independent Shunfeng WMS integration service split from union-agent.",
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def block_mutations_when_disabled(request: Request, call_next):
    if (
        not settings.sf_module_enabled
        and request.method not in {"GET", "HEAD", "OPTIONS"}
        and request.url.path.startswith("/api/sf")
    ):
        return JSONResponse(
            status_code=423,
            content={
                "detail": "顺丰测试服务当前为只读模式，已拒绝写入/推送类请求。",
                "sf_module_enabled": False,
            },
        )
    return await call_next(request)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "union-sf-service",
        "sf_module_enabled": settings.sf_module_enabled,
    }


app.include_router(sf_callback_router)
app.include_router(sf_dashboard_router)
app.include_router(sf_outbound_router)
