import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings as app_settings
from app.models.database import init_db
from app.utils.scheduler import start_scheduler, stop_scheduler
from app.routers import auth, news, analysis, x_sentiment, settings, calendar
from app.routers import quotes
from app.integrations.option_pro import router as option_pro_integration
from app.integrations.option_pro.auth import IntegrationAPIError
from app.utils.http import configure_safe_network_logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
configure_safe_network_logging()
logger = logging.getLogger(__name__)

# Ensure data directory exists
os.makedirs("data", exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting MacroLens backend...")

    # Initialize database
    await init_db()

    # Capability inspection is local-only and never submits a model request.
    from app.services.responses_runtime import OpenAIResponsesProvider

    provider = OpenAIResponsesProvider()
    app.state.openai_capabilities = provider.capabilities()
    await provider.close()
    if app.state.openai_capabilities.status not in {"ok", "not_configured"}:
        logger.warning("OpenAI runtime capability check: %s", app.state.openai_capabilities.status)

    # Start background scheduler
    await start_scheduler()

    logger.info("MacroLens backend ready")
    yield

    # Shutdown
    logger.info("Shutting down MacroLens backend...")
    stop_scheduler()
    logger.info("MacroLens backend stopped")


app = FastAPI(
    title="MacroLens API",
    description="Macro news sentiment analysis platform",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration — reads from Settings (env/.env file)
DEV_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

_cors_origins = [o.strip() for o in app_settings.cors_origins.split(",") if o.strip()]
if not _cors_origins or _cors_origins == ["*"]:
    logger.warning("CORS_ORIGINS is empty or wildcard; using development-only localhost defaults")
    _cors_origins = DEV_CORS_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router)
app.include_router(news.router)
app.include_router(analysis.router)
app.include_router(x_sentiment.router)
app.include_router(settings.router)
app.include_router(calendar.router)
app.include_router(quotes.router)
app.include_router(option_pro_integration.router)


@app.exception_handler(IntegrationAPIError)
async def integration_api_error_handler(request, exc: IntegrationAPIError):
    return option_pro_integration.error_response(request, exc)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request, exc: RequestValidationError):
    if request.url.path.startswith(option_pro_integration.PREFIX):
        return option_pro_integration.error_response(
            request,
            IntegrationAPIError(422, "invalid_request", "The integration request is invalid."),
        )
    return await request_validation_exception_handler(request, exc)


@app.get("/live")
async def live():
    return {"status": "alive", "service": "MacroLens"}


@app.get("/health")
async def health():
    database_status = "unavailable"
    scheduler_status = "stopped"
    db = None
    try:
        from app.models.database import get_db
        from app.utils.scheduler import get_scheduler

        db = await get_db()
        async with db.execute("SELECT 1") as cursor:
            row = await cursor.fetchone()
        if not row or row[0] != 1:
            raise RuntimeError("database readiness query failed")
        database_status = "ok"

        scheduler = get_scheduler()
        if scheduler is None or not scheduler.running:
            raise RuntimeError("scheduler is not running")
        scheduler_status = "running"
    except Exception as exc:
        logger.warning("Readiness check failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "unavailable",
                "database": database_status,
                "scheduler": scheduler_status,
            },
        ) from exc
    finally:
        if db is not None:
            await db.close()

    return {
        "status": "ok",
        "service": "MacroLens",
        "database": database_status,
        "scheduler": scheduler_status,
    }


@app.get("/")
async def root():
    return {
        "service": "MacroLens API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "/api/news",
            "/api/analysis",
            "/api/x-sentiment",
            "/api/settings",
        ],
    }
