import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings as app_settings
from app.models.database import init_db
from app.utils.scheduler import start_scheduler, stop_scheduler
from app.routers import news, analysis, x_sentiment, settings, calendar
from app.routers import quotes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Ensure data directory exists
os.makedirs("data", exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting MacroLens backend...")

    # Initialize database
    await init_db()

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
app.include_router(news.router)
app.include_router(analysis.router)
app.include_router(x_sentiment.router)
app.include_router(settings.router)
app.include_router(calendar.router)
app.include_router(quotes.router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "MacroLens"}


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
