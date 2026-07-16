import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from app.models.database import get_db, init_db
from app.routers.internal import router as internal_router
from app.utils.http import configure_safe_network_logging
from app.utils.scheduler import get_scheduler, start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
configure_safe_network_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting MacroLens ETL service")
    await init_db()
    await start_scheduler()
    logger.info("MacroLens ETL service is ready")
    try:
        yield
    finally:
        logger.info("Stopping MacroLens ETL service")
        stop_scheduler()


app = FastAPI(
    title="MacroLens ETL",
    version="3.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.include_router(internal_router)


@app.get("/health")
async def health() -> dict[str, str]:
    database = "unavailable"
    scheduler = "stopped"
    db = None
    try:
        db = await get_db()
        async with db.execute("SELECT 1") as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] != 1:
            raise RuntimeError("database readiness probe failed")
        database = "ok"
        current_scheduler = get_scheduler()
        if current_scheduler is None or not current_scheduler.running:
            raise RuntimeError("scheduler is not running")
        scheduler = "running"
    except Exception as exc:
        logger.warning("Readiness check failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status": "unavailable",
                "service": "macrolens-etl",
                "database": database,
                "scheduler": scheduler,
            },
        ) from exc
    finally:
        if db is not None:
            await db.close()
    return {
        "status": "ok",
        "service": "macrolens-etl",
        "database": database,
        "scheduler": scheduler,
    }
