import os

os.environ.setdefault("MACROLENS_INTERNAL_TOKEN", "test-owner-token")

import pytest_asyncio


@pytest_asyncio.fixture
async def clean_db(tmp_path, monkeypatch):
    from app.config import INTERNAL_TOKEN_ENV, settings
    from app.models import database
    from app.utils.scheduler import stop_scheduler

    stop_scheduler()
    path = tmp_path / "macrolens-test.db"
    monkeypatch.setattr(database, "DB_PATH", path)
    settings.environment[INTERNAL_TOKEN_ENV] = "test-owner-token"
    await database.init_db()
    try:
        yield path
    finally:
        stop_scheduler()
