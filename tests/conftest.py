from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=MagicMock())
    return db


@pytest.fixture
def client(mock_db):
    from app.api.deps import get_database

    async def override_db():
        return mock_db

    app.dependency_overrides[get_database] = override_db
    yield AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    app.dependency_overrides.clear()
