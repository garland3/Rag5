from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import ObjectId
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    from app.api.deps import get_database

    async def override_db():
        return mock_db

    app.dependency_overrides[get_database] = override_db
    yield AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_list_corpora_returns_accessible(client):
    corpus = {
        "_id": ObjectId("507f1f77bcf86cd799439011"),
        "name": "default",
        "read_group": "rag5-default-readers",
        "write_group": "rag5-default-writers",
        "owner_group": "rag5-default-owners",
        "created_by": "system",
    }

    with patch("app.api.routes.corpora.list_accessible_corpora", AsyncMock(return_value=[corpus])):
        async with client as c:
            response = await c.get("/api/v1/corpora")

    assert response.status_code == 200
    assert response.json()[0]["name"] == "default"
    assert response.json()[0]["can_read"] is True
