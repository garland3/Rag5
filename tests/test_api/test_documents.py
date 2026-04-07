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
async def test_list_documents_empty(client, mock_db):
    mock_cursor = AsyncMock()
    mock_cursor.sort = MagicMock(return_value=mock_cursor)
    mock_cursor.__aiter__ = lambda self: self
    mock_cursor.__anext__ = AsyncMock(side_effect=StopAsyncIteration)
    mock_db.__getitem__ = MagicMock(return_value=MagicMock(find=MagicMock(return_value=mock_cursor)))

    async with client as c:
        response = await c.get("/api/v1/documents")
    assert response.status_code == 200
    assert response.json() == []
