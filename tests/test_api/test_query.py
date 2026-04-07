from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.models.query import QueryResponse


@pytest.fixture
def mock_db():
    from unittest.mock import MagicMock

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
async def test_query_endpoint(client, mock_db):
    mock_response = QueryResponse(
        answer="Test answer",
        sources=[],
    )
    with patch("app.api.routes.query.query_rag", return_value=mock_response):
        async with client as c:
            response = await c.post(
                "/api/v1/query",
                json={"question": "What is the meaning of life?", "corpus_id": "507f1f77bcf86cd799439011"},
            )

    assert response.status_code == 200
    data = response.json()
    assert data["answer"] == "Test answer"
    assert data["sources"] == []
