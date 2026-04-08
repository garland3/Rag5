"""Tests for the streaming SSE query endpoint."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
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
async def test_stream_endpoint_returns_sse(client):
    """The /query/stream endpoint should return SSE events with status updates."""

    async def fake_stream(*args, **kwargs):
        yield {"type": "status", "message": "Checking access permissions..."}
        yield {"type": "status", "message": "Searching with vector retriever..."}
        yield {"type": "status", "message": "Finding relevant documents for: \"test?\""}
        yield {"type": "status", "message": "No relevant documents found."}
        yield {
            "type": "answer",
            "data": {"answer": "No relevant documents found.", "sources": []},
        }
        yield {"type": "done", "message": "Search complete"}

    with patch("app.api.routes.query_stream.query_rag_stream", side_effect=fake_stream):
        async with client as c:
            response = await c.post(
                "/api/v1/query/stream",
                json={
                    "question": "test?",
                    "corpus_id": "507f1f77bcf86cd799439011",
                },
            )

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]

    lines = response.text.strip().split("\n\n")
    events = []
    for line in lines:
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))

    assert len(events) == 6
    assert events[0]["type"] == "status"
    assert events[0]["message"] == "Checking access permissions..."
    assert events[-1]["type"] == "done"


@pytest.mark.asyncio
async def test_stream_endpoint_error_event(client):
    """The stream should emit an error event for missing corpus_id."""

    async def fake_stream(*args, **kwargs):
        yield {"type": "error", "message": "corpus_id is required"}

    with patch("app.api.routes.query_stream.query_rag_stream", side_effect=fake_stream):
        async with client as c:
            response = await c.post(
                "/api/v1/query/stream",
                json={
                    "question": "test?",
                    "corpus_id": "",
                },
            )

    assert response.status_code == 200
    events = []
    for line in response.text.strip().split("\n\n"):
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))
    assert events[0]["type"] == "error"


@pytest.mark.asyncio
async def test_stream_with_sources_and_answer(client):
    """The stream should yield sources and answer events."""
    fake_sources = [
        {
            "document_id": "abc123",
            "filename": "test.pdf",
            "chunk_text": "some text",
            "score": 0.95,
        }
    ]

    async def fake_stream(*args, **kwargs):
        yield {"type": "status", "message": "Checking access permissions..."}
        yield {"type": "status", "message": "Searching with vector retriever..."}
        yield {"type": "sources", "message": "Retrieved sources from 1 documents", "data": fake_sources}
        yield {"type": "status", "message": "Generating answer from retrieved context..."}
        yield {
            "type": "answer",
            "data": {"answer": "The answer is 42.", "sources": fake_sources},
        }
        yield {"type": "done", "message": "Search complete"}

    with patch("app.api.routes.query_stream.query_rag_stream", side_effect=fake_stream):
        async with client as c:
            response = await c.post(
                "/api/v1/query/stream",
                json={
                    "question": "What is the answer?",
                    "corpus_id": "507f1f77bcf86cd799439011",
                },
            )

    assert response.status_code == 200
    events = []
    for line in response.text.strip().split("\n\n"):
        if line.startswith("data: "):
            events.append(json.loads(line[6:]))

    source_events = [e for e in events if e["type"] == "sources"]
    assert len(source_events) == 1
    assert source_events[0]["data"][0]["filename"] == "test.pdf"

    answer_events = [e for e in events if e["type"] == "answer"]
    assert len(answer_events) == 1
    assert answer_events[0]["data"]["answer"] == "The answer is 42."
