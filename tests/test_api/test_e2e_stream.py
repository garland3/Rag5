"""End-to-end test: streaming query through the full stack (mocked DB + LLM)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import ObjectId
from httpx import ASGITransport, AsyncClient

from app.main import app

CORPUS_ID = str(ObjectId())
DOC_ID = str(ObjectId())


def _make_mock_db():
    """Build a mock DB that has one corpus and one document with chunks."""
    db = MagicMock()

    corpus = {
        "_id": ObjectId(CORPUS_ID),
        "name": "test-corpus",
        "read_group": "rag5-default-readers",
        "write_group": "rag5-default-writers",
        "owner_group": "rag5-default-owners",
        "created_by": "system",
    }

    # corpora.find_one returns the corpus
    db["corpora"].find_one = AsyncMock(return_value=corpus)

    # documents.find returns an async iterator with one doc
    doc = {"_id": ObjectId(DOC_ID), "filename": "notes.pdf", "corpus_id": ObjectId(CORPUS_ID)}

    async def _doc_iter():
        yield doc

    db["documents"].find = MagicMock(return_value=_doc_iter())

    return db


@pytest.fixture
def mock_db():
    return _make_mock_db()


@pytest.fixture
def client(mock_db):
    from app.api.deps import get_database

    async def override_db():
        return mock_db

    app.dependency_overrides[get_database] = override_db
    yield AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_e2e_stream_full_pipeline(client):
    """End-to-end: question → status events → sources → answer → done."""
    fake_chunks = [
        {
            "_id": "chunk1",
            "text": "RAG stands for Retrieval-Augmented Generation.",
            "document_id": DOC_ID,
            "chunk_index": 0,
            "metadata": {"source": "notes.pdf"},
            "score": 0.92,
        }
    ]

    mock_retriever = MagicMock()
    mock_retriever.retrieve = AsyncMock(return_value=fake_chunks)

    with (
        patch("app.services.rag_stream.get_retriever", return_value=mock_retriever),
        patch("app.services.rag_stream.generate_answer", new_callable=AsyncMock, return_value="RAG is Retrieval-Augmented Generation."),
    ):
        async with client as c:
            response = await c.post(
                "/api/v1/query/stream",
                json={
                    "question": "What is RAG?",
                    "corpus_id": CORPUS_ID,
                },
            )

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]

    events = []
    for block in response.text.strip().split("\n\n"):
        for line in block.split("\n"):
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))

    # Verify event sequence
    types = [e["type"] for e in events]
    assert "status" in types
    assert "sources" in types
    assert "answer" in types
    assert types[-1] == "done"

    # Verify answer content
    answer_event = next(e for e in events if e["type"] == "answer")
    assert "RAG" in answer_event["data"]["answer"]
    assert answer_event["data"]["sources"][0]["filename"] == "notes.pdf"

    # Verify sources event
    source_event = next(e for e in events if e["type"] == "sources")
    assert len(source_event["data"]) == 1
    assert source_event["data"][0]["score"] == 0.92
