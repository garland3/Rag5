from datetime import datetime
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


def _job_doc(
    *,
    job_id: str = "6543210987abcdef01234567",
    corpus_id: str = "507f1f77bcf86cd799439011",
    status: str = "queued",
    document_id: ObjectId | None = None,
    error: str | None = None,
):
    now = datetime.utcnow()
    return {
        "_id": ObjectId(job_id),
        "corpus_id": ObjectId(corpus_id),
        "filename": "report.pdf",
        "content_type": "application/pdf",
        "size_bytes": 1234,
        "storage_path": "/tmp/rag5_uploads/abc/report.pdf",
        "stage_id": "abc",
        "status": status,
        "document_id": document_id,
        "error": error,
        "created_at": now,
        "updated_at": now,
        "created_by": "bob@test.com",
        "metadata": {},
        "prefect_flow_run_id": None,
    }


@pytest.mark.asyncio
async def test_queue_document_upload_creates_job(client, mock_db):
    fake_job = MagicMock()
    fake_job.model_dump = MagicMock(
        return_value={
            "id": "6543210987abcdef01234567",
            "corpus_id": "507f1f77bcf86cd799439011",
            "filename": "doc.txt",
            "content_type": "text/plain",
            "size_bytes": 11,
            "status": "queued",
            "document_id": None,
            "error": None,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "created_by": "bob@test.com",
            "prefect_flow_run_id": None,
            "metadata": {},
        }
    )

    corpus_doc = {
        "_id": ObjectId("507f1f77bcf86cd799439011"),
        "read_group": "rag5-default-readers",
        "write_group": "rag5-default-writers",
        "owner_group": "rag5-default-owners",
    }

    from app.models.job import IngestJobResponse

    job_response = IngestJobResponse(
        id="6543210987abcdef01234567",
        corpus_id="507f1f77bcf86cd799439011",
        filename="doc.txt",
        content_type="text/plain",
        size_bytes=11,
        status="queued",
        document_id=None,
        error=None,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        created_by="bob@test.com",
        prefect_flow_run_id=None,
        metadata={},
    )

    with (
        patch(
            "app.api.routes.documents.ensure_corpus_access",
            AsyncMock(return_value=corpus_doc),
        ),
        patch(
            "app.api.routes.documents.enqueue_ingest_job",
            AsyncMock(return_value=job_response),
        ) as mock_enqueue,
    ):
        async with client as c:
            response = await c.post(
                "/api/v1/documents/upload",
                params={"corpus_id": "507f1f77bcf86cd799439011"},
                files={"file": ("doc.txt", b"hello world", "text/plain")},
            )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["filename"] == "doc.txt"
    mock_enqueue.assert_awaited_once()
    call_kwargs = mock_enqueue.await_args.kwargs
    assert call_kwargs["corpus_id"] == "507f1f77bcf86cd799439011"
    assert call_kwargs["filename"] == "doc.txt"
    assert call_kwargs["content"] == b"hello world"


@pytest.mark.asyncio
async def test_queue_document_upload_rejects_empty_file(client, mock_db):
    with patch(
        "app.api.routes.documents.ensure_corpus_access",
        AsyncMock(return_value={}),
    ):
        async with client as c:
            response = await c.post(
                "/api/v1/documents/upload",
                params={"corpus_id": "507f1f77bcf86cd799439011"},
                files={"file": ("empty.txt", b"", "text/plain")},
            )

    assert response.status_code == 400
    assert "empty" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_get_job_returns_job(client, mock_db):
    job = _job_doc()

    jobs_collection = MagicMock()
    jobs_collection.find_one = AsyncMock(return_value=job)
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    with patch(
        "app.api.routes.jobs.ensure_corpus_access",
        AsyncMock(return_value={}),
    ):
        async with client as c:
            response = await c.get("/api/v1/jobs/6543210987abcdef01234567")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "6543210987abcdef01234567"
    assert body["status"] == "queued"
    assert body["corpus_id"] == "507f1f77bcf86cd799439011"


@pytest.mark.asyncio
async def test_get_job_404(client, mock_db):
    jobs_collection = MagicMock()
    jobs_collection.find_one = AsyncMock(return_value=None)
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    async with client as c:
        response = await c.get("/api/v1/jobs/6543210987abcdef01234567")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_job_invalid_id(client, mock_db):
    async with client as c:
        response = await c.get("/api/v1/jobs/not-a-valid-id")

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_list_jobs_filtered_by_accessible_corpora(client, mock_db):
    accessible = [
        {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "read_group": "rag5-default-readers",
            "write_group": "rag5-default-writers",
            "owner_group": "rag5-default-owners",
        }
    ]

    cursor = AsyncMock()
    cursor.sort = MagicMock(return_value=cursor)
    cursor.limit = MagicMock(return_value=cursor)
    cursor.__aiter__ = lambda self: self
    cursor.__anext__ = AsyncMock(side_effect=[_job_doc(), StopAsyncIteration()])

    jobs_collection = MagicMock(find=MagicMock(return_value=cursor))
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    with patch(
        "app.api.routes.jobs.list_accessible_corpora",
        AsyncMock(return_value=accessible),
    ):
        async with client as c:
            response = await c.get("/api/v1/jobs")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["id"] == "6543210987abcdef01234567"


@pytest.mark.asyncio
async def test_list_jobs_returns_empty_when_no_corpora(client, mock_db):
    with patch(
        "app.api.routes.jobs.list_accessible_corpora",
        AsyncMock(return_value=[]),
    ):
        async with client as c:
            response = await c.get("/api/v1/jobs")

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_list_jobs_rejects_forbidden_corpus(client, mock_db):
    with patch(
        "app.api.routes.jobs.list_accessible_corpora",
        AsyncMock(return_value=[]),
    ):
        async with client as c:
            response = await c.get(
                "/api/v1/jobs",
                params={"corpus_id": "507f1f77bcf86cd799439011"},
            )

    assert response.status_code == 403
