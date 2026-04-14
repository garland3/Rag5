import asyncio
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import ObjectId

from app.services.job_queue import enqueue_ingest_job, job_record_to_response


@pytest.fixture
def tmp_upload_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.config.settings.upload_storage_dir",
        str(tmp_path),
    )
    return tmp_path


@pytest.mark.asyncio
async def test_enqueue_ingest_job_stages_file_and_creates_job(tmp_upload_dir):
    mock_db = MagicMock()
    inserted_id = ObjectId()
    insert_result = MagicMock(inserted_id=inserted_id)
    jobs_collection = MagicMock(insert_one=AsyncMock(return_value=insert_result))
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    # Custom runner so we don't actually invoke the real Prefect flow.
    runner_called_with: dict = {}

    async def fake_runner(**kwargs):
        runner_called_with.update(kwargs)

    corpus_id = str(ObjectId())
    response = await enqueue_ingest_job(
        mock_db,
        corpus_id=corpus_id,
        filename="doc.txt",
        content_type="text/plain",
        content=b"hello world",
        created_by="alice@test.com",
        runner=fake_runner,
    )

    # Give the background task a chance to run.
    await asyncio.sleep(0)

    assert response.status == "queued"
    assert response.filename == "doc.txt"
    assert response.size_bytes == len(b"hello world")
    assert response.corpus_id == corpus_id
    assert response.created_by == "alice@test.com"

    jobs_collection.insert_one.assert_awaited_once()
    inserted_doc = jobs_collection.insert_one.await_args.args[0]
    assert inserted_doc["status"] == "queued"
    assert inserted_doc["filename"] == "doc.txt"
    assert Path(inserted_doc["storage_path"]).exists()
    assert Path(inserted_doc["storage_path"]).read_bytes() == b"hello world"

    # Runner should be invoked with the right arguments.
    assert runner_called_with["job_id"] == str(inserted_id)
    assert runner_called_with["filename"] == "doc.txt"
    assert runner_called_with["corpus_id"] == corpus_id
    assert runner_called_with["storage_path"] == inserted_doc["storage_path"]


@pytest.mark.asyncio
async def test_enqueue_ingest_job_cleans_up_staged_file_on_db_failure(
    tmp_upload_dir,
):
    mock_db = MagicMock()
    jobs_collection = MagicMock(
        insert_one=AsyncMock(side_effect=RuntimeError("mongo boom"))
    )
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    runner_called = False

    async def fake_runner(**_kwargs):
        nonlocal runner_called
        runner_called = True

    with pytest.raises(RuntimeError, match="mongo boom"):
        await enqueue_ingest_job(
            mock_db,
            corpus_id=str(ObjectId()),
            filename="doc.txt",
            content_type="text/plain",
            content=b"payload",
            created_by="alice@test.com",
            runner=fake_runner,
        )

    # The staged file/dir must not be left behind when the insert fails.
    leftover = [p for p in tmp_upload_dir.rglob("*") if p.is_file()]
    assert leftover == []
    assert runner_called is False


@pytest.mark.asyncio
async def test_enqueue_ingest_job_logs_background_task_exception(
    tmp_upload_dir, caplog
):
    mock_db = MagicMock()
    insert_result = MagicMock(inserted_id=ObjectId())
    jobs_collection = MagicMock(insert_one=AsyncMock(return_value=insert_result))
    mock_db.__getitem__ = MagicMock(return_value=jobs_collection)

    async def failing_runner(**_kwargs):
        raise RuntimeError("flow exploded")

    with caplog.at_level(logging.ERROR, logger="app.services.job_queue"):
        await enqueue_ingest_job(
            mock_db,
            corpus_id=str(ObjectId()),
            filename="doc.txt",
            content_type="text/plain",
            content=b"payload",
            created_by="alice@test.com",
            runner=failing_runner,
        )
        # Yield so the failing background task runs and the done-callback
        # fires (and surfaces the exception into the logger).
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert any(
        "flow exploded" in record.getMessage() or "flow exploded" in str(record.exc_info)
        for record in caplog.records
    )


def test_job_record_to_response_serializes_object_ids():
    now = datetime.utcnow()
    doc = {
        "_id": ObjectId("6543210987abcdef01234567"),
        "corpus_id": ObjectId("507f1f77bcf86cd799439011"),
        "filename": "doc.txt",
        "content_type": "text/plain",
        "size_bytes": 42,
        "status": "completed",
        "document_id": ObjectId("5f9d88b9e6f3c2a4c8b4567a"),
        "error": None,
        "created_at": now,
        "updated_at": now,
        "created_by": "alice@test.com",
        "metadata": {"source": "upload"},
        "prefect_flow_run_id": "flow-run-1",
    }

    response = job_record_to_response(doc)
    assert response.id == "6543210987abcdef01234567"
    assert response.corpus_id == "507f1f77bcf86cd799439011"
    assert response.document_id == "5f9d88b9e6f3c2a4c8b4567a"
    assert response.status == "completed"
    assert response.prefect_flow_run_id == "flow-run-1"
    assert response.metadata == {"source": "upload"}


@pytest.mark.asyncio
async def test_run_ingest_pipeline_updates_job_on_success(tmp_upload_dir):
    from app.services import prefect_flows

    # Stage a fake file
    storage_path = tmp_upload_dir / "input.txt"
    storage_path.write_bytes(b"hello world")

    update_calls: list[tuple[str, dict]] = []

    async def fake_update(db, job_id, updates):
        update_calls.append((job_id, updates))

    corpus_id = str(ObjectId())
    job_id = str(ObjectId())

    fake_db = MagicMock()

    with (
        patch.object(prefect_flows, "_update_job", side_effect=fake_update),
        patch.object(prefect_flows, "get_db", return_value=fake_db),
        patch.object(
            prefect_flows,
            "ingest_document",
            AsyncMock(return_value="5f9d88b9e6f3c2a4c8b4567a"),
        ) as mock_ingest,
    ):
        result = await prefect_flows.run_ingest_pipeline(
            job_id=job_id,
            storage_path=str(storage_path),
            filename="input.txt",
            content_type="text/plain",
            corpus_id=corpus_id,
        )

    assert result == {"job_id": job_id, "document_id": "5f9d88b9e6f3c2a4c8b4567a"}
    mock_ingest.assert_awaited_once()
    kwargs = mock_ingest.await_args.kwargs
    assert kwargs["filename"] == "input.txt"
    assert kwargs["content"] == b"hello world"
    assert kwargs["corpus_id"] == corpus_id

    # Two updates: running -> completed
    statuses = [update[1].get("status") for update in update_calls]
    assert statuses == ["running", "completed"]
    # Storage file should be cleaned up on success.
    assert not storage_path.exists()


@pytest.mark.asyncio
async def test_run_ingest_pipeline_marks_failed(tmp_upload_dir):
    from app.services import prefect_flows

    storage_path = tmp_upload_dir / "input.txt"
    storage_path.write_bytes(b"hello")

    update_calls: list[tuple[str, dict]] = []

    async def fake_update(db, job_id, updates):
        update_calls.append((job_id, updates))

    fake_db = MagicMock()

    with (
        patch.object(prefect_flows, "_update_job", side_effect=fake_update),
        patch.object(prefect_flows, "get_db", return_value=fake_db),
        patch.object(
            prefect_flows,
            "ingest_document",
            AsyncMock(side_effect=RuntimeError("boom")),
        ),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await prefect_flows.run_ingest_pipeline(
                job_id=str(ObjectId()),
                storage_path=str(storage_path),
                filename="input.txt",
                content_type="text/plain",
                corpus_id=str(ObjectId()),
            )

    statuses = [update[1].get("status") for update in update_calls]
    assert "running" in statuses
    assert "failed" in statuses
    failed_update = next(u[1] for u in update_calls if u[1].get("status") == "failed")
    assert "boom" in failed_update.get("error", "")
