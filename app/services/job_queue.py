"""Thin coordinator that turns an upload into a queued Prefect flow run.

The route layer calls ``enqueue_ingest_job`` with the raw file bytes;
this module takes care of staging the file to disk, creating the Mongo
``ingest_jobs`` record, and dispatching the Prefect flow as a background
asyncio task. A reference to every in-flight task is held in
``_RUNNING_TASKS`` so the event loop doesn't garbage-collect them.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Awaitable, Callable

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models.job import IngestJobResponse
from app.services.storage import stage_upload

# Hold strong references to flow run tasks so they don't get GC'd while
# running in the background. Tasks self-remove on completion.
_RUNNING_TASKS: set[asyncio.Task[Any]] = set()

FlowRunner = Callable[..., Awaitable[Any]]


def _default_runner() -> FlowRunner:
    # Imported lazily so tests that monkeypatch the runner don't need
    # prefect installed or initialised.
    from app.services.prefect_flows import ingest_document_flow

    return ingest_document_flow


def _track_task(task: asyncio.Task[Any]) -> None:
    _RUNNING_TASKS.add(task)
    task.add_done_callback(_RUNNING_TASKS.discard)


async def enqueue_ingest_job(
    db: AsyncIOMotorDatabase,
    *,
    corpus_id: str,
    filename: str,
    content_type: str,
    content: bytes,
    created_by: str,
    metadata: dict | None = None,
    runner: FlowRunner | None = None,
) -> IngestJobResponse:
    """Create a job record and submit the Prefect flow.

    Returns the job record immediately; the actual ingest runs in the
    background. Raises if file staging or DB insert fails so the caller
    can surface a proper 5xx to the client.
    """
    stage_id, storage_path = stage_upload(filename, content)

    now = datetime.utcnow()
    job_doc = {
        "corpus_id": ObjectId(corpus_id),
        "filename": filename,
        "content_type": content_type,
        "size_bytes": len(content),
        "storage_path": storage_path,
        "stage_id": stage_id,
        "status": "queued",
        "document_id": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
        "created_by": created_by,
        "metadata": metadata or {},
        "prefect_flow_run_id": None,
    }
    result = await db["ingest_jobs"].insert_one(job_doc)
    job_id = str(result.inserted_id)

    flow_runner = runner or _default_runner()
    task = asyncio.create_task(
        flow_runner(
            job_id=job_id,
            storage_path=storage_path,
            filename=filename,
            content_type=content_type,
            corpus_id=corpus_id,
            metadata=metadata or {},
        ),
        name=f"ingest-job-{job_id}",
    )
    _track_task(task)

    return job_record_to_response({**job_doc, "_id": result.inserted_id})


def job_record_to_response(doc: dict) -> IngestJobResponse:
    return IngestJobResponse(
        id=str(doc["_id"]),
        corpus_id=str(doc["corpus_id"]),
        filename=doc["filename"],
        content_type=doc["content_type"],
        size_bytes=doc.get("size_bytes", 0),
        status=doc.get("status", "queued"),
        document_id=str(doc["document_id"]) if doc.get("document_id") else None,
        error=doc.get("error"),
        created_at=doc.get("created_at") or datetime.utcnow(),
        updated_at=doc.get("updated_at") or datetime.utcnow(),
        created_by=doc.get("created_by", "unknown"),
        prefect_flow_run_id=doc.get("prefect_flow_run_id"),
        metadata=doc.get("metadata", {}),
    )
