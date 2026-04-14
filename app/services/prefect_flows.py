"""Prefect flows/tasks that orchestrate asynchronous document ingestion.

The upload endpoint persists a job record + the raw file to the staging
directory, then submits ``ingest_document_flow`` so that Prefect handles
retries, logging, and state tracking. The flow reads the staged file,
runs the existing ingest pipeline, and updates the Mongo job record so
API clients can poll ``GET /api/v1/jobs/{id}`` for progress.

Flows are executed in-process via ``asyncio.create_task`` so no external
Prefect worker is required for development. Pointing ``PREFECT_API_URL``
at a Prefect server transparently promotes these runs to the UI.

The core logic lives in ``run_ingest_pipeline`` - a plain async function
so it is trivially unit-testable without booting the Prefect engine. The
``@flow`` wrapper ``ingest_document_flow`` just adds orchestration
(retries, logging, state) on top of it.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from prefect import flow, task

from app.core.database import get_db
from app.services.ingest import ingest_document
from app.services.storage import read_staged_async, remove_staged_async

logger = logging.getLogger(__name__)


async def _update_job(
    db: AsyncIOMotorDatabase,
    job_id: str,
    updates: dict[str, Any],
) -> None:
    updates = {**updates, "updated_at": datetime.utcnow()}
    await db["ingest_jobs"].update_one({"_id": ObjectId(job_id)}, {"$set": updates})


async def run_ingest_pipeline(
    job_id: str,
    storage_path: str,
    filename: str,
    content_type: str,
    corpus_id: str,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """End-to-end ingestion work for a single uploaded file.

    This is a plain async function so it can be tested directly and so
    it can be reused outside of Prefect (e.g. scripts, workers). The
    ``@flow``-decorated ``ingest_document_flow`` below just wraps this
    with Prefect's orchestration engine.
    """
    db = get_db()

    await _update_job(
        db, job_id, {"status": "running", "started_at": datetime.utcnow()}
    )

    try:
        # All blocking file I/O and CPU-bound parsing is offloaded to a
        # worker thread (see storage.read_staged_async and
        # ingest.ingest_document) so the ingest flow never blocks the
        # FastAPI event loop while other API requests are served.
        content = await read_staged_async(storage_path)
        logger.info(
            "Ingesting %s (%d bytes) into corpus %s",
            filename,
            len(content),
            corpus_id,
        )
        doc_id = await ingest_document(
            db,
            filename=filename,
            content_type=content_type,
            content=content,
            metadata=metadata or {},
            corpus_id=corpus_id,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced in job status
        logger.exception("Ingest pipeline failed for job %s", job_id)
        await _update_job(db, job_id, {"status": "failed", "error": str(exc)})
        await remove_staged_async(storage_path)
        raise

    await _update_job(
        db,
        job_id,
        {
            "status": "completed",
            "document_id": ObjectId(doc_id),
            "completed_at": datetime.utcnow(),
        },
    )
    await remove_staged_async(storage_path)
    logger.info("Job %s completed, document_id=%s", job_id, doc_id)
    return {"job_id": job_id, "document_id": doc_id}


@task(name="ingest-document-task", retries=1, retry_delay_seconds=2)
async def ingest_document_task(
    job_id: str,
    storage_path: str,
    filename: str,
    content_type: str,
    corpus_id: str,
    metadata: dict | None = None,
) -> dict[str, Any]:
    return await run_ingest_pipeline(
        job_id=job_id,
        storage_path=storage_path,
        filename=filename,
        content_type=content_type,
        corpus_id=corpus_id,
        metadata=metadata,
    )


@flow(name="ingest-document-flow")
async def ingest_document_flow(
    job_id: str,
    storage_path: str,
    filename: str,
    content_type: str,
    corpus_id: str,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """Prefect flow entrypoint for ingesting a single uploaded file.

    Delegates to :func:`run_ingest_pipeline` so the business logic is
    exercised in unit tests without needing the Prefect engine.
    """
    return await ingest_document_task(
        job_id=job_id,
        storage_path=storage_path,
        filename=filename,
        content_type=content_type,
        corpus_id=corpus_id,
        metadata=metadata,
    )
