"""Thin coordinator that turns an upload into a queued Prefect flow run.

The route layer calls ``enqueue_ingest_job`` with the raw file bytes;
this module takes care of staging the file to disk, creating the Mongo
``ingest_jobs`` record, and dispatching the Prefect flow as a background
asyncio task. A reference to every in-flight task is held in
``_RUNNING_TASKS`` so the event loop doesn't garbage-collect them.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models.job import IngestJobResponse
from app.services.storage import remove_staged, stage_upload

logger = logging.getLogger(__name__)

# Hold strong references to flow run tasks so they don't get GC'd while
# running in the background. Tasks self-remove on completion.
_RUNNING_TASKS: set[asyncio.Task[Any]] = set()

FlowRunner = Callable[..., Awaitable[Any]]


def _default_runner() -> FlowRunner:
    """Pick the flow runner based on configuration.

    Two execution modes are supported:

    * **In-process** (default, no external worker): returns the
      ``@flow``-decorated function so the coroutine runs inside the
      FastAPI event loop. Good for local dev and single-node deploys.
    * **Remote / deployment-based**: when
      ``settings.prefect_ingest_deployment`` is set (e.g.
      ``"ingest-document-flow/k8s"``), returns a wrapper around
      :func:`prefect.deployments.run_deployment` that submits a flow
      run to the named deployment. A Prefect worker — typically running
      in Kubernetes — then picks the run up from its work pool and
      executes it out-of-process. See ``docs/prefect.md`` for the full
      setup.

    Imports are lazy so that tests and lightweight dev environments
    don't need Prefect initialised just to import this module.
    """
    from app.config import settings

    deployment_name = settings.prefect_ingest_deployment.strip()
    if deployment_name:
        return _make_deployment_runner(deployment_name)

    from app.services.prefect_flows import ingest_document_flow

    return ingest_document_flow


def _make_deployment_runner(deployment_name: str) -> FlowRunner:
    """Return a runner that submits flow runs to a Prefect deployment.

    The returned coroutine calls ``run_deployment`` with ``timeout=0`` so
    it returns immediately after the run is created (without waiting for
    the worker to finish executing it). This keeps the API responsive:
    ``enqueue_ingest_job`` still returns ``202`` in a few milliseconds,
    and the worker does the heavy lifting asynchronously.
    """

    async def runner(**parameters: Any) -> Any:
        # Lazy import — only pay the cost when this mode is actually used.
        from prefect.deployments import run_deployment

        flow_run = await run_deployment(
            name=deployment_name,
            parameters=parameters,
            timeout=0,  # fire-and-forget; worker executes asynchronously
        )
        logger.info(
            "Submitted ingest job %s to deployment %s as flow run %s",
            parameters.get("job_id"),
            deployment_name,
            getattr(flow_run, "id", "<unknown>"),
        )
        return flow_run

    return runner


def _on_task_done(task: asyncio.Task[Any]) -> None:
    """Drop the task from the tracking set and surface any failure.

    Without this, an exception raised inside a background flow run would
    never be retrieved and would show up as a noisy
    ``Task exception was never retrieved`` warning in the logs, with no
    stack trace attached.
    """
    _RUNNING_TASKS.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.exception(
            "Background ingest flow task %s failed",
            task.get_name(),
            exc_info=exc,
        )


def _track_task(task: asyncio.Task[Any]) -> None:
    _RUNNING_TASKS.add(task)
    task.add_done_callback(_on_task_done)


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
    can surface a proper 5xx to the client. Staged files are cleaned up
    on any pre-dispatch failure so we don't leak files onto disk when
    the job never gets registered.
    """
    stage_id, storage_path = stage_upload(filename, content)

    try:
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
    except Exception:
        # Anything before the flow is dispatched owns the staged file.
        # Clean it up so we don't leak partial uploads onto disk.
        remove_staged(storage_path)
        raise

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
