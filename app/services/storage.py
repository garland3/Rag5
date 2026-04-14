"""Filesystem-backed staging storage for uploaded documents.

Uploaded files are written to disk before ingestion so that the ingest
pipeline can run asynchronously via Prefect without holding the upload
HTTP request open. A real deployment might back this with S3/GCS or
GridFS; for local and container use a filesystem directory is enough.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

from app.config import settings


def _sanitize(filename: str) -> str:
    base = os.path.basename(filename or "untitled")
    # Strip characters that could break filesystem paths on any OS.
    cleaned = "".join(c for c in base if c.isalnum() or c in ("-", "_", ".", " "))
    return cleaned.strip() or "untitled"


def get_storage_root() -> Path:
    root = Path(settings.upload_storage_dir)
    root.mkdir(parents=True, exist_ok=True)
    return root


def stage_upload(filename: str, content: bytes) -> tuple[str, str]:
    """Persist raw upload bytes and return (stage_id, absolute_path).

    Each upload gets its own subdirectory keyed by a uuid, so multiple
    documents with the same filename don't collide.
    """
    stage_id = uuid.uuid4().hex
    target_dir = get_storage_root() / stage_id
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / _sanitize(filename)
    target_path.write_bytes(content)
    return stage_id, str(target_path)


def read_staged(storage_path: str) -> bytes:
    return Path(storage_path).read_bytes()


def remove_staged(storage_path: str) -> None:
    """Delete the staged file and its parent stage directory (best effort)."""
    path = Path(storage_path)
    try:
        if path.exists():
            path.unlink()
        parent = path.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
    except OSError:
        # Staging cleanup is best-effort; don't fail ingestion over it.
        pass


async def read_staged_async(storage_path: str) -> bytes:
    """Non-blocking wrapper around :func:`read_staged`."""
    return await asyncio.to_thread(read_staged, storage_path)


async def remove_staged_async(storage_path: str) -> None:
    """Non-blocking wrapper around :func:`remove_staged`."""
    await asyncio.to_thread(remove_staged, storage_path)
