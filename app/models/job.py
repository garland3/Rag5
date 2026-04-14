from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

JobStatus = Literal["queued", "running", "completed", "failed"]


class IngestJobCreate(BaseModel):
    """Internal model used by the API when a new job is created."""

    corpus_id: str
    filename: str
    content_type: str
    size_bytes: int
    storage_path: str
    created_by: str


class IngestJobResponse(BaseModel):
    """Job record returned to API clients."""

    id: str
    corpus_id: str
    filename: str
    content_type: str
    size_bytes: int
    status: JobStatus
    document_id: str | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    created_by: str
    prefect_flow_run_id: str | None = None
    metadata: dict = Field(default_factory=dict)
