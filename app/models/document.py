from datetime import datetime

from pydantic import BaseModel, Field


class DocumentMetadata(BaseModel):
    filename: str
    content_type: str
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    chunk_count: int = 0
    metadata: dict = Field(default_factory=dict)


class DocumentResponse(BaseModel):
    id: str
    filename: str
    content_type: str
    uploaded_at: datetime
    chunk_count: int
    corpus_id: str
    metadata: dict = Field(default_factory=dict)


class ChunkRecord(BaseModel):
    document_id: str
    text: str
    embedding: list[float]
    chunk_index: int
    metadata: dict = Field(default_factory=dict)
