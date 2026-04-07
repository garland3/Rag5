from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str
    top_k: int = 5
    document_ids: list[str] = Field(default_factory=list)


class SourceChunk(BaseModel):
    document_id: str
    filename: str
    chunk_text: str
    score: float


class QueryResponse(BaseModel):
    answer: str
    sources: list[SourceChunk]
