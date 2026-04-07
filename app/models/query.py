from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str
    corpus_id: str = Field(
        description="Corpus identifier used to scope retrieval to a single tenant corpus.",
    )
    top_k: int = 5
    document_ids: list[str] = Field(default_factory=list)
    retriever: str | None = Field(
        default=None,
        description="Retriever to use: vector, keyword, hybrid, multi_query, agent. "
        "Defaults to the server-configured retriever.",
    )


class SourceChunk(BaseModel):
    document_id: str
    filename: str
    chunk_text: str
    score: float


class QueryResponse(BaseModel):
    answer: str
    sources: list[SourceChunk]
