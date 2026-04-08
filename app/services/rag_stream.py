"""Streaming RAG pipeline that yields status events via SSE."""

from typing import AsyncGenerator

from bson import ObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import settings
from app.core.generator import generate_answer
from app.core.retriever import get_retriever
from app.services.authz import UserContext, ensure_corpus_access


async def query_rag_stream(
    db: AsyncIOMotorDatabase,
    question: str,
    top_k: int = 5,
    corpus_id: str = "",
    document_ids: list[str] | None = None,
    retriever_name: str | None = None,
    user: UserContext | None = None,
) -> AsyncGenerator[dict, None]:
    """Async generator that yields status-update dicts as the RAG pipeline runs.

    Each dict has:
      - type: "status" | "error" | "sources" | "answer" | "done"
      - message: human-readable status text
      - data: (optional) payload for sources/answer events
    """
    if not user:
        yield {"type": "error", "message": "Missing user context"}
        return
    if not corpus_id:
        yield {"type": "error", "message": "corpus_id is required"}
        return

    # --- Auth check ---
    yield {"type": "status", "message": "Checking access permissions..."}
    try:
        await ensure_corpus_access(db, user, corpus_id, mode="read")
    except HTTPException as exc:
        yield {"type": "error", "message": exc.detail}
        return

    # --- Validate document_ids ---
    if document_ids:
        yield {"type": "status", "message": "Validating document selection..."}
        match_count = await db["documents"].count_documents(
            {
                "_id": {"$in": [ObjectId(doc_id) for doc_id in document_ids]},
                "corpus_id": ObjectId(corpus_id),
            }
        )
        if match_count != len(document_ids):
            yield {"type": "error", "message": "All document_ids must belong to the selected corpus"}
            return

    # --- Build retriever ---
    name = retriever_name or settings.retriever
    kwargs: dict = {}
    if name == "multi_query":
        kwargs["num_rewrites"] = settings.multi_query_rewrites
    elif name == "agent":
        kwargs["max_iterations"] = settings.agent_max_iterations

    yield {"type": "status", "message": f"Searching with {name} retriever..."}

    retriever = get_retriever(name, **kwargs)

    # --- Retrieve chunks ---
    yield {"type": "status", "message": f"Finding relevant documents for: \"{question}\""}
    chunks = await retriever.retrieve(
        db,
        question,
        top_k=top_k,
        document_ids=document_ids,
        corpus_id=corpus_id,
    )

    if not chunks:
        yield {"type": "status", "message": "No relevant documents found."}
        yield {
            "type": "answer",
            "data": {
                "answer": "No relevant documents found to answer your question.",
                "sources": [],
            },
        }
        yield {"type": "done", "message": "Search complete"}
        return

    yield {"type": "status", "message": f"Found {len(chunks)} relevant chunks. Resolving sources..."}

    # --- Resolve document filenames ---
    doc_ids = list({c["document_id"] for c in chunks})
    doc_cursor = db["documents"].find(
        {
            "_id": {"$in": [ObjectId(did) for did in doc_ids]},
            "corpus_id": ObjectId(corpus_id),
        }
    )
    doc_map = {}
    async for doc in doc_cursor:
        doc_map[str(doc["_id"])] = doc["filename"]

    sources = [
        {
            "document_id": c["document_id"],
            "filename": doc_map.get(c["document_id"], "unknown"),
            "chunk_text": c["text"],
            "score": c.get("score", 0.0),
        }
        for c in chunks
    ]

    yield {"type": "sources", "message": f"Retrieved sources from {len(doc_map)} documents", "data": sources}

    # --- Generate answer ---
    yield {"type": "status", "message": "Generating answer from retrieved context..."}
    answer = await generate_answer(question, chunks)

    yield {
        "type": "answer",
        "data": {
            "answer": answer,
            "sources": sources,
        },
    }
    yield {"type": "done", "message": "Search complete"}
