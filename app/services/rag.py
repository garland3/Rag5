from bson import ObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import settings
from app.core.generator import generate_answer
from app.core.retriever import get_retriever
from app.models.query import QueryResponse, SourceChunk
from app.services.authz import UserContext, ensure_corpus_access


async def query_rag(
    db: AsyncIOMotorDatabase,
    question: str,
    top_k: int = 5,
    corpus_id: str = "",
    document_ids: list[str] | None = None,
    retriever_name: str | None = None,
    user: UserContext | None = None,
) -> QueryResponse:
    if not user:
        raise HTTPException(status_code=401, detail="Missing user context")
    if not corpus_id:
        raise HTTPException(status_code=400, detail="corpus_id is required")

    await ensure_corpus_access(db, user, corpus_id, mode="read")

    name = retriever_name or settings.retriever
    kwargs: dict = {}
    if name == "multi_query":
        kwargs["num_rewrites"] = settings.multi_query_rewrites
    elif name == "agent":
        kwargs["max_iterations"] = settings.agent_max_iterations

    if document_ids:
        match_count = await db["documents"].count_documents(
            {
                "_id": {"$in": [ObjectId(doc_id) for doc_id in document_ids]},
                "corpus_id": ObjectId(corpus_id),
            }
        )
        if match_count != len(document_ids):
            raise HTTPException(
                status_code=403,
                detail="All document_ids must belong to the selected corpus",
            )

    retriever = get_retriever(name, **kwargs)
    chunks = await retriever.retrieve(
        db,
        question,
        top_k=top_k,
        document_ids=document_ids,
        corpus_id=corpus_id,
    )

    if not chunks:
        return QueryResponse(
            answer="No relevant documents found to answer your question.",
            sources=[],
        )

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

    answer = await generate_answer(question, chunks)

    sources = [
        SourceChunk(
            document_id=c["document_id"],
            filename=doc_map.get(c["document_id"], "unknown"),
            chunk_text=c["text"],
            score=c.get("score", 0.0),
        )
        for c in chunks
    ]

    return QueryResponse(answer=answer, sources=sources)
