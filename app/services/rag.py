from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.generator import generate_answer
from app.core.retriever import vector_search
from app.models.query import QueryResponse, SourceChunk


async def query_rag(
    db: AsyncIOMotorDatabase,
    question: str,
    top_k: int = 5,
    document_ids: list[str] | None = None,
) -> QueryResponse:
    chunks = await vector_search(db, question, top_k=top_k, document_ids=document_ids)

    if not chunks:
        return QueryResponse(
            answer="No relevant documents found to answer your question.",
            sources=[],
        )

    # Resolve filenames from document collection
    doc_ids = list({c["document_id"] for c in chunks})
    from bson import ObjectId

    doc_cursor = db["documents"].find(
        {"_id": {"$in": [ObjectId(did) for did in doc_ids]}}
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
