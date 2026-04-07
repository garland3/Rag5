from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.embeddings import embed_query


async def vector_search(
    db: AsyncIOMotorDatabase,
    query: str,
    top_k: int = 5,
    document_ids: list[str] | None = None,
) -> list[dict]:
    query_embedding = await embed_query(query)

    pipeline: list[dict] = [
        {
            "$vectorSearch": {
                "index": "chunk_vector_index",
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": top_k * 10,
                "limit": top_k,
            }
        },
        {
            "$project": {
                "text": 1,
                "document_id": 1,
                "chunk_index": 1,
                "metadata": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]

    # Apply document filter if specified
    if document_ids:
        oid_filter = [ObjectId(did) for did in document_ids]
        pipeline[0]["$vectorSearch"]["filter"] = {
            "document_id": {"$in": oid_filter}
        }

    chunks = []
    async for doc in db["chunks"].aggregate(pipeline):
        doc["_id"] = str(doc["_id"])
        doc["document_id"] = str(doc["document_id"])
        chunks.append(doc)

    return chunks
