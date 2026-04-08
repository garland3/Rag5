"""Streaming SSE endpoint that sends real-time status updates during RAG search."""

import json
from typing import AsyncGenerator

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from starlette.responses import StreamingResponse

from app.api.deps import get_current_user, get_database
from app.models.query import QueryRequest
from app.services.authz import UserContext
from app.services.rag_stream import query_rag_stream

router = APIRouter()


async def _sse_generator(
    db: AsyncIOMotorDatabase,
    request: QueryRequest,
    user: UserContext,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from the streaming RAG pipeline."""
    async for event in query_rag_stream(
        db,
        question=request.question,
        corpus_id=request.corpus_id,
        top_k=request.top_k,
        document_ids=request.document_ids or None,
        retriever_name=request.retriever,
        user=user,
    ):
        yield f"data: {json.dumps(event)}\n\n"


@router.post("/query/stream")
async def query_stream(
    request: QueryRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    return StreamingResponse(
        _sse_generator(db, request, user),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
