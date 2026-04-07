from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.deps import get_current_user, get_database
from app.models.query import QueryRequest, QueryResponse
from app.services.authz import UserContext
from app.services.rag import query_rag

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query(
    request: QueryRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    return await query_rag(
        db,
        question=request.question,
        corpus_id=request.corpus_id,
        top_k=request.top_k,
        document_ids=request.document_ids or None,
        retriever_name=request.retriever,
        user=user,
    )
