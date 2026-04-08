from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.deps import get_current_user, get_database
from app.models.document import DocumentResponse
from app.services.authz import UserContext, ensure_corpus_access, list_accessible_corpora
from app.services.ingest import ingest_document

router = APIRouter()


@router.post("/documents", response_model=DocumentResponse, status_code=201)
async def upload_document(
    file: UploadFile,
    corpus_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    await ensure_corpus_access(db, user, corpus_id, mode="write")

    content = await file.read()
    doc_id = await ingest_document(
        db,
        filename=file.filename or "untitled",
        content_type=file.content_type or "application/octet-stream",
        content=content,
        corpus_id=corpus_id,
    )

    doc = await db["documents"].find_one({"_id": ObjectId(doc_id)})
    return _doc_to_response(doc)


@router.get("/documents", response_model=list[DocumentResponse])
async def list_documents(
    corpus_id: str | None = Query(default=None),
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    allowed_corpora = await list_accessible_corpora(db, user)
    allowed_ids = {str(c["_id"]) for c in allowed_corpora}

    if corpus_id and corpus_id not in allowed_ids:
        raise HTTPException(status_code=403, detail="You do not have access to this corpus")

    if not corpus_id and not allowed_ids:
        return []

    mongo_filter = {}
    if corpus_id:
        mongo_filter["corpus_id"] = _safe_object_id(corpus_id)
    else:
        mongo_filter["corpus_id"] = {"$in": [_safe_object_id(cid) for cid in allowed_ids]}

    docs = []
    async for doc in db["documents"].find(mongo_filter).sort("_id", -1):
        docs.append(_doc_to_response(doc))
    return docs


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    doc = await db["documents"].find_one({"_id": _safe_object_id(document_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    await ensure_corpus_access(db, user, str(doc["corpus_id"]), mode="read")
    return _doc_to_response(doc)


@router.delete("/documents/{document_id}", status_code=204)
async def delete_document(
    document_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    oid = _safe_object_id(document_id)
    doc = await db["documents"].find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    await ensure_corpus_access(db, user, str(doc["corpus_id"]), mode="write")
    await db["documents"].delete_one({"_id": oid})
    await db["chunks"].delete_many({"document_id": oid})


def _safe_object_id(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid object id") from exc


def _doc_to_response(doc: dict) -> DocumentResponse:
    return DocumentResponse(
        id=str(doc["_id"]),
        filename=doc["filename"],
        content_type=doc["content_type"],
        uploaded_at=doc["_id"].generation_time,
        chunk_count=doc.get("chunk_count", 0),
        corpus_id=str(doc["corpus_id"]),
        metadata=doc.get("metadata", {}),
    )
