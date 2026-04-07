from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.deps import get_database
from app.models.document import DocumentResponse
from app.services.ingest import ingest_document

router = APIRouter()


@router.post("/documents", response_model=DocumentResponse, status_code=201)
async def upload_document(
    file: UploadFile,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    content = await file.read()
    doc_id = await ingest_document(
        db,
        filename=file.filename or "untitled",
        content_type=file.content_type or "application/octet-stream",
        content=content,
    )

    doc = await db["documents"].find_one({"_id": ObjectId(doc_id)})
    return _doc_to_response(doc)


@router.get("/documents", response_model=list[DocumentResponse])
async def list_documents(
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    docs = []
    async for doc in db["documents"].find().sort("_id", -1):
        docs.append(_doc_to_response(doc))
    return docs


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    doc = await db["documents"].find_one({"_id": ObjectId(document_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return _doc_to_response(doc)


@router.delete("/documents/{document_id}", status_code=204)
async def delete_document(
    document_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    oid = ObjectId(document_id)
    result = await db["documents"].delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Document not found")
    await db["chunks"].delete_many({"document_id": oid})


def _doc_to_response(doc: dict) -> DocumentResponse:
    return DocumentResponse(
        id=str(doc["_id"]),
        filename=doc["filename"],
        content_type=doc["content_type"],
        uploaded_at=doc["_id"].generation_time,
        chunk_count=doc.get("chunk_count", 0),
        metadata=doc.get("metadata", {}),
    )
