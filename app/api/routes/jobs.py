from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.deps import get_current_user, get_database
from app.models.job import IngestJobResponse
from app.services.authz import UserContext, ensure_corpus_access, list_accessible_corpora
from app.services.job_queue import job_record_to_response

router = APIRouter()


def _safe_object_id(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid job id") from exc


@router.get("/jobs", response_model=list[IngestJobResponse])
async def list_jobs(
    corpus_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    """List ingest jobs visible to the current user.

    Jobs are filtered to the corpora the user has read access to, so a
    user can monitor ingestion progress for any corpus they can see.
    """
    allowed = await list_accessible_corpora(db, user)
    allowed_ids = {str(c["_id"]) for c in allowed}

    if corpus_id is not None:
        if corpus_id not in allowed_ids:
            raise HTTPException(status_code=403, detail="You do not have access to this corpus")
        mongo_filter: dict = {"corpus_id": _safe_object_id(corpus_id)}
    else:
        if not allowed_ids:
            return []
        mongo_filter = {"corpus_id": {"$in": [_safe_object_id(cid) for cid in allowed_ids]}}

    if status:
        mongo_filter["status"] = status

    jobs: list[IngestJobResponse] = []
    cursor = db["ingest_jobs"].find(mongo_filter).sort("_id", -1).limit(limit)
    async for doc in cursor:
        jobs.append(job_record_to_response(doc))
    return jobs


@router.get("/jobs/{job_id}", response_model=IngestJobResponse)
async def get_job(
    job_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    doc = await db["ingest_jobs"].find_one({"_id": _safe_object_id(job_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")

    await ensure_corpus_access(db, user, str(doc["corpus_id"]), mode="read")
    return job_record_to_response(doc)
