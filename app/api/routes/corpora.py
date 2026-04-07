from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.deps import get_current_user, get_database
from app.models.corpus import CorpusCreateRequest, CorpusResponse
from app.services.authz import UserContext, can_read, can_write, is_owner, list_accessible_corpora

router = APIRouter()


@router.post("/corpora", response_model=CorpusResponse, status_code=201)
async def create_corpus(
    request: CorpusCreateRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    existing = await db["corpora"].find_one({"name": request.name})
    if existing:
        raise HTTPException(status_code=409, detail="Corpus name already exists")
    if request.owner_group not in user.groups:
        raise HTTPException(
            status_code=403,
            detail="You can only create corpora where you belong to the owner group",
        )

    result = await db["corpora"].insert_one(
        {
            "name": request.name,
            "read_group": request.read_group,
            "write_group": request.write_group,
            "owner_group": request.owner_group,
            "created_by": user.user_id,
        }
    )
    corpus = await db["corpora"].find_one({"_id": result.inserted_id})
    return _to_response(corpus, user)


@router.get("/corpora", response_model=list[CorpusResponse])
async def list_corpora(
    db: AsyncIOMotorDatabase = Depends(get_database),
    user: UserContext = Depends(get_current_user),
):
    corpora = await list_accessible_corpora(db, user)
    return [_to_response(c, user) for c in corpora]


def _to_response(corpus: dict, user: UserContext) -> CorpusResponse:
    return CorpusResponse(
        id=str(corpus["_id"]),
        name=corpus["name"],
        read_group=corpus["read_group"],
        write_group=corpus["write_group"],
        owner_group=corpus["owner_group"],
        created_by=corpus.get("created_by", "unknown"),
        created_at=corpus["_id"].generation_time,
        can_read=can_read(corpus, user),
        can_write=can_write(corpus, user),
        is_owner=is_owner(corpus, user),
    )
