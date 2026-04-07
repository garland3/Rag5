from __future__ import annotations

from dataclasses import dataclass

from bson import ObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

DEFAULT_USER_ID = "test-user"
DEFAULT_GROUPS = {
    "rag5-default-readers",
    "rag5-default-writers",
    "rag5-default-owners",
}


@dataclass(slots=True)
class UserContext:
    user_id: str
    groups: set[str]


def build_user_context(user_id: str | None, groups_header: str | None) -> UserContext:
    groups = {g.strip() for g in (groups_header or "").split(",") if g.strip()}
    if not groups:
        groups = set(DEFAULT_GROUPS)
    return UserContext(user_id=user_id or DEFAULT_USER_ID, groups=groups)


def is_owner(corpus: dict, user: UserContext) -> bool:
    return corpus["owner_group"] in user.groups


def can_read(corpus: dict, user: UserContext) -> bool:
    return any(
        g in user.groups
        for g in (corpus["read_group"], corpus["write_group"], corpus["owner_group"])
    )


def can_write(corpus: dict, user: UserContext) -> bool:
    return any(g in user.groups for g in (corpus["write_group"], corpus["owner_group"]))


async def ensure_default_corpus(db: AsyncIOMotorDatabase) -> None:
    existing = await db["corpora"].find_one({"name": "default"})
    if existing:
        return
    await db["corpora"].insert_one(
        {
            "name": "default",
            "read_group": "rag5-default-readers",
            "write_group": "rag5-default-writers",
            "owner_group": "rag5-default-owners",
            "created_by": "system",
        }
    )


async def list_accessible_corpora(db: AsyncIOMotorDatabase, user: UserContext) -> list[dict]:
    await ensure_default_corpus(db)
    corpora: list[dict] = []
    async for corpus in db["corpora"].find().sort("name", 1):
        if can_read(corpus, user):
            corpora.append(corpus)
    return corpora


async def get_corpus_or_404(db: AsyncIOMotorDatabase, corpus_id: str) -> dict:
    try:
        oid = ObjectId(corpus_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid corpus_id") from exc

    corpus = await db["corpora"].find_one({"_id": oid})
    if not corpus:
        raise HTTPException(status_code=404, detail="Corpus not found")
    return corpus


async def ensure_corpus_access(
    db: AsyncIOMotorDatabase,
    user: UserContext,
    corpus_id: str,
    mode: str = "read",
) -> dict:
    if mode not in {"read", "write", "owner"}:
        raise HTTPException(status_code=500, detail="Invalid access mode")

    corpus = await get_corpus_or_404(db, corpus_id)
    authorized = (
        can_read(corpus, user)
        if mode == "read"
        else can_write(corpus, user)
        if mode == "write"
        else is_owner(corpus, user)
    )
    if not authorized:
        raise HTTPException(status_code=403, detail="You do not have access to this corpus")
    return corpus
