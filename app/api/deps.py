from fastapi import Header
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import get_db
from app.services.authz import UserContext, build_user_context


async def get_database() -> AsyncIOMotorDatabase:
    return get_db()


async def get_current_user(
    x_user_id: str | None = Header(default=None),
    x_user_groups: str | None = Header(default=None),
) -> UserContext:
    return build_user_context(x_user_id, x_user_groups)
