from fastapi import Header, Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import settings
from app.core.database import get_db
from app.services.authz import UserContext, build_user_context


async def get_database() -> AsyncIOMotorDatabase:
    return get_db()


async def get_current_user(
    request: Request,
    x_user_id: str | None = Header(default=None),
    x_user_groups: str | None = Header(default=None),
) -> UserContext:
    if settings.debug:
        return build_user_context(settings.testuser, x_user_groups)

    proxy_user = request.headers.get(settings.proxy_user_header)
    user_id = proxy_user or x_user_id
    return build_user_context(user_id, x_user_groups)
