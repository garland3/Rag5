from fastapi import APIRouter

from app.core.database import get_client

router = APIRouter()


@router.get("/health")
async def health_check():
    try:
        await get_client().admin.command("ping")
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    return {"status": "ok", "database": db_status}
