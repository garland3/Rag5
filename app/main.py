from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import documents, health, query
from app.core.database import close_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_client()


app = FastAPI(title="Rag5", description="RAG system with MongoDB", lifespan=lifespan)

app.include_router(health.router, tags=["health"])
app.include_router(documents.router, prefix="/api/v1", tags=["documents"])
app.include_router(query.router, prefix="/api/v1", tags=["query"])
