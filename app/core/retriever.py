"""
Multiple retriever implementations for RAG search.

Retrievers:
- VectorRetriever: MongoDB Atlas vector similarity search
- KeywordRetriever: MongoDB text index keyword search
- HybridRetriever: Combines keyword + vector with reciprocal rank fusion
- MultiQueryRetriever: Rewords query M times, runs sub-retriever, deduplicates
- AgentRetriever: LLM-driven recursive search refinement
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from abc import ABC, abstractmethod

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.embeddings import embed_query

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------
class BaseRetriever(ABC):
    """Abstract base for all retrievers."""

    @abstractmethod
    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        ...


# ---------------------------------------------------------------------------
# Vector (original)
# ---------------------------------------------------------------------------
class VectorRetriever(BaseRetriever):
    """Pure vector similarity search via MongoDB Atlas $vectorSearch."""

    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        return await vector_search(db, query, top_k=top_k, document_ids=document_ids, corpus_id=corpus_id)


async def vector_search(
    db: AsyncIOMotorDatabase,
    query: str,
    top_k: int = 5,
    document_ids: list[str] | None = None,
    corpus_id: str | None = None,
) -> list[dict]:
    query_embedding = await embed_query(query)

    pipeline: list[dict] = [
        {
            "$vectorSearch": {
                "index": "chunk_vector_index",
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": top_k * 10,
                "limit": top_k,
            }
        },
        {
            "$project": {
                "text": 1,
                "document_id": 1,
                "chunk_index": 1,
                "metadata": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]

    filters: list[dict] = []
    if document_ids:
        filters.append({"document_id": {"$in": [ObjectId(did) for did in document_ids]}})
    if corpus_id:
        filters.append({"corpus_id": ObjectId(corpus_id)})
    if filters:
        pipeline[0]["$vectorSearch"]["filter"] = {"$and": filters} if len(filters) > 1 else filters[0]

    chunks: list[dict] = []
    async for doc in db["chunks"].aggregate(pipeline):
        doc["_id"] = str(doc["_id"])
        doc["document_id"] = str(doc["document_id"])
        chunks.append(doc)

    return chunks


# ---------------------------------------------------------------------------
# Keyword
# ---------------------------------------------------------------------------
class KeywordRetriever(BaseRetriever):
    """MongoDB text-index ($text / $search) keyword retriever."""

    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        return await keyword_search(db, query, top_k=top_k, document_ids=document_ids, corpus_id=corpus_id)


async def keyword_search(
    db: AsyncIOMotorDatabase,
    query: str,
    top_k: int = 5,
    document_ids: list[str] | None = None,
    corpus_id: str | None = None,
) -> list[dict]:
    """Full-text keyword search using MongoDB $text index on the chunks collection.

    Requires a text index on the ``text`` field:
        db.chunks.createIndex({"text": "text"})
    """
    text_filter: dict = {"$text": {"$search": query}}
    if document_ids:
        text_filter["document_id"] = {"$in": [ObjectId(did) for did in document_ids]}
    if corpus_id:
        text_filter["corpus_id"] = ObjectId(corpus_id)

    cursor = (
        db["chunks"]
        .find(text_filter, {"score": {"$meta": "textScore"}, "text": 1, "document_id": 1, "chunk_index": 1, "metadata": 1})
        .sort([("score", {"$meta": "textScore"})])
        .limit(top_k)
    )

    chunks: list[dict] = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc["document_id"] = str(doc["document_id"])
        chunks.append(doc)

    return chunks


# ---------------------------------------------------------------------------
# Hybrid  (keyword + vector with Reciprocal Rank Fusion)
# ---------------------------------------------------------------------------
class HybridRetriever(BaseRetriever):
    """Combines keyword and vector search via reciprocal rank fusion (RRF).

    RRF score = sum over methods of 1 / (k + rank), with k=60 by default.
    """

    def __init__(self, rrf_k: int = 60):
        self.rrf_k = rrf_k

    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        # Fetch more per-method so fusion has enough candidates
        fetch_k = top_k * 3

        vec_results, kw_results = await asyncio.gather(
            vector_search(db, query, top_k=fetch_k, document_ids=document_ids, corpus_id=corpus_id),
            keyword_search(db, query, top_k=fetch_k, document_ids=document_ids, corpus_id=corpus_id),
        )

        return _reciprocal_rank_fusion(
            [vec_results, kw_results], top_k=top_k, k=self.rrf_k
        )


def _chunk_key(chunk: dict) -> str:
    """Stable dedup key for a chunk."""
    return chunk.get("_id") or hashlib.md5(chunk.get("text", "").encode()).hexdigest()


def _reciprocal_rank_fusion(
    ranked_lists: list[list[dict]], top_k: int, k: int = 60
) -> list[dict]:
    """Merge multiple ranked lists using RRF."""
    scores: dict[str, float] = {}
    chunk_map: dict[str, dict] = {}

    for ranked in ranked_lists:
        for rank, chunk in enumerate(ranked):
            cid = _chunk_key(chunk)
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
            chunk_map[cid] = chunk

    sorted_ids = sorted(scores, key=lambda cid: scores[cid], reverse=True)[:top_k]

    results = []
    for cid in sorted_ids:
        c = chunk_map[cid]
        c["score"] = scores[cid]
        results.append(c)
    return results


# ---------------------------------------------------------------------------
# MultiQuery  (reword M times → search → deduplicate/synthesize)
# ---------------------------------------------------------------------------
class MultiQueryRetriever(BaseRetriever):
    """Rewrites the user query M times with random variation, runs each through
    a sub-retriever, then deduplicates and ranks the combined results.

    The ``sub_retriever`` can be any BaseRetriever (vector, keyword, hybrid, …).
    """

    def __init__(
        self,
        sub_retriever: BaseRetriever | None = None,
        num_rewrites: int = 3,
    ):
        self.sub_retriever = sub_retriever or VectorRetriever()
        self.num_rewrites = num_rewrites

    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        rewrites = await _generate_query_rewrites(query, m=self.num_rewrites)
        all_queries = [query] + rewrites  # always include original

        tasks = [
            self.sub_retriever.retrieve(db, q, top_k=top_k, document_ids=document_ids, corpus_id=corpus_id)
            for q in all_queries
        ]
        ranked_lists = await asyncio.gather(*tasks)

        return _reciprocal_rank_fusion(list(ranked_lists), top_k=top_k)


async def _generate_query_rewrites(query: str, m: int = 3) -> list[str]:
    """Use the configured LLM to produce *m* rephrasings of *query*."""
    from app.config import settings

    prompt = (
        f"Rewrite the following search query {m} different ways. "
        "Each rewrite should preserve the original intent but use different "
        "wording, synonyms, or phrasing to improve recall in a search engine. "
        "Return ONLY the rewritten queries, one per line, no numbering.\n\n"
        f"Query: {query}"
    )

    if settings.llm_provider == "anthropic":
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        resp = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text
    else:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key)
        resp = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
        )
        text = resp.choices[0].message.content or ""

    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    return lines[:m]


# ---------------------------------------------------------------------------
# Agent  (LLM-driven recursive retrieval)
# ---------------------------------------------------------------------------
class AgentRetriever(BaseRetriever):
    """An agentic retriever that uses an LLM to iteratively refine searches.

    Flow per iteration:
    1. Run the sub-retriever with the current query.
    2. Ask the LLM to evaluate results and decide:
       - DONE  → return accumulated results
       - REFINE <new_query> → loop with the refined query
    3. Repeat up to ``max_iterations``.

    All unique chunks from every iteration are accumulated and re-ranked at the end.
    """

    def __init__(
        self,
        sub_retriever: BaseRetriever | None = None,
        max_iterations: int = 3,
    ):
        self.sub_retriever = sub_retriever or VectorRetriever()
        self.max_iterations = max_iterations

    async def retrieve(
        self,
        db: AsyncIOMotorDatabase,
        query: str,
        top_k: int = 5,
        document_ids: list[str] | None = None,
        corpus_id: str | None = None,
    ) -> list[dict]:
        all_chunks: list[dict] = []
        seen_ids: set[str] = set()
        current_query = query

        for iteration in range(self.max_iterations):
            logger.info(
                "AgentRetriever iteration %d/%d — query: %s",
                iteration + 1,
                self.max_iterations,
                current_query,
            )

            new_chunks = await self.sub_retriever.retrieve(
                db, current_query, top_k=top_k, document_ids=document_ids, corpus_id=corpus_id
            )

            for c in new_chunks:
                cid = _chunk_key(c)
                if cid not in seen_ids:
                    seen_ids.add(cid)
                    all_chunks.append(c)

            decision = await _agent_decide(query, current_query, all_chunks, iteration, self.max_iterations)

            if decision.action == "DONE":
                logger.info("AgentRetriever decided DONE at iteration %d", iteration + 1)
                break

            current_query = decision.refined_query
            logger.info("AgentRetriever refining → %s", current_query)

        # Final ranking: score by how recently seen + original score
        for rank, chunk in enumerate(reversed(all_chunks)):
            chunk["score"] = chunk.get("score", 0.0) + (rank + 1) * 0.001

        all_chunks.sort(key=lambda c: c.get("score", 0.0), reverse=True)
        return all_chunks[:top_k]


class _AgentDecision:
    __slots__ = ("action", "refined_query")

    def __init__(self, action: str, refined_query: str = ""):
        self.action = action
        self.refined_query = refined_query


async def _agent_decide(
    original_query: str,
    current_query: str,
    chunks_so_far: list[dict],
    iteration: int,
    max_iterations: int,
) -> _AgentDecision:
    """Ask the LLM whether the retrieved chunks sufficiently answer the query."""
    from app.config import settings

    snippets = "\n---\n".join(
        c.get("text", "")[:300] for c in chunks_so_far[:10]
    )

    prompt = (
        "You are a search-quality evaluator for a RAG system.\n\n"
        f"ORIGINAL USER QUESTION: {original_query}\n"
        f"CURRENT SEARCH QUERY (iteration {iteration + 1}/{max_iterations}): {current_query}\n\n"
        f"RETRIEVED CHUNKS SO FAR ({len(chunks_so_far)} total):\n{snippets}\n\n"
        "Decide whether the retrieved chunks contain enough information to answer "
        "the original question.\n\n"
        "Reply with EXACTLY one of:\n"
        '  DONE — if the chunks are sufficient\n'
        '  REFINE: <new search query> — if you need to search again with a better query\n\n'
        "Think about what information is missing and craft a targeted query to find it. "
        "Reply with ONLY the decision line, nothing else."
    )

    if settings.llm_provider == "anthropic":
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        resp = await client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
    else:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key)
        resp = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        text = (resp.choices[0].message.content or "").strip()

    if text.upper().startswith("REFINE"):
        new_query = text.split(":", 1)[-1].strip() if ":" in text else text[6:].strip()
        if new_query:
            return _AgentDecision("REFINE", new_query)

    return _AgentDecision("DONE")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
RETRIEVER_REGISTRY: dict[str, type[BaseRetriever]] = {
    "vector": VectorRetriever,
    "keyword": KeywordRetriever,
    "hybrid": HybridRetriever,
    "multi_query": MultiQueryRetriever,
    "agent": AgentRetriever,
}


def get_retriever(name: str = "vector", **kwargs) -> BaseRetriever:
    """Instantiate a retriever by name.

    Extra kwargs are forwarded to the constructor.  For retrievers that accept
    a ``sub_retriever``, you can pass ``sub_retriever="hybrid"`` (a string) and
    it will be resolved automatically.
    """
    # Resolve sub_retriever string → instance
    sub = kwargs.get("sub_retriever")
    if isinstance(sub, str):
        kwargs["sub_retriever"] = get_retriever(sub)

    cls = RETRIEVER_REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"Unknown retriever '{name}'. Choose from: {list(RETRIEVER_REGISTRY)}"
        )
    return cls(**kwargs)
