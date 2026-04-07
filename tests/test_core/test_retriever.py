from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.retriever import (
    AgentRetriever,
    BaseRetriever,
    HybridRetriever,
    KeywordRetriever,
    MultiQueryRetriever,
    VectorRetriever,
    _reciprocal_rank_fusion,
    get_retriever,
    vector_search,
)


class MockAsyncCursor:
    def __init__(self, docs):
        self._docs = iter(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._docs)
        except StopIteration:
            raise StopAsyncIteration


# ---------------------------------------------------------------------------
# vector_search (original function, still works standalone)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_vector_search_builds_pipeline():
    mock_collection = MagicMock()
    mock_collection.aggregate = MagicMock(return_value=MockAsyncCursor([]))

    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("app.core.retriever.embed_query", return_value=[0.1] * 1536):
        results = await vector_search(mock_db, "test query", top_k=3)

    assert results == []
    mock_collection.aggregate.assert_called_once()
    pipeline = mock_collection.aggregate.call_args[0][0]
    assert pipeline[0]["$vectorSearch"]["limit"] == 3


# ---------------------------------------------------------------------------
# Factory / registry
# ---------------------------------------------------------------------------

def test_get_retriever_returns_correct_types():
    assert isinstance(get_retriever("vector"), VectorRetriever)
    assert isinstance(get_retriever("keyword"), KeywordRetriever)
    assert isinstance(get_retriever("hybrid"), HybridRetriever)
    assert isinstance(get_retriever("multi_query"), MultiQueryRetriever)
    assert isinstance(get_retriever("agent"), AgentRetriever)


def test_get_retriever_unknown_raises():
    with pytest.raises(ValueError, match="Unknown retriever"):
        get_retriever("nonexistent")


def test_get_retriever_resolves_sub_retriever_string():
    r = get_retriever("multi_query", sub_retriever="hybrid")
    assert isinstance(r, MultiQueryRetriever)
    assert isinstance(r.sub_retriever, HybridRetriever)


# ---------------------------------------------------------------------------
# Reciprocal Rank Fusion
# ---------------------------------------------------------------------------

def test_rrf_deduplicates_and_ranks():
    list_a = [
        {"_id": "1", "text": "a", "score": 0.9},
        {"_id": "2", "text": "b", "score": 0.7},
    ]
    list_b = [
        {"_id": "2", "text": "b", "score": 0.95},
        {"_id": "3", "text": "c", "score": 0.5},
    ]

    fused = _reciprocal_rank_fusion([list_a, list_b], top_k=3, k=60)
    ids = [c["_id"] for c in fused]

    # "2" appears in both lists → highest RRF score
    assert ids[0] == "2"
    assert len(fused) == 3


# ---------------------------------------------------------------------------
# VectorRetriever (class wrapper)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_vector_retriever_delegates_to_vector_search():
    r = VectorRetriever()
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_collection.aggregate = MagicMock(return_value=MockAsyncCursor([]))
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    with patch("app.core.retriever.embed_query", return_value=[0.1] * 1536):
        result = await r.retrieve(mock_db, "test", top_k=2)

    assert result == []


# ---------------------------------------------------------------------------
# HybridRetriever
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hybrid_retriever_merges_results():
    vec_chunks = [{"_id": "1", "text": "vec", "document_id": "d1", "score": 0.9}]
    kw_chunks = [{"_id": "2", "text": "kw", "document_id": "d1", "score": 0.8}]

    with (
        patch("app.core.retriever.vector_search", return_value=vec_chunks),
        patch("app.core.retriever.keyword_search", return_value=kw_chunks),
    ):
        r = HybridRetriever()
        results = await r.retrieve(MagicMock(), "test", top_k=5)

    assert len(results) == 2


# ---------------------------------------------------------------------------
# MultiQueryRetriever
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multi_query_retriever_calls_sub_retriever_per_rewrite():
    mock_sub = MagicMock(spec=BaseRetriever)
    mock_sub.retrieve = AsyncMock(return_value=[
        {"_id": "1", "text": "chunk", "document_id": "d1", "score": 0.9}
    ])

    with patch(
        "app.core.retriever._generate_query_rewrites",
        return_value=["rewrite1", "rewrite2"],
    ):
        r = MultiQueryRetriever(sub_retriever=mock_sub, num_rewrites=2)
        results = await r.retrieve(MagicMock(), "original query", top_k=5)

    # original + 2 rewrites = 3 calls
    assert mock_sub.retrieve.call_count == 3
    assert len(results) >= 1


# ---------------------------------------------------------------------------
# AgentRetriever
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_retriever_stops_on_done():
    mock_sub = MagicMock(spec=BaseRetriever)
    mock_sub.retrieve = AsyncMock(return_value=[
        {"_id": "1", "text": "good chunk", "document_id": "d1", "score": 0.95}
    ])

    with patch(
        "app.core.retriever._agent_decide",
        return_value=MagicMock(action="DONE", refined_query=""),
    ):
        r = AgentRetriever(sub_retriever=mock_sub, max_iterations=3)
        results = await r.retrieve(MagicMock(), "my question", top_k=5)

    # Only 1 iteration since agent said DONE immediately
    assert mock_sub.retrieve.call_count == 1
    assert len(results) == 1


@pytest.mark.asyncio
async def test_agent_retriever_refines_query():
    mock_sub = MagicMock(spec=BaseRetriever)
    call_count = [0]

    async def fake_retrieve(db, query, top_k=5, document_ids=None):
        call_count[0] += 1
        return [{"_id": str(call_count[0]), "text": f"chunk {call_count[0]}", "document_id": "d1", "score": 0.5}]

    mock_sub.retrieve = fake_retrieve

    decisions = iter([
        MagicMock(action="REFINE", refined_query="better query"),
        MagicMock(action="DONE", refined_query=""),
    ])

    with patch("app.core.retriever._agent_decide", side_effect=lambda *a, **kw: next(decisions)):
        r = AgentRetriever(sub_retriever=mock_sub, max_iterations=3)
        results = await r.retrieve(MagicMock(), "initial", top_k=5)

    assert call_count[0] == 2  # initial + 1 refinement
    assert len(results) == 2
