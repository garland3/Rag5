from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.retriever import vector_search


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
