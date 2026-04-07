from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.generator import _build_user_prompt, generate_answer


def test_build_user_prompt():
    chunks = [
        {"text": "chunk one", "metadata": {"source": "file.pdf"}},
        {"text": "chunk two", "metadata": {"source": "file2.pdf"}},
    ]
    prompt = _build_user_prompt("What is X?", chunks)
    assert "chunk one" in prompt
    assert "chunk two" in prompt
    assert "What is X?" in prompt
    assert "[Source 1: file.pdf]" in prompt
    assert "[Source 2: file2.pdf]" in prompt


@pytest.mark.asyncio
async def test_generate_answer_openai():
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="The answer"))]

    with patch("app.core.generator.settings") as mock_settings:
        mock_settings.llm_provider = "openai"
        mock_settings.openai_api_key = "fake"
        with patch("app.core.generator._openai_client") as mock_client:
            mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
            answer = await generate_answer("question", [{"text": "context", "metadata": {}}])

    assert answer == "The answer"
