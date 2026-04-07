import anthropic
from openai import AsyncOpenAI

from app.config import settings

_openai_client: AsyncOpenAI | None = None
_anthropic_client: anthropic.AsyncAnthropic | None = None

SYSTEM_PROMPT = (
    "You are a helpful assistant that answers questions based on the provided context. "
    "Use only the context below to answer. If the context doesn't contain enough "
    "information to answer, say so. Cite which source chunks you used."
)


def _build_user_prompt(question: str, context_chunks: list[dict]) -> str:
    context_parts = []
    for i, chunk in enumerate(context_chunks, 1):
        source = chunk.get("metadata", {}).get("source", "unknown")
        context_parts.append(f"[Source {i}: {source}]\n{chunk['text']}")

    context_block = "\n\n---\n\n".join(context_parts)
    return f"Context:\n{context_block}\n\nQuestion: {question}"


async def generate_answer(question: str, context_chunks: list[dict]) -> str:
    user_prompt = _build_user_prompt(question, context_chunks)

    if settings.llm_provider == "anthropic":
        return await _generate_anthropic(user_prompt)
    return await _generate_openai(user_prompt)


async def _generate_openai(user_prompt: str) -> str:
    global _openai_client
    if _openai_client is None:
        _openai_client = AsyncOpenAI(api_key=settings.openai_api_key)

    response = await _openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )
    return response.choices[0].message.content or ""


async def _generate_anthropic(user_prompt: str) -> str:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    response = await _anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return response.content[0].text
