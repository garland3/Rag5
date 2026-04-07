import io

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from PyPDF2 import PdfReader

from app.core.chunking import chunk_text
from app.core.embeddings import embed_texts


def extract_text(content: bytes, content_type: str, filename: str) -> str:
    if content_type == "application/pdf" or filename.endswith(".pdf"):
        reader = PdfReader(io.BytesIO(content))
        return "\n\n".join(page.extract_text() or "" for page in reader.pages)

    if (
        content_type
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        or filename.endswith(".docx")
    ):
        import docx

        doc = docx.Document(io.BytesIO(content))
        return "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())

    # Default: treat as plain text (txt, md, etc.)
    return content.decode("utf-8", errors="replace")


async def ingest_document(
    db: AsyncIOMotorDatabase,
    filename: str,
    content_type: str,
    content: bytes,
    metadata: dict | None = None,
) -> str:
    text = extract_text(content, content_type, filename)
    chunks = chunk_text(text)

    # Store document metadata
    doc_result = await db["documents"].insert_one(
        {
            "filename": filename,
            "content_type": content_type,
            "chunk_count": len(chunks),
            "metadata": metadata or {},
        }
    )
    doc_id = doc_result.inserted_id

    # Embed and store chunks in batches
    batch_size = 100
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i : i + batch_size]
        embeddings = await embed_texts(batch)

        chunk_docs = [
            {
                "document_id": doc_id,
                "text": batch[j],
                "embedding": embeddings[j],
                "chunk_index": i + j,
                "metadata": {"source": filename},
            }
            for j in range(len(batch))
        ]
        await db["chunks"].insert_many(chunk_docs)

    return str(doc_id)
