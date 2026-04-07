# RAG System with MongoDB — Implementation Plan

## Overview

A Retrieval-Augmented Generation (RAG) system that uses **MongoDB Atlas Vector Search** for document storage and retrieval, exposed via a **FastAPI** REST API. Users upload documents, which get chunked and embedded, then query via a chat endpoint that retrieves relevant context and generates answers using an LLM.

---

## Architecture

```
┌─────────────┐       ┌──────────────┐       ┌──────────────────┐
│  Client /    │──────▶│  FastAPI API  │──────▶│  MongoDB Atlas   │
│  Frontend    │◀──────│  (Python)    │◀──────│  (Vector Search) │
└─────────────┘       └──────┬───────┘       └──────────────────┘
                             │
                      ┌──────┴───────┐
                      │  LLM Provider │
                      │  (OpenAI /    │
                      │   Anthropic)  │
                      └──────────────┘
```

---

## Tech Stack

| Component         | Choice                          | Rationale                                    |
|-------------------|---------------------------------|----------------------------------------------|
| Language          | Python 3.11+                    | Rich ML/NLP ecosystem                        |
| Web Framework     | FastAPI                         | Async, auto-docs, type-safe                  |
| Database          | MongoDB Atlas                   | Native vector search, flexible schema        |
| Embeddings        | OpenAI `text-embedding-3-small` | Good quality/cost ratio, 1536 dims           |
| LLM               | OpenAI `gpt-4o` or Anthropic Claude | Generation step                         |
| ODM               | Motor (async MongoDB driver)    | Native async support for FastAPI             |
| Document Parsing  | PyPDF2, python-docx, Unstructured | Handle common file formats                |
| Chunking          | LangChain text splitters        | Configurable, recursive character splitting  |
| Containerization  | Docker + docker-compose         | Easy local dev and deployment                |

---

## Project Structure

```
rag5/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app entry point
│   ├── config.py               # Settings via pydantic-settings
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes/
│   │   │   ├── __init__.py
│   │   │   ├── documents.py    # Upload, list, delete documents
│   │   │   ├── query.py        # RAG query endpoint
│   │   │   └── health.py       # Health check
│   │   └── deps.py             # Shared dependencies
│   ├── core/
│   │   ├── __init__.py
│   │   ├── database.py         # MongoDB connection manager
│   │   ├── embeddings.py       # Embedding generation
│   │   ├── chunking.py         # Document chunking logic
│   │   ├── retriever.py        # Vector search queries
│   │   └── generator.py        # LLM response generation
│   ├── models/
│   │   ├── __init__.py
│   │   ├── document.py         # Document schemas (Pydantic)
│   │   └── query.py            # Query/response schemas
│   └── services/
│       ├── __init__.py
│       ├── ingest.py           # Document ingestion pipeline
│       └── rag.py              # RAG orchestration (retrieve + generate)
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_api/
│   │   ├── test_documents.py
│   │   └── test_query.py
│   └── test_core/
│       ├── test_chunking.py
│       ├── test_retriever.py
│       └── test_generator.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

---

## MongoDB Schema

### Collection: `documents`

Stores document metadata.

```json
{
  "_id": "ObjectId",
  "filename": "report.pdf",
  "content_type": "application/pdf",
  "uploaded_at": "2026-04-07T00:00:00Z",
  "chunk_count": 42,
  "metadata": {}
}
```

### Collection: `chunks`

Stores embedded document chunks with vector index.

```json
{
  "_id": "ObjectId",
  "document_id": "ObjectId",
  "text": "The chunk text content...",
  "embedding": [0.012, -0.034, ...],  // 1536-dim float array
  "chunk_index": 0,
  "metadata": {
    "source": "report.pdf",
    "page": 3
  }
}
```

### Vector Search Index (Atlas)

```json
{
  "name": "chunk_vector_index",
  "type": "vectorSearch",
  "definition": {
    "fields": [
      {
        "path": "embedding",
        "type": "vector",
        "numDimensions": 1536,
        "similarity": "cosine"
      }
    ]
  }
}
```

---

## API Endpoints

| Method | Path                  | Description                        |
|--------|-----------------------|------------------------------------|
| GET    | `/health`             | Health check + DB connectivity     |
| POST   | `/api/v1/documents`   | Upload a document (file upload)    |
| GET    | `/api/v1/documents`   | List all documents                 |
| GET    | `/api/v1/documents/{id}` | Get document details            |
| DELETE | `/api/v1/documents/{id}` | Delete document + its chunks    |
| POST   | `/api/v1/query`       | Ask a question (RAG query)         |

### Query Request/Response

**Request:**
```json
{
  "question": "What are the key findings?",
  "top_k": 5,
  "document_ids": ["optional-filter-to-specific-docs"]
}
```

**Response:**
```json
{
  "answer": "The key findings include...",
  "sources": [
    {
      "document_id": "...",
      "filename": "report.pdf",
      "chunk_text": "...",
      "score": 0.92
    }
  ]
}
```

---

## RAG Pipeline Flow

1. **Ingest** (on document upload):
   - Parse uploaded file (PDF, DOCX, TXT, MD)
   - Split into chunks (1000 chars, 200 char overlap)
   - Generate embeddings for each chunk via OpenAI API
   - Store document metadata in `documents` collection
   - Store chunks + embeddings in `chunks` collection

2. **Query** (on user question):
   - Embed the user's question
   - Run MongoDB Atlas `$vectorSearch` aggregation against `chunks`
   - Retrieve top-k most similar chunks
   - Build prompt with retrieved context + user question
   - Send to LLM for answer generation
   - Return answer with source references

---

## Implementation Steps

### Phase 1: Project Setup
1. Initialize project with `pyproject.toml` and dependencies
2. Create `.gitignore`, `.env.example`, `Dockerfile`, `docker-compose.yml`
3. Set up FastAPI app skeleton with health endpoint
4. Configure MongoDB connection with Motor (async driver)
5. Add pydantic-settings for configuration management

### Phase 2: Core RAG Components
6. Implement document chunking (`core/chunking.py`)
7. Implement embedding generation (`core/embeddings.py`)
8. Implement vector search retriever (`core/retriever.py`)
9. Implement LLM response generator (`core/generator.py`)

### Phase 3: API & Services
10. Build document ingestion service (`services/ingest.py`)
11. Build RAG orchestration service (`services/rag.py`)
12. Implement document upload/list/delete routes
13. Implement query route

### Phase 4: Testing & Polish
14. Write unit tests for chunking, retriever, generator
15. Write API integration tests
16. Add error handling, logging, and input validation
17. Update README with setup and usage instructions

---

## Key Dependencies

```
fastapi>=0.115
uvicorn[standard]>=0.30
motor>=3.5                   # async MongoDB driver
pymongo>=4.8
pydantic-settings>=2.4
openai>=1.40                 # embeddings + optional LLM
anthropic>=0.34              # optional LLM provider
python-multipart>=0.0.9      # file uploads
pypdf2>=3.0                  # PDF parsing
python-docx>=1.1             # DOCX parsing
langchain-text-splitters>=0.2 # chunking
httpx>=0.27                  # async HTTP (testing)
pytest>=8.0
pytest-asyncio>=0.23
```

---

## Configuration (`.env`)

```
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/rag5
MONGODB_DB_NAME=rag5
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...   # optional
LLM_PROVIDER=openai            # or "anthropic"
EMBEDDING_MODEL=text-embedding-3-small
CHUNK_SIZE=1000
CHUNK_OVERLAP=200
TOP_K=5
```

---

## Notes

- **MongoDB Atlas Vector Search** is used instead of a separate vector DB — simplifies infrastructure by keeping documents and vectors in one place.
- **Only API access** — no frontend, no CLI. All interaction is via REST endpoints.
- **Async throughout** — Motor + FastAPI for non-blocking I/O.
- The vector search index must be created in Atlas (via UI or API) before queries work. The app will include a setup script or startup check for this.
