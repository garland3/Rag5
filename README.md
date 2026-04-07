# Rag5

RAG (Retrieval-Augmented Generation) system using MongoDB Atlas Vector Search, exposed via a FastAPI REST API.

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Copy and fill in environment variables
cp .env.example .env

# Run the API server
uvicorn app.main:app --reload

# Run tests
pytest tests/ -v
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/documents` | Upload a document |
| GET | `/api/v1/documents` | List documents |
| GET | `/api/v1/documents/{id}` | Get document details |
| DELETE | `/api/v1/documents/{id}` | Delete a document |
| POST | `/api/v1/query` | Ask a question (RAG) |

## Docker

```bash
docker compose up --build
```

See [PLAN.md](PLAN.md) for full architecture and design details.
