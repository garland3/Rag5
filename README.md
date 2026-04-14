# Rag5

RAG (Retrieval-Augmented Generation) system using MongoDB Atlas Vector Search, exposed via a FastAPI REST API with a Svelte frontend.

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

## Frontend

The frontend is a SvelteKit app in `frontend/`. It connects to the API through a reverse proxy (or Vite dev proxy during development).

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/api` and `/health` to `http://localhost:8000`, so run the API server first.

To build for production:

```bash
cd frontend
npm run build   # outputs to frontend/build/
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/corpora` | Create a corpus (tenant boundary) |
| GET | `/api/v1/corpora` | List corpora you can read |
| POST | `/api/v1/documents?corpus_id=<id>` | Upload + synchronously ingest a document (legacy) |
| POST | `/api/v1/documents/upload?corpus_id=<id>` | **Queue** a document for async ingest via Prefect — returns 202 + job id |
| GET | `/api/v1/documents` | List documents in accessible corpora |
| GET | `/api/v1/documents/{id}` | Get document details |
| DELETE | `/api/v1/documents/{id}` | Delete a document |
| GET | `/api/v1/jobs` | List ingest jobs across accessible corpora (filter with `corpus_id`, `status`) |
| GET | `/api/v1/jobs/{id}` | Get status of a single ingest job |
| POST | `/api/v1/query` | Ask a question (RAG), `corpus_id` required for tenant isolation |
| POST | `/api/v1/query/stream` | **Streaming** RAG query via SSE with real-time status updates |

## Document Upload & Ingestion Orchestration (Prefect)

The multi-tenant upload path uses **Prefect** as the orchestration engine so
uploads don't block while parsing, chunking, and embedding are happening.

> **Full Prefect guide:** [`docs/prefect.md`](docs/prefect.md) — covers
> the in-process vs. remote-worker modes, registering a deployment, and
> running a Prefect worker in Kubernetes (with manifests under
> [`deploy/k8s/`](deploy/k8s/)).

### Request lifecycle

1. `POST /api/v1/corpora` — a tenant creates a corpus they own.
2. `POST /api/v1/documents/upload?corpus_id=<id>` — users upload files as
   `multipart/form-data`. The API:
   - checks write access on the corpus
   - writes the raw bytes to `UPLOAD_STORAGE_DIR/<stage-id>/<sanitized-filename>`
     (the on-disk name is sanitized, so it may not exactly match the original
     uploaded filename)
   - inserts a record into the `ingest_jobs` Mongo collection with
     `status=queued`
   - dispatches the `ingest-document-flow` Prefect flow (see the two modes
     below)
   - returns `202 Accepted` with the job id
3. The Prefect flow executes:
   - transitions the job to `running`
   - parses/chunks/embeds the staged file via the existing ingest pipeline
   - writes `documents` + `chunks` to Mongo
   - marks the job `completed` (or `failed` with an error message)
4. Clients poll `GET /api/v1/jobs/{id}` for status until it reaches
   `completed` or `failed`. The RAG query endpoint becomes available for
   the new content as soon as the job completes.

### Two execution modes

| Mode | When to use | How to enable |
|------|-------------|---------------|
| **In-process** (default) | Local dev, single-node deploys | Do nothing. The flow runs in the FastAPI event loop via `asyncio.create_task`. No Prefect server or worker required. |
| **Remote worker** | Production, Kubernetes, scale-out | Point `PREFECT_API_URL` at a Prefect server, register the flow with `scripts/deploy_prefect_flow.py`, and set `PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/<name>`. A Prefect worker polling the same work pool then executes each flow run. |

Even in in-process mode, setting `PREFECT_API_URL` at a
[Prefect server](https://docs.prefect.io) gives you the web UI, live
run logs, and alerting for free — no code changes. The remote mode
additionally moves the CPU work off the API replicas and lets you
scale ingest capacity independently. See
[`docs/prefect.md`](docs/prefect.md) for step-by-step setup.

### Quick start — in-process (default)

```bash
# Nothing to configure. Just run the API.
uvicorn app.main:app --reload
```

### Quick start — remote worker (Kubernetes)

```bash
# 1. Start a Prefect server (or use Prefect Cloud)
prefect server start

# 2. Point the API at it
export PREFECT_API_URL=http://127.0.0.1:4200/api

# 3. Create the work pool and register the deployment
prefect work-pool create --type process rag5-pool
python scripts/deploy_prefect_flow.py --name k8s --work-pool rag5-pool

# 4. Tell the API to dispatch remotely
export PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/k8s

# 5. Start a worker (locally, or see deploy/k8s/ for the K8s manifests)
prefect worker start --pool rag5-pool
```

For the full Kubernetes walkthrough including Deployment manifests,
PVC setup, and troubleshooting, see [`docs/prefect.md`](docs/prefect.md)
and [`deploy/k8s/README.md`](deploy/k8s/README.md).

### Streaming Search (`/api/v1/query/stream`)

Returns Server-Sent Events (SSE) that report progress as the search pipeline runs:

```
data: {"type": "status", "message": "Checking access permissions..."}
data: {"type": "status", "message": "Searching with vector retriever..."}
data: {"type": "status", "message": "Finding relevant documents for: \"your question\""}
data: {"type": "status", "message": "Found 5 relevant chunks. Resolving sources..."}
data: {"type": "sources", "message": "Retrieved sources from 2 documents", "data": [...]}
data: {"type": "status", "message": "Generating answer from retrieved context..."}
data: {"type": "answer", "data": {"answer": "...", "sources": [...]}}
data: {"type": "done", "message": "Search complete"}
```

Event types: `status`, `sources`, `answer`, `error`, `done`.

## Authentication

The API is designed to sit behind a reverse proxy that handles authentication.

- **Production**: The proxy injects a header (default `x-forwarded-user`) with the authenticated username. Configure the header name via `PROXY_USER_HEADER`.
- **Development**: Set `DEBUG=true` in `.env` to bypass auth checks and use `TESTUSER` (defaults to `bob@test.com`).

Without DEBUG mode, the API also accepts `x-user-id` and `x-user-groups` headers as a fallback.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DB_NAME` | `rag5` | Database name |
| `OPENAI_API_KEY` | | OpenAI API key |
| `ANTHROPIC_API_KEY` | | Anthropic API key |
| `LLM_PROVIDER` | `openai` | `openai` or `anthropic` |
| `DEBUG` | `false` | Bypass auth, use TESTUSER |
| `TESTUSER` | `bob@test.com` | User identity when DEBUG=true |
| `PROXY_USER_HEADER` | `x-forwarded-user` | Header from reverse proxy with username |
| `UPLOAD_STORAGE_DIR` | `/tmp/rag5_uploads` | Local staging directory for files awaiting ingest |
| `PREFECT_API_URL` | _(unset)_ | Optional Prefect server URL — enables the flow-run UI |
| `PREFECT_INGEST_DEPLOYMENT` | _(unset)_ | Optional `flow/deployment` name. When set, the API dispatches ingest jobs to a Prefect deployment instead of running the flow in-process — required for the Kubernetes worker setup. See [`docs/prefect.md`](docs/prefect.md). |

## Docker

```bash
docker compose up --build
```

See [PLAN.md](PLAN.md) for full architecture and design details.
