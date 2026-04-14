# Using Prefect with Rag5

This document is the single source of truth for how ingestion is
orchestrated with [Prefect](https://docs.prefect.io). It covers:

1. What Prefect does in this codebase (and what it doesn't)
2. The two execution modes — in-process vs. remote worker
3. Running a Prefect server locally
4. Registering the ingest flow as a deployment
5. Running a Prefect worker in Kubernetes
6. Operational notes (shared storage, scaling, observability)

---

## 1. What Prefect does in Rag5

When a client uploads a document to `POST /api/v1/documents/upload`,
the API does four things:

1. Stages the raw bytes on disk under `UPLOAD_STORAGE_DIR/<stage-id>/...`
2. Inserts an `ingest_jobs` record in MongoDB with `status=queued`
3. **Dispatches the `ingest-document-flow` Prefect flow**
4. Returns `202 Accepted` with the job id

The Prefect flow is the part of the system that actually parses the
file, chunks it, generates embeddings, and writes documents + chunks
to Mongo. It also updates the `ingest_jobs` row so clients polling
`GET /api/v1/jobs/{id}` can see progress.

The flow is defined once in `app/services/prefect_flows.py`. The
business logic lives in plain async functions (`run_ingest_pipeline`)
so it can be unit-tested without booting the Prefect engine; the
`@flow` wrapper just adds Prefect's orchestration features (retries,
logging, state tracking, UI visibility).

### What Prefect is NOT used for

* Scheduled jobs — there are no cron-style flows.
* The sync query path (`POST /api/v1/query`). That endpoint runs
  entirely inside the request handler.
* Long-running background jobs other than ingest.

---

## 2. Execution modes

Rag5 supports two modes. Both run the exact same flow code; the
difference is **where** the flow executes.

### Mode A — In-process (default, zero setup)

* `PREFECT_INGEST_DEPLOYMENT` is **unset**.
* `enqueue_ingest_job` wraps the flow coroutine in
  `asyncio.create_task(...)` and runs it inside the FastAPI event
  loop.
* No Prefect server, no worker, no work pool — nothing external to
  install or operate.
* Setting `PREFECT_API_URL` at a Prefect server in this mode is still
  useful: flow runs show up in the UI with live logs, but the API
  process is still the one doing the work.

**When to use it:** local development, unit tests, single-node
deployments where you don't need to scale the ingest workers
independently from the API.

### Mode B — Remote (Prefect worker in Kubernetes, or anywhere)

* `PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/<deployment-name>`.
* `enqueue_ingest_job` calls `prefect.deployments.run_deployment(...)`
  with `timeout=0`, which creates a flow run record on the Prefect
  server and returns immediately.
* A separate **Prefect worker** — running in its own pod, on its own
  machine, or wherever — polls the work pool, claims the run, and
  executes the flow in its own process.
* The API never spends CPU on PDF parsing or embedding generation.

**When to use it:** production, multi-tenant deployments, workloads
where you want to scale the number of ingest workers up and down
independently from the API replicas, or workloads that need GPU-backed
embedding workers.

The code branch that picks the mode lives in
`app/services/job_queue.py::_default_runner`. It reads
`settings.prefect_ingest_deployment` lazily, so the in-process path
has zero extra cost when the remote mode is off.

---

## 3. Running a Prefect server

You need a Prefect server (or Prefect Cloud) for Mode B — it is where
deployments, work pools, and flow-run records live.

### Option 1 — Local dev server

```bash
pip install -e ".[dev]"            # Prefect is already in pyproject.toml
prefect server start               # default: http://127.0.0.1:4200
```

Point the API at it:

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
uvicorn app.main:app --reload
```

Visit `http://127.0.0.1:4200` in a browser — flow runs appear there
in real time, even in Mode A.

### Option 2 — Prefect Cloud

```bash
prefect cloud login                 # browser-based
prefect cloud workspace set <ws>
```

No `PREFECT_API_URL` needed — `prefect cloud login` writes the URL
and API key into your Prefect profile. The worker Deployment in
Kubernetes consumes those via the `prefect-worker-env` Secret.

### Option 3 — Self-hosted server in the cluster

Install Prefect's official Helm chart into a sibling namespace:

```bash
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm install prefect-server prefect/prefect-server -n prefect --create-namespace
```

Then set `PREFECT_API_URL=http://prefect-server.prefect.svc.cluster.local:4200/api`
in `deploy/k8s/prefect-worker-secret.yaml`.

---

## 4. Registering a deployment

A Prefect **deployment** is a named, stored reference to a specific
flow, bound to a specific work pool. It tells the worker "when you
see a run for `ingest-document-flow/k8s`, execute this code".

Rag5 ships a one-shot script:

```bash
# Create the work pool once (process-type is simplest — the worker
# runs the flow in its own subprocess inside the same pod).
prefect work-pool create --type process rag5-pool

# Register / update the deployment
python scripts/deploy_prefect_flow.py --name k8s --work-pool rag5-pool
```

The script is idempotent — re-running updates the existing deployment
in place, so it's safe to call from CI on every release. See
`scripts/deploy_prefect_flow.py` for the full argument list.

After the deployment exists, tell the API to use it:

```bash
export PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/k8s
```

Uploads from that point forward are dispatched remotely instead of
running in-process.

---

## 5. Running a Prefect worker in Kubernetes

This is the main subject of this guide. All manifests live under
[`deploy/k8s/`](../deploy/k8s). The quick version:

```bash
# 0. Build and push an image that contains the app/ source. The
#    default Dockerfile at the repo root already ships it.
docker build -t ghcr.io/your-org/rag5:latest .
docker push ghcr.io/your-org/rag5:latest

# 1. Namespace
kubectl apply -f deploy/k8s/namespace.yaml

# 2. Secret with PREFECT_API_URL, MongoDB URI, OpenAI key, etc.
cp deploy/k8s/prefect-worker-secret.example.yaml /tmp/secret.yaml
$EDITOR /tmp/secret.yaml
kubectl apply -f /tmp/secret.yaml

# 3. Shared staging PVC (needs ReadWriteMany storage class)
kubectl apply -f deploy/k8s/upload-staging-pvc.yaml

# 4. Worker + API Deployments
kubectl apply -f deploy/k8s/prefect-worker-deployment.yaml
kubectl apply -f deploy/k8s/rag5-api-deployment.yaml
```

### Why the worker uses the same image as the API

The Prefect worker process does not know the shape of your code —
it imports the flow dynamically by name. That means the worker pod
**must** be able to `import app.services.prefect_flows` at runtime.
The simplest way to guarantee that is to reuse the exact same Docker
image you already build for the API and just override the container
command:

```yaml
command: ["prefect", "worker", "start"]
args: ["--pool", "rag5-pool", "--name", "$(POD_NAME)"]
```

That is what `deploy/k8s/prefect-worker-deployment.yaml` does.

### The shared-storage problem (and how we solve it)

The API pod stages the uploaded file at
`UPLOAD_STORAGE_DIR/<stage-id>/<name>` **on the filesystem**. The
worker then re-reads that file inside its own pod. If the two pods
don't share a filesystem, the worker sees `FileNotFoundError`.

Two ways out:

1. **Shared ReadWriteMany PVC** (what `deploy/k8s/upload-staging-pvc.yaml`
   does). Both the API Deployment and the worker Deployment mount
   it at `/var/rag5/uploads` and set `UPLOAD_STORAGE_DIR` to that
   path. Works on EKS+EFS, GKE+Filestore, AKS+AzureFiles, on-prem NFS.
2. **Object storage**. Replace `app/services/storage.py` with an S3/
   GCS client and stop staging on disk entirely. That removes the
   RWX requirement but needs a small code change — deliberately not
   done in this repo to keep the default dev path simple.

If you only ever run one API pod *and* one worker pod *and* co-locate
them on the same node, you can also use a `hostPath` volume, but
that does not survive a reschedule.

### Scaling workers

```bash
kubectl scale -n rag5 deploy/prefect-worker --replicas=5
```

Prefect's work pool is the authoritative queue — new replicas just
start pulling more runs from it. There is no leader election, no
coordination config, no cache to warm. You can auto-scale on CPU
usage with an HPA; a ready-made HPA spec is **not** included because
the right metrics depend heavily on your embedding model's latency.

### Per-run Kubernetes pods (advanced)

A `process`-type work pool runs every flow inside the worker pod's
own Python subprocess, which is good enough for most cases. If you
need strong isolation between runs, you can switch to a
`kubernetes`-type work pool instead:

```bash
prefect work-pool create --type kubernetes rag5-pool
python scripts/deploy_prefect_flow.py \
    --name k8s \
    --work-pool rag5-pool \
    --image ghcr.io/your-org/rag5:latest
```

With this setup the worker pod itself is tiny — its only job is to
call the Kubernetes API and spawn a fresh pod for each flow run.
That gives you per-run resource limits, per-run node selectors,
and per-run image overrides at the cost of pod startup latency on
every ingest.

---

## 6. Operational notes

### Observability

* `GET /api/v1/jobs/{id}` remains the canonical status source for
  clients. The `status` field transitions `queued → running →
  completed | failed` whether the flow ran in-process or remotely.
* In Mode B, the flow run id is also written to
  `ingest_jobs.prefect_flow_run_id`, so operators can jump straight
  from the job record to the corresponding run in the Prefect UI.
* Worker logs are available via `kubectl logs -n rag5
  deploy/prefect-worker -f`. They include the Prefect engine's own
  log output plus anything the flow code logs via the standard
  `logging` module.

### Retries

`ingest_document_task` is decorated with `@task(retries=1,
retry_delay_seconds=2)`, so a transient failure (flaky network to
Mongo, brief OpenAI rate limit) retries once automatically. Bump the
numbers in `app/services/prefect_flows.py` if your workload needs
more.

### Permanent failures

When the flow raises after its retries are exhausted, the job
record is updated with `status="failed"` and the exception message
is stored in `error`. The staged file is removed so we don't leak
partial uploads onto disk.

### Switching back to in-process mode

Just unset the env var and restart the API pods:

```bash
kubectl set env -n rag5 deploy/rag5-api PREFECT_INGEST_DEPLOYMENT-
kubectl rollout restart -n rag5 deploy/rag5-api
```

The worker Deployment can stay up — it will simply stop seeing runs
once the API stops dispatching them. Scale it to zero when you're
done:

```bash
kubectl scale -n rag5 deploy/prefect-worker --replicas=0
```

---

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `RuntimeError: No work pool named 'rag5-pool' found.` | The worker started before you ran `prefect work-pool create`. Create the pool, then restart the worker pod. |
| `ModuleNotFoundError: app.services.prefect_flows` in the worker logs | The worker image does not contain the `app/` source. Rebuild with the repo-root `Dockerfile` and push. |
| Jobs stick at `queued` and never reach `running` | API is configured to dispatch to a deployment but no worker is polling the pool. Check `kubectl get pods -n rag5` and `kubectl logs deploy/prefect-worker`. |
| Worker logs say `FileNotFoundError` on the staged file | The API and worker pods don't share `UPLOAD_STORAGE_DIR`. Apply `upload-staging-pvc.yaml` and make sure both deployments mount it. |
| Flow runs appear in the Prefect UI but never leave `Scheduled` | A worker **is** connected but not to this pool. Double-check `--pool rag5-pool` matches the deployment's work pool. |
| `ValueError: Deployment 'ingest-document-flow/k8s' not found.` | You set `PREFECT_INGEST_DEPLOYMENT` without first running `scripts/deploy_prefect_flow.py`. |
