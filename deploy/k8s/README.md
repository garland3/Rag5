# Kubernetes manifests for Rag5 + Prefect worker

These manifests run the Rag5 API **and** a long-lived Prefect worker
in the same namespace. The API submits ingest jobs to a Prefect
deployment; the worker polls the corresponding work pool and
executes them.

See [`docs/prefect.md`](../../docs/prefect.md) for the full walkthrough
— this README is just the "which file does what" index.

## Files

| File | Purpose |
|------|---------|
| `namespace.yaml` | Creates the `rag5` namespace. |
| `prefect-worker-secret.example.yaml` | Template for `PREFECT_API_URL`, `PREFECT_API_KEY`, MongoDB and OpenAI credentials. Copy, fill in, apply. |
| `upload-staging-pvc.yaml` | `ReadWriteMany` PVC shared by the API and worker pods so uploaded files survive the hand-off. |
| `rag5-api-deployment.yaml` | Reference API Deployment + Service. Sets `PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/k8s` so uploads are dispatched to the worker. |
| `prefect-worker-deployment.yaml` | The worker Deployment itself — runs `prefect worker start --pool rag5-pool`. |

## Order of operations

```bash
# 0. Build an image that contains the app/ source (the default
#    Dockerfile at the repo root already does this) and push it.
docker build -t ghcr.io/your-org/rag5:latest .
docker push ghcr.io/your-org/rag5:latest

# 1. Create the namespace
kubectl apply -f deploy/k8s/namespace.yaml

# 2. Populate secrets (DO NOT commit the filled-in copy)
cp deploy/k8s/prefect-worker-secret.example.yaml /tmp/secret.yaml
$EDITOR /tmp/secret.yaml
kubectl apply -f /tmp/secret.yaml

# 3. Shared staging volume
kubectl apply -f deploy/k8s/upload-staging-pvc.yaml

# 4. Register the flow as a Prefect deployment, one-off
#    (run this from a machine with PREFECT_API_URL set and network
#    reachability to the Prefect server)
prefect work-pool create --type process rag5-pool   # first time only
python scripts/deploy_prefect_flow.py --name k8s --work-pool rag5-pool

# 5. Start the worker and API pods
kubectl apply -f deploy/k8s/prefect-worker-deployment.yaml
kubectl apply -f deploy/k8s/rag5-api-deployment.yaml
```

## Verifying it works

```bash
# Watch the worker pick up a run
kubectl logs -n rag5 deploy/prefect-worker -f

# From the API pod (or a port-forward), submit a test upload
curl -X POST \
  -F file=@sample.pdf \
  "http://<api-host>/api/v1/documents/upload?corpus_id=<id>"

# You should see a flow run show up in the worker logs within a few
# seconds and the job transition queued -> running -> completed in
# `GET /api/v1/jobs/<id>`.
```

## Scaling

```bash
# More parallel workers = more concurrent ingests
kubectl scale -n rag5 deploy/prefect-worker --replicas=5
```

Because Prefect's queue is authoritative, additional replicas just
start pulling more work from `rag5-pool`. No coordination config is
needed.
