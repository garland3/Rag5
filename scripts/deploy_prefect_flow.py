"""Register the ingest flow as a Prefect deployment.

This script is the one-shot registration step you run after a Prefect
server is available and a work pool has been created. After it
succeeds, the API (with ``PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/<name>``
set) dispatches ingest jobs to the deployment, and any Prefect worker
polling the same work pool — for example a worker running as a
Kubernetes Deployment — picks the runs up and executes them.

Example usage::

    # 1. Point at your Prefect server
    export PREFECT_API_URL=http://prefect-server.prefect.svc.cluster.local:4200/api

    # 2. (First time only) create the work pool the worker will poll
    prefect work-pool create --type process rag5-pool

    # 3. Register the deployment
    python scripts/deploy_prefect_flow.py \
        --name k8s \
        --work-pool rag5-pool

    # 4. Tell the API to use it
    export PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/k8s

The script is idempotent: re-running it updates the existing deployment
in place, so it is safe to call from CI on every release.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from app.services.prefect_flows import ingest_document_flow


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--name",
        default="k8s",
        help="Deployment name (suffix after the flow name). Default: k8s",
    )
    parser.add_argument(
        "--work-pool",
        required=True,
        help="Name of the Prefect work pool the worker polls.",
    )
    parser.add_argument(
        "--work-queue",
        default=None,
        help="Optional work queue inside the work pool.",
    )
    parser.add_argument(
        "--image",
        default=None,
        help=(
            "Optional Docker image override. Only needed for work pools "
            "that build per-run containers (e.g. kubernetes-type pools)."
        ),
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Deployment tag (repeatable).",
    )
    return parser.parse_args()


async def _deploy(args: argparse.Namespace) -> str:
    deploy_kwargs: dict = {
        "name": args.name,
        "work_pool_name": args.work_pool,
        "tags": args.tag or ["rag5", "ingest"],
    }
    if args.work_queue:
        deploy_kwargs["work_queue_name"] = args.work_queue
    if args.image:
        deploy_kwargs["image"] = args.image

    # Prefect 3's ``flow.deploy`` is synchronous in a script context but
    # returns a coroutine in an async context — wrap it defensively.
    result = ingest_document_flow.deploy(**deploy_kwargs)
    if asyncio.iscoroutine(result):
        result = await result
    return str(result)


def main() -> int:
    args = _parse_args()
    try:
        deployment_id = asyncio.run(_deploy(args))
    except Exception as exc:  # noqa: BLE001 - CLI surface
        print(f"error: failed to create deployment: {exc}", file=sys.stderr)
        return 1
    print(
        f"Registered deployment ingest-document-flow/{args.name} "
        f"(id={deployment_id}) on work pool {args.work_pool!r}."
    )
    print("Set PREFECT_INGEST_DEPLOYMENT=ingest-document-flow/"
          f"{args.name} on the API to start using it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
