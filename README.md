# TaskFlow – Distributed Task Processing System

A distributed task processing system with a scalable API and worker
architecture, using Redis-based job queues for asynchronous execution.
Containerized with Docker, orchestrated with Kubernetes, and provisioned on
AWS (S3, IAM) via Terraform for reproducible deployments.

## Architecture

```
                 ┌─────────────┐        ┌───────────────┐
   HTTP POST     │             │  RPUSH │               │
  ─────────────▶ │  API (Flask)│───────▶│  Redis Queue  │
                  │             │        │               │
                 └─────────────┘        └───────┬───────┘
                        ▲                        │ BLPOP
                        │ GET /tasks/<id>         ▼
                        │                 ┌───────────────┐
                        └─────────────────│ Worker(s)     │
                                          │ (autoscaled)  │
                                          └───────┬───────┘
                                                  │ PutObject
                                                  ▼
                                          ┌───────────────┐
                                          │  S3 (results) │
                                          └───────────────┘
```

- **API service** (`api/`) — Flask app that accepts task submissions over
  HTTP, enqueues them onto a Redis list, and exposes a status endpoint that
  clients can poll for results.
- **Worker service** (`worker/`) — a pool of stateless consumers that block
  on the Redis queue (`BLPOP`), process jobs, write status back to Redis, and
  optionally upload results to S3.
- **Redis** — the job queue and status store. Runs as its own deployment in
  the cluster (or can be swapped for ElastiCache in production).
- **Kubernetes** (`k8s/`) — Deployments, Services, and HorizontalPodAutoscalers
  for the API and worker, so both scale independently based on load.
- **Terraform** (`terraform/`) — provisions the S3 results bucket and an IAM
  role (IRSA-style) scoped to just `PutObject`/`GetObject`/`ListBucket` on
  that bucket, so workers never hold broad AWS credentials.

## Running locally

```bash
docker compose up --build
```

This starts Redis, the API on `localhost:8080`, and two worker replicas.

Submit a task:

```bash
curl -X POST localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"job": "resize-image", "url": "https://example.com/photo.png"}'
```

Response:

```json
{
  "task_id": "b3f1e6b2-...",
  "state": "queued",
  "created_at": "2026-08-07T12:00:00+00:00",
  "result": null,
  "error": null
}
```

Poll for the result:

```bash
curl localhost:8080/tasks/b3f1e6b2-...
```

## Deploying to Kubernetes

1. Build and push images to your registry (e.g. ECR):
   ```bash
   docker build -t <registry>/taskflow-api:latest ./api
   docker build -t <registry>/taskflow-worker:latest ./worker
   docker push <registry>/taskflow-api:latest
   docker push <registry>/taskflow-worker:latest
   ```
2. Update the `image:` fields in `k8s/api.yaml` and `k8s/worker.yaml` to
   point at your registry.
3. Apply manifests in order:
   ```bash
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/redis.yaml
   kubectl apply -f k8s/api.yaml
   kubectl apply -f k8s/worker.yaml
   ```

## Provisioning AWS infrastructure

```bash
cd terraform
terraform init
terraform plan \
  -var="oidc_provider_arn=<your-eks-oidc-arn>" \
  -var="oidc_provider_url=https://oidc.eks.<region>.amazonaws.com/id/<id>"
terraform apply
```

This creates:
- An encrypted, versioned S3 bucket for task results with a lifecycle rule
  to expire old objects.
- An IAM role trusted only by the `taskflow-worker` Kubernetes service
  account (via OIDC), with a policy scoped to just that bucket.

## Notes on scope

This is a portfolio/reference implementation. `process_job()` in
`worker/worker.py` is a placeholder — swap in real task logic there. The
Redis deployment in `k8s/redis.yaml` is a single, non-persistent instance
intended for demo/dev use; a production setup would use a managed Redis
(e.g. ElastiCache) with persistence and failover.
