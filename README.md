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
  -d '{"job":"analyze-csv","csv":"name,sales\\nAsha,1200\\nRavi,900\\nMina,"}'
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

### Included real task: CSV analysis

Every job sends its CSV content in the `csv` field. The worker now supports:

- `analyze-csv` — returns the row count, column names, missing-value counts,
  and summaries (`min`, `max`, `sum`, and `average`) for numeric columns.
- `clean-csv` — trims unwanted whitespace from cells and removes completely
  blank rows, then returns a cleaned CSV string.
- `generate-report` — produces the same analysis plus a ready-to-share
  Markdown report for a dashboard or email.

For example, a shop can asynchronously clean and analyse a daily sales export
without making the API wait for the report calculation to finish.

Example completed result:

```json
{
  "job": "analyze-csv",
  "rows_processed": 3,
  "columns": ["name", "sales"],
  "missing_values": {"name": 0, "sales": 1},
  "numeric_summary": {
    "sales": {"count": 2, "min": 900.0, "max": 1200.0, "sum": 2100.0, "average": 1050.0}
  }
}
```

### Upload a real CSV and download a report

`POST /tasks/upload` accepts a real `.csv` file using multipart form data.
The API stores it in the shared TaskFlow data volume and a worker reads it.
Every supported job writes a downloadable file:

- `generate-report` creates `taskflow-report.md`.
- `clean-csv` creates `cleaned-data.csv`.
- `analyze-csv` creates `csv-analysis.json`.

PowerShell example:

```powershell
$task = Invoke-RestMethod -Uri "http://localhost:8080/tasks/upload" `
  -Method Post `
  -Form @{ job = "generate-report"; file = Get-Item ".\sales.csv" }
$task
```

After the task reaches `completed`, download its result at:

```text
http://localhost:8080/tasks/<task-id>/download
```

The same upload endpoint also supports `analyze-csv` and `clean-csv`. Uploaded
files and downloadable reports are stored in the Docker `taskflow-data` volume.

### Automatic retries

Every new task is attempted up to three times by default. If processing fails,
the worker records `last_error`, puts the task back in the queue, and retries it
after a short delay. Only after the final attempt does the task become `failed`.
Set `TASKFLOW_MAX_ATTEMPTS` to change the maximum number of attempts.

### Monitoring dashboard

Open `http://localhost:8080/dashboard` while TaskFlow is running to see queue
depth, task counts by state, retry attempts, errors, and recent tasks. The same
information is available as JSON from `GET /metrics` for future dashboards or
observability tools.

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
`worker/worker.py` includes CSV analysis, cleaning, and report generation as
example real workloads; add additional job types there for your chosen use
case. The
Redis deployment in `k8s/redis.yaml` is a single, non-persistent instance
intended for demo/dev use; a production setup would use a managed Redis
(e.g. ElastiCache) with persistence and failover.
