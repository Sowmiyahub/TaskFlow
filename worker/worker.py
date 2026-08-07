"""
TaskFlow worker service
------------------------
Polls the Redis-backed job queue (BLPOP, blocking pop) and processes tasks
one at a time. Processing here is a stand-in for real work: replace
`process_job` with whatever business logic the task actually needs.

Results (and errors) are written back to Redis under the same status key
the API uses, so clients polling GET /tasks/<id> see live updates. If
S3_RESULT_BUCKET is set, the worker also uploads the JSON result to S3.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import redis

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # boto3 is optional if S3 upload isn't used
    boto3 = None
    ClientError = Exception

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s worker[%(process)d] %(levelname)s %(message)s",
)
log = logging.getLogger("taskflow-worker")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("TASKFLOW_QUEUE_KEY", "taskflow:queue")
STATUS_KEY_PREFIX = "taskflow:status:"
BLOCK_TIMEOUT_SECONDS = int(os.environ.get("TASKFLOW_BLOCK_TIMEOUT", 5))

S3_RESULT_BUCKET = os.environ.get("S3_RESULT_BUCKET")  # optional
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
s3_client = boto3.client("s3", region_name=AWS_REGION) if (boto3 and S3_RESULT_BUCKET) else None

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    log.info("received signal %s, shutting down after current job", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def _status_key(task_id: str) -> str:
    return f"{STATUS_KEY_PREFIX}{task_id}"


def _update_status(task_id: str, **fields):
    key = _status_key(task_id)
    raw = r.get(key)
    record = json.loads(raw) if raw else {"task_id": task_id}
    record.update(fields)
    record["updated_at"] = datetime.now(timezone.utc).isoformat()
    r.set(key, json.dumps(record))
    return record


def process_job(job: dict) -> dict:
    """
    Placeholder business logic. Replace with real task processing
    (image resize, report generation, data pipeline step, etc).
    """
    payload = job.get("payload", {})
    time.sleep(1)  # simulate work
    return {"echo": payload, "processed": True}


def upload_result_to_s3(task_id: str, result: dict):
    if not s3_client:
        return None
    key = f"results/{task_id}.json"
    try:
        s3_client.put_object(
            Bucket=S3_RESULT_BUCKET,
            Key=key,
            Body=json.dumps(result).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{S3_RESULT_BUCKET}/{key}"
    except ClientError as exc:
        log.error("S3 upload failed for task %s: %s", task_id, exc)
        return None


def handle_message(raw_message: str):
    job = json.loads(raw_message)
    task_id = job["task_id"]

    log.info("picked up task %s", task_id)
    _update_status(task_id, state="processing")

    try:
        result = process_job(job)
        s3_uri = upload_result_to_s3(task_id, result)
        _update_status(
            task_id,
            state="completed",
            result=result,
            result_location=s3_uri,
            error=None,
        )
        log.info("completed task %s", task_id)
    except Exception as exc:  # noqa: BLE001 - worker must never crash the loop
        log.exception("task %s failed", task_id)
        _update_status(task_id, state="failed", error=str(exc))


def main():
    log.info(
        "worker starting, redis=%s:%s queue=%s s3_bucket=%s",
        REDIS_HOST, REDIS_PORT, QUEUE_KEY, S3_RESULT_BUCKET or "(disabled)",
    )
    while not _shutdown:
        try:
            item = r.blpop(QUEUE_KEY, timeout=BLOCK_TIMEOUT_SECONDS)
        except redis.exceptions.RedisError as exc:
            log.error("redis error, retrying in 2s: %s", exc)
            time.sleep(2)
            continue

        if item is None:
            continue  # timed out waiting, loop and check _shutdown

        _, raw_message = item
        try:
            handle_message(raw_message)
        except Exception:
            log.exception("unhandled error processing message")

    log.info("worker stopped cleanly")
    sys.exit(0)


if __name__ == "__main__":
    main()
