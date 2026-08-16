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
from csv import DictReader, DictWriter
from datetime import datetime, timezone
from io import StringIO
from math import isfinite
from pathlib import Path

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
RETRY_DELAY_SECONDS = float(os.environ.get("TASKFLOW_RETRY_DELAY_SECONDS", "1"))

S3_RESULT_BUCKET = os.environ.get("S3_RESULT_BUCKET")  # optional
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DATA_DIR = Path(os.environ.get("TASKFLOW_DATA_DIR", "/data"))
UPLOAD_DIR = DATA_DIR / "uploads"
RESULT_DIR = DATA_DIR / "results"

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


def _read_csv(csv_text: object) -> tuple[list[str], list[dict]]:
    if not isinstance(csv_text, str) or not csv_text.strip():
        raise ValueError("The 'csv' field must be a non-empty CSV string.")

    reader = DictReader(StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("CSV must include a header row.")

    columns = reader.fieldnames
    if not all(column.strip() for column in columns) or len(set(columns)) != len(columns):
        raise ValueError("CSV headers must be non-empty and unique.")
    return columns, list(reader)


def _csv_text_from_payload(payload: dict) -> object:
    """Read inline CSV text, or a CSV uploaded through the API shared volume."""
    if "csv" in payload:
        return payload["csv"]

    input_path = payload.get("input_path")
    if not isinstance(input_path, str):
        return None
    candidate = Path(input_path).resolve()
    upload_root = UPLOAD_DIR.resolve()
    if upload_root not in candidate.parents or candidate.suffix.lower() != ".csv":
        raise ValueError("The uploaded CSV path is invalid.")
    try:
        return candidate.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ValueError("Unable to read the uploaded CSV file.") from exc


def _analyse_csv(columns: list[str], rows: list[dict]) -> dict:
    """Return reusable statistics for a parsed CSV file."""
    missing_values = {column: 0 for column in columns}
    numeric_values = {column: [] for column in columns}
    for row in rows:
        for column in columns:
            value = row.get(column)
            if value is None or not value.strip():
                missing_values[column] += 1
                continue
            try:
                number = float(value)
            except ValueError:
                continue
            if isfinite(number):
                numeric_values[column].append(number)

    numeric_summary = {
        column: {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "sum": sum(values),
            "average": sum(values) / len(values),
        }
        for column, values in numeric_values.items()
        if values
    }
    return {
        "rows_processed": len(rows),
        "columns": columns,
        "missing_values": missing_values,
        "numeric_summary": numeric_summary,
    }


def _clean_csv(columns: list[str], rows: list[dict]) -> dict:
    """Trim cell whitespace and remove rows that contain no data."""
    cleaned_rows = []
    removed_blank_rows = 0
    for row in rows:
        cleaned_row = {column: (row.get(column) or "").strip() for column in columns}
        if not any(cleaned_row.values()):
            removed_blank_rows += 1
            continue
        cleaned_rows.append(cleaned_row)

    output = StringIO(newline="")
    writer = DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(cleaned_rows)
    return {
        "job": "clean-csv",
        "rows_processed": len(rows),
        "rows_output": len(cleaned_rows),
        "blank_rows_removed": removed_blank_rows,
        "cleaned_csv": output.getvalue(),
    }


def _generate_report(analysis: dict) -> dict:
    """Create a compact Markdown report suitable for a dashboard or email."""
    lines = [
        "# CSV Processing Report",
        "",
        f"- Rows processed: {analysis['rows_processed']}",
        f"- Columns: {', '.join(analysis['columns'])}",
        "",
        "## Missing values",
    ]
    lines.extend(f"- {column}: {count}" for column, count in analysis["missing_values"].items())
    lines.extend(["", "## Numeric summary"])
    if not analysis["numeric_summary"]:
        lines.append("No numeric columns were found.")
    for column, summary in analysis["numeric_summary"].items():
        lines.append(
            f"- {column}: count={summary['count']}, min={summary['min']}, "
            f"max={summary['max']}, sum={summary['sum']}, average={summary['average']}"
        )
    return {"job": "generate-report", **analysis, "report_markdown": "\n".join(lines)}


def process_job(job: dict) -> dict:
    """Process an ``analyze-csv``, ``clean-csv``, or ``generate-report`` job."""
    payload = job.get("payload", {})
    job_type = payload.get("job")
    columns, rows = _read_csv(_csv_text_from_payload(payload))

    if job_type == "analyze-csv":
        return {"job": job_type, **_analyse_csv(columns, rows)}
    if job_type == "clean-csv":
        return _clean_csv(columns, rows)
    if job_type == "generate-report":
        return _generate_report(_analyse_csv(columns, rows))

    raise ValueError(
        "Unsupported job. Use 'analyze-csv', 'clean-csv', or 'generate-report'."
    )


def save_result_file(task_id: str, result: dict) -> dict | None:
    """Persist a job's output so the API can serve it as a download."""
    job_type = result.get("job")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    if job_type == "generate-report":
        report = result.get("report_markdown")
        if not isinstance(report, str):
            return None
        extension, download_name, contents = ".md", "taskflow-report.md", report
    elif job_type == "clean-csv":
        cleaned_csv = result.get("cleaned_csv")
        if not isinstance(cleaned_csv, str):
            return None
        extension, download_name, contents = ".csv", "cleaned-data.csv", cleaned_csv
    elif job_type == "analyze-csv":
        extension, download_name = ".json", "csv-analysis.json"
        contents = json.dumps(result, indent=2)
    else:
        return None

    (RESULT_DIR / f"{task_id}{extension}").write_text(contents, encoding="utf-8")
    return {
        "download_url": f"/tasks/{task_id}/download",
        "download_name": download_name,
        "download_extension": extension,
    }


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
    attempt = int(job.get("attempt", 1))
    max_attempts = max(1, int(job.get("max_attempts", 3)))

    log.info("picked up task %s (attempt %s/%s)", task_id, attempt, max_attempts)
    _update_status(task_id, state="processing", attempts=attempt, max_attempts=max_attempts)

    try:
        result = process_job(job)
        s3_uri = upload_result_to_s3(task_id, result)
        download = save_result_file(task_id, result) or {}
        _update_status(
            task_id,
            state="completed",
            result=result,
            result_location=s3_uri,
            **download,
            error=None,
            last_error=None,
        )
        log.info("completed task %s", task_id)
    except Exception as exc:  # noqa: BLE001 - worker must never crash the loop
        if attempt < max_attempts:
            retry_delay = RETRY_DELAY_SECONDS * attempt
            log.warning(
                "task %s failed on attempt %s/%s; retrying in %.1fs: %s",
                task_id,
                attempt,
                max_attempts,
                retry_delay,
                exc,
            )
            _update_status(
                task_id,
                state="queued",
                attempts=attempt,
                max_attempts=max_attempts,
                error=None,
                last_error=str(exc),
            )
            time.sleep(retry_delay)
            job["attempt"] = attempt + 1
            r.rpush(QUEUE_KEY, json.dumps(job))
            return

        log.exception("task %s failed after %s attempts", task_id, attempt)
        _update_status(
            task_id,
            state="failed",
            attempts=attempt,
            max_attempts=max_attempts,
            error=str(exc),
            last_error=str(exc),
        )


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
