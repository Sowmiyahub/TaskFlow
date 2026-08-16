"""
TaskFlow API service
---------------------
A small Flask service that accepts task submissions, pushes them onto a
Redis-backed job queue, and lets clients poll for status/results.

Endpoints:
  POST /tasks            -> enqueue a new task, returns a task_id
  POST /tasks/upload     -> upload a CSV file and enqueue its processing
  GET  /tasks/<task_id>  -> get status/result of a task
  GET  /tasks/<task_id>/download -> download generated job output
  GET  /metrics          -> queue and task-state metrics
  GET  /dashboard        -> browser monitoring dashboard
  GET  /healthz          -> liveness/readiness probe
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis
from flask import Flask, Response, jsonify, request, send_file

app = Flask(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("TASKFLOW_QUEUE_KEY", "taskflow:queue")
STATUS_KEY_PREFIX = "taskflow:status:"
TASK_INDEX_KEY = "taskflow:tasks"
DATA_DIR = Path(os.environ.get("TASKFLOW_DATA_DIR", "/data"))
UPLOAD_DIR = DATA_DIR / "uploads"
RESULT_DIR = DATA_DIR / "results"
MAX_ATTEMPTS = max(1, int(os.environ.get("TASKFLOW_MAX_ATTEMPTS", "3")))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _status_key(task_id: str) -> str:
    return f"{STATUS_KEY_PREFIX}{task_id}"


def _task_records(limit: int | None = 20) -> list[dict]:
    """Return recent status records without exposing potentially large results."""
    task_ids = r.smembers(TASK_INDEX_KEY)
    raw_records = r.mget([_status_key(task_id) for task_id in task_ids]) if task_ids else []
    records = [json.loads(raw) for raw in raw_records if raw]
    records.sort(key=lambda record: record.get("updated_at", ""), reverse=True)
    return records[:limit] if limit is not None else records


def _create_task(payload: dict, task_id: str | None = None) -> dict:
    """Create the status record and atomically enqueue a worker job."""
    task_id = task_id or str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    job = {
        "task_id": task_id,
        "payload": payload,
        "created_at": created_at,
        "attempt": 1,
        "max_attempts": MAX_ATTEMPTS,
    }
    status_record = {
        "task_id": task_id,
        "state": "queued",
        "created_at": created_at,
        "updated_at": created_at,
        "result": None,
        "error": None,
        "attempts": 0,
        "max_attempts": MAX_ATTEMPTS,
        "last_error": None,
    }
    pipe = r.pipeline()
    pipe.set(_status_key(task_id), json.dumps(status_record))
    pipe.sadd(TASK_INDEX_KEY, task_id)
    pipe.rpush(QUEUE_KEY, json.dumps(job))
    pipe.execute()
    return status_record


@app.route("/healthz", methods=["GET"])
def healthz():
    try:
        r.ping()
    except redis.exceptions.RedisError:
        return jsonify({"status": "unhealthy", "redis": "unreachable"}), 503
    return jsonify({"status": "ok"}), 200


@app.route("/tasks", methods=["POST"])
def create_task():
    """Accepts an arbitrary JSON payload describing the task and enqueues it."""
    payload = request.get_json(silent=True) or {}
    return jsonify(_create_task(payload)), 202


@app.route("/tasks/upload", methods=["POST"])
def upload_csv_task():
    """Accept a real CSV file and queue a job that reads the shared file."""
    uploaded_file = request.files.get("file")
    job_type = request.form.get("job", "generate-report")
    if uploaded_file is None or not uploaded_file.filename:
        return jsonify({"error": "Send a CSV file in the 'file' form field."}), 400
    if not uploaded_file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only .csv files are supported."}), 400
    if job_type not in {"analyze-csv", "clean-csv", "generate-report"}:
        return jsonify({"error": "Unsupported job type."}), 400

    task_id = str(uuid.uuid4())
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    input_path = UPLOAD_DIR / f"{task_id}.csv"
    uploaded_file.save(input_path)

    # Keep the uploaded filename for display only; the generated UUID path
    # prevents a client-controlled filename from being used as a filesystem path.
    payload = {
        "job": job_type,
        "input_path": str(input_path),
        "original_filename": uploaded_file.filename,
    }
    return jsonify(_create_task(payload, task_id=task_id)), 202


@app.route("/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    raw = r.get(_status_key(task_id))
    if raw is None:
        return jsonify({"error": "task not found"}), 404
    return jsonify(json.loads(raw)), 200


@app.route("/tasks/<task_id>/download", methods=["GET"])
def download_report(task_id):
    """Download the file produced by a completed task."""
    raw = r.get(_status_key(task_id))
    if raw is None:
        return jsonify({"error": "task not found"}), 404
    record = json.loads(raw)
    extension = record.get("download_extension")
    download_name = record.get("download_name")
    if extension not in {".md", ".csv", ".json"} or not isinstance(download_name, str):
        return jsonify({"error": "download is not ready"}), 404
    output_path = RESULT_DIR / f"{task_id}{extension}"
    if not output_path.is_file():
        return jsonify({"error": "download is not ready"}), 404
    return send_file(output_path, as_attachment=True, download_name=download_name)


@app.route("/tasks", methods=["GET"])
def list_queue_depth():
    """Lightweight introspection endpoint: current queue depth."""
    return jsonify({"queue_depth": r.llen(QUEUE_KEY)}), 200


@app.route("/metrics", methods=["GET"])
def metrics():
    """Return lightweight operational metrics for the local dashboard."""
    records = _task_records(limit=None)
    state_counts = {"queued": 0, "processing": 0, "completed": 0, "failed": 0}
    for record in records:
        state = record.get("state")
        if state in state_counts:
            state_counts[state] += 1
    recent_tasks = [
        {
            "task_id": record.get("task_id"),
            "state": record.get("state"),
            "attempts": record.get("attempts", 0),
            "max_attempts": record.get("max_attempts", MAX_ATTEMPTS),
            "updated_at": record.get("updated_at"),
            "error": record.get("error") or record.get("last_error"),
        }
        for record in records[:20]
    ]
    return jsonify(
        {
            "queue_depth": r.llen(QUEUE_KEY),
            "task_states": state_counts,
            "tracked_tasks": len(r.smembers(TASK_INDEX_KEY)),
            "recent_tasks": recent_tasks,
        }
    ), 200


@app.route("/dashboard", methods=["GET"])
def dashboard():
    """A no-dependency monitoring page that refreshes every two seconds."""
    return Response(
        """<!doctype html><html><head><meta charset=\"utf-8\"><title>TaskFlow Dashboard</title>
        <style>body{font-family:system-ui;margin:2rem;background:#f6f8fb;color:#172033}h1{margin-bottom:.2rem}.cards{display:flex;gap:1rem;flex-wrap:wrap}.card{background:#fff;border-radius:8px;padding:1rem;min-width:120px;box-shadow:0 1px 3px #ccd}table{background:#fff;border-collapse:collapse;width:100%;margin-top:1rem}th,td{padding:.7rem;text-align:left;border-bottom:1px solid #e5e7eb}.queued{color:#a16207}.processing{color:#2563eb}.completed{color:#15803d}.failed{color:#dc2626}</style>
        </head><body><h1>TaskFlow Dashboard</h1><p>Refreshes automatically every 2 seconds.</p><div class=\"cards\" id=\"cards\"></div><table><thead><tr><th>Task ID</th><th>State</th><th>Attempts</th><th>Updated</th><th>Last error</th></tr></thead><tbody id=\"tasks\"></tbody></table>
        <script>function cell(row,value){const td=document.createElement('td');td.textContent=value ?? '';row.appendChild(td)}async function refresh(){const data=await fetch('/metrics').then(r=>r.json());const values=[['Queue depth',data.queue_depth],['Queued',data.task_states.queued],['Processing',data.task_states.processing],['Completed',data.task_states.completed],['Failed',data.task_states.failed],['Tracked tasks',data.tracked_tasks]];const cards=document.getElementById('cards');cards.replaceChildren(...values.map(([label,value])=>{const el=document.createElement('div');el.className='card';el.innerHTML='<strong></strong><div></div>';el.children[0].textContent=label;el.children[1].textContent=value;return el}));const body=document.getElementById('tasks');body.replaceChildren(...data.recent_tasks.map(task=>{const row=document.createElement('tr');cell(row,task.task_id);const state=document.createElement('td');state.textContent=task.state;state.className=task.state;row.appendChild(state);cell(row,`${task.attempts}/${task.max_attempts}`);cell(row,task.updated_at);cell(row,task.error);return row}));}refresh();setInterval(refresh,2000)</script></body></html>""",
        mimetype="text/html",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
