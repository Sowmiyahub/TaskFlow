"""
TaskFlow API service
---------------------
A small Flask service that accepts task submissions, pushes them onto a
Redis-backed job queue, and lets clients poll for status/results.

Endpoints:
  POST /tasks            -> enqueue a new task, returns a task_id
  GET  /tasks/<task_id>  -> get status/result of a task
  GET  /healthz          -> liveness/readiness probe
"""
import json
import os
import uuid
from datetime import datetime, timezone

import redis
from flask import Flask, jsonify, request

app = Flask(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("TASKFLOW_QUEUE_KEY", "taskflow:queue")
STATUS_KEY_PREFIX = "taskflow:status:"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _status_key(task_id: str) -> str:
    return f"{STATUS_KEY_PREFIX}{task_id}"


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
    task_id = str(uuid.uuid4())

    job = {
        "task_id": task_id,
        "payload": payload,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    status_record = {
        "task_id": task_id,
        "state": "queued",
        "created_at": job["created_at"],
        "updated_at": job["created_at"],
        "result": None,
        "error": None,
    }

    pipe = r.pipeline()
    pipe.set(_status_key(task_id), json.dumps(status_record))
    pipe.rpush(QUEUE_KEY, json.dumps(job))
    pipe.execute()

    return jsonify(status_record), 202


@app.route("/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    raw = r.get(_status_key(task_id))
    if raw is None:
        return jsonify({"error": "task not found"}), 404
    return jsonify(json.loads(raw)), 200


@app.route("/tasks", methods=["GET"])
def list_queue_depth():
    """Lightweight introspection endpoint: current queue depth."""
    return jsonify({"queue_depth": r.llen(QUEUE_KEY)}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
