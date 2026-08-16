"""
TaskFlow load test — real CSV analysis jobs
---------------------------------------------
Uploads titanic.csv as an analyze-csv job N times, in parallel, and times
how long it takes for every task to reach "completed".

Usage:
    1. Put this file in the same folder as titanic.csv
    2. pip install requests
    3. python loadtest_real.py
"""
import time
import statistics
import concurrent.futures
import requests

BASE = "http://localhost:8080"
CSV_FILE = "titanic.csv"
N_TASKS = 100


def submit():
    with open(CSV_FILE, "rb") as f:
        r = requests.post(
            f"{BASE}/tasks/upload",
            files={"file": f},
            data={"job": "analyze-csv"},
            timeout=30,
        )
    r.raise_for_status()
    return r.json()["task_id"]


def poll_until_done(task_id, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        r = requests.get(f"{BASE}/tasks/{task_id}", timeout=10)
        data = r.json()
        if data["state"] in ("completed", "failed"):
            return data["state"], time.time() - start
        time.sleep(0.2)
    return "timeout", timeout


def main():
    print(f"Submitting {N_TASKS} analyze-csv tasks...")
    submit_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        task_ids = list(ex.map(lambda _: submit(), range(N_TASKS)))
    submit_elapsed = time.time() - submit_start
    print(f"All {N_TASKS} tasks submitted in {submit_elapsed:.2f}s")

    process_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(poll_until_done, task_ids))
    process_elapsed = time.time() - process_start

    states = [s for s, _ in results]
    completed = states.count("completed")
    failed = states.count("failed")
    timed_out = states.count("timeout")
    latencies = [t for _, t in results]

    total_time = submit_elapsed + process_elapsed
    print("\n--- RESULTS ---")
    print(f"Total tasks: {N_TASKS}")
    print(f"Completed: {completed}  Failed: {failed}  Timed out: {timed_out}")
    print(f"Total wall time: {total_time:.2f}s")
    print(f"Throughput: {N_TASKS / total_time:.2f} tasks/sec")
    print(f"Avg time-to-completion: {statistics.mean(latencies):.2f}s")
    print(f"p95 time-to-completion: {sorted(latencies)[int(0.95 * len(latencies)) - 1]:.2f}s")


if __name__ == "__main__":
    main()