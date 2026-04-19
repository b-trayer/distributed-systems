from fastapi import FastAPI, Query
from pydantic import BaseModel
import uuid
import time
import threading
import requests
import os

app = FastAPI()

all_tasks = {}
WORKER_URL = os.getenv("WORKER_URL", "http://worker:8080")
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "60"))
WORKERS_COUNT = int(os.getenv("WORKERS_COUNT", "1"))


class CrackRequest(BaseModel):
    hash: str
    maxLength: int
    algorithm: str = "MD5"
    alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789"


class WorkerResult(BaseModel):
    requestId: str
    partNumber: int
    words: list = []
    checked: int = 0
    elapsed: float = 0
    error: str = None


def count_combinations(alphabet_length, max_word_length):
    return alphabet_length ** max_word_length


def check_if_already_exists(hash_value, max_length, algorithm, alphabet):
    for request_id, task_data in all_tasks.items():
        if (task_data["hash"] == hash_value
                and task_data["maxLength"] == max_length
                and task_data["algorithm"] == algorithm
                and task_data["alphabet"] == alphabet):
            return request_id
    return None


def try_send_task_to_worker(request_id, hash_value, max_length,
                            part_number, total_parts, algorithm, alphabet):
    """POST a shard to WORKER_URL; retries on failure (compose DNS round-robins replicas)."""
    payload = {
        "requestId": request_id,
        "hash": hash_value,
        "maxLength": max_length,
        "partNumber": part_number,
        "partCount": total_parts,
        "algorithm": algorithm,
        "alphabet": alphabet
    }

    for attempt in range(3):
        try:
            response = requests.post(
                f"{WORKER_URL}/internal/api/worker/hash/crack/task",
                json=payload,
                timeout=10
            )
            if response.status_code == 200:
                return True
            print(f"worker HTTP {response.status_code}, attempt {attempt + 1}")
        except Exception as err:
            print(f"worker POST failed: {err}, attempt {attempt + 1}")
            time.sleep(2)
    return False


def send_all_parts_to_workers(request_id):
    """Enqueue all parts; on failure marks ERROR. Schedules timeout watchdog after enqueue."""
    task_data = all_tasks.get(request_id)
    if not task_data:
        return

    total_parts = task_data["partCount"]

    for part_num in range(total_parts):
        current_status = all_tasks.get(request_id, {}).get("status")
        if current_status == "CANCELLED":
            print(f"task {request_id} cancelled, stopping dispatch")
            return

        success = try_send_task_to_worker(
            request_id,
            task_data["hash"],
            task_data["maxLength"],
            part_num,
            total_parts,
            task_data["algorithm"],
            task_data["alphabet"]
        )

        if not success:
            all_tasks[request_id]["status"] = "ERROR"
            all_tasks[request_id]["error"] = f"failed to dispatch part {part_num} to worker"
            return

    checker = threading.Thread(
        target=wait_and_check_missing_parts,
        args=(request_id, 0),
        daemon=True
    )
    checker.start()


def wait_and_check_missing_parts(request_id, retry_number):
    """After TASK_TIMEOUT, re-dispatch missing parts; gives up after 3 rounds."""
    time.sleep(TASK_TIMEOUT)

    task_data = all_tasks.get(request_id)
    if not task_data:
        return
    if task_data["status"] != "IN_PROGRESS":
        return

    missing_parts = []
    for part_num in range(task_data["partCount"]):
        if part_num not in task_data["completed_parts"]:
            missing_parts.append(part_num)

    if not missing_parts:
        return

    if retry_number >= 3:
        all_tasks[request_id]["status"] = "ERROR"
        all_tasks[request_id]["error"] = (
            f"parts {missing_parts} still incomplete after {retry_number} rounds"
        )
        print(f"task {request_id} failed: no response for parts {missing_parts}")
        return

    print(f"missing parts {missing_parts}, re-dispatch (round {retry_number + 1})")

    for part_num in missing_parts:
        if all_tasks.get(request_id, {}).get("status") != "IN_PROGRESS":
            break
        try_send_task_to_worker(
            request_id,
            task_data["hash"],
            task_data["maxLength"],
            part_num,
            task_data["partCount"],
            task_data["algorithm"],
            task_data["alphabet"]
        )

    next_check = threading.Thread(
        target=wait_and_check_missing_parts,
        args=(request_id, retry_number + 1),
        daemon=True
    )
    next_check.start()


@app.get("/")
def root():
    return {"service": "CrackHash Manager"}


@app.get("/api/hash/tasks")
def get_all_tasks():
    result = []
    for request_id, task_data in all_tasks.items():
        result.append({
            "requestId": request_id,
            "hash": task_data["hash"],
            "maxLength": task_data["maxLength"],
            "status": task_data["status"],
            "found_words": task_data["found_words"],
            "error": task_data["error"],
        })
    return result


@app.post("/api/hash/crack")
def start_cracking(req: CrackRequest):
    # Idempotent while IN_PROGRESS or after READY for the same crack parameters.
    existing_id = check_if_already_exists(
        req.hash, req.maxLength, req.algorithm, req.alphabet
    )
    if existing_id:
        existing_task = all_tasks[existing_id]
        if existing_task["status"] == "IN_PROGRESS":
            return {
                "requestId": existing_id,
                "estimatedCombinations": existing_task["total_combinations"]
            }
        if existing_task["status"] == "READY":
            return {
                "requestId": existing_id,
                "estimatedCombinations": existing_task["total_combinations"],
                "data": existing_task["found_words"]
            }

    request_id = str(uuid.uuid4())
    total_combinations = count_combinations(len(req.alphabet), req.maxLength)

    how_many_parts = WORKERS_COUNT
    if total_combinations / how_many_parts > 500000:
        how_many_parts = (total_combinations // 500000) + 1

    all_tasks[request_id] = {
        "hash": req.hash,
        "maxLength": req.maxLength,
        "algorithm": req.algorithm,
        "alphabet": req.alphabet,
        "status": "IN_PROGRESS",
        "total_combinations": total_combinations,
        "partCount": how_many_parts,
        "completed_parts": set(),
        "found_words": [],
        "words_checked": 0,
        "start_time": time.time(),
        "end_time": None,
        "error": None,
    }

    bg = threading.Thread(target=send_all_parts_to_workers, args=(request_id,), daemon=True)
    bg.start()

    return {"requestId": request_id, "estimatedCombinations": total_combinations}


@app.get("/api/hash/status")
def get_status(requestId: str = Query(...)):
    if requestId not in all_tasks:
        return {"status": "ERROR", "data": None, "error": "request not found"}

    task_data = all_tasks[requestId]

    if task_data["status"] == "IN_PROGRESS":
        return {"status": "IN_PROGRESS", "data": None}

    if task_data["status"] == "READY":
        return {"status": "READY", "data": task_data["found_words"]}

    if task_data["status"] == "ERROR":
        return {"status": "ERROR", "data": None, "error": task_data["error"]}

    if task_data["status"] == "CANCELLED":
        return {"status": "CANCELLED", "data": None}


@app.delete("/api/hash/crack")
def cancel_task(requestId: str = Query(...)):
    if requestId not in all_tasks:
        return {"error": "request not found"}

    all_tasks[requestId]["status"] = "CANCELLED"

    try:
        requests.delete(
            f"{WORKER_URL}/internal/api/worker/hash/crack/task",
            params={"requestId": requestId},
            timeout=3
        )
    except:
        pass

    return {"status": "CANCELLED"}


@app.get("/api/metrics")
def get_metrics():
    active_count = 0
    done_count = 0
    total_time_ms = 0

    for task_data in all_tasks.values():
        if task_data["status"] == "IN_PROGRESS":
            active_count += 1
        if task_data["status"] == "READY":
            done_count += 1
            if task_data["end_time"]:
                duration = task_data["end_time"] - task_data["start_time"]
                total_time_ms += duration * 1000

    avg_time = int(total_time_ms / done_count) if done_count > 0 else 0

    return {
        "totalTasks": len(all_tasks),
        "activeTasks": active_count,
        "completedTasks": done_count,
        "avgExecutionTime": avg_time
    }


@app.patch("/internal/api/manager/hash/crack/request")
def receive_worker_result(result: WorkerResult):
    if result.requestId not in all_tasks:
        print(f"result for unknown requestId {result.requestId}")
        return {"status": "not_found"}

    task_data = all_tasks[result.requestId]

    if task_data["status"] != "IN_PROGRESS":
        return {"status": "ignored"}

    if result.error:
        print(f"worker error on part {result.partNumber}: {result.error}")
        return {"status": "error_noted"}

    if result.partNumber in task_data["completed_parts"]:
        return {"status": "duplicate"}

    task_data["completed_parts"].add(result.partNumber)
    if result.words:
        task_data["found_words"].extend(result.words)
    task_data["words_checked"] += result.checked

    done = len(task_data["completed_parts"])
    total = task_data["partCount"]
    print(f"part {result.partNumber + 1}/{total} done, "
          f"words in batch: {len(result.words)}, progress {done}/{total}")

    if done >= total:
        task_data["status"] = "READY"
        task_data["end_time"] = time.time()
        elapsed = task_data["end_time"] - task_data["start_time"]
        print(f"task {result.requestId} finished in {elapsed:.1f}s, "
              f"found: {task_data['found_words']}")

    return {"status": "ok"}
