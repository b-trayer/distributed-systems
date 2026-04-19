from fastapi import FastAPI, Query
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
import os
import time
import uuid

app = FastAPI()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
WORKERS_COUNT = int(os.getenv("WORKERS_COUNT", "1"))

client = None
db = None


def wait_for_mongo():
    global client, db
    uri = MONGO_URI
    for attempt in range(60):
        try:
            client = MongoClient(
                uri,
                w="majority",
                wTimeoutMS=10000,
                serverSelectionTimeoutMS=5000,
            )
            client.admin.command("ping")
            db = client["crackhash2"]
            db.requests.create_index("requestId", unique=True)
            db.parts.create_index([("requestId", 1), ("partNumber", 1)], unique=True)
            print("manager: mongo ok")
            return
        except Exception as e:
            print(f"manager: mongo not ready ({e}), sleep 2")
            time.sleep(2)
    raise RuntimeError("manager: gave up waiting for mongo")


@app.on_event("startup")
def startup():
    wait_for_mongo()


class CrackRequest(BaseModel):
    hash: str
    maxLength: int
    algorithm: str = "MD5"
    alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789"


def count_combinations(alphabet_length, max_word_length):
    return alphabet_length ** max_word_length


def find_existing_request(req: CrackRequest):
    return db.requests.find_one(
        {
            "hash": req.hash,
            "maxLength": req.maxLength,
            "algorithm": req.algorithm,
            "alphabet": req.alphabet,
        }
    )


@app.get("/")
def root():
    return {"service": "CrackHash Manager (lab2 / mongo)"}


@app.get("/api/hash/tasks")
def list_tasks():
    out = []
    for row in db.requests.find({}, {"_id": 0}):
        out.append(
            {
                "requestId": row.get("requestId"),
                "hash": row.get("hash"),
                "maxLength": row.get("maxLength"),
                "status": row.get("status"),
                "found_words": row.get("found_words") or [],
                "error": row.get("error"),
            }
        )
    return out


@app.post("/api/hash/crack")
def start_cracking(req: CrackRequest):
    existing = find_existing_request(req)
    if existing:
        st = existing["status"]
        if st == "IN_PROGRESS":
            return {
                "requestId": existing["requestId"],
                "estimatedCombinations": existing["estimatedCombinations"],
            }
        if st == "READY":
            return {
                "requestId": existing["requestId"],
                "estimatedCombinations": existing["estimatedCombinations"],
                "data": existing.get("found_words") or [],
            }

    request_id = str(uuid.uuid4())
    est = count_combinations(len(req.alphabet), req.maxLength)
    doc = {
        "requestId": request_id,
        "hash": req.hash,
        "maxLength": req.maxLength,
        "algorithm": req.algorithm,
        "alphabet": req.alphabet,
        "status": "IN_PROGRESS",
        "estimatedCombinations": est,
        "partCount": None,
        "found_words": [],
        "words_checked": 0,
        "start_time": time.time(),
        "end_time": None,
        "error": None,
    }
    try:
        db.requests.insert_one(doc)
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        print(f"manager: insert failed {e}")
        return {"error": "could not persist request", "detail": str(e)}, 503

    return {"requestId": request_id, "estimatedCombinations": est}


@app.get("/api/hash/status")
def get_status(requestId: str = Query(...)):
    row = db.requests.find_one({"requestId": requestId}, {"_id": 0})
    if not row:
        return {"status": "ERROR", "data": None, "error": "request not found"}

    st = row["status"]
    if st == "IN_PROGRESS":
        return {"status": "IN_PROGRESS", "data": None}
    if st == "READY":
        return {"status": "READY", "data": row.get("found_words") or []}
    if st == "ERROR":
        return {"status": "ERROR", "data": None, "error": row.get("error") or "error"}
    if st == "CANCELLED":
        return {"status": "CANCELLED", "data": None}
    return {"status": "ERROR", "data": None, "error": "unknown status"}


@app.delete("/api/hash/crack")
def cancel_task(requestId: str = Query(...)):
    res = db.requests.update_one(
        {"requestId": requestId},
        {"$set": {"status": "CANCELLED"}},
    )
    if res.matched_count == 0:
        return {"error": "request not found"}
    return {"status": "CANCELLED"}


@app.get("/api/metrics")
def metrics():
    active = 0
    done = 0
    total_ms = 0
    for row in db.requests.find({}, {"status": 1, "start_time": 1, "end_time": 1}):
        if row.get("status") == "IN_PROGRESS":
            active += 1
        if row.get("status") == "READY":
            done += 1
            if row.get("end_time") and row.get("start_time"):
                total_ms += (row["end_time"] - row["start_time"]) * 1000
    avg = int(total_ms / done) if done else 0
    total = db.requests.count_documents({})
    return {
        "totalTasks": total,
        "activeTasks": active,
        "completedTasks": done,
        "avgExecutionTime": avg,
    }
