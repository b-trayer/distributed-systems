"""
CrackHash lab2 dispatcher — dumb loop + rabbit.
Not fancy: polls mongo, shoves tasks into the queue, reads results.
"""

import json
import os
import threading
import time

import pika
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
WORKERS_COUNT = int(os.getenv("WORKERS_COUNT", "1"))
PART_STALE_SEC = int(os.getenv("PART_STALE_SEC", "1800"))

EXCHANGE = "crack.direct"
TASK_QUEUE = "worker_tasks"
RESULT_QUEUE = "worker_results"
RK_TASK = "task"
RK_RESULT = "result"

mongo = None
db = None


def wait_mongo():
    global mongo, db
    for _ in range(60):
        try:
            mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            mongo.admin.command("ping")
            db = mongo["crackhash2"]
            print("dispatcher: mongo ok")
            return
        except Exception as e:
            print(f"dispatcher: mongo wait {e}")
            time.sleep(2)
    raise SystemExit("dispatcher: no mongo")


def connect_rabbit():
    for _ in range(40):
        try:
            params = pika.URLParameters(RABBIT_URL)
            params.heartbeat = 600
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
            ch.queue_declare(queue=TASK_QUEUE, durable=True)
            ch.queue_declare(queue=RESULT_QUEUE, durable=True)
            ch.queue_bind(queue=TASK_QUEUE, exchange=EXCHANGE, routing_key=RK_TASK)
            ch.queue_bind(queue=RESULT_QUEUE, exchange=EXCHANGE, routing_key=RK_RESULT)
            print("dispatcher: rabbit ok")
            return conn, ch
        except Exception as e:
            print(f"dispatcher: rabbit wait {e}")
            time.sleep(2)
    raise SystemExit("dispatcher: no rabbit")


def worker_space_size(alphabet_len, max_len):
    n = 0
    for L in range(1, max_len + 1):
        n += alphabet_len**L
    return n


def part_count_for_job(total_comb):
    n = WORKERS_COUNT
    if n < 1:
        n = 1
    if total_comb / n > 500_000:
        n = (total_comb // 500_000) + 1
    return max(1, n)


def ensure_parts_for_requests():
    for req in db.requests.find({"status": "IN_PROGRESS"}):
        rid = req["requestId"]
        if db.parts.count_documents({"requestId": rid}) > 0:
            continue
        alen = len(req["alphabet"])
        mx = req["maxLength"]
        total = worker_space_size(alen, mx)
        pc = part_count_for_job(total)
        parts = []
        for i in range(pc):
            parts.append(
                {
                    "requestId": rid,
                    "partNumber": i,
                    "partCount": pc,
                    "hash": req["hash"],
                    "maxLength": mx,
                    "algorithm": req.get("algorithm") or "MD5",
                    "alphabet": req["alphabet"],
                    "status": "NEW",
                    "sentAt": None,
                }
            )
        try:
            if parts:
                db.parts.insert_many(parts, ordered=False)
        except Exception as e:
            print(f"dispatcher: insert_many parts {e}")
            continue
        db.requests.update_one(
            {"requestId": rid},
            {"$set": {"partCount": pc}},
        )
        print(f"dispatcher: planned {pc} parts for {rid}")


def publish_pending(pub_ch):
    for part in db.parts.find({"status": {"$in": ["NEW", "QUEUED_PENDING"]}}):
        rid = part["requestId"]
        req = db.requests.find_one({"requestId": rid})
        if not req or req["status"] != "IN_PROGRESS":
            continue
        body = json.dumps(
            {
                "requestId": rid,
                "hash": part["hash"],
                "maxLength": part["maxLength"],
                "partNumber": part["partNumber"],
                "partCount": part["partCount"],
                "algorithm": part["algorithm"],
                "alphabet": part["alphabet"],
            }
        )
        try:
            pub_ch.basic_publish(
                exchange=EXCHANGE,
                routing_key=RK_TASK,
                body=body.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )
            db.parts.update_one(
                {"_id": part["_id"]},
                {"$set": {"status": "SENT", "sentAt": time.time()}},
            )
        except Exception as e:
            print(f"dispatcher: publish fail {e}")
            db.parts.update_one(
                {"_id": part["_id"]},
                {"$set": {"status": "QUEUED_PENDING"}},
            )


def sweep_stale_parts():
    cutoff = time.time() - PART_STALE_SEC
    for part in db.parts.find({"status": "SENT"}):
        st = part.get("sentAt") or 0
        if st < cutoff:
            db.parts.update_one(
                {"_id": part["_id"]},
                {"$set": {"status": "NEW"}, "$unset": {"sentAt": ""}},
            )
            print(f"dispatcher: stale part -> NEW {part['requestId']} #{part['partNumber']}")


def merge_result_and_maybe_finish(rid, part_number, words, checked, err):
    req = db.requests.find_one({"requestId": rid})
    if not req:
        return
    if req["status"] == "ERROR":
        db.parts.update_one(
            {"requestId": rid, "partNumber": part_number},
            {"$set": {"status": "DONE"}},
        )
        return
    if req["status"] == "CANCELLED":
        db.parts.update_one(
            {"requestId": rid, "partNumber": part_number},
            {"$set": {"status": "DONE"}},
        )
        return

    if err:
        db.parts.update_one(
            {"requestId": rid, "partNumber": part_number, "status": {"$ne": "DONE"}},
            {"$set": {"status": "DONE"}},
        )
        db.requests.update_one(
            {"requestId": rid, "status": "IN_PROGRESS"},
            {"$set": {"status": "ERROR", "error": str(err), "end_time": time.time()}},
        )
        return

    upd = db.parts.update_one(
        {"requestId": rid, "partNumber": part_number, "status": {"$ne": "DONE"}},
        {"$set": {"status": "DONE"}},
    )
    if upd.matched_count == 0:
        return

    if words:
        db.requests.update_one(
            {"requestId": rid},
            {"$addToSet": {"found_words": {"$each": words}}},
        )
    if checked:
        db.requests.update_one({"requestId": rid}, {"$inc": {"words_checked": int(checked)}})

    pc = req.get("partCount")
    if not pc:
        return
    done = db.parts.count_documents({"requestId": rid, "status": "DONE"})
    if done >= pc:
        db.requests.update_one(
            {"requestId": rid, "status": "IN_PROGRESS"},
            {"$set": {"status": "READY", "end_time": time.time()}},
        )
        print(f"dispatcher: request READY {rid}")


def loop_plan_publish():
    wait_mongo()
    conn, ch = None, None
    while True:
        try:
            if conn is None:
                conn, ch = connect_rabbit()
            ensure_parts_for_requests()
            publish_pending(ch)
            sweep_stale_parts()
        except Exception as e:
            print(f"dispatcher: loop err {e}")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn, ch = None, None
            time.sleep(1)
        time.sleep(0.4)


def on_result_message(ch, method, _props, body):
    try:
        data = json.loads(body.decode("utf-8"))
        rid = data["requestId"]
        pn = int(data["partNumber"])
        words = data.get("words") or []
        checked = int(data.get("checked") or 0)
        err = data.get("error")
        merge_result_and_maybe_finish(rid, pn, words, checked, err)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"dispatcher: bad result msg {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def loop_results():
    wait_mongo()
    conn, ch = connect_rabbit()
    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=RESULT_QUEUE, on_message_callback=on_result_message, auto_ack=False)
    print("dispatcher: consuming results")
    while True:
        try:
            ch.start_consuming()
        except Exception as e:
            print(f"dispatcher: consume died {e}, reconnect")
            time.sleep(2)
            conn, ch = connect_rabbit()
            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(
                queue=RESULT_QUEUE,
                on_message_callback=on_result_message,
                auto_ack=False,
            )


if __name__ == "__main__":
    threading.Thread(target=loop_plan_publish, daemon=True).start()
    loop_results()
