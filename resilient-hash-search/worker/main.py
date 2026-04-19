"""
Lab2 worker — pulls tasks from rabbit, same brute MD5 idea as lab1.
"""

import hashlib
import json
import os
import time

import pika
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")

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
            print("worker: mongo ok")
            return
        except Exception as e:
            print(f"worker: mongo wait {e}")
            time.sleep(2)
    raise SystemExit("worker: no mongo")


def connect_rabbit():
    params = pika.URLParameters(RABBIT_URL)
    params.heartbeat = 1800
    params.blocked_connection_timeout = 600
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    ch.queue_declare(queue=TASK_QUEUE, durable=True)
    ch.queue_declare(queue=RESULT_QUEUE, durable=True)
    ch.queue_bind(queue=TASK_QUEUE, exchange=EXCHANGE, routing_key=RK_TASK)
    ch.queue_bind(queue=RESULT_QUEUE, exchange=EXCHANGE, routing_key=RK_RESULT)
    ch.basic_qos(prefetch_count=1)
    return conn, ch


def count_combinations(alphabet_length, max_word_length):
    total = 0
    for length in range(1, max_word_length + 1):
        total += alphabet_length**length
    return total


def number_to_word(number, alphabet):
    alphabet_size = len(alphabet)
    word_length = 1
    words_before = 0
    while True:
        words_of_this_length = alphabet_size**word_length
        if number < words_before + words_of_this_length:
            position = number - words_before
            letters = []
            for _ in range(word_length):
                letters.append(alphabet[position % alphabet_size])
                position //= alphabet_size
            return "".join(reversed(letters))
        words_before += words_of_this_length
        word_length += 1


def is_cancelled(request_id):
    row = db.requests.find_one({"requestId": request_id}, {"status": 1})
    return row and row.get("status") == "CANCELLED"


def run_part(task, conn):
    rid = task["requestId"]
    started = time.time()
    total_combinations = count_combinations(len(task["alphabet"]), task["maxLength"])
    chunk = total_combinations // task["partCount"]
    my_start = task["partNumber"] * chunk
    my_end = my_start + chunk
    if task["partNumber"] == task["partCount"] - 1:
        my_end = total_combinations

    found = []
    checked = 0
    target = task["hash"].lower()

    for i in range(my_start, my_end):
        if checked % 40000 == 0 and checked > 0:
            try:
                conn.process_data_events(time_limit=0)
            except Exception:
                pass
        if is_cancelled(rid):
            break
        word = number_to_word(i, task["alphabet"])
        h = hashlib.md5(word.encode()).hexdigest()
        if h == target:
            found.append(word)
        checked += 1

    elapsed = time.time() - started
    return {
        "requestId": rid,
        "partNumber": task["partNumber"],
        "words": found,
        "checked": checked,
        "elapsed": elapsed,
        "error": None,
    }


def publish_result(ch, payload):
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=RK_RESULT,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )


def handle_delivery(ch, method, _props, body, conn):
    try:
        task = json.loads(body.decode("utf-8"))
        out = run_part(task, conn)
        publish_result(ch, out)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"worker: task failed {e}")
        try:
            task = json.loads(body.decode("utf-8"))
            publish_result(
                ch,
                {
                    "requestId": task.get("requestId", ""),
                    "partNumber": int(task.get("partNumber", 0)),
                    "words": [],
                    "checked": 0,
                    "elapsed": 0,
                    "error": str(e),
                },
            )
        except Exception:
            pass
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    wait_mongo()
    while True:
        try:
            conn, ch = connect_rabbit()
            print("worker: consuming tasks")

            def _cb(ch_, method, props, body):
                handle_delivery(ch_, method, props, body, conn)

            ch.basic_consume(queue=TASK_QUEUE, on_message_callback=_cb, auto_ack=False)
            ch.start_consuming()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"worker: loop died {e}, sleep reconnect")
            time.sleep(3)


if __name__ == "__main__":
    main()
