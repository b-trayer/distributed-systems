from fastapi import FastAPI, Query
from pydantic import BaseModel
import hashlib
import time
import requests
import threading
import os

app = FastAPI()

MANAGER_URL = os.getenv("MANAGER_URL", "http://manager:8080")
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "5"))

cancelled_tasks = set()


class CrackTask(BaseModel):
    requestId: str
    hash: str
    maxLength: int
    partNumber: int
    partCount: int
    algorithm: str = "MD5"
    alphabet: str = "abcdefghijklmnopqrstuvwxyz0123456789"


def count_combinations(alphabet_length, max_word_length):
    total = 0
    for length in range(1, max_word_length + 1):
        total += alphabet_length ** length
    return total


def number_to_word(number, alphabet):
    """Map a linear index to its string in shortlex order for the alphabet."""
    alphabet_size = len(alphabet)

    word_length = 1
    words_before = 0
    while True:
        words_of_this_length = alphabet_size ** word_length
        if number < words_before + words_of_this_length:
            position = number - words_before
            letters = []
            for _ in range(word_length):
                letters.append(alphabet[position % alphabet_size])
                position //= alphabet_size
            return ''.join(reversed(letters))
        words_before += words_of_this_length
        word_length += 1


def process_task(task: CrackTask):
    try:
        started_at = time.time()

        total_combinations = count_combinations(len(task.alphabet), task.maxLength)

        chunk_size = total_combinations // task.partCount
        my_start = task.partNumber * chunk_size
        my_end = my_start + chunk_size
        if task.partNumber == task.partCount - 1:
            my_end = total_combinations

        print(f"part {task.partNumber}, range [{my_start}..{my_end}), "
              f"{my_end - my_start} candidates")

        found_words = []
        words_checked = 0
        target_hash = task.hash.lower()

        for i in range(my_start, my_end):
            if task.requestId in cancelled_tasks:
                print(f"task {task.requestId} cancelled")
                break

            word = number_to_word(i, task.alphabet)
            word_hash = hashlib.md5(word.encode()).hexdigest()

            if word_hash == target_hash:
                found_words.append(word)
                print(f"match: '{word}' -> {word_hash}")

            words_checked += 1

        elapsed = time.time() - started_at
        print(f"part {task.partNumber} done in {elapsed:.1f}s, "
              f"checked {words_checked}, found {len(found_words)}")

        send_result_to_manager({
            "requestId": task.requestId,
            "partNumber": task.partNumber,
            "words": found_words,
            "checked": words_checked,
            "elapsed": elapsed
        })

    except Exception as err:
        print(f"process_task error: {err}")
        send_result_to_manager({
            "requestId": task.requestId,
            "partNumber": task.partNumber,
            "words": [],
            "checked": 0,
            "elapsed": 0,
            "error": str(err)
        })

    cancelled_tasks.discard(task.requestId)


def send_result_to_manager(result_data):
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.patch(
                f"{MANAGER_URL}/internal/api/manager/hash/crack/request",
                json=result_data,
                timeout=10
            )
            if response.status_code == 200:
                return
            print(f"manager HTTP {response.status_code}, attempt {attempt + 1}")
        except Exception as err:
            print(f"PATCH to manager failed: {err}, attempt {attempt + 1}/{RETRY_COUNT}")
            time.sleep(2 * (attempt + 1))

    print(f"gave up sending result after {RETRY_COUNT} attempts")


@app.get("/")
def root():
    return {"service": "CrackHash Worker"}


@app.post("/internal/api/worker/hash/crack/task")
def receive_task(task: CrackTask):
    print(f"task {task.requestId}, shard {task.partNumber}/{task.partCount}")

    worker_thread = threading.Thread(target=process_task, args=(task,), daemon=True)
    worker_thread.start()

    return {"status": "accepted"}


@app.delete("/internal/api/worker/hash/crack/task")
def cancel_task(requestId: str = Query(...)):
    cancelled_tasks.add(requestId)
    print(f"cancel requested for {requestId}")
    return {"status": "cancelling"}
