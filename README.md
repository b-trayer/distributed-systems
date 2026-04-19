# distributed-systems

Two folders, two small python demos. Both brute-force MD5 (find string for a hash). Nothing fancy.

**distributed-hash-search** — first one. Manager + workers, docker compose, workers get work over HTTP. Manager keeps state in memory. API on port **8080**.

**resilient-hash-search** — second one. Same kind of API but data sits in Mongo (3-node replica set) and workers pull jobs from RabbitMQ. There is a separate dispatcher process that shuffles tasks. API on port **8081** so it does not fight with the first stack.

How to run: go into the folder, then `docker compose up --build`. For the second one you can add `--scale worker=2` or more.

There are `test.http` files in each folder with example calls.
