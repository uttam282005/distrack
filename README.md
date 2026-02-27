# distrack
A small distributed task scheduler composed of three services:
- Coordinator (gRPC, default :8080): tracks workers, assigns tasks, updates task status.
- Worker (gRPC, default :8000): executes shell commands and reports status.
- Scheduler (HTTP, default :8081): API to schedule tasks and query status.
- Postgres: storage for tasks.

## Architecture
- Scheduler writes tasks into Postgres with a scheduled time.
- Coordinator polls the DB and dispatches ready tasks to workers via gRPC (round-robin).
- Workers send heartbeats to the Coordinator and execute received commands, writing output to `worker_output/`.
- Workers report task status updates back to the Coordinator.

## Addresses and ports
- Coordinator listens on `:8080`. Workers must dial a resolvable host+port, e.g. `coordinator:8080` in Docker.
- Worker listens on `:8000`. It reports its address via heartbeats:
  - If `WORKER_ADDRESS` is set (e.g. `worker`), it reports `WORKER_ADDRESS + serverPort` → `worker:8000`.
  - If not set, it reports its listener bind address (often `0.0.0.0:8000`), which is typically NOT reachable from other containers.
- In `docker-compose.yml`, `WORKER_ADDRESS=worker` ensures the Coordinator can reach the worker at `worker:8000`.

## Run with Docker Compose
Environment variables required by Postgres (used by all services):
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Compose defines service names with internal DNS:
- Coordinator is reachable at `coordinator:8080`.
- Postgres is reachable at `postgres:5432`.
- Worker reports itself as `worker:8000`.

Start the stack:
```/dev/null/shell#L1-3
docker compose up --build --scale worker=n
where n is the number of workers

# Services: postgres, scheduler (:8081), coordinator (:8080), worker (:8000, internal)

# API usage (Scheduler)
Schedule a task (execute a shell command after N seconds):
```/dev/null/shell#L1-4
curl -X POST http://localhost:8081/schedule \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hello", "delay_seconds": 5}'
```

## Check task status:
```/dev/null/shell#L1-1
curl "http://localhost:8081/status?task_id=<TASK_ID>"
```

## Data and outputs
- Task outputs from workers are written to `worker_output/` on the host (mounted into the worker container at `/app/output`).
- Task lifecycle fields: scheduled_at, picked_at, started_at, completed_at, failed_at.

## Benchmarking
Run the benchmark script to test system performance:
```shell
./benchmark.sh
```

The benchmark suite tests:
1. **Task Scheduling Throughput**: How many tasks can be scheduled per second
2. **Task Execution Latency**: End-to-end time from scheduling to completion
3. **System Load Test**: Sustained load over 30 seconds
4. **Worker Scalability**: How the system scales with multiple workers

Configure the benchmark with environment variables:
```shell
NUM_TASKS=500 CONCURRENCY=20 WORKER_COUNT=5 ./benchmark.sh
```

Available options:
- `SCHEDULER_URL`: Scheduler endpoint (default: http://localhost:8081)
- `NUM_TASKS`: Number of tasks to schedule (default: 100)
- `DELAY_SECONDS`: Task delay in seconds (default: 1)
- `CONCURRENCY`: Concurrent requests (default: 10)
- `WORKER_COUNT`: Number of workers (default: 3)

Test with different worker counts:
```shell
docker compose up --build --scale worker=10
WORKER_COUNT=10 ./benchmark.sh
```

## Notes and caveats
- The worker's `coordinator` flag defaults to `:8080` (port-only). In containers, rely on Compose to pass a resolvable address. With Compose, the worker connects to `coordinator:8080`.
- If you run services outside Compose, pass explicit hostnames:
  - Worker: `--worker_port=:8000 --coordinator=localhost:8080`
  - Coordinator: `--coordinator_port=:8080`
  - Scheduler: `--scheduler_port=:8081`
- Multiple workers should each have unique, reachable addresses. If you set the same `WORKER_ADDRESS` for all workers, the Coordinator will treat them as distinct IDs but connect them all to the same endpoint, losing per-instance addressing.

>> NOTE: This is not for production use. It is intended for development and testing purposes only.
