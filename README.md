![Request flow](/assets/request-flow.png) 

# distrack
A small distributed task scheduler composed of three services:
- Coordinator (gRPC, default :8080): tracks workers, assigns tasks, updates task status.
- Worker (gRPC, default :8000): executes shell commands and reports status.
- Scheduler (HTTP, default :8081): API to schedule tasks and query status.
- Postgres: storage for tasks.

## Architecture

- **Scheduler** (HTTP `:8081`): API to schedule tasks into Postgres with a target execution time. Injects W3C `traceparent` context into DB records.
- **Coordinator** (gRPC `:8080`): Polls Postgres for ready tasks, reifies trace context from the database, and dispatches tasks to registered workers via gRPC (round-robin).
- **Workers** (gRPC `:8000`): Send heartbeats to the Coordinator, execute assigned shell commands, write output to `worker_output/`, and report execution lifecycle status back to the Coordinator.
- **Postgres** (`:5433` host / `:5432` internal): Relational task queue and storage.
- **LGTM Observability Stack**: Built-in OpenTelemetry instrumentation for distributed tracing (Tempo), metrics (Prometheus), and structured logs (Loki) visualized in Grafana.

---

## Observability (LGTM Stack + OpenTelemetry)

`distrack` features end-to-end observability out of the box using **OpenTelemetry (OTel)** and the **LGTM Stack**:

```
 ┌───────────────┐     ┌─────────────────┐     ┌──────────────┐
 │   Scheduler   │     │   Coordinator   │     │  Worker(s)   │
 └───────┬───────┘     └────────┬────────┘     └──────┬───────┘
         │ (OTLP gRPC)          │ (OTLP gRPC)         │ (OTLP gRPC)
         └──────────────┬───────┴─────────────────────┘
                        ▼
         ┌──────────────────────────────┐
         │ OpenTelemetry Collector :4317│
         └──────┬───────────────┬───────┘
                │               │
        ┌───────┴──────┐ ┌──────┴────────┐ ┌────────────────┐
        │ Tempo :3200  │ │Prometheus:9090│ │  Loki :3100    │
        │   (Traces)   │ │   (Metrics)   │ │ (Corr. Logs)   │
        └───────┬──────┘ └──────┬────────┘ └──────┬─────────┘
                └───────────────┼─────────────────┘
                                ▼
                     ┌────────────────────┐
                     │   Grafana :3000    │
                     └────────────────────┘
```

### Telemetry Signals

1. **Distributed Tracing (Tempo)**:
   - Tracks requests seamlessly through **HTTP (Scheduler) → PostgreSQL (Asynchronous Context Reification) → gRPC (Coordinator) → gRPC (Worker)**.
   - W3C `traceparent` is persisted in the PostgreSQL `tasks` table, preserving causal context across asynchronous polling boundaries.
2. **Metrics (Prometheus)**:
   - Captures RED metrics (Request Rate, Error Rate, Duration) and worker saturation (USE method).
   - Exported periodically via OTLP gRPC to the OpenTelemetry Collector and scraped by Prometheus.
3. **Structured Logs (Loki)**:
   - High-performance structured logging with trace and span IDs injected automatically for bidirectional Log-to-Trace correlation in Grafana.
4. **Unified Visualizations (Grafana)**:
   - Pre-provisioned datasources for Prometheus, Tempo, and Loki with Trace-to-Logs and Trace-to-Metrics cross-navigation enabled.

### Observability Configuration Layout

All observability configurations reside in the [`config/`](file:///home/uttam/dev/distrack/config/) directory:

```
config/
├── docker-compose.lgtm.yml                       # LGTM stack Docker Compose specification
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── datasources.yaml                  # Pre-configured Grafana datasources
├── loki/
│   └── loki-config.yaml                          # Loki storage & TSDB schema config
├── otel-collector/
│   └── otel-collector-config.yaml                # OpenTelemetry Collector pipelines
├── prometheus/
│   └── prometheus.yml                            # Prometheus scrape configuration
└── tempo/
    └── tempo.yaml                                # Tempo trace storage & ingestion config
```

---

## Start the Stack Locally

Use the local startup script ([`./scripts/start-local.sh`](file:///home/uttam/dev/distrack/scripts/start-local.sh) or `./start-local.sh`) to start both the core services and the observability infrastructure with automated health checks:

```bash
# Start all services with 1 worker
./scripts/start-local.sh

# Rebuild container images and scale to 3 workers
./scripts/start-local.sh -w 3 -b

# Check health and status of all containers
./scripts/start-local.sh status

# Follow real-time logs across all services
./scripts/start-local.sh logs

# Gracefully stop and tear down all containers
./scripts/start-local.sh down
```

### Endpoints & Web UIs

| Service | Port / URL | Description |
|---|---|---|
| **Scheduler API** | `http://localhost:8081` | HTTP API for task scheduling and status |
| **Coordinator** | `localhost:8080` | gRPC coordinator service |
| **Postgres** | `localhost:5433` | Host port for PostgreSQL database |
| **Grafana** | `http://localhost:3000` | Web UI (**User**: `admin`, **Password**: `admin`) |
| **Prometheus** | `http://localhost:9090` | Prometheus Metrics Explorer |
| **Tempo** | `http://localhost:3200` | Distributed tracing backend |
| **Loki** | `http://localhost:3100` | Structured log aggregation API |
| **OTel Collector** | `localhost:4317` (gRPC) / `4318` (HTTP) | Telemetry ingestion endpoint |

---

## API Usage (Scheduler)

### Schedule a task
Schedule a shell command to execute after $N$ seconds:

```bash
curl -X POST http://localhost:8081/schedule \
  -H "Content-Type: application/json" \
  -d '{"command":"echo hello from distrack", "delay_seconds": 2}'
```

Response:
```json
{
  "task_id": "3743f36b-eb21-4b8c-82d3-6461c94c84f6",
  "command": "echo hello from distrack",
  "scheduled_at": "2026-09-10T13:55:15.590224Z"
}
```

### Check task status

```bash
curl "http://localhost:8081/status?task_id=3743f36b-eb21-4b8c-82d3-6461c94c84f6"
```

Response:
```json
{
  "task_id": "3743f36b-eb21-4b8c-82d3-6461c94c84f6",
  "command": "echo hello from distrack",
  "scheduled_at": "2026-09-10T13:55:15.590224Z",
  "picked_at": "2026-09-10T13:55:19.869541Z",
  "started_at": "2026-09-10T13:55:20.722359Z",
  "completed_at": "2026-09-10T13:55:20.742031Z"
}
```

---

## Data and Outputs

- Task execution outputs are saved to `worker_output/<worker_id>_<task_id>.txt` on the host (mounted to `/app/output` inside worker containers).
- Task lifecycle timestamp fields: `scheduled_at`, `picked_at`, `started_at`, `completed_at`, `failed_at`.

---

## Benchmark Script

Run end-to-end load tests against the Scheduler API using [`scripts/benchmark.sh`](file:///home/uttam/dev/distrack/scripts/benchmark.sh):

```bash
# Submit 100 tasks and measure throughput & latency
./scripts/benchmark.sh -u http://localhost:8081 -n 100 -d 0 -c "echo benchmark"
```

View options:
```bash
./scripts/benchmark.sh -h
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `distrack` | Database name |
| `POSTGRES_USER` | `distrack` | Database user |
| `POSTGRES_PASSWORD` | `password` | Database password |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OpenTelemetry Collector endpoint |
| `OTEL_SERVICE_NAME` | Service-specific | Service identifier in traces/metrics |
| `WORKER_ADDRESS` | `worker` | Hostname reported by worker to coordinator |
