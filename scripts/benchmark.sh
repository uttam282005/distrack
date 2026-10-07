#!/usr/bin/env bash
set -euo pipefail

SCHEDULER_URL="http://localhost:8081"
PROMETHEUS_URL="http://localhost:9090"
TASKS=50
DELAY_SECONDS=0
POLL_INTERVAL=0.5
TIMEOUT_SECONDS=120
COMMAND='echo benchmark'
CONNECT_TIMEOUT=3
REQUEST_TIMEOUT=30
MAX_RETRIES=3
RETRY_DELAY=0.5

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

High-throughput batch benchmark for distrack.
Submits tasks in a single batch via /schedule/batch, monitors execution
via Prometheus metrics without per-task status polling, and computes
whole-system throughput using:
  Throughput = number_of_tasks / (max(task_completed_time) - min(task_submission_time))

Options:
  -u URL     Scheduler base URL (default: ${SCHEDULER_URL})
  -m URL     Prometheus base URL (default: ${PROMETHEUS_URL})
  -n COUNT   Number of tasks to schedule in batch (default: ${TASKS})
  -d SEC     delay_seconds for each task (default: ${DELAY_SECONDS})
  -p SEC     Progress check interval in seconds (default: ${POLL_INTERVAL})
  -t SEC     Overall timeout in seconds (default: ${TIMEOUT_SECONDS})
  -c CMD     Command payload for each task (default: ${COMMAND})
  -h         Show this help
USAGE
}

is_positive_int() {
  [[ "$1" =~ ^[0-9]+$ ]] && [ "$1" -gt 0 ]
}

is_non_negative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_number() {
  [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]] && awk "BEGIN {exit !($1 > 0)}"
}

while getopts ":u:m:n:d:p:t:c:h" opt; do
  case "$opt" in
    u) SCHEDULER_URL="$OPTARG" ;;
    m) PROMETHEUS_URL="$OPTARG" ;;
    n) TASKS="$OPTARG" ;;
    d) DELAY_SECONDS="$OPTARG" ;;
    p) POLL_INTERVAL="$OPTARG" ;;
    t) TIMEOUT_SECONDS="$OPTARG" ;;
    c) COMMAND="$OPTARG" ;;
    h)
      usage
      exit 0
      ;;
    :)
      echo "Missing value for -$OPTARG" >&2
      usage
      exit 1
      ;;
    \?)
      echo "Unknown option: -$OPTARG" >&2
      usage
      exit 1
      ;;
  esac
done

if ! is_positive_int "$TASKS"; then
  echo "-n COUNT must be a positive integer" >&2
  exit 1
fi
if ! is_non_negative_int "$DELAY_SECONDS"; then
  echo "-d SEC must be a non-negative integer" >&2
  exit 1
fi
if ! is_positive_number "$POLL_INTERVAL"; then
  echo "-p SEC must be a positive number" >&2
  exit 1
fi
if ! is_positive_number "$TIMEOUT_SECONDS"; then
  echo "-t SEC must be a positive number" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

now_epoch() {
  date +%s.%3N
}

normalize_base_url() {
  printf '%s' "$1" | sed 's:/*$::'
}

SCHEDULER_URL="$(normalize_base_url "$SCHEDULER_URL")"
PROMETHEUS_URL="$(normalize_base_url "$PROMETHEUS_URL")"

# Query Prometheus instant vector query
query_prom() {
  local query="$1"
  python3 - "$PROMETHEUS_URL" "$query" <<'PY'
import json
import sys
import urllib.parse
import urllib.request

base_url = sys.argv[1].rstrip('/')
query = sys.argv[2]
url = f"{base_url}/api/v1/query?query={urllib.parse.quote(query)}"

try:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        results = data.get("data", {}).get("result", [])
        if results and "value" in results[0]:
            val = float(results[0]["value"][1])
            print(val)
        else:
            print("NaN")
except Exception:
    print("NaN")
PY
}

echo "=========================================================="
echo " Starting Distrack Batch Benchmark (Prometheus Telemetry) "
echo "=========================================================="
echo "Scheduler URL : $SCHEDULER_URL"
echo "Prometheus URL: $PROMETHEUS_URL"
echo "Tasks         : $TASKS"
echo "Delay seconds : $DELAY_SECONDS"
echo "Check interval: $POLL_INTERVAL"
echo "Timeout       : $TIMEOUT_SECONDS"
echo "Command       : $COMMAND"
echo

# 1. Verify Connectivity
echo "[1/4] Checking Prometheus connection..."
prom_test=$(query_prom "up or vector(1)")
if [ "$prom_test" = "NaN" ]; then
  echo "Error: Unable to connect to Prometheus at $PROMETHEUS_URL" >&2
  echo "Please verify that the LGTM stack is running (e.g. ./scripts/start-local.sh)" >&2
  exit 1
fi

# 2. Capture Initial Baseline Metrics from Prometheus
echo "[2/4] Capturing baseline metrics from Prometheus..."
initial_executed=$(query_prom "sum(distrack_tasks_executed_total) or vector(0)")
initial_success=$(query_prom "sum(distrack_tasks_executed_total{status='success'}) or vector(0)")
initial_failed=$(query_prom "sum(distrack_tasks_executed_total{status='failed'}) or vector(0)")

if [ "$initial_executed" = "NaN" ]; then initial_executed=0; fi
if [ "$initial_success" = "NaN" ]; then initial_success=0; fi
if [ "$initial_failed" = "NaN" ]; then initial_failed=0; fi

printf "Baseline: executed=%.0f, success=%.0f, failed=%.0f\n" "$initial_executed" "$initial_success" "$initial_failed"

# 3. Generate and Submit Batch Request
echo "[3/4] Preparing batch payload for $TASKS tasks..."
batch_payload=$(python3 - "$COMMAND" "$DELAY_SECONDS" "$TASKS" <<'PY'
import json, sys
cmd = sys.argv[1]
delay = int(sys.argv[2])
n = int(sys.argv[3])
payload = {
    "tasks": [{"command": cmd, "delay_seconds": delay} for _ in range(n)]
}
print(json.dumps(payload))
PY
)

echo "Submitting batch to $SCHEDULER_URL/schedule/batch..."
min_submission_time=$(now_epoch)

submit_response=$(curl -sS \
  --connect-timeout "$CONNECT_TIMEOUT" \
  --max-time "$REQUEST_TIMEOUT" \
  -H "Content-Type: application/json" \
  -w "\n%{http_code}" \
  -X POST "$SCHEDULER_URL/schedule/batch" \
  -d "$batch_payload") || true

http_code=$(echo "$submit_response" | tail -n1)
resp_body=$(echo "$submit_response" | sed '$d')

if [ "$http_code" != "200" ]; then
  echo "Batch submission failed with HTTP $http_code: $resp_body" >&2
  exit 1
fi

submission_end_time=$(now_epoch)
submitted_count=$(python3 - "$resp_body" <<'PY'
import json, sys
try:
    data = json.loads(sys.argv[1])
    print(data.get("count", len(data.get("tasks", []))))
except Exception:
    print(0)
PY
)

if [ "$submitted_count" -le 0 ]; then
  echo "Invalid or empty response from scheduler: $resp_body" >&2
  exit 1
fi

submit_duration_sec=$(python3 - "$submission_end_time" "$min_submission_time" <<'PY'
import sys
print(round(float(sys.argv[1]) - float(sys.argv[2]), 4))
PY
)

printf "Successfully submitted %d tasks in batch (took %.3fs).\n" "$submitted_count" "$submit_duration_sec"

# 4. Monitor Execution via Prometheus (NO polling of /status)
echo "[4/4] Monitoring task execution via Prometheus (no /status polling)..."

target_executed=$(python3 - "$initial_executed" "$submitted_count" <<'PY'
import sys
print(float(sys.argv[1]) + float(sys.argv[2]))
PY
)

wait_start=$(now_epoch)
max_completed_time=""
timed_out=false

while true; do
  current_now=$(now_epoch)
  elapsed=$(python3 - "$current_now" "$min_submission_time" <<'PY'
import sys
print(round(float(sys.argv[1]) - float(sys.argv[2]), 2))
PY
)

  current_executed=$(query_prom "sum(distrack_tasks_executed_total) or vector(0)")
  if [ "$current_executed" != "NaN" ]; then
    completed_so_far=$(python3 - "$current_executed" "$initial_executed" <<'PY'
import sys
print(max(0, int(float(sys.argv[1]) - float(sys.argv[2]))))
PY
)
    printf "\r[Progress] Executed: %d / %d tasks (%.1fs elapsed)..." "$completed_so_far" "$submitted_count" "$elapsed"
    
    # Check if target reached
    is_done=$(python3 - "$current_executed" "$target_executed" <<'PY'
import sys
print("yes" if float(sys.argv[1]) >= float(sys.argv[2]) else "no")
PY
)
    if [ "$is_done" = "yes" ]; then
      max_completed_time=$(now_epoch)
      break
    fi
  fi

  # Check timeout
  is_timeout=$(python3 - "$elapsed" "$TIMEOUT_SECONDS" <<'PY'
import sys
print("yes" if float(sys.argv[1]) >= float(sys.argv[2]) else "no")
PY
)
  if [ "$is_timeout" = "yes" ]; then
    timed_out=true
    max_completed_time=$(now_epoch)
    break
  fi

  sleep "$POLL_INTERVAL"
done

echo
echo

# Final Metrics Collection from Prometheus
final_executed=$(query_prom "sum(distrack_tasks_executed_total) or vector(0)")
final_success=$(query_prom "sum(distrack_tasks_executed_total{status='success'}) or vector(0)")
final_failed=$(query_prom "sum(distrack_tasks_executed_total{status='failed'}) or vector(0)")

tasks_completed=$(python3 - "$final_success" "$initial_success" <<'PY'
import sys
print(max(0, int(float(sys.argv[1]) - float(sys.argv[2]))))
PY
)

tasks_failed=$(python3 - "$final_failed" "$initial_failed" <<'PY'
import sys
print(max(0, int(float(sys.argv[1]) - float(sys.argv[2]))))
PY
)

total_processed=$((tasks_completed + tasks_failed))

# Throughput using the user's proposed formula:
# number_of_tasks / (max(task_completed_time) - min(task_submission_time))
read -r throughput total_duration < <(python3 - "$total_processed" "$max_completed_time" "$min_submission_time" <<'PY'
import sys
n = float(sys.argv[1])
max_comp = float(sys.argv[2])
min_sub = float(sys.argv[3])
duration = max(0.0001, max_comp - min_sub)
tput = n / duration
print(f"{tput:.2f} {duration:.3f}")
PY
)

# Fetch Prometheus Latency Percentiles (exact ground-truth from OTel histograms)
exec_p50=$(query_prom "histogram_quantile(0.50, sum(increase(distrack_task_execution_duration_seconds_bucket[5m])) by (le))")
exec_p95=$(query_prom "histogram_quantile(0.95, sum(increase(distrack_task_execution_duration_seconds_bucket[5m])) by (le))")
exec_p99=$(query_prom "histogram_quantile(0.99, sum(increase(distrack_task_execution_duration_seconds_bucket[5m])) by (le))")
exec_avg=$(query_prom "sum(increase(distrack_task_execution_duration_seconds_sum[5m])) / sum(increase(distrack_task_execution_duration_seconds_count[5m]))")
dispatch_p95=$(query_prom "histogram_quantile(0.95, sum(increase(distrack_coordinator_dispatch_duration_seconds_bucket[5m])) by (le))")
active_workers=$(query_prom "distrack_coordinator_active_workers or vector(0)")
max_queue=$(query_prom "max_over_time(distrack_worker_queue_depth[5m]) or vector(0)")

fmt_metric() {
  local val="$1"
  if [ "$val" = "NaN" ]; then
    echo "n/a"
  else
    python3 -c "import sys; print(f'{float(sys.argv[1])*1000:.2f} ms')" "$val"
  fi
}

fmt_num() {
  local val="$1"
  if [ "$val" = "NaN" ]; then
    echo "n/a"
  else
    python3 -c "import sys; print(f'{float(sys.argv[1]):.0f}')" "$val"
  fi
}

echo "=========================================================="
echo "                  Benchmark Results                       "
echo "=========================================================="
printf "Submitted tasks in batch : %d\n" "$submitted_count"
printf "Completed tasks (success): %d\n" "$tasks_completed"
printf "Failed tasks             : %d\n" "$tasks_failed"
if [ "$timed_out" = true ]; then
  printf "Status                   : TIMED OUT (after %ss)\n" "$TIMEOUT_SECONDS"
else
  printf "Status                   : COMPLETED\n"
fi
echo "----------------------------------------------------------"
echo " Whole System Throughput (Proposed Formula):"
echo "   Throughput = number_of_tasks / (max(completed) - min(submitted))"
printf "   Formula calculation   : %d tasks / %.3fs\n" "$total_processed" "$total_duration"
printf "   Whole System Throughput: %s tasks/sec\n" "$throughput"
echo "----------------------------------------------------------"
echo " Prometheus Ground-Truth Latency Breakdown:"
printf "   Worker Execution p50  : %s\n" "$(fmt_metric "$exec_p50")"
printf "   Worker Execution p95  : %s\n" "$(fmt_metric "$exec_p95")"
printf "   Worker Execution p99  : %s\n" "$(fmt_metric "$exec_p99")"
printf "   Worker Execution Avg  : %s\n" "$(fmt_metric "$exec_avg")"
printf "   Coordinator Dispatch p95: %s\n" "$(fmt_metric "$dispatch_p95")"
echo "----------------------------------------------------------"
echo " Cluster Saturation Signals (Prometheus USE):"
printf "   Active Workers in Pool: %s\n" "$(fmt_num "$active_workers")"
printf "   Peak Worker Queue Depth: %s tasks\n" "$(fmt_num "$max_queue")"
echo "=========================================================="
