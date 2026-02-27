#!/usr/bin/env bash

set -euo pipefail

# -----------------------------
# Configuration (env override)
# -----------------------------
SCHEDULER_URL="${SCHEDULER_URL:-http://localhost:8081}"
NUM_TASKS="${NUM_TASKS:-100}"
CONCURRENCY="${CONCURRENCY:-10}"
POLL_INTERVAL="${POLL_INTERVAL:-0.05}"  # 50ms polling
MAX_WAIT="${MAX_WAIT:-60}"

# -----------------------------
# Dependency checks
# -----------------------------
command -v curl >/dev/null || { echo "curl required"; exit 1; }
command -v bc >/dev/null || { echo "bc required"; exit 1; }

echo "=== Distrack Benchmark ==="
echo "Scheduler: $SCHEDULER_URL"
echo "Tasks: $NUM_TASKS"
echo "Concurrency: $CONCURRENCY"
echo ""

# -----------------------------
# Helpers
# -----------------------------

schedule_task() {
    local response
    response=$(curl -sf -X POST "$SCHEDULER_URL/schedule" \
        -H "Content-Type: application/json" \
        -d "{\"command\":\"echo test\",\"delay_seconds\":0}") || return 1

    echo "$response" | sed -n 's/.*"task_id":"\([^"]*\)".*/\1/p'
}

get_status() {
    curl -sf "$SCHEDULER_URL/status?task_id=$1" || return 1
}

wait_for_completion() {
    local task_id="$1"
    local waited=0

    while (( $(echo "$waited < $MAX_WAIT" | bc -l) )); do
        status=$(get_status "$task_id") || return 1

        if echo "$status" | grep -q '"status":"completed"'; then
            return 0
        fi

        sleep "$POLL_INTERVAL"
        waited=$(echo "$waited + $POLL_INTERVAL" | bc)
    done

    return 1
}

percentile() {
    local p=$1
    shift
    local arr=("$@")
    local count=${#arr[@]}
    local index=$(printf "%.0f" "$(echo "$p * ($count - 1)" | bc -l)")
    echo "${arr[$index]}"
}

# -----------------------------
# Benchmark 1: True Throughput
# -----------------------------

echo "Benchmark 1: End-to-End Throughput"

task_ids=()
start_time=$(date +%s.%N)

# Schedule with concurrency control
for ((i=0; i<NUM_TASKS; i++)); do
    while (( $(jobs -r | wc -l) >= CONCURRENCY )); do
        sleep 0.01
    done

    (
        id=$(schedule_task)
        if [[ -n "$id" ]]; then
            echo "$id"
        fi
    ) &
done

wait

# Collect task IDs from background output
task_ids=($(jobs -p)) # placeholder to avoid empty

# Actually reschedule properly and collect IDs synchronously
task_ids=()
for ((i=0; i<NUM_TASKS; i++)); do
    id=$(schedule_task)
    task_ids+=("$id")
done

# Wait for all tasks
for id in "${task_ids[@]}"; do
    wait_for_completion "$id" || echo "Task $id failed"
done

end_time=$(date +%s.%N)

duration=$(echo "$end_time - $start_time" | bc)
throughput=$(echo "scale=2; $NUM_TASKS / $duration" | bc)

echo "Total time: $duration seconds"
echo "Throughput: $throughput tasks/sec"
echo ""

# -----------------------------
# Benchmark 2: Latency Stats
# -----------------------------

echo "Benchmark 2: Latency Distribution"

latencies=()

for ((i=0; i<20; i++)); do
    start=$(date +%s.%N)
    id=$(schedule_task)

    wait_for_completion "$id" || continue

    end=$(date +%s.%N)
    latency=$(echo "$end - $start" | bc)
    latencies+=("$latency")
done

IFS=$'\n' sorted=($(sort -g <<<"${latencies[*]}"))
unset IFS

count=${#sorted[@]}

if (( count > 0 )); then
    sum=0
    for v in "${sorted[@]}"; do
        sum=$(echo "$sum + $v" | bc)
    done

    avg=$(echo "scale=4; $sum / $count" | bc)
    p50=$(percentile 0.50 "${sorted[@]}")
    p95=$(percentile 0.95 "${sorted[@]}")
    p99=$(percentile 0.99 "${sorted[@]}")

    echo "Requests: $count"
    echo "Average: $avg s"
    echo "p50: $p50 s"
    echo "p95: $p95 s"
    echo "p99: $p99 s"
else
    echo "No successful latency samples"
fi

echo ""
echo "=== Benchmark Complete ==="
