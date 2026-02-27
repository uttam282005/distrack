#!/usr/bin/env bash
set -euo pipefail

SCHEDULER_URL="http://localhost:8081"
TASKS=50
DELAY_SECONDS=0
POLL_INTERVAL=0.5
TIMEOUT_SECONDS=120
COMMAND='echo benchmark'
CONNECT_TIMEOUT=3
REQUEST_TIMEOUT=10
MAX_RETRIES=3
RETRY_DELAY=0.2

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Robust end-to-end benchmark for distrack.
The script submits tasks via /schedule, polls /status until each task is completed/failed,
and prints throughput + latency metrics.

Options:
  -u URL     Scheduler base URL (default: ${SCHEDULER_URL})
  -n COUNT   Number of tasks to schedule (default: ${TASKS})
  -d SEC     delay_seconds for each task (default: ${DELAY_SECONDS})
  -p SEC     Poll interval in seconds (default: ${POLL_INTERVAL})
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

while getopts ":u:n:d:p:t:c:h" opt; do
  case "$opt" in
    u) SCHEDULER_URL="$OPTARG" ;;
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

now_ms() {
  date +%s%3N
}

normalize_base_url() {
  printf '%s' "$1" | sed 's:/*$::'
}

extract_task_id() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import base64
import json
import sys

raw = sys.argv[1]
if not raw:
    raise SystemExit(1)

try:
    value = json.loads(raw)
except Exception:
    raise SystemExit(1)

obj = None
if isinstance(value, dict):
    obj = value
elif isinstance(value, str):
    # Scheduler can return a base64-encoded JSON blob due to []byte encoding.
    try:
        decoded = base64.b64decode(value).decode("utf-8")
        nested = json.loads(decoded)
        if isinstance(nested, dict):
            obj = nested
    except Exception:
        pass

    if obj is None:
        try:
            nested = json.loads(value)
            if isinstance(nested, dict):
                obj = nested
        except Exception:
            pass

if not isinstance(obj, dict):
    raise SystemExit(1)

task_id = obj.get("task_id")
if not isinstance(task_id, str) or not task_id.strip():
    raise SystemExit(1)

print(task_id)
PY
}

extract_status() {
  local raw="$1"
  python3 - "$raw" <<'PY'
import json
import sys

raw = sys.argv[1]
if not raw:
    print("pending")
    raise SystemExit(0)

try:
    obj = json.loads(raw)
except Exception:
    print("pending")
    raise SystemExit(0)

if not isinstance(obj, dict):
    print("pending")
    raise SystemExit(0)

if obj.get("completed_at"):
    print("completed")
elif obj.get("failed_at"):
    print("failed")
else:
    print("pending")
PY
}

curl_json() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"

  local -i attempt=1
  local response body status

  while [ "$attempt" -le "$MAX_RETRIES" ]; do
    if [ "$method" = "GET" ]; then
      response=$(curl -sS \
        --connect-timeout "$CONNECT_TIMEOUT" \
        --max-time "$REQUEST_TIMEOUT" \
        -w $'\n%{http_code}' \
        -X GET "$url" 2>&1) || true
    else
      response=$(curl -sS \
        --connect-timeout "$CONNECT_TIMEOUT" \
        --max-time "$REQUEST_TIMEOUT" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' \
        -X "$method" "$url" \
        -d "$payload" 2>&1) || true
    fi

    status=${response##*$'\n'}
    body=${response%$'\n'*}

    if [[ "$status" =~ ^[0-9]{3}$ ]]; then
      if [ "$status" -ge 200 ] && [ "$status" -lt 300 ]; then
        printf '%s' "$body"
        return 0
      fi
      if [ "$status" -ge 500 ] && [ "$attempt" -lt "$MAX_RETRIES" ]; then
        sleep "$RETRY_DELAY"
        attempt=$((attempt + 1))
        continue
      fi
      echo "HTTP $status from $url: $body" >&2
      return 1
    fi

    if [ "$attempt" -lt "$MAX_RETRIES" ]; then
      sleep "$RETRY_DELAY"
      attempt=$((attempt + 1))
      continue
    fi

    echo "Request failed for $url: $response" >&2
    return 1
  done

  return 1
}

percentile_95() {
  python3 - "$1" <<'PY'
import sys
vals = [int(x) for x in open(sys.argv[1], encoding='utf-8') if x.strip()]
if not vals:
    print("n/a n/a n/a n/a")
    raise SystemExit(0)
vals.sort()
n = len(vals)
idx = max(0, min(n - 1, int((n * 0.95) - 1)))
print(vals[0], round(sum(vals)/n, 2), vals[idx], vals[-1])
PY
}

SCHEDULER_URL="$(normalize_base_url "$SCHEDULER_URL")"

declare -a TASK_IDS=()
declare -a FAILED_IDS=()
declare -a TIMED_OUT_IDS=()
declare -A SUBMIT_TIME_MS=()
declare -A FINISH_TIME_MS=()

echo "Starting benchmark"
echo "Scheduler URL : $SCHEDULER_URL"
echo "Tasks         : $TASKS"
echo "Delay seconds : $DELAY_SECONDS"
echo "Poll interval : $POLL_INTERVAL"
echo "Timeout       : $TIMEOUT_SECONDS"
echo "Command       : $COMMAND"
echo

echo "Submitting tasks..."
submit_start_ms=$(now_ms)

for i in $(seq 1 "$TASKS"); do
  payload=$(python3 - "$COMMAND" "$DELAY_SECONDS" <<'PY'
import json, sys
print(json.dumps({"command": sys.argv[1], "delay_seconds": int(sys.argv[2])}))
PY
)

  if ! response=$(curl_json "POST" "$SCHEDULER_URL/schedule" "$payload"); then
    echo "Submission failed for task #$i" >&2
    continue
  fi

  if ! task_id=$(extract_task_id "$response"); then
    echo "Could not parse task_id for task #$i. Response: $response" >&2
    continue
  fi

  TASK_IDS+=("$task_id")
  SUBMIT_TIME_MS["$task_id"]=$(now_ms)
done

submit_end_ms=$(now_ms)
submitted_count=${#TASK_IDS[@]}
if [ "$submitted_count" -eq 0 ]; then
  echo "No tasks submitted successfully. Exiting." >&2
  exit 1
fi

echo "Submitted $submitted_count/$TASKS task(s)."
echo "Polling task statuses..."

wait_start_ms=$(now_ms)
timeout_ms=$(python3 - <<PY
print(int(float("$TIMEOUT_SECONDS")*1000))
PY
)
deadline_ms=$((wait_start_ms + timeout_ms))
pending=("${TASK_IDS[@]}")

while [ "${#pending[@]}" -gt 0 ]; do
  current_ms=$(now_ms)
  if [ "$current_ms" -ge "$deadline_ms" ]; then
    TIMED_OUT_IDS=("${pending[@]}")
    break
  fi

  next_pending=()
  for task_id in "${pending[@]}"; do
    if ! status_response=$(curl_json "GET" "$SCHEDULER_URL/status?task_id=$task_id"); then
      next_pending+=("$task_id")
      continue
    fi

    status=$(extract_status "$status_response")
    case "$status" in
      completed)
        FINISH_TIME_MS["$task_id"]=$current_ms
        ;;
      failed)
        FINISH_TIME_MS["$task_id"]=$current_ms
        FAILED_IDS+=("$task_id")
        ;;
      *)
        next_pending+=("$task_id")
        ;;
    esac
  done

  pending=("${next_pending[@]}")
  [ "${#pending[@]}" -gt 0 ] && sleep "$POLL_INTERVAL"
done

end_ms=$(now_ms)
completed_count=$((submitted_count - ${#FAILED_IDS[@]} - ${#TIMED_OUT_IDS[@]}))
failed_count=${#FAILED_IDS[@]}
timeout_count=${#TIMED_OUT_IDS[@]}
submit_duration_ms=$((submit_end_ms - submit_start_ms))
total_duration_ms=$((end_ms - submit_start_ms))

lat_file=$(mktemp)
for task_id in "${TASK_IDS[@]}"; do
  finish=${FINISH_TIME_MS[$task_id]:-}
  submit=${SUBMIT_TIME_MS[$task_id]:-}
  if [ -n "$finish" ] && [ -n "$submit" ]; then
    echo "$((finish - submit))" >> "$lat_file"
  fi
done

read -r min_ms avg_ms p95_ms max_ms < <(percentile_95 "$lat_file")
rm -f "$lat_file"

echo
echo "=== Benchmark Results ==="
printf "Submitted tasks      : %d\n" "$submitted_count"
printf "Completed tasks      : %d\n" "$completed_count"
printf "Failed tasks         : %d\n" "$failed_count"
printf "Timed out tasks      : %d\n" "$timeout_count"
printf "Submit duration      : %.3fs\n" "$(python3 - <<PY
print($submit_duration_ms/1000)
PY
)"
printf "Total duration       : %.3fs\n" "$(python3 - <<PY
print($total_duration_ms/1000)
PY
)"
printf "Throughput (complete): %.2f tasks/s\n" "$(python3 - <<PY
completed=$completed_count
duration=$total_duration_ms/1000
print((completed/duration) if duration > 0 else 0)
PY
)"
printf "E2E latency min/avg/p95/max (ms): %s / %s / %s / %s\n" "$min_ms" "$avg_ms" "$p95_ms" "$max_ms"

if [ "$timeout_count" -gt 0 ]; then
  echo
  echo "Timed out task IDs:"
  printf '%s\n' "${TIMED_OUT_IDS[@]}"
fi

if [ "$failed_count" -gt 0 ]; then
  echo
  echo "Failed task IDs:"
  printf '%s\n' "${FAILED_IDS[@]}"
fi
