#!/usr/bin/env bash
# ==============================================================================
# distrack - Local Development Startup Script
#
# Starts the full distrack distributed task scheduler system locally,
# including the LGTM observability stack (Loki, Grafana, Tempo, Prometheus,
# OpenTelemetry Collector) and core services (Postgres, Coordinator, Scheduler,
# Worker).
# ==============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Default configurations
WORKERS=1
BUILD=false
FOLLOW_LOGS=false
ACTION="up"
TIMEOUT_SECONDS=60

# Colors for terminal output
if [[ -t 1 ]]; then
  GREEN='\033[0;32m'
  YELLOW='\033[1;33m'
  BLUE='\033[0;34m'
  CYAN='\033[0;36m'
  RED='\033[0;31m'
  BOLD='\033[1m'
  DIM='\033[2m'
  NC='\033[0m' # No Color
else
  GREEN=''
  YELLOW=''
  BLUE=''
  CYAN=''
  RED=''
  BOLD=''
  DIM=''
  NC=''
fi

log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1" >&2
}

usage() {
  cat <<USAGE
${BOLD}Usage:${NC} $(basename "$0") [options]

${BOLD}Commands:${NC}
  up              Start all services locally (default)
  down, stop      Stop and tear down all services
  restart         Restart all services
  status          Show status of all running containers
  logs            Tail logs across all services

${BOLD}Options:${NC}
  -w, --workers N    Number of worker instances to launch (default: 1)
  -b, --build        Rebuild docker images before starting
  -f, --follow       Follow logs after services are ready
  -t, --timeout SEC  Timeout in seconds when waiting for service health (default: 60)
  -h, --help         Show this help message

${BOLD}Examples:${NC}
  $(basename "$0")                     # Start everything with 1 worker
  $(basename "$0") -w 3 -b             # Rebuild images and start with 3 workers
  $(basename "$0") down                # Tear down all local containers
  $(basename "$0") status              # Check health and status of containers
USAGE
}

# Parse CLI arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    up)
      ACTION="up"
      shift
      ;;
    down|stop)
      ACTION="down"
      shift
      ;;
    restart)
      ACTION="restart"
      shift
      ;;
    status)
      ACTION="status"
      shift
      ;;
    logs)
      ACTION="logs"
      shift
      ;;
    -w|--workers)
      if [[ -z "${2:-}" ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
        log_error "Option '$1' requires a positive integer argument."
        exit 1
      fi
      WORKERS="$2"
      shift 2
      ;;
    -b|--build)
      BUILD=true
      shift
      ;;
    -f|--follow)
      FOLLOW_LOGS=true
      shift
      ;;
    -t|--timeout)
      if [[ -z "${2:-}" ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
        log_error "Option '$1' requires a positive integer argument."
        exit 1
      fi
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

check_prerequisites() {
  if ! command -v docker >/dev/null 2>&1; then
    log_error "Docker is required but not installed or not in PATH."
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    log_error "Docker daemon is not running. Please start Docker and try again."
    exit 1
  fi

  # Determine compose command
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
  else
    log_error "Docker Compose (v2 or v1) is required but not found."
    exit 1
  fi
}

setup_environment() {
  # Load .env if present, otherwise set standard defaults
  if [[ -f "${REPO_ROOT}/.env" ]]; then
    log_info "Loading environment from .env"
    set -a
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.env"
    set +a
  else
    export POSTGRES_DB="${POSTGRES_DB:-distrack}"
    export POSTGRES_USER="${POSTGRES_USER:-distrack}"
    export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-password}"
  fi

  # Ensure data and output directories exist
  mkdir -p "${REPO_ROOT}/data"
  mkdir -p "${REPO_ROOT}/worker_output"
}

ensure_network() {
  if ! docker network inspect distrack-net >/dev/null 2>&1; then
    log_info "Creating Docker bridge network: distrack-net"
    docker network create distrack-net >/dev/null
    log_success "Network distrack-net created."
  else
    log_info "Docker network distrack-net already exists."
  fi
}

start_observability() {
  log_info "Starting LGTM Observability stack (OTel Collector, Prometheus, Tempo, Loki, Grafana)..."
  local build_arg=()
  if [[ "$BUILD" == true ]]; then
    build_arg=("--build")
  fi

  ${COMPOSE_CMD} -f "${REPO_ROOT}/config/docker-compose.lgtm.yml" up -d "${build_arg[@]}"
  log_success "Observability containers started."
}

start_distrack() {
  log_info "Starting Distrack core services (Postgres, Coordinator, Scheduler, ${WORKERS} Worker(s))..."
  local build_arg=()
  if [[ "$BUILD" == true ]]; then
    build_arg=("--build")
  fi

  ${COMPOSE_CMD} -f "${REPO_ROOT}/docker-compose.yml" up -d --scale worker="${WORKERS}" "${build_arg[@]}"
  log_success "Distrack core containers started."
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local desc="$3"
  local max_wait="$4"
  local elapsed=0

  log_info "Waiting for ${desc} (${host}:${port})..."
  while ! (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ $elapsed -ge $max_wait ]]; then
      log_warn "Timeout reached waiting for ${desc} on ${host}:${port}. It may still be starting."
      return 1
    fi
  done
  log_success "${desc} is ready (${elapsed}s)."
  return 0
}

wait_for_http() {
  local url="$1"
  local desc="$2"
  local max_wait="$3"
  local elapsed=0

  log_info "Waiting for ${desc} (${url})..."
  while true; do
    if curl -s -f -o /dev/null "$url" >/dev/null 2>&1; then
      log_success "${desc} is ready (${elapsed}s)."
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ $elapsed -ge $max_wait ]]; then
      log_warn "Timeout reached waiting for ${desc} at ${url}. It may still be initializing."
      return 1
    fi
  done
}

wait_for_health() {
  echo ""
  log_info "Verifying service readiness..."

  # 1. Observability services
  wait_for_port "127.0.0.1" 4317 "OTel Collector (gRPC)" 15 || true
  wait_for_http "http://localhost:9090/-/ready" "Prometheus" 20 || true
  wait_for_http "http://localhost:3200/ready" "Tempo" 20 || true
  wait_for_http "http://localhost:3100/ready" "Loki" 20 || true
  wait_for_http "http://localhost:3000/api/health" "Grafana Web UI" 25 || true

  # 2. Database & Core Services
  wait_for_port "127.0.0.1" 5433 "Postgres (Host Port 5433)" 20 || true
  wait_for_port "127.0.0.1" 8080 "Coordinator (gRPC)" 20 || true
  wait_for_port "127.0.0.1" 8081 "Scheduler (HTTP)" 20 || true
}

show_status() {
  echo ""
  echo -e "${BOLD}=== LGTM Observability Stack Status ===${NC}"
  ${COMPOSE_CMD} -f "${REPO_ROOT}/config/docker-compose.lgtm.yml" ps
  echo ""
  echo -e "${BOLD}=== Distrack Core Services Status ===${NC}"
  ${COMPOSE_CMD} -f "${REPO_ROOT}/docker-compose.yml" ps
}

show_summary() {
  echo ""
  echo -e "${GREEN}${BOLD}================================================================${NC}"
  echo -e "${GREEN}${BOLD}     Distrack & Observability Stack Started Successfully!       ${NC}"
  echo -e "${GREEN}${BOLD}================================================================${NC}"
  echo ""
  echo -e "${BOLD}Application Services:${NC}"
  echo -e "  - ${CYAN}Scheduler API${NC}:     http://localhost:8081"
  echo -e "  - ${CYAN}Coordinator gRPC${NC}:  localhost:8080"
  echo -e "  - ${CYAN}Postgres${NC}:          localhost:5433 (user: ${POSTGRES_USER:-distrack}, db: ${POSTGRES_DB:-distrack})"
  echo -e "  - ${CYAN}Worker Instances${NC}:  ${WORKERS} running (outputs written to ./worker_output/)"
  echo ""
  echo -e "${BOLD}Observability & Monitoring (LGTM Stack):${NC}"
  echo -e "  - ${CYAN}Grafana UI${NC}:        http://localhost:3000 ${DIM}(admin / admin)${NC}"
  echo -e "  - ${CYAN}Prometheus UI${NC}:     http://localhost:9090"
  echo -e "  - ${CYAN}Tempo Traces${NC}:      http://localhost:3200"
  echo -e "  - ${CYAN}Loki Logs${NC}:         http://localhost:3100"
  echo -e "  - ${CYAN}OTel Collector${NC}:    localhost:4317 (gRPC) / localhost:4318 (HTTP)"
  echo ""
  echo -e "${BOLD}Quick Verification Commands:${NC}"
  echo -e "  ${DIM}# Schedule a task:${NC}"
  echo -e "  curl -X POST http://localhost:8081/schedule \\"
  echo -e "    -H 'Content-Type: application/json' \\"
  echo -e "    -d '{\"command\":\"echo hello from distrack\", \"delay_seconds\": 2}'"
  echo ""
  echo -e "  ${DIM}# Run the benchmark suite:${NC}"
  echo -e "  ./scripts/benchmark.sh -u http://localhost:8081 -n 20 -d 0 -c 'echo test'"
  echo ""
  echo -e "  ${DIM}# View logs:${NC}"
  echo -e "  $(basename "$0") logs"
  echo ""
  echo -e "  ${DIM}# Tear down all containers:${NC}"
  echo -e "  $(basename "$0") down"
  echo -e "${GREEN}================================================================${NC}"
  echo ""
}

stop_all() {
  log_info "Stopping Distrack core services..."
  ${COMPOSE_CMD} -f "${REPO_ROOT}/docker-compose.yml" down || true

  log_info "Stopping LGTM Observability stack..."
  ${COMPOSE_CMD} -f "${REPO_ROOT}/config/docker-compose.lgtm.yml" down || true

  log_success "All services stopped."
}

# ------------------------------------------------------------------------------
# Main Flow
# ------------------------------------------------------------------------------
check_prerequisites

case "$ACTION" in
  down)
    stop_all
    ;;
  restart)
    stop_all
    setup_environment
    ensure_network
    start_observability
    start_distrack
    wait_for_health
    show_summary
    ;;
  status)
    show_status
    ;;
  logs)
    ${COMPOSE_CMD} -f "${REPO_ROOT}/docker-compose.yml" -f "${REPO_ROOT}/config/docker-compose.lgtm.yml" logs -f
    ;;
  up)
    setup_environment
    ensure_network
    start_observability
    start_distrack
    wait_for_health
    show_summary

    if [[ "$FOLLOW_LOGS" == true ]]; then
      log_info "Following logs across all services (Ctrl+C to stop viewing logs)..."
      ${COMPOSE_CMD} -f "${REPO_ROOT}/docker-compose.yml" logs -f
    fi
    ;;
esac
