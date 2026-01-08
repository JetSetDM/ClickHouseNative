#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CLICKHOUSE_CONTAINER_NAME:-clickhouse-native-test}"
IMAGE="${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:25.9.2.1}"
IMAGE_MATRIX="${CLICKHOUSE_IMAGE_MATRIX:-}"
TLS_CONTAINER_NAME="${CLICKHOUSE_TLS_CONTAINER_NAME:-${CONTAINER_NAME}-tls}"

HOST_PORT_TCP="${CLICKHOUSE_TCP_PORT:-9000}"
HOST_PORT_HTTP="${CLICKHOUSE_HTTP_PORT:-8123}"
HOST_PORT_TCP_SECURE="${CLICKHOUSE_TCP_SECURE_PORT:-9440}"

USER_NAME="${CLICKHOUSE_USER:-default}"
PASSWORD="${CLICKHOUSE_PASSWORD:-default}"

STEP_TIMEOUT_UP="${CLICKHOUSE_TIMEOUT_UP:-300}"          # seconds
STEP_TIMEOUT_WAIT="${CLICKHOUSE_TIMEOUT_WAIT:-120}"      # seconds (overall wait)
STEP_TIMEOUT_WAIT_TRY="${CLICKHOUSE_TIMEOUT_WAIT_TRY:-5}" # seconds (per docker exec try)
STEP_TIMEOUT_TEST="${CLICKHOUSE_TIMEOUT_TEST:-900}"      # seconds (per swift test invocation)
STEP_TIMEOUT_TOTAL="${CLICKHOUSE_TIMEOUT_TOTAL:-1800}"   # seconds (entire script)
STEP_TIMEOUT_RESTART="${CLICKHOUSE_TIMEOUT_RESTART:-600}" # seconds (restart suite)
SWIFT_SCRATCH_PATH="${CLICKHOUSE_SWIFT_SCRATCH_PATH:-}"
RUN_RESTART_TESTS="${CLICKHOUSE_RUN_RESTART_TESTS:-1}"
RUN_TLS_TESTS="${CLICKHOUSE_RUN_TLS_TESTS:-1}"
RESTART_MODE="${CLICKHOUSE_RESTART_MODE:-probe}" # probe|swift-test
VERBOSE="${CLICKHOUSE_VERBOSE:-0}"

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { printf '[%s] %s\n' "$(ts)" "$*" >&2; }

if [[ "$VERBOSE" == "1" ]]; then
  export PS4='+ [$(date "+%H:%M:%S")] ${BASH_SOURCE##*/}:${LINENO}: '
  set -x
fi

have_cmd() { command -v "$1" >/dev/null 2>&1; }

repo_root() {
  (cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
}

CURRENT_CHILD_PID=""
CURRENT_CHILD_PGID=""
CURRENT_CHILD_CMD=""
TOTAL_WATCHDOG_PID=""
SCRIPT_PID="$$"

on_abort() {
  local reason="${1:-aborted}"
  log "ABORT: $reason"
  log "Last command: ${CURRENT_CHILD_CMD:-<none>}"
  dump_diagnostics
  cleanup_swiftpm_helpers
  exit 124
}

start_total_watchdog() {
  local seconds="$1"
  if [[ "$seconds" == "0" ]]; then
    return 0
  fi
  (
    sleep "$seconds"
    log "ERROR: total timeout reached (${seconds}s)"
    log "Last command: ${CURRENT_CHILD_CMD:-<none>}"
    dump_diagnostics
    cleanup_swiftpm_helpers
    if [[ -n "${CURRENT_CHILD_PGID:-}" ]]; then
      kill -TERM "-$CURRENT_CHILD_PGID" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "-$CURRENT_CHILD_PGID" >/dev/null 2>&1 || true
    elif [[ -n "${CURRENT_CHILD_PID:-}" ]]; then
      kill -TERM "$CURRENT_CHILD_PID" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "$CURRENT_CHILD_PID" >/dev/null 2>&1 || true
    fi
    kill -TERM "$SCRIPT_PID" >/dev/null 2>&1 || true
  ) &
  TOTAL_WATCHDOG_PID="$!"
}

stop_total_watchdog() {
  if [[ -n "${TOTAL_WATCHDOG_PID:-}" ]]; then
    kill -TERM "$TOTAL_WATCHDOG_PID" >/dev/null 2>&1 || true
    wait "$TOTAL_WATCHDOG_PID" >/dev/null 2>&1 || true
    TOTAL_WATCHDOG_PID=""
  fi
}

cleanup_swiftpm_helpers() {
  if ! have_cmd pgrep; then
    return 0
  fi

  local root
  root="$(repo_root)"

  local build_root="$root/.build"
  if [[ -n "${SWIFT_SCRATCH_PATH:-}" ]]; then
    build_root="$SWIFT_SCRATCH_PATH"
  fi
  local tools_build_root="$root/Tools/.build"
  if [[ -n "${SWIFT_SCRATCH_PATH:-}" ]]; then
    tools_build_root="${SWIFT_SCRATCH_PATH}-tools"
  fi

  # Always clear stale lock files (swiftpm sometimes leaves them behind after an interrupted run).
  rm -f "$root/.build/.lock" >/dev/null 2>&1 || true
  rm -f "$root/.build-docker-scratch/.lock" >/dev/null 2>&1 || true
  rm -f "$build_root/.lock" >/dev/null 2>&1 || true
  rm -f "$tools_build_root/.lock" >/dev/null 2>&1 || true

  local pids
  pids="$(
    pgrep -fl 'swiftpm-testing-helper' \
      | grep -F -e "$build_root" -e "$tools_build_root" \
      | awk '{print $1}' \
      | tr '\n' ' ' \
      || true
  )"

  if [[ -z "$pids" ]]; then
    return 0
  fi

  log "Cleaning up stale swiftpm-testing-helper PIDs: $pids"
  kill -TERM $pids >/dev/null 2>&1 || true
  sleep 1
  kill -KILL $pids >/dev/null 2>&1 || true
}

swift_build_probe() {
  local root tools_root scratch
  root="$(repo_root)"
  tools_root="$root/Tools"
  local args=(build -c debug --package-path "$tools_root" --product ClickHouseNativeProbe)
  if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
    scratch="${SWIFT_SCRATCH_PATH}-tools"
    args+=(--scratch-path "$scratch")
  fi
  log "=== swift build ClickHouseNativeProbe (timeout=${STEP_TIMEOUT_TEST}s) ==="
  cleanup_swiftpm_helpers
  run_with_timeout "$STEP_TIMEOUT_TEST" swift "${args[@]}"
}

probe_bin_path() {
  local root tools_root
  root="$(repo_root)"
  tools_root="$root/Tools"
  if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
    # If scratch path is set, assume the standard SwiftPM layout.
    echo "${SWIFT_SCRATCH_PATH}-tools"
    return 0
  fi
  echo "$tools_root/.build"
}

run_probe_docker_suite() {
  local root
  root="$(repo_root)"

  if ! swift_build_probe; then
    log "ERROR: failed to build ClickHouseNativeProbe"
    return 1
  fi

  local base
  base="$(probe_bin_path)"

  # Resolve the actual platform triple directory.
  local exe
  exe="$(find "$base" -maxdepth 3 -type f -name 'ClickHouseNativeProbe' -path '*/debug/*' 2>/dev/null | head -n 1 || true)"
  if [[ -z "$exe" ]]; then
    log "ERROR: could not find built ClickHouseNativeProbe under: $base"
    return 1
  fi

  log "=== probe docker-suite (exe=$(basename "$exe"), timeout=${STEP_TIMEOUT_RESTART}s) ==="
  cleanup_swiftpm_helpers
  run_with_timeout "$STEP_TIMEOUT_RESTART" "$exe" docker-suite
}

run_with_timeout() {
  local seconds="$1"
  shift

  if [[ "$seconds" == "0" ]]; then
    "$@"
    return $?
  fi

  # Manual watchdog that kills the whole process group (avoids orphaned swiftpm-testing-helper).
  local pid=""
  local pgid=""
  local status=0
  local timeout_flag
  timeout_flag="$(mktemp -t chn_timeout.XXXXXX 2>/dev/null || echo "/tmp/chn_timeout.$$")"
  rm -f "$timeout_flag" >/dev/null 2>&1 || true

  CURRENT_CHILD_CMD="$*"

  if have_cmd setsid; then
    setsid "$@" &
    pid="$!"
    pgid="$pid"
  else
    "$@" &
    pid="$!"
    pgid=""
  fi
  CURRENT_CHILD_PID="$pid"
  CURRENT_CHILD_PGID="$pgid"

  (
    sleep "$seconds"
    if kill -0 "$pid" >/dev/null 2>&1; then
      echo 1 >"$timeout_flag" 2>/dev/null || true
      if [[ -n "$pgid" ]]; then
        kill -TERM "-$pgid" >/dev/null 2>&1 || true
      else
        kill -TERM "$pid" >/dev/null 2>&1 || true
      fi
      sleep 2
      if [[ -n "$pgid" ]]; then
        kill -KILL "-$pgid" >/dev/null 2>&1 || true
      else
        kill -KILL "$pid" >/dev/null 2>&1 || true
      fi
    fi
  ) &
  local watchdog_pid="$!"

  wait "$pid" || status=$?
  kill -TERM "$watchdog_pid" >/dev/null 2>&1 || true
  wait "$watchdog_pid" >/dev/null 2>&1 || true

  if [[ -f "$timeout_flag" ]]; then
    rm -f "$timeout_flag" >/dev/null 2>&1 || true
    log "Timed out after ${seconds}s: $CURRENT_CHILD_CMD"
  fi
  rm -f "$timeout_flag" >/dev/null 2>&1 || true
  CURRENT_CHILD_PID=""
  CURRENT_CHILD_PGID=""
  CURRENT_CHILD_CMD=""

  return "$status"
}

dump_diagnostics() {
  log "--- diagnostics (container=$CONTAINER_NAME) ---"
  docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' | sed -n '1,10p' >&2 || true
  if docker ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME" 2>/dev/null; then
    docker logs --tail 80 "$CONTAINER_NAME" >&2 || true
  fi
  if have_cmd pgrep; then
    log "--- swiftpm processes ---"
    pgrep -fl 'swiftpm-testing-helper|swift-test|swift-package|swift-build' >&2 || true
  fi
  log "--- end diagnostics ---"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") <up|wait|down|test>

Env:
  CLICKHOUSE_CONTAINER_NAME   (default: clickhouse-native-test)
  CLICKHOUSE_IMAGE            (default: clickhouse/clickhouse-server:25.9.2.1)
  CLICKHOUSE_IMAGE_MATRIX     comma-separated list of images to test
  CLICKHOUSE_TCP_PORT         (default: 9000)
  CLICKHOUSE_HTTP_PORT        (default: 8123)
  CLICKHOUSE_USER             (default: default)
  CLICKHOUSE_PASSWORD         (default: default)
  CLICKHOUSE_TIMEOUT_UP        seconds (default: 300)
  CLICKHOUSE_TIMEOUT_WAIT      seconds (default: 120)
  CLICKHOUSE_TIMEOUT_WAIT_TRY  seconds (default: 5)
  CLICKHOUSE_TIMEOUT_TEST      seconds (default: 900)
  CLICKHOUSE_TIMEOUT_TOTAL     seconds (default: 1800)
  CLICKHOUSE_TIMEOUT_RESTART   seconds (default: 600)
  CLICKHOUSE_SWIFT_SCRATCH_PATH  path (default: empty)
  CLICKHOUSE_RUN_RESTART_TESTS   0|1 (default: 1)
  CLICKHOUSE_RUN_TLS_TESTS       0|1 (default: 1)
  CLICKHOUSE_STRESS_TESTS        0|1 (default: 0)
  CLICKHOUSE_TLS_CONTAINER_NAME  (default: <container>-tls)
  CLICKHOUSE_TCP_SECURE_PORT     host port for TLS (default: 9440)
  CLICKHOUSE_RESTART_MODE        probe|swift-test (default: probe)
  CLICKHOUSE_VERBOSE             0|1 (default: 0)

Examples:
  Scripts/clickhouse-docker.sh up
  Scripts/clickhouse-docker.sh test
EOF
}

is_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"
}

is_running_tls() {
  docker ps --format '{{.Names}}' | grep -Fxq "$TLS_CONTAINER_NAME"
}

gen_tls_assets() {
  local root
  root="$(repo_root)"
  local dir="$root/.clickhouse-tls"
  mkdir -p "$dir"

  if [[ -f "$dir/server.crt" && -f "$dir/server.key" && -f "$dir/ssl.xml" && -f "$dir/ca.crt" && -f "$dir/client.crt" && -f "$dir/client.key" ]]; then
    return 0
  fi

  log "Generating TLS assets in: $dir"
  rm -f "$dir/ca.key" "$dir/ca.crt" "$dir/server.key" "$dir/server.csr" "$dir/server.crt" "$dir/openssl.cnf" \
    "$dir/client.key" "$dir/client.csr" "$dir/client.crt" "$dir/client_ext.cnf" "$dir/ssl.xml" >/dev/null 2>&1 || true

  cat >"$dir/openssl.cnf" <<'EOF'
[ req ]
prompt = no
distinguished_name = dn

[ dn ]
CN = ClickHouse Native Test

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

  # CA
  openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -subj "/CN=ClickHouse Native Test CA" \
    -keyout "$dir/ca.key" -out "$dir/ca.crt" >/dev/null 2>&1

  # Server key + CSR
  openssl req -newkey rsa:2048 -nodes -keyout "$dir/server.key" -out "$dir/server.csr" \
    -config "$dir/openssl.cnf" >/dev/null 2>&1

  # Sign server cert with CA and include SANs
  openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/server.crt" -days 3650 -sha256 -extensions v3_req -extfile "$dir/openssl.cnf" >/dev/null 2>&1

  # Client cert
  cat >"$dir/client_ext.cnf" <<'EOF'
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

  openssl req -newkey rsa:2048 -nodes -keyout "$dir/client.key" -out "$dir/client.csr" \
    -subj "/CN=ClickHouse Native Test Client" >/dev/null 2>&1
  openssl x509 -req -in "$dir/client.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/client.crt" -days 3650 -sha256 -extfile "$dir/client_ext.cnf" >/dev/null 2>&1

  # ClickHouse server config snippet for native TCP TLS.
  cat >"$dir/ssl.xml" <<EOF
<clickhouse>
    <tcp_port_secure>9440</tcp_port_secure>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
        </server>
    </openSSL>
</clickhouse>
EOF
}

cmd_up() {
  if is_running; then
    echo "Already running: $CONTAINER_NAME"
    return 0
  fi

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

  log "Starting ClickHouse container (timeout=${STEP_TIMEOUT_UP}s): $CONTAINER_NAME ($IMAGE)"
  if ! run_with_timeout "$STEP_TIMEOUT_UP" docker run -d \
      --name "$CONTAINER_NAME" \
      -p "${HOST_PORT_TCP}:9000" \
      -p "${HOST_PORT_HTTP}:8123" \
      -e "CLICKHOUSE_USER=${USER_NAME}" \
      -e "CLICKHOUSE_PASSWORD=${PASSWORD}" \
      "$IMAGE" >/dev/null; then
    log "ERROR: docker run timed out or failed"
    dump_diagnostics
    return 1
  fi

  echo "Started: $CONTAINER_NAME ($IMAGE)"
}

cmd_up_tls() {
  if [[ "$RUN_TLS_TESTS" != "1" ]]; then
    return 0
  fi
  if is_running_tls; then
    log "Already running: $TLS_CONTAINER_NAME"
    return 0
  fi

  gen_tls_assets

  docker rm -f "$TLS_CONTAINER_NAME" >/dev/null 2>&1 || true
  local root
  root="$(repo_root)"
  local dir="$root/.clickhouse-tls"

  log "Starting ClickHouse TLS container (timeout=${STEP_TIMEOUT_UP}s): $TLS_CONTAINER_NAME ($IMAGE)"
  if ! run_with_timeout "$STEP_TIMEOUT_UP" docker run -d \
      --name "$TLS_CONTAINER_NAME" \
      -p "${HOST_PORT_TCP_SECURE}:9440" \
      -e "CLICKHOUSE_USER=${USER_NAME}" \
      -e "CLICKHOUSE_PASSWORD=${PASSWORD}" \
      -v "$dir/server.crt:/etc/clickhouse-server/server.crt:ro" \
      -v "$dir/server.key:/etc/clickhouse-server/server.key:ro" \
      -v "$dir/ssl.xml:/etc/clickhouse-server/config.d/ssl.xml:ro" \
      "$IMAGE" >/dev/null; then
    log "ERROR: docker run (tls) timed out or failed"
    dump_diagnostics
    return 1
  fi
  log "Started TLS: $TLS_CONTAINER_NAME ($IMAGE)"
}

cmd_wait() {
  local timeout="${1:-$STEP_TIMEOUT_WAIT}"
  local deadline=$(( $(date +%s) + timeout ))
  local tries=0

  while true; do
    tries=$((tries + 1))
    local now
    now="$(date +%s)"
    local remaining=$(( deadline - now ))
    if (( remaining <= 0 )); then
      break
    fi

    local per_try="$STEP_TIMEOUT_WAIT_TRY"
    if (( per_try > remaining )); then
      per_try="$remaining"
    fi

    if run_with_timeout "$per_try" docker exec "$CONTAINER_NAME" clickhouse-client --user "$USER_NAME" --password "$PASSWORD" --query "SELECT 1" >/dev/null 2>&1; then
      echo "Ready: $CONTAINER_NAME"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ClickHouse in container: $CONTAINER_NAME" >&2
  dump_diagnostics
  return 1
}

cmd_wait_tls() {
  if [[ "$RUN_TLS_TESTS" != "1" ]]; then
    return 0
  fi
  local old="$CONTAINER_NAME"
  CONTAINER_NAME="$TLS_CONTAINER_NAME"
  cmd_wait "${1:-90}"
  CONTAINER_NAME="$old"
}

cmd_down() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  docker rm -f "$TLS_CONTAINER_NAME" >/dev/null 2>&1 || true
  echo "Removed: $CONTAINER_NAME"
  echo "Removed: $TLS_CONTAINER_NAME"
}

cmd_test_single() {
  cmd_up
  cmd_up_tls
  cmd_wait 90
  cmd_wait_tls 90

  export CLICKHOUSE_HOST=127.0.0.1
  export CLICKHOUSE_PORT="$HOST_PORT_TCP"
  export CLICKHOUSE_USER="$USER_NAME"
  export CLICKHOUSE_PASSWORD="$PASSWORD"
  export CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
  export CLICKHOUSE_DOCKER_CONTAINER_NAME="$CONTAINER_NAME"
  export CLICKHOUSE_DOCKER_RESTART_TESTS=1
  export CLICKHOUSE_RESTART_TESTS_ONLY=0
  if [[ "$RUN_TLS_TESTS" == "1" ]]; then
    local root
    root="$(repo_root)"
    local dir="$root/.clickhouse-tls"
    export CLICKHOUSE_TLS_HOST=127.0.0.1
    export CLICKHOUSE_TLS_PORT="$HOST_PORT_TCP_SECURE"
    export CLICKHOUSE_TLS_CONTAINER_NAME="$TLS_CONTAINER_NAME"
    export CLICKHOUSE_TLS_CA_PATH="$dir/ca.crt"
    export CLICKHOUSE_TLS_CLIENT_CERT_PATH="$dir/client.crt"
    export CLICKHOUSE_TLS_CLIENT_KEY_PATH="$dir/client.key"
  fi

  local swift_args=(test)
  if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
    swift_args+=(--scratch-path "$SWIFT_SCRATCH_PATH")
  fi

  local matrix="${CLICKHOUSE_COMPRESSION_MATRIX:-1}"
  if [[ "$matrix" == "1" ]]; then
    for comp in 0 1; do
      log "=== swift test (CLICKHOUSE_COMPRESSION=$comp, timeout=${STEP_TIMEOUT_TEST}s) ==="
      CLICKHOUSE_COMPRESSION="$comp"
      export CLICKHOUSE_COMPRESSION
      cleanup_swiftpm_helpers
      if ! run_with_timeout "$STEP_TIMEOUT_TEST" swift "${swift_args[@]}"; then
        log "ERROR: swift test failed or timed out (CLICKHOUSE_COMPRESSION=$comp)"
        dump_diagnostics
        cleanup_swiftpm_helpers
        return 1
      fi
      cleanup_swiftpm_helpers
    done
  else
    log "=== swift test (timeout=${STEP_TIMEOUT_TEST}s) ==="
    cleanup_swiftpm_helpers
    if ! run_with_timeout "$STEP_TIMEOUT_TEST" swift "${swift_args[@]}"; then
      log "ERROR: swift test failed or timed out"
      dump_diagnostics
      cleanup_swiftpm_helpers
      return 1
    fi
    cleanup_swiftpm_helpers
  fi

  if [[ "$RUN_RESTART_TESTS" == "1" ]]; then
    if [[ "$RESTART_MODE" == "swift-test" ]]; then
      log "=== swift test (restart-only, CLICKHOUSE_COMPRESSION=0, timeout=${STEP_TIMEOUT_TEST}s) ==="
      export CLICKHOUSE_COMPRESSION=0
      export CLICKHOUSE_RESTART_TESTS_ONLY=1
      cleanup_swiftpm_helpers
      if ! run_with_timeout "$STEP_TIMEOUT_TEST" swift "${swift_args[@]}"; then
        log "ERROR: restart-only swift test failed or timed out"
        dump_diagnostics
        cleanup_swiftpm_helpers
        return 1
      fi
      cleanup_swiftpm_helpers
    else
      export CLICKHOUSE_COMPRESSION=0
      export CLICKHOUSE_RESTART_TESTS_ONLY=1
      # Prefer running a built executable over `swift test`/`swift run` to avoid SwiftPM lock hangs.
      if ! run_probe_docker_suite; then
        log "ERROR: probe docker-suite failed or timed out"
        dump_diagnostics
        cleanup_swiftpm_helpers
        return 1
      fi
      cleanup_swiftpm_helpers
    fi
  fi
}

cmd_test() {
  start_total_watchdog "$STEP_TIMEOUT_TOTAL"
  trap stop_total_watchdog EXIT
  trap 'stop_total_watchdog; on_abort "terminated"' TERM INT

  if [[ -n "$IMAGE_MATRIX" ]]; then
    local original_image="$IMAGE"
    IFS=',' read -r -a images <<<"$IMAGE_MATRIX"
    for img in "${images[@]}"; do
      IMAGE="$img"
      log "=== ClickHouse image: $IMAGE ==="
      cmd_down
      if ! cmd_test_single; then
        return 1
      fi
      cmd_down
    done
    IMAGE="$original_image"
  else
    cmd_test_single
  fi
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    up) cmd_up ;;
    up-tls) cmd_up_tls ;;
    wait) shift; cmd_wait "${1:-60}" ;;
    wait-tls) shift; cmd_wait_tls "${1:-60}" ;;
    down) cmd_down ;;
    test) cmd_test ;;
    ""|-h|--help) usage ;;
    *) echo "Unknown command: $cmd" >&2; usage; exit 2 ;;
  esac
}

main "$@"
