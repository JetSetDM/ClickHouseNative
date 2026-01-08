#!/usr/bin/env bash
set -euo pipefail

STEP_TIMEOUT_TEST="${SWIFT_TEST_TIMEOUT:-300}"          # seconds (not currently used separately)
STEP_TIMEOUT_TOTAL="${SWIFT_TEST_TOTAL_TIMEOUT:-600}"   # seconds (whole command)
SWIFT_SCRATCH_PATH="${SWIFT_TEST_SCRATCH_PATH:-}"       # optional scratch-path to avoid .build locks

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() { printf '[%s] %s\n' "$(ts)" "$*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

cleanup_swiftpm() {
  rm -f .build/.lock .build-docker-scratch/.lock .build-docker-debug/.lock 2>/dev/null || true
  if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
    rm -f "$SWIFT_SCRATCH_PATH/.lock" 2>/dev/null || true
  fi
  if have_cmd pkill; then
    pkill -9 -f swiftpm-testing-helper >/dev/null 2>&1 || true
  fi
}

SCRIPT_PID="$$"
CURRENT_CHILD_CMD=""

dump_diagnostics() {
  log "--- diagnostics ---"
  ls -la .build/.lock 2>/dev/null || true
  if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
    ls -la "$SWIFT_SCRATCH_PATH/.lock" 2>/dev/null || true
  fi
  if have_cmd pgrep; then
    pgrep -fl 'swiftpm-testing-helper|swift-test|swift-package|swift-build' >&2 || true
  fi
  log "--- end diagnostics ---"
}

run_with_timeout() {
  local seconds="$1"
  shift

  CURRENT_CHILD_CMD="$*"
  local pid=""
  local pgid=""
  local status=0

  if command -v setsid >/dev/null 2>&1; then
    setsid "$@" &
    pid="$!"
    pgid="$pid"
  else
    "$@" &
    pid="$!"
  fi

  (
    sleep "$seconds"
    if kill -0 "$pid" >/dev/null 2>&1; then
      log "Timed out after ${seconds}s: $CURRENT_CHILD_CMD"
      dump_diagnostics
      cleanup_swiftpm
      if [[ -n "$pgid" ]]; then
        kill -TERM "-$pgid" >/dev/null 2>&1 || true
        sleep 1
        kill -KILL "-$pgid" >/dev/null 2>&1 || true
      else
        kill -TERM "$pid" >/dev/null 2>&1 || true
        sleep 1
        kill -KILL "$pid" >/dev/null 2>&1 || true
      fi
      kill -TERM "$SCRIPT_PID" >/dev/null 2>&1 || true
    fi
  ) &
  local watchdog_pid="$!"

  wait "$pid" || status=$?
  kill -TERM "$watchdog_pid" >/dev/null 2>&1 || true
  wait "$watchdog_pid" >/dev/null 2>&1 || true
  return "$status"
}

cleanup_swiftpm
log "=== swift test (total=${STEP_TIMEOUT_TOTAL}s, scratch=${SWIFT_SCRATCH_PATH:-<default>}) ==="
args=(test)
if [[ -n "$SWIFT_SCRATCH_PATH" ]]; then
  args+=(--scratch-path "$SWIFT_SCRATCH_PATH")
fi
run_with_timeout "$STEP_TIMEOUT_TOTAL" swift "${args[@]}"
