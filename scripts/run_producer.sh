#!/usr/bin/env bash
set -euo pipefail

MODULE="${1:-app.main}"
shift || true

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f ".env" ]]; then
  echo "[run_producer] Missing .env file in $REPO_ROOT" >&2
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  echo "[run_producer] Missing virtualenv (.venv). Create it with 'python -m venv .venv'." >&2
  exit 1
fi

source ".venv/bin/activate"

set -a
source ".env"
set +a

export PYTHONUNBUFFERED=1
mkdir -p "$REPO_ROOT/logs"

LOG_NAME="$(echo "${MODULE}" | tr '.' '_')"
exec python -m "${MODULE}" "$@" >> "$REPO_ROOT/logs/${LOG_NAME}.log" 2>&1
