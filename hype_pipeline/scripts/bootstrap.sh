#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"

echo "[bootstrap] creating venvs with ${PYTHON_BIN} and installing deps"

create_venv () {
  local dir="$1"
  local req="$2"
  echo "[bootstrap] setting up venv in $dir"
  rm -rf "$dir/.venv"
  "${PYTHON_BIN}" -m venv "$dir/.venv"
  # activate only if venv created successfully
  if [ -f "$dir/.venv/bin/activate" ]; then
    source "$dir/.venv/bin/activate"
    python -m pip install --upgrade pip
    if [ -f "$dir/$req" ]; then
      pip install -r "$dir/$req"
    fi
    deactivate || true
  else
    echo "[bootstrap] ERROR: failed to create venv in $dir"
    exit 1
  fi
}

create_venv "apps/indicators_with_signals" "requirements.txt"
create_venv "apps/watchlist" "requirements.txt"
create_venv "apps/email_alerts" "requirements.txt"

mkdir -p data
touch data/.gitkeep

echo "[bootstrap] done"

