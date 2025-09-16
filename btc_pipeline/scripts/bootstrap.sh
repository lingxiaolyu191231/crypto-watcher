#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"
echo "[bootstrap] creating venvs with ${PYTHON_BIN} and installing deps"

safe_clean () {
  local path="$1"
  if [ -d "$path" ]; then
    chflags -R nouchg "$path" 2>/dev/null || true
    chmod  -R u+w   "$path" 2>/dev/null || true
    rm -rf "$path"
  fi
}

create_venv () {
  local dir="$1"
  local req="$2"
  echo "[bootstrap] setting up venv in $dir"
  safe_clean "$dir/.venv"
  "${PYTHON_BIN}" -m venv "$dir/.venv"
  if [ ! -f "$dir/.venv/bin/activate" ]; then
    echo "[bootstrap] ERROR: failed to create venv in $dir"
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$dir/.venv/bin/activate"
  python -m pip install --upgrade pip
  if [ -f "$dir/$req" ]; then
    pip install -r "$dir/$req"
  fi
  deactivate || true
}

create_venv "apps/indicators_with_signals" "requirements.txt"
create_venv "apps/watchlist" "requirements.txt"
create_venv "apps/email_alerts" "requirements.txt"   # empty is fine

# Root-level shared deps (e.g., requests)
if [ -f "requirements.txt" ]; then
  echo "[bootstrap] installing shared deps from requirements.txt (user site)"
  "${PYTHON_BIN}" -m pip install --user -r requirements.txt
fi

mkdir -p data
touch data/.gitkeep
echo "[bootstrap] done"

