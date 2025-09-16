#!/bin/bash
set -euo pipefail
BASE="/Users/lingxiaolyu/projects/hype_pipeline"
LOG="$BASE/hype_pipeline.log"
cd "$BASE"
# Ensure our fixed .env is used and machine stays awake
export SUBJECT_PREFIX='[HYPE Watchlist]'
if command -v caffeinate >/dev/null 2>&1; then
  exec /usr/bin/caffeinate -i /bin/bash -lc 'bash scripts/pipeline.sh hourly' >> "$LOG" 2>&1
else
  exec /bin/bash -lc 'bash scripts/pipeline.sh hourly' >> "$LOG" 2>&1
fi
