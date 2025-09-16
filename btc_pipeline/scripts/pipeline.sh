#!/usr/bin/env bash
set -euo pipefail


# Simple orchestrator for daily run
# Usage: DATA_DIR=./data SYMBOL=BTCUSDT ./scripts/pipeline.sh daily


TARGET=${1:-run}


case "$TARGET" in
daily)
make daily ;;
indicators)
make indicators ;;
watchlist)
make watchlist ;;
email)
make email ;;
run)
make run ;;
*)
echo "Unknown target: $TARGET" && exit 1 ;;
esac
