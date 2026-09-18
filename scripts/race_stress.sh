#!/usr/bin/env bash
#
# Seed-sweeping stress driver for the Hypertile race harness.
#
# The harness itself runs a handful of random iterations so it stays cheap enough for
# every CI run. This script is the "hunt" mode: it sweeps many seeds at high iteration
# counts. Every scenario derives its schedule from a seed, so anything this finds is
# replayable exactly:
#
#   HYPERTILE_RACE_SEED=<seed> cargo test -p hypertile-core --test race_harness -- --nocapture
#
# Usage:
#   scripts/race_stress.sh [debug|release]
#
# Environment:
#   HYPERTILE_STRESS_SEEDS        number of seeds to sweep            (default 16)
#   HYPERTILE_RACE_ITERATIONS     task-state iterations per seed      (default 100)
#   HYPERTILE_RACE_CHURN_ITERATIONS  registry-churn iterations per seed (default 25)
#   HYPERTILE_RACE_TIMEOUT_SECS   per-iteration watchdog timeout      (default 30)
#
# debug is the default because it keeps overflow checks on, which catches bugs in the
# harness's own schedule arithmetic. release builds the same scenarios faster if you want
# a longer sweep.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="${1:-debug}"
case "$MODE" in
  debug)   CARGO_FLAGS=() ;;
  release) CARGO_FLAGS=(--release) ;;
  *) echo "usage: $0 [debug|release]" >&2; exit 2 ;;
esac

SEEDS="${HYPERTILE_STRESS_SEEDS:-16}"
ITERATIONS="${HYPERTILE_RACE_ITERATIONS:-100}"
CHURN_ITERATIONS="${HYPERTILE_RACE_CHURN_ITERATIONS:-25}"
TIMEOUT="${HYPERTILE_RACE_TIMEOUT_SECS:-30}"

echo "hypertile race stress: mode=$MODE seeds=$SEEDS task-state=$ITERATIONS churn=$CHURN_ITERATIONS watchdog=${TIMEOUT}s"

for ((seed = 1; seed <= SEEDS; seed++)); do
  printf '  seed %d ... ' "$seed"
  if HYPERTILE_RACE_SEED="$seed" \
     HYPERTILE_RACE_ITERATIONS="$ITERATIONS" \
     HYPERTILE_RACE_CHURN_ITERATIONS="$CHURN_ITERATIONS" \
     HYPERTILE_RACE_TIMEOUT_SECS="$TIMEOUT" \
       cargo test -p hypertile-core --test race_harness "${CARGO_FLAGS[@]}" \
         -- --test-threads=1 > /tmp/hypertile-race-seed-"$seed".log 2>&1; then
    echo "clean"
  else
    echo "FAILED"
    echo
    echo "See /tmp/hypertile-race-seed-$seed.log. The failing iteration printed a replay seed:"
    grep -E "replay with HYPERTILE_RACE_SEED|lost during|leaked|polled to Ready" \
      /tmp/hypertile-race-seed-"$seed".log || true
    exit 1
  fi
done

echo "hypertile race stress: all $SEEDS seeds clean"
