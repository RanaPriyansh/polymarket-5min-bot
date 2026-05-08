#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION_NAME="${SESSION_NAME:-pm-paper}"
UNIVERSE_MODE="${UNIVERSE_MODE:-all_liquid}"
MARKET_SOURCE="${MARKET_SOURCE:-official_cli}"
LOG_PATH="${LOG_PATH:-$REPO_DIR/logs/paper_runtime.log}"
RESET_STATE="${RESET_STATE:-1}"
STATE_PATH="$REPO_DIR/data/runtime/paper_broker_state.json"

mkdir -p "$REPO_DIR/logs" "$REPO_DIR/data/runtime"
if [[ "$RESET_STATE" == "1" ]]; then
  rm -f "$STATE_PATH"
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  tmux kill-session -t "$SESSION_NAME"
fi

CMD="cd $REPO_DIR && stdbuf -oL -eL .venv/bin/python -u cli.py run --mode paper --market-source $MARKET_SOURCE --universe-mode $UNIVERSE_MODE 2>&1 | tee -a $LOG_PATH"

tmux new-session -d -s "$SESSION_NAME" "$CMD"
echo "Started tmux session: $SESSION_NAME"
echo "Log: $LOG_PATH"
echo "Attach: tmux attach -t $SESSION_NAME"
echo "Snapshot: $REPO_DIR/scripts/runtime_snapshot.sh $SESSION_NAME"
