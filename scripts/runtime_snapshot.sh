#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION_NAME="${1:-pm-paper}"
STATUS_PATH="$REPO_DIR/data/runtime/status.json"
EVENTS_PATH="$REPO_DIR/data/runtime/events.jsonl"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "== tmux:$SESSION_NAME =="
  tmux capture-pane -pt "$SESSION_NAME" -S -40
else
  echo "tmux session not found: $SESSION_NAME"
fi

echo
if [[ -f "$STATUS_PATH" ]]; then
  echo "== status.json =="
  cat "$STATUS_PATH"
else
  echo "status file missing: $STATUS_PATH"
fi

echo
if [[ -f "$EVENTS_PATH" ]]; then
  echo "== recent events =="
  tail -n 20 "$EVENTS_PATH"
else
  echo "events file missing: $EVENTS_PATH"
fi
