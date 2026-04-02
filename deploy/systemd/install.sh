#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVICE_FILE="$REPO_ROOT/deploy/systemd/polymarket-paper-bot.service"
TARGET="/etc/systemd/system/polymarket-paper-bot.service"
BOT_NAME="polymarket-paper-bot"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root (sudo bash deploy/systemd/install.sh)" >&2
  exit 1
fi

if [ ! -f "$SERVICE_FILE" ]; then
  echo "ERROR: service file not found at $SERVICE_FILE" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/data/runtime" "$REPO_ROOT/data/research"

if [ ! -x "$REPO_ROOT/.venv/bin/python" ]; then
  echo "ERROR: missing virtualenv python at $REPO_ROOT/.venv/bin/python" >&2
  exit 1
fi

if [ -f "$TARGET" ]; then
  cp "$TARGET" "$TARGET.bak"
  echo "Backed up existing unit to $TARGET.bak"
fi

cp "$SERVICE_FILE" "$TARGET"
chmod 644 "$TARGET"

systemctl daemon-reload
systemctl enable "$BOT_NAME"
systemctl restart "$BOT_NAME"

echo
echo "=== systemd status ==="
systemctl status "$BOT_NAME" --no-pager

echo
echo "=== ExecStart verification ==="
systemctl show "$BOT_NAME" -p ExecStart -p WorkingDirectory --no-pager

echo
echo "=== recent logs ==="
journalctl -u "$BOT_NAME" -n 30 --no-pager

echo
echo "Follow logs: journalctl -u $BOT_NAME -f"
echo "Rollback: cp $TARGET.bak $TARGET && systemctl daemon-reload && systemctl restart $BOT_NAME"
