#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BOT_NAME="polymarket-paper-bot"
RESEARCH_SERVICE="polymarket-paper-research.service"
RESEARCH_TIMER="polymarket-paper-research.timer"

SERVICE_FILE="$REPO_ROOT/deploy/systemd/${BOT_NAME}.service"
RESEARCH_SERVICE_FILE="$REPO_ROOT/deploy/systemd/${RESEARCH_SERVICE}"
RESEARCH_TIMER_FILE="$REPO_ROOT/deploy/systemd/${RESEARCH_TIMER}"

TARGET_SERVICE="/etc/systemd/system/${BOT_NAME}.service"
TARGET_RESEARCH_SERVICE="/etc/systemd/system/${RESEARCH_SERVICE}"
TARGET_RESEARCH_TIMER="/etc/systemd/system/${RESEARCH_TIMER}"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root (sudo bash deploy/systemd/install.sh)" >&2
  exit 1
fi

for file in "$SERVICE_FILE" "$RESEARCH_SERVICE_FILE" "$RESEARCH_TIMER_FILE"; do
  if [ ! -f "$file" ]; then
    echo "ERROR: required systemd asset missing: $file" >&2
    exit 1
  fi
done

mkdir -p "$REPO_ROOT/data/runtime" "$REPO_ROOT/data/research"

if [ ! -x "$REPO_ROOT/.venv/bin/python" ]; then
  echo "ERROR: missing virtualenv python at $REPO_ROOT/.venv/bin/python" >&2
  exit 1
fi

backup_if_present() {
  local src="$1"
  if [ -f "$src" ]; then
    cp "$src" "$src.bak"
    echo "Backed up existing unit to $src.bak"
  fi
}

backup_if_present "$TARGET_SERVICE"
backup_if_present "$TARGET_RESEARCH_SERVICE"
backup_if_present "$TARGET_RESEARCH_TIMER"

cp "$SERVICE_FILE" "$TARGET_SERVICE"
cp "$RESEARCH_SERVICE_FILE" "$TARGET_RESEARCH_SERVICE"
cp "$RESEARCH_TIMER_FILE" "$TARGET_RESEARCH_TIMER"
chmod 644 "$TARGET_SERVICE" "$TARGET_RESEARCH_SERVICE" "$TARGET_RESEARCH_TIMER"

systemctl daemon-reload
systemctl enable "$BOT_NAME"
systemctl restart "$BOT_NAME"
systemctl enable --now "$RESEARCH_TIMER"

echo
echo "=== systemd status ==="
systemctl status "$BOT_NAME" --no-pager

echo
echo "=== unit verification ==="
systemctl show "$BOT_NAME" -p ExecStart -p WorkingDirectory -p Restart -p RestartUSec --no-pager

echo
echo "=== research timer ==="
systemctl status "$RESEARCH_TIMER" --no-pager
systemctl list-timers "$RESEARCH_TIMER" --no-pager

echo
echo "=== recent bot logs ==="
journalctl -u "$BOT_NAME" -n 30 --no-pager

echo
echo "=== recent research logs ==="
journalctl -u polymarket-paper-research.service -n 30 --no-pager || true

echo
echo "Follow bot logs: journalctl -u $BOT_NAME -f"
echo "Follow research logs: journalctl -u polymarket-paper-research.service -f"
echo "Rollback bot: cp $TARGET_SERVICE.bak $TARGET_SERVICE && systemctl daemon-reload && systemctl restart $BOT_NAME"
echo "Rollback research: cp $TARGET_RESEARCH_SERVICE.bak $TARGET_RESEARCH_SERVICE; cp $TARGET_RESEARCH_TIMER.bak $TARGET_RESEARCH_TIMER; systemctl daemon-reload && systemctl restart $BOT_NAME && systemctl restart $RESEARCH_TIMER"
