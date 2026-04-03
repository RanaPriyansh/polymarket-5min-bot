#!/usr/bin/env bash
set -euo pipefail

BOT_NAME="polymarket-paper-bot"
RESEARCH_SERVICE="polymarket-paper-research.service"
RESEARCH_TIMER="polymarket-paper-research.timer"
SERVICE_DST="/etc/systemd/system/${BOT_NAME}.service"
RESEARCH_SERVICE_DST="/etc/systemd/system/${RESEARCH_SERVICE}"
RESEARCH_TIMER_DST="/etc/systemd/system/${RESEARCH_TIMER}"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: Run as root (sudo bash deploy/systemd/uninstall.sh)" >&2
  exit 1
fi

systemctl stop "$RESEARCH_TIMER" 2>/dev/null || true
systemctl disable "$RESEARCH_TIMER" 2>/dev/null || true
systemctl stop "$BOT_NAME" 2>/dev/null || true
systemctl disable "$BOT_NAME" 2>/dev/null || true

restore_or_remove() {
  local dst="$1"
  if [ -f "$dst.bak" ]; then
    cp "$dst.bak" "$dst"
    echo "Restored backup unit to $dst"
  else
    rm -f "$dst"
    echo "Removed unit file $dst"
  fi
}

restore_or_remove "$SERVICE_DST"
restore_or_remove "$RESEARCH_SERVICE_DST"
restore_or_remove "$RESEARCH_TIMER_DST"

systemctl daemon-reload
echo "Uninstall/rollback complete for $BOT_NAME and research timer"
