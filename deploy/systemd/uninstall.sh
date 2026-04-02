#!/usr/bin/env bash
set -euo pipefail

BOT_NAME="polymarket-paper-bot"
SERVICE_DST="/etc/systemd/system/${BOT_NAME}.service"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: Run as root (sudo bash deploy/systemd/uninstall.sh)" >&2
  exit 1
fi

systemctl stop "$BOT_NAME" 2>/dev/null || true
systemctl disable "$BOT_NAME" 2>/dev/null || true

if [ -f "$SERVICE_DST.bak" ]; then
  cp "$SERVICE_DST.bak" "$SERVICE_DST"
  echo "Restored backup unit to $SERVICE_DST"
else
  rm -f "$SERVICE_DST"
  echo "Removed unit file $SERVICE_DST"
fi

systemctl daemon-reload
echo "Uninstall/rollback complete for $BOT_NAME"
