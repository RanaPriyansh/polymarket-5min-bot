#!/usr/bin/env bash
set -euo pipefail
BOT_NAME="polymarket-paper-bot"
systemctl stop "$BOT_NAME" 2>/dev/null || true
systemctl disable "$BOT_NAME" 2>/dev/null || true
rm -f "/etc/systemd/system/$BOT_NAME.service"
systemctl daemon-reload
echo "Uninstalled $BOT_NAME"

#!/bin/bash
set -euo pipefail

# uninstall.sh — rollback the polymarket-paper-bot systemd unit
set -euo pipefail

SERVICE_DST="/etc/systemd/system/polymarket-paper-bot.service"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: Run as root (sudo $0)" >&2
  exit 1
fi

echo "=== Rolling back polymarket-paper-bot ==="
systemctl stop polymarket-paper-bot 2>/dev/null || true
systemctl disable polymarket-paper-bot 2>/dev/null || true

if [ -f "$SERVICE_DST.bak" ]; then
  cp "$SERVICE_DST.bak" "$SERVICE_DST"
  echo "Restored backup unit."
else
  rm -f "$SERVICE_DST"
  echo "Removed unit file (no backup found)."
fi

systemctl daemon-reload
echo "Rollback complete."
