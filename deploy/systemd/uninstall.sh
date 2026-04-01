#!/usr/bin/env bash
set -euo pipefail
BOT_NAME="polymarket-paper-bot"
systemctl stop "$BOT_NAME" 2>/dev/null || true
systemctl disable "$BOT_NAME" 2>/dev/null || true
rm -f "/etc/systemd/system/$BOT_NAME.service"
systemctl daemon-reload
echo "Uninstalled $BOT_NAME"
