#!/usr/bin/env bash
set -euo pipefail

SYSTEMD_DIR="/etc/systemd/system"
UNITS=(
  polymarket-paper-bot.service
  polymarket-paper-research.service
  polymarket-paper-research.timer
  polymarket-paper-bakeoff.service
  polymarket-paper-bakeoff.timer
  polymarket-paper-bot-healthcheck.service
  polymarket-paper-bot-healthcheck.timer
  polymarket-paper-ops-hourly.service
  polymarket-paper-ops-hourly.timer
)

TIMERS=(
  polymarket-paper-research.timer
  polymarket-paper-bakeoff.timer
  polymarket-paper-bot-healthcheck.timer
  polymarket-paper-ops-hourly.timer
)

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: Run as root (sudo bash deploy/systemd/uninstall.sh)" >&2
  exit 1
fi

for timer in "${TIMERS[@]}"; do
  systemctl stop "$timer" 2>/dev/null || true
  systemctl disable "$timer" 2>/dev/null || true
done
systemctl stop polymarket-paper-bot.service 2>/dev/null || true
systemctl disable polymarket-paper-bot.service 2>/dev/null || true

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

for unit in "${UNITS[@]}"; do
  restore_or_remove "$SYSTEMD_DIR/$unit"
done

systemctl daemon-reload
echo "Uninstall/rollback complete for Polymarket paper units"
