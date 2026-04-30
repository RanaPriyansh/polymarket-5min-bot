#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
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

ENABLED_TIMERS=(
  polymarket-paper-research.timer
  polymarket-paper-bakeoff.timer
  polymarket-paper-ops-hourly.timer
)

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root (sudo bash deploy/systemd/install.sh)" >&2
  exit 1
fi

if [ ! -x "$REPO_ROOT/.venv/bin/python" ]; then
  echo "ERROR: missing virtualenv python at $REPO_ROOT/.venv/bin/python" >&2
  exit 1
fi

for unit in "${UNITS[@]}"; do
  src="$REPO_ROOT/deploy/systemd/$unit"
  if [ ! -f "$src" ]; then
    echo "ERROR: required systemd asset missing: $src" >&2
    exit 1
  fi
done

mkdir -p "$REPO_ROOT/data/runtime" "$REPO_ROOT/data/research" "$REPO_ROOT/data/experiments"

backup_if_present() {
  local src="$1"
  if [ -f "$src" ]; then
    cp "$src" "$src.bak"
    echo "Backed up existing unit to $src.bak"
  fi
}

for unit in "${UNITS[@]}"; do
  dst="$SYSTEMD_DIR/$unit"
  backup_if_present "$dst"
  cp "$REPO_ROOT/deploy/systemd/$unit" "$dst"
  chmod 644 "$dst"
done

systemd-analyze verify "${UNITS[@]/#/$SYSTEMD_DIR/}"
systemctl daemon-reload
systemctl enable polymarket-paper-bot.service
systemctl restart polymarket-paper-bot.service
for timer in "${ENABLED_TIMERS[@]}"; do
  systemctl enable --now "$timer"
done
# Healthcheck is intentionally alert-only and not auto-enabled unless explicitly requested.
systemctl disable polymarket-paper-bot-healthcheck.timer 2>/dev/null || true

printf '\n=== systemd status ===\n'
systemctl status polymarket-paper-bot.service --no-pager

printf '\n=== enabled timers ===\n'
systemctl list-timers 'polymarket-paper-*' --all --no-pager

printf '\n=== unit verification ===\n'
systemctl show polymarket-paper-bot.service -p ExecStart -p WorkingDirectory -p Restart -p RestartPreventExitStatus --no-pager
systemctl show polymarket-paper-research.service polymarket-paper-bakeoff.service -p ExecStart -p TimeoutStartUSec -p RuntimeMaxUSec --no-pager

printf '\nFollow bot logs: journalctl -u polymarket-paper-bot.service -f\n'
printf 'Rollback: copy *.bak files in /etc/systemd/system back into place, then systemctl daemon-reload\n'
