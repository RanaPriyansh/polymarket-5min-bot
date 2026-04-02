#!/usr/bin/env bash
set -euo pipefail

# install.sh — install polymarket-paper-bot systemd unit
# Usage: sudo bash deploy/systemd/install.sh
#
# ROLLBACK:
#   sudo systemctl stop polymarket-paper-bot
#   sudo systemctl disable polymarket-paper-bot
#   sudo cp /etc/systemd/system/polymarket-paper-bot.service.bak \
#      /etc/systemd/system/polymarket-paper-bot.service
#   sudo cp /etc/systemd/system/polymarket-paper-bot.service.bak /etc/systemd/system/polymarket-paper-bot.service
#   sudo systemctl daemon-reload

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVICE_FILE="$REPO_ROOT/deploy/systemd/polymarket-paper-bot.service"
SYSTEMD_DIR="/etc/systemd/system"
TARGET="$SYSTEMD_DIR/polymarket-paper-bot.service"

if [ "$(id -u)" -ne 0 ]; then
    echo "Error: run as root (sudo $0)" >&2
    exit 1
fi

if [ ! -f "$SERVICE_FILE" ]; then
    echo "Error: service file not found at $SERVICE_FILE" >&2
    exit 1
fi

echo "=== Polymarket Paper Bot — systemd install ==="
echo "Source:      $SERVICE_FILE"
echo "Destination: $TARGET"
echo ""
echo "Installing polymarket-paper-bot.service ..."

# Backup existing unit if present
if [ -f "$TARGET" ]; then
    cp "$TARGET" "$TARGET.bak"
    echo "Backed up existing unit -> $TARGET.bak"
fi

# Install
cp "$SERVICE_FILE" "$TARGET"
chmod 644 "$TARGET"
echo "Installed unit file."

# Activate
cp "$SERVICE_FILE" "$TARGET"
chmod 644 "$TARGET"

systemctl daemon-reload
systemctl enable polymarket-paper-bot
systemctl restart polymarket-paper-bot

echo ""
echo "=== Service status ==="
systemctl status polymarket-paper-bot --no-pager

echo ""
echo "=== Recent logs ==="
journalctl -u polymarket-paper-bot --no-pager -n 30

echo ""
echo "Status:  systemctl status polymarket-paper-bot"
echo "Follow:  journalctl -u polymarket-paper-bot -f"
echo "Stop:    sudo systemctl stop polymarket-paper-bot"
echo ""
echo "Rollback:"
echo "  sudo cp $TARGET.bak $TARGET"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl restart polymarket-paper-bot"
journalctl -u polymarket-paper-bot --no-pager -n 20

echo ""
echo "Status:     systemctl status polymarket-paper-bot"
echo "Logs:       journalctl -u polymarket-paper-bot -f"
echo "Stop:       sudo systemctl stop polymarket-paper-bot"
echo "Rollback:   sudo cp $TARGET.bak $TARGET && sudo systemctl daemon-reload && sudo systemctl restart polymarket-paper-bot"
