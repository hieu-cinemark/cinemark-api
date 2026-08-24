#!/bin/bash
# Installs and starts the ingest-consumer systemd service. Run on the
# server, as root/sudo, after the repo is deployed to /opt/cinemark-api
# (or after editing this file's WorkingDirectory/ExecStart/EnvironmentFile
# paths to match wherever it actually lives).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

sudo cp "$SCRIPT_DIR/cinemark-ingest-consumer.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-ingest-consumer

echo "Installed. Check status with:"
echo "  systemctl status cinemark-ingest-consumer"
echo "  journalctl -u cinemark-ingest-consumer -f"
