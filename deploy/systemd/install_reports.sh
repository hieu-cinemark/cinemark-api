#!/bin/bash
# Installs the nightly Social Topic reports timer on the VPS. Prerequisites:
#   - torch (CPU) + transformers installed in /opt/cinemark-api/.venv (requirements-ml.txt)
#   - Bee API key and lake (R2) credentials in /opt/cinemark-api/.env
# The e5 embedding model downloads on first run; the first run per film embeds all its comments (can take an hour).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

"$APP_DIR/.venv/bin/python" -c "import torch, transformers" || { echo "torch/transformers not installed in $APP_DIR/.venv"; exit 1; }
sudo -u app mkdir -p "$APP_DIR/data/clusters"

sudo cp "$SCRIPT_DIR/cinemark-reports.service" "$SCRIPT_DIR/cinemark-reports.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-reports.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-reports.timer     # next run"
echo "  sudo systemctl start cinemark-reports.service     # run once now"
echo "  journalctl -u cinemark-reports -n 50 --no-pager   # last run"
