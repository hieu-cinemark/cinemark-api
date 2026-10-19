#!/bin/bash
# Installs the ViSoBERT labelling timer on the VPS. Prerequisites:
#   - models/visobert-cinemark/ copied to /opt/cinemark-api/models/ (not in git)
#   - torch (CPU) + transformers installed in /opt/cinemark-api/.venv (requirements-ml.txt)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

test -f "$APP_DIR/models/visobert-cinemark/config.json" || { echo "missing $APP_DIR/models/visobert-cinemark - copy the model first"; exit 1; }
"$APP_DIR/.venv/bin/python" -c "import torch, transformers" || { echo "torch/transformers not installed in $APP_DIR/.venv"; exit 1; }

sudo cp "$SCRIPT_DIR/cinemark-visobert.service" "$SCRIPT_DIR/cinemark-visobert.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-visobert.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-visobert.timer     # next run"
echo "  sudo systemctl start cinemark-visobert.service     # run once now"
echo "  journalctl -u cinemark-visobert -n 30 --no-pager   # last run"
