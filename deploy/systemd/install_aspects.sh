#!/bin/bash
# Installs the aspect labelling timer (Review Angles) on the VPS. Prerequisites:
#   - models/visobert-aspects/ copied to /opt/cinemark-api/models/ (not in git - trained by scripts/ml/aspect_finetune.py)
#   - torch (CPU) + transformers installed in /opt/cinemark-api/.venv (requirements-ml.txt)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

test -f "$APP_DIR/models/visobert-aspects/aspect_labels.json" || { echo "missing $APP_DIR/models/visobert-aspects - copy the model first"; exit 1; }
"$APP_DIR/.venv/bin/python" -c "import torch, transformers" || { echo "torch/transformers not installed in $APP_DIR/.venv"; exit 1; }

sudo cp "$SCRIPT_DIR/cinemark-aspects.service" "$SCRIPT_DIR/cinemark-aspects.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-aspects.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-aspects.timer     # next run"
echo "  sudo systemctl start cinemark-aspects.service     # run once now"
echo "  journalctl -u cinemark-aspects -n 30 --no-pager   # last run"
