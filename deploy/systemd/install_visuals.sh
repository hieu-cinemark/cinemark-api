#!/bin/bash
# Installs the post cover image timer on the VPS. Prerequisites:
#   - torch (CPU) + transformers + pillow in /opt/cinemark-api/.venv (requirements-ml.txt)
#   - R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY in /opt/cinemark-api/.env (same token as the lake writer)
# The SigLIP base weights (~800 MB) download to ~app/.cache/huggingface on the first run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

"$APP_DIR/.venv/bin/python" -c "import torch, transformers, PIL" || { echo "torch/transformers/pillow not installed in $APP_DIR/.venv (pip install -r requirements-ml.txt)"; exit 1; }
grep -q '^R2_ACCESS_KEY_ID=.' "$APP_DIR/.env" || { echo "R2_* missing in $APP_DIR/.env"; exit 1; }

sudo cp "$SCRIPT_DIR/cinemark-visuals.service" "$SCRIPT_DIR/cinemark-visuals.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-visuals.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-visuals.timer     # next run"
echo "  sudo systemctl start cinemark-visuals.service     # run once now"
echo "  journalctl -u cinemark-visuals -n 30 --no-pager   # last run"
