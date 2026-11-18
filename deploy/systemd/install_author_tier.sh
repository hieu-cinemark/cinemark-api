#!/bin/bash
# Installs the author tier model timers on the VPS (daily labelling + weekly retrain). Prerequisites:
#   - scikit-learn installed in /opt/cinemark-api/.venv (requirements.txt pins the version the model was trained with)
#   - models/author_tier/model.joblib present, OR let the first retrain build it:
#       sudo systemctl start cinemark-author-retrain.service
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

"$APP_DIR/.venv/bin/python" -c "import sklearn, scipy, joblib" || { echo "scikit-learn not installed in $APP_DIR/.venv (pip install -r requirements.txt)"; exit 1; }

sudo cp "$SCRIPT_DIR/cinemark-author-tier.service" "$SCRIPT_DIR/cinemark-author-tier.timer" \
  "$SCRIPT_DIR/cinemark-author-retrain.service" "$SCRIPT_DIR/cinemark-author-retrain.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-author-tier.timer cinemark-author-retrain.timer

test -f "$APP_DIR/models/author_tier/model.joblib" || echo "No model yet: run  sudo systemctl start cinemark-author-retrain.service  once."
echo "Installed. Useful commands:"
echo "  systemctl list-timers 'cinemark-author-*'               # next runs"
echo "  sudo systemctl start cinemark-author-retrain.service     # retrain now (export + train + keep if not worse)"
echo "  sudo systemctl start cinemark-author-tier.service        # label now"
echo "  journalctl -u cinemark-author-tier -n 30 --no-pager      # last labelling run"
