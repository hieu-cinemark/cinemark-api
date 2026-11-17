#!/bin/bash
# Installs the hourly comment-embedding timer (Attack Detection, signal B). Prerequisites:
#   - sentence-transformers + torch (CPU) in /opt/cinemark-api/.venv (requirements-ml.txt) - e5-small downloads on first run
#   - optional: copy data/comment_embeddings.sqlite from the Mac once so old comments are not embedded again
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

"$APP_DIR/.venv/bin/python" -c "from app.ai.local.embedding import embed_texts; print(embed_texts(['thử']).shape)" \
  || { echo "embedding model not usable in $APP_DIR/.venv (pip install -r requirements-ml.txt)"; exit 1; }
sudo mkdir -p "$APP_DIR/data" && sudo chown app:app "$APP_DIR/data"

sudo cp "$SCRIPT_DIR/cinemark-embed.service" "$SCRIPT_DIR/cinemark-embed.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-embed.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-embed.timer     # next run"
echo "  sudo systemctl start cinemark-embed.service     # run once now"
echo "  journalctl -u cinemark-embed -n 30 --no-pager   # last run"
