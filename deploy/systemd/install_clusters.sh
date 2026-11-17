#!/bin/bash
# Installs the nightly comment-cluster timer. Needs the embeddings written by cinemark-embed on THIS machine
# (data/comment_embeddings.sqlite) - install_embed.sh first; Bee API key in /opt/cinemark-api/.env.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/cinemark-api

sudo -u app mkdir -p "$APP_DIR/data/clusters"
sudo cp "$SCRIPT_DIR/cinemark-clusters.service" "$SCRIPT_DIR/cinemark-clusters.timer" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cinemark-clusters.timer

echo "Installed. Useful commands:"
echo "  systemctl list-timers cinemark-clusters.timer      # next run"
echo "  sudo -u app $APP_DIR/.venv/bin/python -m scripts.jobs.topic_clusters --dry-run   # which films would run"
echo "  journalctl -u cinemark-clusters -n 50 --no-pager   # last run"
