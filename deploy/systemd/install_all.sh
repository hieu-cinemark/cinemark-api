#!/bin/bash
# One command to install every cinemark-api timer on the VPS (idempotent; safe to re-run after a deploy).
# Order matters only for embeddings -> clusters. Each install_*.sh checks its own prerequisites and stops on failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for name in visobert aspects embed visuals reports clusters; do
  echo "== install_${name}.sh"
  bash "$SCRIPT_DIR/install_${name}.sh"
done
systemctl list-timers 'cinemark-*' --no-pager
