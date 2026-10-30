#!/usr/bin/env bash
# Deploy một repo trên VPS: pull (bằng user sở hữu repo), cài lại thư viện nếu requirements.txt đổi, restart các service
# ĐANG CHẠY của repo đó, rồi kiểm tra /health. GitHub Actions gọi qua SSH (xem .github/workflows/ci.yml của cinemark-api
# và spider-hub); chạy tay được: sudo /usr/local/sbin/deploy-repo cinemark-api
#
# Cài một lần (root):
#   install -o root -g root -m 755 /opt/cinemark-api/deploy/vps/deploy-repo.sh /usr/local/sbin/deploy-repo
#   echo 'deploy ALL=(root) NOPASSWD: /usr/local/sbin/deploy-repo' > /etc/sudoers.d/deploy-repo && chmod 440 /etc/sudoers.d/deploy-repo
set -euo pipefail

repo="${1:-}"
case "$repo" in
  cinemark-api)
    dir=/opt/cinemark-api
    units="cinemark-api-web cinemark-scheduler cinemark-ingest cinemark-lake-writer cinemark-relabel"
    health=http://127.0.0.1:8000/health
    ;;
  spider-hub)
    dir=/opt/spider-hub
    units="spider-hub-crawl"
    health=""
    ;;
  *)
    echo "usage: deploy-repo cinemark-api|spider-hub" >&2
    exit 2
    ;;
esac

owner=$(stat -c %U "$dir")
as_owner() { sudo -u "$owner" "$@"; }

before=$(as_owner git -C "$dir" rev-parse HEAD)
as_owner git -C "$dir" pull --ff-only
after=$(as_owner git -C "$dir" rev-parse HEAD)
if [ "$before" = "$after" ]; then
  echo "$repo: đã ở $after, không có gì mới"
  exit 0
fi
echo "$repo: $before -> $after"

if as_owner git -C "$dir" diff --name-only "$before" "$after" | grep -qx requirements.txt; then
  echo "requirements.txt đổi - cài lại thư viện"
  as_owner "$dir/.venv/bin/pip" install -q -r "$dir/requirements.txt"
fi

for unit in $units; do
  if systemctl is-active --quiet "$unit"; then
    systemctl restart "$unit"
    echo "restarted $unit"
  fi
done

if [ -n "$health" ]; then
  for _ in $(seq 1 30); do
    if curl -fsS -m 3 "$health" >/dev/null; then
      echo "health ok"
      exit 0
    fi
    sleep 2
  done
  echo "health check FAILED sau 60s" >&2
  journalctl -u cinemark-api-web -n 40 --no-pager >&2 || true
  exit 1
fi
