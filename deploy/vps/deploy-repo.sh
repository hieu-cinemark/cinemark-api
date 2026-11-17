#!/usr/bin/env bash
# Deploy một repo trên VPS: pull (bằng user sở hữu repo), cài lại thư viện nếu requirements*.txt đổi, đồng bộ + bật các timer
# systemd của repo (deploy/systemd/*.timer), restart các service ĐANG CHẠY, rồi kiểm tra /health. Script tự cập nhật bản cài ở
# /usr/local/sbin khi bản trong repo đổi, nên không còn bước cài tay nào sau mỗi lần deploy. GitHub Actions gọi qua SSH (xem .github/workflows/ci.yml của cinemark-api
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
    # Tên unit trên VPS là spider-hub-crawl, file trong repo là spider-hub-crawl-consumer - unit nào đang chạy thì restart.
    units="spider-hub-crawl spider-hub-crawl-consumer"
    health=""
    ;;
  *)
    echo "usage: deploy-repo cinemark-api|spider-hub" >&2
    exit 2
    ;;
esac

owner=$(stat -c %U "$dir")
as_owner() { sudo -u "$owner" "$@"; }
self_installed=/usr/local/sbin/deploy-repo

# Timer chạy job nặng (deploy/systemd/<tên>.timer + <tên>.service): chép bản mới vào /etc/systemd/system khi khác, bật timer.
# Chỉ đụng các cặp timer/service - service chạy lâu (ingest, scheduler) trên VPS mang tên khác file trong repo nên không chép.
sync_timers() {
  local src="$dir/deploy/systemd" changed=0 timer name
  [ -d "$src" ] || return 0
  for timer in "$src"/*.timer; do
    [ -e "$timer" ] || continue
    name=$(basename "$timer" .timer)
    for unit in "$name.timer" "$name.service"; do
      if [ -f "$src/$unit" ] && ! cmp -s "$src/$unit" "/etc/systemd/system/$unit"; then
        install -m 644 "$src/$unit" "/etc/systemd/system/$unit"
        echo "đồng bộ $unit"
        changed=1
      fi
    done
  done
  if [ "$changed" = 1 ]; then
    systemctl daemon-reload
  fi
  as_owner mkdir -p "$dir/data/clusters"
  for timer in "$src"/*.timer; do
    [ -e "$timer" ] || continue
    name=$(basename "$timer")
    systemctl is-enabled --quiet "$name" 2>/dev/null || { systemctl enable --now "$name" && echo "bật $name"; }
  done
}

if [ -n "${DEPLOY_BEFORE:-}" ]; then
  # Lần chạy lại bằng bản script mới (xem bên dưới): code đã pull rồi.
  before=$DEPLOY_BEFORE
else
  before=$(as_owner git -C "$dir" rev-parse HEAD)
  as_owner git -C "$dir" pull --ff-only
fi
after=$(as_owner git -C "$dir" rev-parse HEAD)

# Bản script trong repo mới hơn bản đang chạy: cài đè rồi chạy lại bằng bản mới (một lần, cùng mốc `before`).
if [ -z "${DEPLOY_BEFORE:-}" ] && [ -f "$dir/deploy/vps/deploy-repo.sh" ] && ! cmp -s "$dir/deploy/vps/deploy-repo.sh" "$self_installed"; then
  install -o root -g root -m 755 "$dir/deploy/vps/deploy-repo.sh" "$self_installed"
  echo "deploy-repo đã cập nhật, chạy lại bằng bản mới"
  DEPLOY_BEFORE=$before exec "$self_installed" "$repo"
fi

if [ "$before" = "$after" ]; then
  echo "$repo: đã ở $after, không có gì mới"
  if [ "$repo" = cinemark-api ]; then
    sync_timers
  fi
  exit 0
fi
echo "$repo: $before -> $after"

# .venv có thể tạo bằng uv (không có pip bên trong) - thử pip, python -m pip, rồi uv.
install_requirements() {
  local req="${1:-$dir/requirements.txt}" py="$dir/.venv/bin/python"
  if [ -x "$dir/.venv/bin/pip" ]; then
    as_owner "$dir/.venv/bin/pip" install -q -r "$req"
  elif as_owner "$py" -m pip --version >/dev/null 2>&1; then
    as_owner "$py" -m pip install -q -r "$req"
  elif uv_bin=$(command -v uv || ls /root/.local/bin/uv /home/"$owner"/.local/bin/uv 2>/dev/null | head -1) && [ -n "$uv_bin" ]; then
    as_owner "$uv_bin" pip install -q --python "$py" -r "$req"
  else
    echo "KHÔNG cài được thư viện: .venv không có pip và không tìm thấy uv" >&2
    return 1
  fi
}

changed_files=$(as_owner git -C "$dir" diff --name-only "$before" "$after")
if grep -qx requirements.txt <<<"$changed_files"; then
  echo "requirements.txt đổi - cài lại thư viện"
  install_requirements
  if [ "$repo" = spider-hub ]; then
    as_owner "$dir/.venv/bin/python" -m patchright install chromium
  fi
fi

# Thư viện ML (torch, transformers, sentence-transformers) cho các timer: cài khi file đổi hoặc .venv chưa có torch.
if [ -f "$dir/requirements-ml.txt" ] && { grep -qx requirements-ml.txt <<<"$changed_files" || ! as_owner "$dir/.venv/bin/python" -c "import torch" 2>/dev/null; }; then
  echo "cài thư viện ML (requirements-ml.txt)"
  install_requirements "$dir/requirements-ml.txt" || echo "CẢNH BÁO: không cài được requirements-ml.txt - các timer ML sẽ lỗi" >&2
fi

if [ "$repo" = cinemark-api ]; then
  sync_timers
fi

# Trước khi restart: thiếu trình duyệt / lệch phiên bản / mất Redis thì service cũ chạy tiếp, không để job đầu tiên lỗi trên
# tài khoản thật. Không mở trang nào của nền tảng.
if [ "$repo" = spider-hub ]; then
  as_owner bash -c "cd '$dir' && .venv/bin/python -m scripts.preflight_browser"
fi

restarted=0
for unit in $units; do
  if systemctl is-active --quiet "$unit"; then
    systemctl restart "$unit"
    echo "restarted $unit"
    restarted=$((restarted + 1))
  fi
done
if [ "$restarted" = 0 ]; then
  echo "CẢNH BÁO: không unit nào của $repo đang chạy, chưa restart gì ($units)" >&2
fi

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
