#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

temporary_root="$(mktemp -d)"
cleanup() {
  sudo chmod -R u+rwX "$temporary_root" 2>/dev/null || true
  sudo rm -rf "$temporary_root"
}
trap cleanup EXIT

verify_layout() {
  local label="$1"
  local owner_uid="$2"
  local owner_gid="$3"
  local data_dir="$temporary_root/$label"
  local expected_original_owner="$owner_uid:$owner_gid"

  sudo install -d -m 0750 -o "$owner_uid" -g "$owner_gid" "$data_dir"
  local files=(
    "$data_dir/macrolens.db"
    "$data_dir/macrolens.db-wal"
    "$data_dir/macrolens.db-shm"
    "$data_dir/calendar_cache.json"
    "$data_dir/unrelated.txt"
  )
  sudo touch "${files[@]}"
  sudo chown "$owner_uid:$owner_gid" "${files[@]}"

  MACROLENS_DATA_DIR="$data_dir" docker compose run --rm --no-deps data-init

  test "$(sudo stat -c '%u:%g' "$data_dir")" = "10001:10001"
  for managed_file in \
    macrolens.db \
    macrolens.db-wal \
    macrolens.db-shm \
    calendar_cache.json
  do
    test "$(sudo stat -c '%u:%g' "$data_dir/$managed_file")" = "10001:10001"
  done
  test "$(sudo stat -c '%u:%g' "$data_dir/unrelated.txt")" = "$expected_original_owner"
}

verify_layout legacy-root 0 0
verify_layout migrated-service-user 10001 10001
