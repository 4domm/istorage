#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

tests=(
  "core_data_path_test.py"
  "control_plane_test.py"
  "range_get_integration_test.py"
  "concurrent_same_key_test.py"
  "special_keys_and_delete_test.py"
)

for test in "${tests[@]}"; do
  out="$(mktemp)"
  if "./${test}" >"${out}" 2>&1; then
    echo "PASS ${test}"
    rm -f "${out}"
    continue
  fi
  echo "FAIL ${test}"
  cat "${out}"
  rm -f "${out}"
  exit 1
done

echo "PASS all"
