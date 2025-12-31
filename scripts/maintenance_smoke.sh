#!/usr/bin/env bash
set -euo pipefail

data_dir="${SEGLAKE_DATA_DIR:-./data}"
seglake_bin="${SEGLAKE_BIN:-./build/seglake}"
ops_access="${OPS_ACCESS_KEY:-ops}"
ops_secret="${OPS_SECRET_KEY:-opsecret}"

if [ ! -x "$seglake_bin" ]; then
  echo "maintenance_smoke: seglake binary not found or not executable: $seglake_bin" >&2
  exit 1
fi
if [ ! -d "$data_dir" ]; then
  echo "maintenance_smoke: data dir not found: $data_dir" >&2
  exit 1
fi

echo "==> ensuring ops key"
"$seglake_bin" -mode keys -data-dir "$data_dir" -keys-action create -key-access "$ops_access" -key-secret "$ops_secret" -key-policy ops >/dev/null

echo "==> enable maintenance"
"$seglake_bin" -mode maintenance -data-dir "$data_dir" -maintenance-action enable >/dev/null

echo "==> wait for quiesced"
for i in $(seq 1 40); do
  state=$("$seglake_bin" -mode maintenance -data-dir "$data_dir" -maintenance-action status | awk -F'[ =]' '{print $2}')
  if [ "$state" = "quiesced" ]; then
    break
  fi
  sleep 0.1
  if [ "$i" -eq 40 ]; then
    echo "maintenance_smoke: maintenance did not reach quiesced" >&2
    exit 1
  fi
done

echo "==> gc-run via ops"
"$seglake_bin" -mode gc-run -data-dir "$data_dir" -gc-force -ops-access-key "$ops_access" -ops-secret-key "$ops_secret"

echo "==> disable maintenance"
"$seglake_bin" -mode maintenance -data-dir "$data_dir" -maintenance-action disable >/dev/null

echo "==> wait for off"
for i in $(seq 1 40); do
  state=$("$seglake_bin" -mode maintenance -data-dir "$data_dir" -maintenance-action status | awk -F'[ =]' '{print $2}')
  if [ "$state" = "off" ]; then
    break
  fi
  sleep 0.1
  if [ "$i" -eq 40 ]; then
    echo "maintenance_smoke: maintenance did not reach off" >&2
    exit 1
  fi
done

echo "==> done"
