#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
seglake_bin="${SEGLAKE_BIN:-go run ./cmd/seglake}"

work_dir="$(mktemp -d)"
trap 'kill "$pid_a" "$pid_b" >/dev/null 2>&1 || true; wait "$pid_a" "$pid_b" >/dev/null 2>&1 || true; rm -rf "$work_dir"' EXIT

log_a="$work_dir/server-a.log"
log_b="$work_dir/server-b.log"
data_a="$work_dir/data-a"
data_b="$work_dir/data-b"
mkdir -p "$data_a" "$data_b"

addr_a="127.0.0.1:9900"
addr_b="127.0.0.1:9901"

start_server() {
  local data_dir="$1"
  local addr="$2"
  local log_file="$3"

  (cd "$root_dir" && $seglake_bin -mode server -data-dir "$data_dir" -addr "$addr" -access-key test -secret-key testsecret >"$log_file" 2>&1) &
  echo $!
}

pid_a=$(start_server "$data_a" "$addr_a" "$log_a")
pid_b=$(start_server "$data_b" "$addr_b" "$log_b")

wait_admin_ready() {
  local data_dir="$1"
  for i in $(seq 1 80); do
    if [[ -S "$data_dir/.seglake-admin.sock" && -f "$data_dir/.seglake-admin.token" ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

wait_admin_ready "$data_a"
wait_admin_ready "$data_b"

echo "==> maintenance + ops (admin socket)"
(cd "$root_dir" && $seglake_bin -mode maintenance -data-dir "$data_a" -maintenance-action enable)
(cd "$root_dir" && $seglake_bin -mode gc-run -data-dir "$data_a" -gc-force)
(cd "$root_dir" && $seglake_bin -mode maintenance -data-dir "$data_a" -maintenance-action disable)

echo "==> repl keys"
(cd "$root_dir" && $seglake_bin -mode keys -data-dir "$data_a" -keys-action create -key-access repl -key-secret replsecret -key-policy rw)
(cd "$root_dir" && $seglake_bin -mode keys -data-dir "$data_b" -keys-action create -key-access repl -key-secret replsecret -key-policy rw)
echo "==> s3 keys"
(cd "$root_dir" && $seglake_bin -mode keys -data-dir "$data_a" -keys-action create -key-access test -key-secret testsecret -key-policy rw)
(cd "$root_dir" && $seglake_bin -mode keys -data-dir "$data_b" -keys-action create -key-access test -key-secret testsecret -key-policy rw)

# Simple S3 PUT via curl + SigV4 presign using segctl helper
bucket="smoke"
key="hello.txt"
payload="hello-admin-socket"

echo "==> create bucket + put object on A"
(cd "$root_dir" && $seglake_bin -mode buckets -data-dir "$data_a" -bucket-action create -bucket "$bucket")

# Use awscli if available for PUT/GET.
if command -v aws >/dev/null 2>&1; then
  payload_file="$(mktemp)"
  printf "%s" "$payload" >"$payload_file"
  AWS_EC2_METADATA_DISABLED=TRUE AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
    aws s3api put-object --bucket "$bucket" --key "$key" --body "$payload_file" --endpoint-url "http://$addr_a" --cli-connect-timeout 5 --cli-read-timeout 20 >/dev/null
else
  echo "awscli not found; skipping direct PUT via awscli"
fi

echo "==> repl pull B <- A (fetch data)"
(cd "$root_dir" && /opt/homebrew/bin/timeout 30s $seglake_bin -mode repl-pull -data-dir "$data_b" -repl-remote "http://$addr_a" -repl-fetch-data=true -repl-limit 100 -repl-retry-timeout 10s -repl-access-key repl -repl-secret-key replsecret)

if command -v aws >/dev/null 2>&1; then
  echo "==> get object from B"
  AWS_EC2_METADATA_DISABLED=TRUE AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
    aws s3api get-object --bucket "$bucket" --key "$key" /tmp/seglake-admin-socket-smoke.out --endpoint-url "http://$addr_b" --cli-connect-timeout 5 --cli-read-timeout 20 >/dev/null
  if ! diff "$payload_file" /tmp/seglake-admin-socket-smoke.out >/dev/null; then
    echo "admin_socket_smoke: payload mismatch after repl pull" >&2
    exit 1
  fi
fi

echo "==> done"
