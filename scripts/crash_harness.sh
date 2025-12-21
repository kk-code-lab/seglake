#!/usr/bin/env bash
set -euo pipefail

ITERATIONS="${1:-3}"
ADDR="${ADDR:-:9100}"
HOST="http://localhost${ADDR}"
DATA_DIR="${DATA_DIR:-$(mktemp -d)}"
BIN="${BIN:-build/seglake}"

mkdir -p "$(dirname "$BIN")"
if [[ ! -x "$BIN" ]]; then
  go build -o "$BIN" ./cmd/seglake
fi

echo "data_dir=$DATA_DIR"
echo "addr=$ADDR"

SERVER_PID=""
SERVER_LOG="${SERVER_LOG:-/tmp/seglake_crash_harness.log}"

start_server() {
  "$BIN" -addr "$ADDR" -data-dir "$DATA_DIR" >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 20); do
    if curl -sS "$HOST/v1/meta/stats" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not start (check $SERVER_LOG)" >&2
  return 1
}

kill_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

run_ops_json() {
  local mode="$1"
  local out
  if ! out=$("$BIN" -mode "$mode" -data-dir "$DATA_DIR" -json); then
    echo "ops $mode failed (see $SERVER_LOG)" >&2
    exit 1
  fi
  if [[ -z "$out" ]]; then
    echo "ops $mode returned empty output" >&2
    exit 1
  fi
  echo "$out"
}

assert_report_ok() {
  python3 - "$1" "$2" <<'PY'
import json, sys
report = json.loads(sys.argv[2])
errors = report.get("errors", 0)
invalid = report.get("invalid_manifests", 0)
missing = report.get("missing_segments", 0)
oob = report.get("out_of_bounds_chunks", 0)
if errors or invalid or missing or oob:
    raise SystemExit(f"report {sys.argv[1]} not clean: errors={errors} invalid={invalid} missing={missing} oob={oob}")
PY
}

put_object() {
  local key="$1"
  local data="$2"
  curl -sS -X PUT "$HOST/demo/$key" --data-binary "$data" -o /dev/null
}

put_object_file() {
  local key="$1"
  local path="$2"
  curl -sS -X PUT "$HOST/demo/$key" --data-binary "@$path" -o /dev/null
}

multipart_upload() {
  local key="$1"
  local part1="$2"
  local part2="$3"
  curl -sS -X POST "$HOST/demo/$key?uploads" -o /tmp/seglake_mpu_init.xml
  local upload_id
  upload_id=$(python3 - <<'PY'
import xml.etree.ElementTree as ET
xml = open('/tmp/seglake_mpu_init.xml','rb').read()
root = ET.fromstring(xml)
print(root.findtext('UploadId'))
PY
)
  local etag1 etag2
  etag1=$(curl -sS -D - -X PUT "$HOST/demo/$key?partNumber=1&uploadId=$upload_id" --data-binary "@$part1" -o /dev/null | awk 'tolower($1)=="etag:"{print $2}' | tr -d '\r')
  etag2=$(curl -sS -D - -X PUT "$HOST/demo/$key?partNumber=2&uploadId=$upload_id" --data-binary "@$part2" -o /dev/null | awk 'tolower($1)=="etag:"{print $2}' | tr -d '\r')
  cat > /tmp/seglake_mpu_complete.xml <<XML
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>${etag1}</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>${etag2}</ETag></Part>
</CompleteMultipartUpload>
XML
  curl -sS -X POST "$HOST/demo/$key?uploadId=$upload_id" --data-binary @/tmp/seglake_mpu_complete.xml -o /dev/null
}

python3 - <<'PY'
with open('/tmp/seglake_large.bin','wb') as f:
    f.write(b'a' * (5 * 1024 * 1024))
with open('/tmp/seglake_small.bin','wb') as f:
    f.write(b'b' * 16)
PY

for i in $(seq 1 "$ITERATIONS"); do
  echo "iteration=$i start"
  start_server

  put_object "iter-$i/small.txt" "hello-$i"
  put_object_file "iter-$i/large.bin" "/tmp/seglake_large.bin"
  multipart_upload "iter-$i/mpu.bin" "/tmp/seglake_large.bin" "/tmp/seglake_small.bin"

  kill_server
  sleep 0.2

  fsck_report=$(run_ops_json fsck)
  assert_report_ok fsck "$fsck_report"
  rebuild_report=$(run_ops_json rebuild-index)
  assert_report_ok rebuild-index "$rebuild_report"

  start_server
  curl -sS "$HOST/demo/iter-$i/small.txt" -o /dev/null
  kill_server
  echo "iteration=$i ok"
done

echo "done"
