#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

endpoint="${S3_ENDPOINT:-http://localhost:9000}"
host="${S3_HOST:-localhost:9000}"
access="${AWS_ACCESS_KEY_ID:-test}"
secret="${AWS_SECRET_ACCESS_KEY:-testsecret}"
region="${AWS_DEFAULT_REGION:-us-east-1}"
bucket="${S3_BUCKET:-stress}"
prefix="${PREFIX:-stress/$(date +%Y%m%dT%H%M%S)}"
duration_raw="${DURATION:-60s}"
concurrency="${CONCURRENCY:-4}"
obj_size_raw="${OBJ_SIZE:-1MiB}"
mix_raw="${MIX_PUT_GET_DEL:-70/25/5}"
list_interval_raw="${LIST_INTERVAL:-0s}"
mpu_ratio_raw="${MULTIPART_RATIO:-0%}"
mpu_size_raw="${MULTIPART_SIZE:-64MiB}"
mpu_part_size_raw="${MULTIPART_PART_SIZE:-8MiB}"
out_path="${OUT:-}"
read_after_put="${READ_AFTER_PUT:-0}"
log_errors="${LOG_ERRORS:-0}"

dry_run=1
force=0

usage() {
  cat <<EOF
Usage: $0 [-force] [--dry-run]

Env:
  S3_ENDPOINT, S3_HOST, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  S3_BUCKET, PREFIX, DURATION, CONCURRENCY, OBJ_SIZE, MIX_PUT_GET_DEL, LIST_INTERVAL
  MULTIPART_RATIO, MULTIPART_SIZE, MULTIPART_PART_SIZE, OUT
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -force) force=1; dry_run=0; shift ;;
    --dry-run) dry_run=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [ "$force" -eq 0 ]; then
  dry_run=1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "awscli not found in PATH" >&2
  exit 1
fi

if [[ "$endpoint" != http://localhost:* && "$endpoint" != http://127.0.0.1:* ]]; then
  if [ "$force" -ne 1 ]; then
    echo "Refusing to run against non-local endpoint without -force: $endpoint" >&2
    exit 1
  fi
fi

parse_duration() {
  local v="$1" n unit
  n="${v%[smh]}"
  unit="${v##$n}"
  if [ "$n" = "$v" ]; then
    echo "$v"
    return
  fi
  case "$unit" in
    s) echo "$n" ;;
    m) echo $((n * 60)) ;;
    h) echo $((n * 3600)) ;;
    *) echo "$v" ;;
  esac
}

parse_size() {
  local v="$1" n unit
  n="${v%[KMG]iB}"
  unit="${v##$n}"
  if [ "$n" = "$v" ]; then
    echo "$v"
    return
  fi
  case "$unit" in
    KiB) echo $((n * 1024)) ;;
    MiB) echo $((n * 1024 * 1024)) ;;
    GiB) echo $((n * 1024 * 1024 * 1024)) ;;
    *) echo "$v" ;;
  esac
}

parse_percent() {
  local v="$1"
  v="${v%%%}"
  echo "$v"
}

now_ms() {
  python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

IFS='/' read -r mix_put mix_get mix_del <<<"$mix_raw"
if [ -z "${mix_put:-}" ] || [ -z "${mix_get:-}" ] || [ -z "${mix_del:-}" ]; then
  echo "invalid MIX_PUT_GET_DEL: $mix_raw" >&2
  exit 1
fi

mix_total=$((mix_put + mix_get + mix_del))
if [ "$mix_total" -le 0 ]; then
  echo "invalid MIX_PUT_GET_DEL: $mix_raw" >&2
  exit 1
fi

list_interval=$(parse_duration "$list_interval_raw")
run_seconds=$(parse_duration "$duration_raw")
obj_size=$(parse_size "$obj_size_raw")
mpu_size=$(parse_size "$mpu_size_raw")
mpu_part_size=$(parse_size "$mpu_part_size_raw")
mpu_ratio=$(parse_percent "$mpu_ratio_raw")
min_mpu_part=$((5 * 1024 * 1024))

if [ "$mpu_ratio" -gt 0 ] && [ "$mpu_part_size" -lt "$min_mpu_part" ] && [ "$mpu_size" -gt "$mpu_part_size" ]; then
  echo "MULTIPART_PART_SIZE must be >= 5MiB (except last part). Got ${mpu_part_size}B." >&2
  exit 1
fi

if [ "$dry_run" -eq 1 ]; then
  cat <<EOF
[stress_s3] dry-run
endpoint=$endpoint
bucket=$bucket
prefix=$prefix
duration=${run_seconds}s
concurrency=$concurrency
obj_size=${obj_size}B
mix=$mix_put/$mix_get/$mix_del
list_interval=${list_interval}s
multipart_ratio=${mpu_ratio}%
multipart_size=${mpu_size}B
multipart_part_size=${mpu_part_size}B
read_after_put=$read_after_put
EOF
  exit 0
fi

export AWS_ACCESS_KEY_ID="$access"
export AWS_SECRET_ACCESS_KEY="$secret"
export AWS_DEFAULT_REGION="$region"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"
export AWS_MAX_ATTEMPTS="${AWS_MAX_ATTEMPTS:-1}"
export AWS_RETRY_MODE="${AWS_RETRY_MODE:-standard}"

aws_args=(--endpoint-url "$endpoint")
if aws --version 2>/dev/null | grep -q 'aws-cli/2'; then
  cli_connect="${AWS_CLI_CONNECT_TIMEOUT:-2}"
  cli_read="${AWS_CLI_READ_TIMEOUT:-10}"
  aws_args+=(--cli-connect-timeout "$cli_connect" --cli-read-timeout "$cli_read")
fi

aws s3 mb "s3://$bucket" "${aws_args[@]}" >/dev/null 2>&1 || true

workdir=$(mktemp -d /tmp/seglake-stress.XXXXXX)
trap 'rm -rf "$workdir"' EXIT

make_payload() {
  local path="$1" size="$2"
  if [ ! -f "$path" ]; then
    dd if=/dev/zero of="$path" bs=1 count=0 seek="$size" 2>/dev/null
  fi
}

op_put() {
	local key="$1" payload="$2" start end dur
	start=$(now_ms)
	if [ "$log_errors" -eq 1 ]; then
		aws s3 cp "$payload" "s3://$bucket/$key" "${aws_args[@]}" >/dev/null
	else
		aws s3 cp "$payload" "s3://$bucket/$key" "${aws_args[@]}" >/dev/null 2>&1
	fi
	local rc=$?
	end=$(now_ms)
	dur=$((end - start))
	return $rc
}

op_get() {
	local key="$1" start end dur
	start=$(now_ms)
	if [ "$log_errors" -eq 1 ]; then
		aws s3 cp "s3://$bucket/$key" - "${aws_args[@]}" >/dev/null
	else
		aws s3 cp "s3://$bucket/$key" - "${aws_args[@]}" >/dev/null 2>&1
	fi
	local rc=$?
	end=$(now_ms)
	dur=$((end - start))
	return $rc
}

op_del() {
	local key="$1" start end dur
	start=$(now_ms)
	if [ "$log_errors" -eq 1 ]; then
		aws s3 rm "s3://$bucket/$key" "${aws_args[@]}" >/dev/null
	else
		aws s3 rm "s3://$bucket/$key" "${aws_args[@]}" >/dev/null 2>&1
	fi
	local rc=$?
	end=$(now_ms)
	dur=$((end - start))
	return $rc
}

op_list() {
  aws s3 ls "s3://$bucket" "${aws_args[@]}" >/dev/null 2>&1 || true
}

op_multipart_put() {
	local key="$1" payload="$2"
	local upload_id etag part_number part_start part_len part_count
	part_count=$(( (mpu_size + mpu_part_size - 1) / mpu_part_size ))
	if [ "$log_errors" -eq 1 ]; then
		upload_id=$(aws s3api create-multipart-upload --bucket "$bucket" --key "$key" "${aws_args[@]}" --query UploadId --output text)
	else
		upload_id=$(aws s3api create-multipart-upload --bucket "$bucket" --key "$key" "${aws_args[@]}" --query UploadId --output text 2>/dev/null)
	fi
	if [ -z "$upload_id" ] || [ "$upload_id" = "None" ]; then
		return 1
	fi
	local parts_file="$workdir/parts_${upload_id}.json"
	local parts_tmp="$workdir/parts_${upload_id}.txt"
	: > "$parts_tmp"
  for part_number in $(seq 1 "$part_count"); do
    part_start=$(( (part_number - 1) * mpu_part_size ))
    part_len=$mpu_part_size
    if [ $((part_start + part_len)) -gt "$mpu_size" ]; then
      part_len=$((mpu_size - part_start))
    fi
    local part_file="$workdir/part_${key//\//_}_${part_number}.bin"
    python3 - "$payload" "$part_file" "$part_start" "$part_len" <<'PY'
import sys

src, dst, start, length = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
with open(src, "rb") as f:
    f.seek(start)
    data = f.read(length)
with open(dst, "wb") as f:
    f.write(data)
PY
		if [ "$log_errors" -eq 1 ]; then
			etag=$(aws s3api upload-part --bucket "$bucket" --key "$key" --part-number "$part_number" --upload-id "$upload_id" --body "$part_file" "${aws_args[@]}" --query ETag --output text)
		else
			etag=$(aws s3api upload-part --bucket "$bucket" --key "$key" --part-number "$part_number" --upload-id "$upload_id" --body "$part_file" "${aws_args[@]}" --query ETag --output text 2>/dev/null)
		fi
		if [ -z "$etag" ] || [ "$etag" = "None" ]; then
			if [ "$log_errors" -eq 1 ]; then
				aws s3api abort-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" "${aws_args[@]}" >/dev/null || true
			else
				aws s3api abort-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" "${aws_args[@]}" >/dev/null 2>&1 || true
			fi
			return 1
		fi
		echo "${part_number}|${etag}" >> "$parts_tmp"
	done
	if [ ! -s "$parts_tmp" ]; then
		if [ "$log_errors" -eq 1 ]; then
			aws s3api abort-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" "${aws_args[@]}" >/dev/null || true
		else
			aws s3api abort-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" "${aws_args[@]}" >/dev/null 2>&1 || true
		fi
		return 1
	fi
	python3 - "$parts_tmp" "$parts_file" <<'PY'
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
parts = []
with open(src, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        part_number_str, etag = line.split("|", 1)
        etag = etag.strip().strip('"')
        parts.append({"ETag": etag, "PartNumber": int(part_number_str)})
with open(dst, "w", encoding="utf-8") as f:
    json.dump({"Parts": parts}, f)
PY
	if [ "$log_errors" -eq 1 ]; then
		aws s3api complete-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" --multipart-upload "file://$parts_file" "${aws_args[@]}" >/dev/null
	else
		aws s3api complete-multipart-upload --bucket "$bucket" --key "$key" --upload-id "$upload_id" --multipart-upload "file://$parts_file" "${aws_args[@]}" >/dev/null 2>&1
	fi
}

measure_op() {
  local op="$1" dur_file="$2" err_file="$3"
  shift 3
  local start end dur
  start=$(now_ms)
  set +e
  "$@"
  local rc=$?
  set -e
  end=$(now_ms)
  dur=$((end - start))
  if [ "$rc" -eq 0 ]; then
    echo "$dur" >> "$dur_file"
  else
    echo "$dur" >> "$err_file"
  fi
}

worker() {
  local id="$1"
  local payload="$workdir/payload_${id}.bin"
  local mpu_payload="$workdir/mpu_payload_${id}.bin"
  make_payload "$payload" "$obj_size"
  make_payload "$mpu_payload" "$mpu_size"

  local end_time=$(( $(date +%s) + run_seconds ))
  local last_list=$(( $(date +%s) ))
  local counter=0
  local last_key=""

  local dur_put="$workdir/put_${id}.dur"
  local dur_get="$workdir/get_${id}.dur"
  local dur_del="$workdir/del_${id}.dur"
  local err_put="$workdir/put_${id}.err"
  local err_get="$workdir/get_${id}.err"
  local err_del="$workdir/del_${id}.err"

  while [ "$(date +%s)" -lt "$end_time" ]; do
    local r=$((RANDOM % mix_total))
    if [ "$list_interval" -gt 0 ]; then
      local now=$(date +%s)
      if [ $((now - last_list)) -ge "$list_interval" ]; then
        op_list
        last_list="$now"
      fi
    fi

    if [ "$r" -lt "$mix_put" ]; then
      counter=$((counter + 1))
      local key="$prefix/w${id}/obj_${counter}"
      last_key="$key"
      local mpu_roll=$((RANDOM % 100))
      if [ "$mpu_ratio" -gt 0 ] && [ "$mpu_roll" -lt "$mpu_ratio" ]; then
        measure_op mpu "$dur_put" "$err_put" op_multipart_put "$key" "$mpu_payload"
      else
        measure_op put "$dur_put" "$err_put" op_put "$key" "$payload"
      fi
      if [ "$read_after_put" -eq 1 ]; then
        measure_op get "$dur_get" "$err_get" op_get "$key"
      fi
    elif [ "$r" -lt $((mix_put + mix_get)) ]; then
      if [ -n "$last_key" ]; then
        measure_op get "$dur_get" "$err_get" op_get "$last_key"
      fi
    else
      if [ -n "$last_key" ]; then
        measure_op del "$dur_del" "$err_del" op_del "$last_key"
        last_key=""
      fi
    fi
  done
}

for i in $(seq 1 "$concurrency"); do
  worker "$i" &
done
wait

summarize_glob() {
  local dur_glob="$1" err_glob="$2" label="$3"
  local dur_tmp="$workdir/${label}_all.dur"
  local err_tmp="$workdir/${label}_all.err"
  : > "$dur_tmp"
  : > "$err_tmp"
  local dur_files=()
  local err_files=()
  local f
  for f in $dur_glob; do
    if [ "$f" != "$dur_tmp" ]; then
      dur_files+=("$f")
    fi
  done
  for f in $err_glob; do
    if [ "$f" != "$err_tmp" ]; then
      err_files+=("$f")
    fi
  done
  if [ "${#dur_files[@]}" -gt 0 ]; then
    cat "${dur_files[@]}" >> "$dur_tmp" 2>/dev/null || true
  fi
  if [ "${#err_files[@]}" -gt 0 ]; then
    cat "${err_files[@]}" >> "$err_tmp" 2>/dev/null || true
  fi
  local ok_count err_count avg p95 p99
  ok_count=$(wc -l < "$dur_tmp" 2>/dev/null || echo 0)
  err_count=$(wc -l < "$err_tmp" 2>/dev/null || echo 0)
  if [ "$ok_count" -gt 0 ]; then
    avg=$(awk '{s+=$1} END {printf "%.2f", s/NR}' "$dur_tmp")
    p95=$(sort -n "$dur_tmp" | awk -v n="$ok_count" 'BEGIN{p=int(n*0.95)} p<1{p=1} {a[NR]=$1} END{print a[p]}')
    p99=$(sort -n "$dur_tmp" | awk -v n="$ok_count" 'BEGIN{p=int(n*0.99)} p<1{p=1} {a[NR]=$1} END{print a[p]}')
  else
    avg=0; p95=0; p99=0
  fi
  echo "$label ok=$ok_count err=$err_count avg_ms=$avg p95_ms=$p95 p99_ms=$p99"
}

puts=$(summarize_glob "$workdir/put_*.dur" "$workdir/put_*.err" "put")
gets=$(summarize_glob "$workdir/get_*.dur" "$workdir/get_*.err" "get")
dels=$(summarize_glob "$workdir/del_*.dur" "$workdir/del_*.err" "del")

report="[stress_s3] duration=${run_seconds}s concurrency=$concurrency\n$puts\n$gets\n$dels"

echo -e "$report"

if [ -n "$out_path" ]; then
  echo -e "$report" > "$out_path"
fi
