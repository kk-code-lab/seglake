#!/usr/bin/env bash
set -euo pipefail

endpoint="${S3_ENDPOINT:-http://localhost:9000}"
host="${S3_HOST:-localhost:9000}"
access="${S3_ACCESS_KEY:-test}"
secret="${S3_SECRET_KEY:-testsecret}"
region="${S3_REGION:-us-east-1}"
service="s3"
payload_hash="UNSIGNED-PAYLOAD"
bucket="${S3_BUCKET:-public}"
object_key="${S3_OBJECT_KEY:-hello.txt}"
data_file="${S3_DATA_FILE:-./docs/spec.md}"

sign() {
  local method="$1" path="$2" query="$3" amz_date="$4"
  python3 - "$method" "$path" "$query" "$amz_date" "$access" "$secret" "$region" "$service" "$host" "$payload_hash" <<'PY'
import sys, hashlib, hmac
from urllib.parse import quote

method, path, query, amz_date, access, secret, region, service, host, payload_hash = sys.argv[1:]

def enc(s: str) -> str:
    return quote(s, safe='-_.~')

def canonical_query(q: str) -> str:
    if not q:
        return ''
    pairs = []
    for part in q.split('&'):
        if '=' in part:
            k, v = part.split('=', 1)
        else:
            k, v = part, ''
        pairs.append((enc(k), enc(v)))
    pairs.sort()
    return '&'.join([f"{k}={v}" for k, v in pairs])

canonical_uri = quote(path, safe='/-_.~') or '/'
canonical_headers = (
    f"host:{host}\n"
    f"x-amz-content-sha256:{payload_hash}\n"
    f"x-amz-date:{amz_date}\n"
)
signed_headers = "host;x-amz-content-sha256;x-amz-date"
canonical_request = "\n".join([
    method,
    canonical_uri,
    canonical_query(query),
    canonical_headers,
    signed_headers,
    payload_hash,
])

cr_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
date_scope = amz_date[:8]
scope = f"{date_scope}/{region}/{service}/aws4_request"
string_to_sign = "\n".join([
    "AWS4-HMAC-SHA256",
    amz_date,
    scope,
    cr_hash,
])

def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

k_date = sign(("AWS4" + secret).encode(), date_scope)
k_region = sign(k_date, region)
k_service = sign(k_region, service)
k_signing = sign(k_service, "aws4_request")
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

auth = (
    "AWS4-HMAC-SHA256 "
    f"Credential={access}/{scope},"
    f"SignedHeaders={signed_headers},"
    f"Signature={signature}"
)
print(auth)
PY
}

curl_req_signed() {
  local method="$1" path="$2" query="$3" data_file="$4"
  shift 4
  local -a headers=($@)
  local amz_date auth url
  amz_date=$(date -u +%Y%m%dT%H%M%SZ)
  auth=$(sign "$method" "$path" "$query" "$amz_date")
  url="$endpoint$path"
  if [ -n "$query" ]; then
    url+="?$query"
  fi
  local hdrfile="/tmp/curl_headers.out"
  local bodyfile="/tmp/curl_body.out"
  local -a cmd=(curl -sS -m 5 -D "$hdrfile" -o "$bodyfile" -X "$method" "$url" \
    -H "Host: $host" \
    -H "x-amz-content-sha256: $payload_hash" \
    -H "x-amz-date: $amz_date" \
    -H "Authorization: $auth")
  for h in "${headers[@]}"; do
    cmd+=(-H "$h")
  done
  if [ -n "$data_file" ]; then
    cmd+=(--data-binary "@$data_file")
  fi
  echo ""
  echo "==> signed $method $url"
  "${cmd[@]}"
  head -1 "$hdrfile"
}

curl_req_unsigned() {
  local method="$1" path="$2" data_file="$3"
  local url="$endpoint$path"
  local hdrfile="/tmp/curl_headers.out"
  local bodyfile="/tmp/curl_body.out"
  local -a cmd=(curl -sS -m 5 -D "$hdrfile" -o "$bodyfile" -X "$method" "$url")
  if [ -n "$data_file" ]; then
    cmd+=(--data-binary "@$data_file")
  fi
  echo ""
  echo "==> unsigned $method $url"
  "${cmd[@]}"
  head -1 "$hdrfile"
  echo "-- body (first 200 bytes) --"
  head -c 200 "$bodyfile"; echo
}

mkdir -p /tmp

if [ ! -f "$data_file" ]; then
  echo "data file not found: $data_file" >&2
  exit 1
fi

# Ensure bucket exists (signed)
curl_req_signed PUT "/$bucket" "" ""

# Upload object (signed)
curl_req_signed PUT "/$bucket/$object_key" "" "$data_file" "Content-Type: text/plain"

# Unsigned GET (should succeed for public bucket with policy)
curl_req_unsigned GET "/$bucket/$object_key" ""

# Unsigned PUT (should be forbidden)
curl_req_unsigned PUT "/$bucket/forbidden.txt" "$data_file"

echo ""
echo "done"
