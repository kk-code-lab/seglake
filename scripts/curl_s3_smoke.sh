#!/usr/bin/env bash
set -euo pipefail

endpoint="${S3_ENDPOINT:-http://localhost:9000}"
host="${S3_HOST:-localhost:9000}"
access="${S3_ACCESS_KEY:-test}"
secret="${S3_SECRET_KEY:-testsecret}"
region="${S3_REGION:-us-east-1}"
service="s3"
payload_hash="UNSIGNED-PAYLOAD"
bucket="${S3_BUCKET:-demo}"
object_key="${S3_OBJECT_KEY:-spec.md}"
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

curl_req() {
  local method="$1" path="$2" query="$3" data_file="$4"
  shift 4
  local -a headers=("$@")
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
  echo "==> $method $url"
  "${cmd[@]}"
  head -1 "$hdrfile"
  echo "-- headers --"
  sed -n '1,12p' "$hdrfile"
  echo "-- body (first 200 bytes) --"
  head -c 200 "$bodyfile"; echo
}

mkdir -p /tmp

if [ ! -f "$data_file" ]; then
  echo "data file not found: $data_file" >&2
  exit 1
fi

md5_b64=$(openssl md5 -binary "$data_file" | openssl base64)

# Ensure bucket exists (idempotent)
curl_req PUT "/$bucket" "" ""

# PUT with Content-MD5 + Content-Type
curl_req PUT "/$bucket/$object_key" "" "$data_file" "Content-MD5: $md5_b64" "Content-Type: text/markdown"

# HEAD (use -I to avoid curl waiting for body)
amz_date=$(date -u +%Y%m%dT%H%M%SZ)
auth=$(sign HEAD "/$bucket/$object_key" "" "$amz_date")
echo ""
echo "==> HEAD $endpoint/$bucket/$object_key"
curl -sS -m 5 -I -D - -o /dev/null "$endpoint/$bucket/$object_key" \
  -H "Host: $host" \
  -H "x-amz-content-sha256: $payload_hash" \
  -H "x-amz-date: $amz_date" \
  -H "Authorization: $auth" | sed -n '1,12p'

# GET
curl_req GET "/$bucket/$object_key" "" ""

# Range GET
curl_req GET "/$bucket/$object_key" "" "" "Range: bytes=0-9"

# If-None-Match -> 304
etag=$(tr -d '\r' < /tmp/curl_headers.out | grep -i '^ETag:' | tail -1 | awk '{print $2}')
curl_req GET "/$bucket/$object_key" "" "" "If-None-Match: $etag"

# If-Modified-Since (future) -> 304
future=$(LC_ALL=C date -u -v+1H '+%a, %d %b %Y %H:%M:%S GMT' 2>/dev/null || LC_ALL=C date -u -d '+1 hour' '+%a, %d %b %Y %H:%M:%S GMT')
curl_req GET "/$bucket/$object_key" "" "" "If-Modified-Since: $future"

# If-Unmodified-Since (past) -> 412
past=$(LC_ALL=C date -u -v-1H '+%a, %d %b %Y %H:%M:%S GMT' 2>/dev/null || LC_ALL=C date -u -d '-1 hour' '+%a, %d %b %Y %H:%M:%S GMT')
curl_req GET "/$bucket/$object_key" "" "" "If-Unmodified-Since: $past"

# Bad Content-MD5 -> 400 BadDigest
bad_md5=$(printf 'bad' | openssl md5 -binary | openssl base64)
curl_req PUT "/$bucket/bad.md" "" "$data_file" "Content-MD5: $bad_md5" "Content-Type: text/markdown"

# CORS preflight
curl_req OPTIONS "/$bucket/$object_key" "" "" \
  "Origin: https://app.example.com" \
  "Access-Control-Request-Method: PUT" \
  "Access-Control-Request-Headers: authorization,content-md5,x-amz-date,x-amz-content-sha256"

# DELETE objects
curl_req DELETE "/$bucket/$object_key" "" ""
curl_req DELETE "/$bucket/bad.md" "" ""

echo ""
echo "done"
