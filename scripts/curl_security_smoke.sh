#!/usr/bin/env bash
set -euo pipefail

endpoint="${S3_ENDPOINT:-http://localhost:9000}"
host="${S3_HOST:-localhost:9000}"
access="${S3_ACCESS_KEY:-test}"
secret="${S3_SECRET_KEY:-testsecret}"
region="${S3_REGION:-us-east-1}"
service="s3"
payload_hash="UNSIGNED-PAYLOAD"

sign() {
  local method="$1" path="$2" query="$3" amz_date="$4"
  python3 - "$method" "$path" "$query" "$amz_date" "$access" "$secret" "$region" "$service" "$host" "$payload_hash" <<'PY'
import sys, hashlib, hmac
from urllib.parse import quote
method, path, query, amz_date, access, secret, region, service, host, payload_hash = sys.argv[1:]

def enc(s): return quote(s, safe='-_.~')
def canonical_query(q):
    if not q: return ''
    pairs=[]
    for part in q.split('&'):
        if '=' in part: k,v=part.split('=',1)
        else: k,v=part,''
        pairs.append((enc(k), enc(v)))
    pairs.sort()
    return '&'.join([f"{k}={v}" for k,v in pairs])

canonical_uri = quote(path, safe='/-_.~') or '/'
canonical_headers = f"host:{host}\n" + f"x-amz-content-sha256:{payload_hash}\n" + f"x-amz-date:{amz_date}\n"
signed_headers = "host;x-amz-content-sha256;x-amz-date"
canonical_request = "\n".join([method, canonical_uri, canonical_query(query), canonical_headers, signed_headers, payload_hash])
cr_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
date_scope = amz_date[:8]
scope = f"{date_scope}/{region}/{service}/aws4_request"
string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, cr_hash])

def sign(key,msg): return hmac.new(key, msg.encode(), hashlib.sha256).digest()
k_date = sign(("AWS4"+secret).encode(), date_scope)
k_region = sign(k_date, region)
k_service = sign(k_region, service)
k_signing = sign(k_service, "aws4_request")
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
auth = f"AWS4-HMAC-SHA256 Credential={access}/{scope},SignedHeaders={signed_headers},Signature={signature}"
print(auth)
PY
}

req() {
  local label="$1" method="$2" path="$3" query="$4"
  shift 4
  local -a headers=("$@")
  local url="$endpoint$path"
  [ -n "$query" ] && url+="?$query"
  echo ""
  echo "==> $label"
  curl -sS -m 5 -D - -o /dev/null -X "$method" "$url" "${headers[@]}"
}

# No auth -> 403 AccessDenied
req "No auth GET (expect 403)" GET "/demo" ""

# Malformed Authorization -> 400 AuthorizationHeaderMalformed
req "Malformed Authorization (expect 400)" GET "/" "" -H "Authorization: AWS ak:deadbeef"

# Invalid signature -> 403 SignatureDoesNotMatch
amz_date=$(date -u +%Y%m%dT%H%M%SZ)
invalid_auth="AWS4-HMAC-SHA256 Credential=${access}/${amz_date:0:8}/${region}/${service}/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=deadbeef"
req "Invalid signature (expect 403)" GET "/" "" \
  -H "Host: $host" \
  -H "x-amz-content-sha256: $payload_hash" \
  -H "x-amz-date: $amz_date" \
  -H "Authorization: $invalid_auth"

# Time skew +10 min -> 403 RequestTimeTooSkewed
skew_date=$(date -u -v+10M +%Y%m%dT%H%M%SZ 2>/dev/null || date -u -d '+10 min' +%Y%m%dT%H%M%SZ)
auth=$(sign GET "/" "" "$skew_date")
req "Time skew +10m (expect 403)" GET "/" "" \
  -H "Host: $host" \
  -H "x-amz-content-sha256: $payload_hash" \
  -H "x-amz-date: $skew_date" \
  -H "Authorization: $auth"

# Missing Content-Length -> 411 MissingContentLength
amz_date=$(date -u +%Y%m%dT%H%M%SZ)
auth=$(sign PUT "/demo/missing-len" "" "$amz_date")
req "Missing Content-Length (expect 411)" PUT "/demo/missing-len" "" \
  -H "Host: $host" \
  -H "x-amz-content-sha256: $payload_hash" \
  -H "x-amz-date: $amz_date" \
  -H "Authorization: $auth" \
  -H "Transfer-Encoding: chunked"

# Invalid Content-MD5 (non-base64) -> 400 InvalidDigest
amz_date=$(date -u +%Y%m%dT%H%M%SZ)
auth=$(sign PUT "/demo/badmd5" "" "$amz_date")
req "Invalid Content-MD5 (expect 400)" PUT "/demo/badmd5" "" \
  -H "Host: $host" \
  -H "x-amz-content-sha256: $payload_hash" \
  -H "x-amz-date: $amz_date" \
  -H "Authorization: $auth" \
  -H "Content-MD5: not-base64" \
  --data-binary @/etc/hosts

# Meta stats without auth -> 403 (when auth enabled)
req "Meta stats without auth (expect 403)" GET "/v1/meta/stats" ""

echo ""
echo "done"
