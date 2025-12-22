# Ops / Deployment Notes

## TLS reverse proxy checklist

1) Terminate TLS in a reverse proxy (nginx, Caddy, Envoy).
2) Keep Seglake on HTTP behind the proxy on a trusted network.
3) Enforce HTTPS at the edge (redirect or 301).
4) Pass through `Host` and `X-Forwarded-For` only from trusted IPs.
5) If using virtual-hosted-style, ensure DNS and proxy routing by host.
6) Set request size limits at the proxy if needed (S3 SDKs may retry on 413).
7) Disable request/response buffering for large PUT/GET when possible.
8) Enable access logs at the proxy; Seglake logs redact presigned secrets.

Example (nginx, minimal):
```
server {
  listen 443 ssl;
  server_name s3.example.com;
  ssl_certificate /etc/ssl/cert.pem;
  ssl_certificate_key /etc/ssl/key.pem;

  location / {
    proxy_pass http://127.0.0.1:9000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_request_buffering off;
    proxy_buffering off;
  }
}
```

## awscli examples (SigV4)

List buckets:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 ls --endpoint-url http://localhost:9000
```

List objects:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 ls s3://demo --endpoint-url http://localhost:9000
```

PUT object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp ./file.bin s3://demo/file.bin --endpoint-url http://localhost:9000
```

GET object:
```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=testsecret AWS_DEFAULT_REGION=us-east-1 \
  aws s3 cp s3://demo/file.bin ./file.bin --endpoint-url http://localhost:9000
```

## s3cmd examples

List buckets:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret ls
```

List objects:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret ls s3://demo
```

PUT object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret put ./file.bin s3://demo/file.bin
```

GET object:
```
s3cmd --no-ssl --host=localhost:9000 --host-bucket=localhost:9000 \
  --access_key=test --secret_key=testsecret get s3://demo/file.bin ./file.bin
```
