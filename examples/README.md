# Examples

This folder contains deployment snippets you can adapt to your environment.

## systemd (Ubuntu)
- `seglake.service` runs seglake bound to localhost and enables public bucket access.
- Configure secrets in `/etc/seglake/secrets.env` and update the binary path as needed.

Enable:
```
sudo cp examples/seglake.service /etc/systemd/system/seglake.service
sudo install -m 600 -o root -g root /dev/null /etc/seglake/secrets.env
sudoedit /etc/seglake/secrets.env
sudo systemctl daemon-reload
sudo systemctl enable --now seglake
```

Example `/etc/seglake/secrets.env`:
```
SEGLAKE_ACCESS_KEY=ROOT_ACCESS
SEGLAKE_SECRET_KEY=ROOT_SECRET
```

## Caddy
- `Caddyfile` includes both path-style and virtual-hosted-style examples.
- Uncomment the virtual-hosted block only if you have wildcard DNS.

## Public bucket policy
- `public_policy.json` allows unsigned read-only access when used with `-public-buckets public`.

Apply:
```
./build/seglake -mode bucket-policy \
  --data-dir /var/lib/seglake \
  --bucket-policy-action set \
  --bucket-policy-bucket public \
  --bucket-policy-file examples/public_policy.json
```
