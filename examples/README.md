# Examples

This folder contains deployment snippets you can adapt to your environment.

## systemd (Ubuntu)
- `seglake.service` runs seglake bound to localhost and enables public bucket access.
- Configure secrets in `/etc/seglake/secrets.env` (loaded via `-secrets-file`) and update the binary path as needed.

Create user and directories:
```
sudo useradd --system --home /var/lib/seglake --shell /usr/sbin/nologin seglake
sudo install -d -m 750 -o seglake -g seglake /var/lib/seglake
sudo install -d -m 750 -o seglake -g seglake /etc/seglake
```

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

Permissions note:
```
sudo chown root:seglake /etc/seglake/secrets.env
sudo chmod 640 /etc/seglake/secrets.env
```

Check status and logs:
```
sudo systemctl status seglake
sudo journalctl -u seglake -f
```

Smoke test (auth enabled):
```
AWS_ACCESS_KEY_ID=ROOT_ACCESS \
AWS_SECRET_ACCESS_KEY=ROOT_SECRET \
AWS_DEFAULT_REGION=us-east-1 \
aws s3 ls --endpoint-url http://localhost:9000
```

Flags note:
- Bool flags must use `--flag=false` syntax (no space) when disabling.
- In `systemd`, line continuation requires a trailing `\` on every continued line.

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
