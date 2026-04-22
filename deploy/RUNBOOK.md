# Datalake API — Production Deployment Runbook

Follow top-to-bottom on a fresh VPS. Commands assume Ubuntu 22.04 / 24.04 LTS
and a user with sudo. Substitutions you need to decide up front:

| Placeholder | Example | Notes |
|---|---|---|
| `<VPS_IP>` | `185.X.Y.Z` | public IPv4 |
| `<DOMAIN>` | `datalake.lucasguerin.fr` | or leave Caddyfile on `:80` for IP-only |
| `<REPO_URL>` | `git@github.com:you/datalake-api.git` | or https clone |
| `<BROKER_LOGIN>` / `<BROKER_PASSWORD>` / `<BROKER_SERVER>` | from your MT5 account | |

---

## 1. Provision + DNS

1. Create the VPS (2 vCPU / 4GB RAM / 40GB disk is plenty for the datalake
   footprint we have today — bump RAM if you enable many concurrent instruments).
2. If you're using a domain, point an **A record** at `<VPS_IP>` now. TLS won't
   issue until DNS resolves.

## 2. Base OS hardening

SSH in as root (or your sudo user) and run:

```bash
# System updates + unattended security patches
apt update && apt upgrade -y
apt install -y unattended-upgrades fail2ban ufw git curl ca-certificates
dpkg-reconfigure --priority=low unattended-upgrades  # accept defaults

# Firewall: only SSH + HTTP/HTTPS
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

# fail2ban ships with sane SSH defaults; verify it's running
systemctl enable --now fail2ban
```

Create a deploy user (avoid running the stack as root). Because we use
`--disabled-password`, the account has **no password at all** — login is only by SSH
key. That also means plain `sudo` would have no password to prompt for, so we grant
a `NOPASSWD` rule. Key-only auth + NOPASSWD sudo is the standard setup for a deploy
user on a personal VPS.

```bash
# Still in the ROOT session
adduser --disabled-password --gecos "" datalake
usermod -aG sudo datalake

# NOPASSWD sudo for this user (needed because the account has no password)
echo 'datalake ALL=(ALL) NOPASSWD:ALL' | sudo tee /etc/sudoers.d/90-datalake
sudo chmod 0440 /etc/sudoers.d/90-datalake
sudo visudo -cf /etc/sudoers.d/90-datalake   # syntax check, must print "parsed OK"

# Copy your SSH key over so you can log in as datalake
mkdir -p /home/datalake/.ssh
cp /root/.ssh/authorized_keys /home/datalake/.ssh/
chown -R datalake:datalake /home/datalake/.ssh
chmod 700 /home/datalake/.ssh
chmod 600 /home/datalake/.ssh/authorized_keys
```

### Verify key-auth works as `datalake` (CRITICAL before hardening SSH)

**Keep the root session open.** From a **new terminal on your laptop**, run:

```bash
ssh datalake@<VPS_IP>
```

Expected: logs you straight in, no password prompt, shell lands in `/home/datalake`.

Troubleshooting:
- Asks for a password → your SSH key didn't propagate. In the root session, redo
  the `authorized_keys` copy block above.
- `Permission denied (publickey)` → same fix; `ssh -v datalake@<VPS_IP>` from your
  laptop shows which key it tried.

Sanity check sudo works without a password (still in the datalake session):

```bash
sudo -n true && echo "sudo OK"
```

### Harden SSH (from the ROOT session)

Now that you've confirmed the datalake session works, harden `sshd`. Run this from
the **original root session** — that's where sudo is already authenticated and
where you can recover from mistakes:

```bash
sudo tee /etc/ssh/sshd_config.d/50-hardening.conf > /dev/null <<'EOF'
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
EOF

sudo sshd -t                 # validates the config; must print nothing
sudo systemctl restart ssh   # some distros use 'sshd'; try that if 'ssh' is unknown
```

### Confirm the hardening didn't break anything

Open **a third terminal** on your laptop:

```bash
ssh datalake@<VPS_IP>
```

- Works → close the root session. From now on you log in as `datalake` only.
- Fails → don't panic. The still-open datalake session (from the verify step) has
  NOPASSWD sudo. Undo with:
  ```bash
  sudo rm /etc/ssh/sshd_config.d/50-hardening.conf
  sudo systemctl restart ssh
  ```

## 3. Docker

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker datalake
# log out and back in as datalake so the new group applies; `groups` should now show 'docker'
```

Verify (as `datalake`, in a fresh session): `docker run --rm hello-world`.

## 4. Clone the repo

```bash
sudo mkdir -p /opt/datalake-api
sudo chown datalake:datalake /opt/datalake-api
cd /opt
git clone <REPO_URL> datalake-api
cd datalake-api
```

## 5. `.env`

```bash
cp .env.example .env
```

Generate real secrets:

```bash
# SECRET_KEY (JWT signing key)
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'

# POSTGRES_PASSWORD
python3 -c 'import secrets; print(secrets.token_urlsafe(24))'
```

Edit `.env` and set the values below. Two ways:

**Option A — interactive editor:**

```bash
nano .env   # arrow keys to navigate, Ctrl+O Enter to save, Ctrl+X to exit
```

Set:
- `POSTGRES_PASSWORD=<generated>`
- `SECRET_KEY=<generated>`
- `ALLOW_PUBLIC_READS=false`
- `ALLOW_REGISTRATION=false`
- `RATE_LIMIT_ENABLED=true` (default)
- `MT5_BRIDGE_URL=http://host.docker.internal:18812` (default)

**Option B — paste these one-liners (no editor):**

```bash
SECRET=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
PGPASS=$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')

sed -i "s|^SECRET_KEY=.*|SECRET_KEY=${SECRET}|"              .env
sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${PGPASS}|" .env
sed -i "s|^ALLOW_PUBLIC_READS=.*|ALLOW_PUBLIC_READS=false|"   .env
sed -i "s|^ALLOW_REGISTRATION=.*|ALLOW_REGISTRATION=false|"   .env

# Print the updated lines to sanity-check
grep -E "^(SECRET_KEY|POSTGRES_PASSWORD|ALLOW_PUBLIC_READS|ALLOW_REGISTRATION)=" .env
```

Tighten perms:

```bash
chmod 600 .env
```

## 6. Caddyfile — domain vs IP-only

- **Domain**: keep the Caddyfile as-is (it already has `datalake.lucasguerin.fr`
  with your email for Let's Encrypt).
- **IP-only (temporary)**: edit `Caddyfile`, replace the `datalake.lucasguerin.fr {`
  line with `:80 {` and remove the `{ email ... }` global block. TLS will not be
  issued; this is fine for testing.

## 7. Wine + MT5 + wine-python (the fiddly part)

The MT5 Python package is Windows-only. We run MT5 + a Wine-Python inside Wine
so our Linux FastAPI can call it via `scripts/mt5_bridge.py`.

```bash
# Enable 32-bit + install Wine stable
sudo dpkg --add-architecture i386
sudo apt update
sudo apt install -y wine64 wine32 winetricks xvfb

# Create a dedicated prefix so MT5 state doesn't pollute ~/.wine
export WINEPREFIX=/home/datalake/.wine-mt5
export WINEARCH=win64
wineboot -u   # first init, accepts any dialogs
```

Install Python **inside Wine** (use 3.11 or 3.12 — match what `MetaTrader5`
publishes wheels for; 3.11 is safest):

```bash
# Grab the Windows Python installer
cd /tmp
curl -LO https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
xvfb-run -a wine python-3.11.9-amd64.exe /quiet PrependPath=1 Include_pip=1 InstallAllUsers=1
# Verify
wine python --version
wine python -m pip install --upgrade pip
wine python -m pip install MetaTrader5
```

Install the **MT5 terminal** itself:

```bash
# Download the official installer from your broker or mql5.com
curl -LO https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe
xvfb-run -a wine mt5setup.exe /auto
```

Log into your broker account interactively **once** so MT5 persists credentials:

```bash
xvfb-run -a wine "$WINEPREFIX/drive_c/Program Files/MetaTrader 5/terminal64.exe"
# Use File → Login to Trade Account, then close the window.
```

Sanity-check the bridge manually:

```bash
cd /opt/datalake-api
wine python scripts/mt5_bridge.py --host 127.0.0.1 --port 18812
# In another shell:
curl http://127.0.0.1:18812/ping
# Expect: {"status": "ok", "mt5_initialized": true}
# Ctrl-C the bridge once verified.
```

## 8. MT5 bridge as a systemd service

Edit `deploy/mt5-bridge.service` if your wine-python path differs (check with
`ls /home/datalake/.wine-mt5/drive_c/`). Then:

```bash
sudo cp deploy/mt5-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mt5-bridge
sudo systemctl status mt5-bridge   # should be active (running)
journalctl -u mt5-bridge -n 50     # confirm initialization log
```

Confirm the API container will be able to reach it: from the host,
`curl http://127.0.0.1:18812/ping` must succeed.

## 9. Boot the stack

```bash
cd /opt/datalake-api
./deploy/deploy.sh      # builds datalake-api:<sha>, brings up compose, pings /healthcheck
```

Expected output ends with `>> deployed datalake-api:<sha>`.

Verify:

```bash
curl https://<DOMAIN>/healthcheck          # or http://<VPS_IP>/healthcheck
curl https://<DOMAIN>/healthcheck/ready    # must return 200 with both checks "ok"
```

If readiness returns 503, check `docker compose logs api postgres`.

## 10. Create your user + mint a long-lived API key

Registration is disabled in prod, so seed your user directly via `/auth/register`
with the flag temporarily on, OR mint through Python. The one-shot path:

```bash
# Flip registration on for 30 seconds
docker compose -f docker-compose.prod.yml exec api sh -c 'ALLOW_REGISTRATION=true python -c "
from src.core.database import get_db_context, create_user, get_user_by_username
from src.auth.auth import get_password_hash
with get_db_context() as db:
    if not get_user_by_username(db, \"lucas\"):
        create_user(db, \"lucas\", \"luca.guer1@gmail.com\", get_password_hash(\"Password\"))
        print(\"created\")
    else:
        print(\"exists\")
"'
```

Mint the long-lived key:

```bash
docker compose -f docker-compose.prod.yml exec api \
    python -m scripts.mint_api_key --username lucas --name "local-dev" --scopes admin
```

**Copy the printed key into a local password manager immediately** — it's shown
once, never again. From your laptop you now call the API with
`-H "X-API-Key: dk_..."`.

## 11. Scheduled jobs (systemd timers + cronjob.org)

### Liveness monitoring — cronjob.org

Point a job at `GET https://<DOMAIN>/healthcheck/ready`, every 5 min, email on
non-2xx. This is the one that wakes you up at night; systemd timers can't page
you if the whole VPS is down.

### Weekly refresh + backup — systemd timers on the VPS

Two timer pairs ship under `deploy/`:

- `datalake-refresh.{service,timer}` — `POST /ingest/refresh` with `{"days": 7}`
  every Sunday 03:00 UTC. Depends on `mt5-bridge.service`.
- `datalake-backup.{service,timer}` — `POST /backup/run?keep=8` every Sunday
  05:00 UTC. Runs after refresh so the weekend's fresh data is exported.

Install once:

```bash
# 1. Mint an admin API key if you don't already have one
docker compose -f docker-compose.prod.yml exec api \
    python -m scripts.mint_api_key --username lucas --name "cron" --scopes admin
# Copy the dk_... string.

# 2. Stash the key in a root-readable env file
sudo mkdir -p /etc/datalake
echo "API_KEY=dk_paste_here" | sudo tee /etc/datalake/api-key > /dev/null
sudo chmod 600 /etc/datalake/api-key
sudo chown root:root /etc/datalake/api-key

# 3. Install + enable the units
sudo cp deploy/datalake-backup.service  /etc/systemd/system/
sudo cp deploy/datalake-backup.timer    /etc/systemd/system/
sudo cp deploy/datalake-refresh.service /etc/systemd/system/
sudo cp deploy/datalake-refresh.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now datalake-backup.timer datalake-refresh.timer
```

Verify:

```bash
# Next scheduled fire-time per timer
systemctl list-timers datalake-*

# Run once now to smoke-test
sudo systemctl start datalake-backup.service
journalctl -u datalake-backup.service -n 50
```

Rotate the key: edit `/etc/datalake/api-key` and `sudo systemctl daemon-reload`.
No service restart needed — `EnvironmentFile` is read on each activation.

## 12. Backups — retrieving artifacts

The API only exposes the manifest. To actually copy parquet off the VPS:

```bash
# From your laptop
rsync -avz --progress datalake@<VPS_IP>:/opt/datalake-api/backups/ ./vps-backups/
```

Latest manifest for verification:

```bash
curl -H "X-API-Key: $KEY" https://<DOMAIN>/backup/latest
```

## 13. Updates

```bash
cd /opt/datalake-api
./deploy/deploy.sh     # pulls, rebuilds tagged by SHA, redeploys
```

To roll back to a previous SHA image still present locally:

```bash
API_IMAGE=datalake-api:<old-sha> docker compose -f docker-compose.prod.yml up -d
```

## 14. Troubleshooting cheatsheet

| Symptom | Check |
|---|---|
| `/healthcheck/ready` → 503, `postgres: error` | `docker compose logs postgres`; usually password mismatch between `.env` and existing volume |
| `/ingest/refresh` → 503 "MT5 bridge unreachable" | `systemctl status mt5-bridge`, `journalctl -u mt5-bridge -n 100`, test `curl http://127.0.0.1:18812/ping` |
| TLS not issuing on Caddy | `docker compose logs caddy`; DNS A record propagated? Port 80 reachable from internet? |
| 429 on login | rate limit tripped — wait a minute or lower `RATE_LIMIT_ENABLED` temporarily |
| MT5 returns 0 bars for a symbol | broker hasn't enabled the symbol; open the terminal, right-click Market Watch → Show All, or add the symbol manually |

## 15. What I'm explicitly *not* doing here

- No Redis / task queue — in-memory jobs are enough at this scale.
- No Prometheus/Grafana — cronjob.org + journalctl covers you until you feel pain.
- No automated off-box backup rotation — `rsync` from your laptop is fine.
- No CI/CD — you deploy by SSHing and running `deploy.sh`. Add GH Actions later
  if it becomes a bottleneck.

Lock in these tripwires instead of over-engineering.
