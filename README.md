# Copytrade Bot

FastAPI + Next.js control plane for copying public Hyperliquid perpetual fills into follower execution venues.
Hyperliquid execution is the default primary venue, with Binance USD-M Futures available as an optional fallback.

Default mode is dry-run. Live execution requires all gates:

1. `.env` has `TRADING_ENABLED=true`.
2. The venue-specific trading flag is true (`HYPERLIQUID_TRADING_ENABLED=true` or `BINANCE_TRADING_ENABLED=true`).
3. Kill switch is off and that venue's preflight is ready.

No Binance API key is ever sent to the frontend. Session cookies are HTTPOnly, CSRF-protected, and API secrets are masked in logs.

## Project Structure

```text
copytrade-bot/
  backend/
    app/
      api/                 FastAPI routes
      core/                config, logging, crypto, security
      db/                  async SQLAlchemy session
      models/              SQLAlchemy tables
      schemas/             Pydantic request models
      services/            Hyperliquid, Binance, mapper, risk, executor
    alembic/               database migrations
    tests/                 pytest coverage for core trading logic
    Dockerfile
    requirements.txt
  frontend/
    src/app/               Next.js app routes
    src/components/        shell and navigation
    src/lib/               API client
    Dockerfile
    package.json
  deploy/
    nginx.conf             HTTPS reverse proxy template
  scripts/
    dev-backend.sh
    dev-frontend.sh
    dry-run-tests.sh
    backup-postgres.sh
    restore-postgres.sh
  docker-compose.yml
  .env.example
```

## Disaster Recovery / New Server Deployment

The GitHub repository is intended to restore the application code and deployment
templates on a new server. It deliberately does not contain live secrets or live
database data.

Not stored in Git:

- `.env` with private keys, admin bootstrap password, app secrets, and wallet addresses.
- TLS files under `deploy/certs/`.
- Optional Nginx basic-auth file `deploy/htpasswd`.
- Postgres and Redis Docker volumes.
- Database backup files under `backups/`.

For a full recovery, keep these outside Git in a password manager or encrypted
backup store:

- A current `.env` or the values needed to rebuild it.
- A recent Postgres backup produced by `scripts/backup-postgres.sh`.
- TLS certificate files, or the ability to issue new certificates.

### 1. Prepare The Server

Install Docker Engine, Docker Compose v2, Git, and either GitHub CLI or an SSH
deploy key that can read the private repository.

Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git openssl
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker
```

Authenticate to GitHub or install an SSH deploy key, then clone:

```bash
cd /opt
git clone git@github.com:Kiyoakiiii/copytrade-bot.git
cd copytrade-bot
```

### 2. Recreate Runtime-Only Files

Create `.env` from the template and fill the live values manually. Do not paste
private keys into GitHub issues, README files, commits, screenshots, or logs.

```bash
cp .env.example .env
chmod 600 .env
nano .env
```

Minimum Hyperliquid production fields:

```env
DATABASE_URL=postgresql+asyncpg://postgres/copytrade
REDIS_URL=redis://redis:6379/0
DEFAULT_PREFERRED_VENUE=HYPERLIQUID
ENABLE_HYPERLIQUID_EXECUTION=true
ENABLE_BINANCE_EXECUTION=false
ENABLE_BINANCE_FALLBACK=false

HYPERLIQUID_EXECUTION_NETWORK=mainnet
HYPERLIQUID_ACCOUNT_ADDRESS=<follower_account_address>
HYPERLIQUID_SIGNER_PRIVATE_KEY=<never_commit_this>
HYPERLIQUID_API_WALLET_ADDRESS=<api_wallet_address_if_used>
HYPERLIQUID_DEFAULT_LEVERAGE=10
HYPERLIQUID_DEFAULT_MARGIN_MODE=CROSS
HYPERLIQUID_TRADING_ENABLED=false

TRADING_ENABLED=false
APP_SECRET_KEY=<openssl_rand_hex_32>
ENCRYPTION_MASTER_KEY=<openssl_rand_hex_32>
ADMIN_EMAIL=<your_admin_email>
ADMIN_PASSWORD_BOOTSTRAP=<temporary_first_login_password>
REQUIRE_TOTP=true
COOKIE_SECURE=true
NEXT_PUBLIC_API_BASE=/api
```

Generate app secrets with:

```bash
openssl rand -hex 32
openssl rand -hex 32
```

Create the Nginx runtime files. The private certificate key must not be committed.

For a real domain, issue a certificate with your preferred ACME client and copy
or install the files as:

```bash
mkdir -p deploy/certs
sudo install -m 644 /etc/letsencrypt/live/<domain>/fullchain.pem deploy/certs/fullchain.pem
sudo install -m 600 /etc/letsencrypt/live/<domain>/privkey.pem deploy/certs/privkey.pem
```

For a temporary emergency recovery without a domain, create a self-signed cert:

```bash
mkdir -p deploy/certs
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout deploy/certs/privkey.pem \
  -out deploy/certs/fullchain.pem \
  -days 30 \
  -subj "/CN=copytrade-local"
chmod 600 deploy/certs/privkey.pem
```

If you are not using Nginx basic auth, create an empty mount target:

```bash
touch deploy/htpasswd
```

If you enable the commented basic-auth lines in `deploy/nginx.conf`, generate
the file instead:

```bash
docker run --rm httpd:2.4-alpine htpasswd -nbB <user> '<password>' > deploy/htpasswd
chmod 600 deploy/htpasswd
```

### 3. Start A Fresh Instance

Start in dry-run first. The backend automatically runs Alembic migrations.

```bash
docker compose up -d --build
docker compose ps
docker compose exec backend curl -fsS http://localhost:8000/health
```

Open:

```text
https://<server>/login
https://<server>/preflight
```

If this is a fresh database with no backup, add leaders again in the frontend.
Set each leader's `copy_multiplier`, fixed `Account value used`, caps, allowed
coins, and blocked coins before enabling live trading.

### 4. Restore Existing Bot State From Backup

Git alone does not restore database state. To preserve leader settings,
fixed account values, allocations, baselines, risk settings, order history, and
frontend-managed configuration, restore a Postgres backup.

On the old server, while it is still available:

```bash
cd /home/ubuntu/copytrade-bot
./scripts/backup-postgres.sh
```

Copy the resulting `backups/copytrade_*.dump` file to the new server using
`scp`, `rsync`, or another encrypted transfer. Do not commit the dump to Git.
Treat database dumps as sensitive because they contain account configuration,
leader settings, allocation state, order history, and admin/auth metadata.

On the new server:

```bash
cd /opt/copytrade-bot
mkdir -p backups
# put the dump file under backups/
CONFIRM_RESTORE=1 ./scripts/restore-postgres.sh backups/copytrade_YYYYMMDDTHHMMSSZ.dump
docker compose up -d --build
```

After restore, confirm:

```bash
docker compose exec postgres pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB
docker compose exec backend curl -fsS http://localhost:8000/health
```

Then open `/leaders`, `/dashboard`, `/positions/allocations`, and `/preflight`.
Confirm leaders, multipliers, fixed account values, caps, active allocations,
and follower account state before turning off the kill switch.

### 5. Live Cutover Checklist

Before live trading on a replacement server:

1. Keep `TRADING_ENABLED=false` and `HYPERLIQUID_TRADING_ENABLED=false` until
   `/preflight` is green.
2. Confirm the follower address shown in the UI is the intended new/live follower.
3. Confirm all three readiness flags in the runtime status are true:
   watcher running, websocket connected, and ready for low-latency live.
4. Confirm `stale_over_2s` allocations are zero in the UI or DB checks.
5. Confirm no unexpected active allocations from old/deleted leaders.
6. Turn on `TRADING_ENABLED=true` and `HYPERLIQUID_TRADING_ENABLED=true`.
7. Restart the stack.
8. Turn off the frontend kill switch only after the first dry-run/live checks
   match expected sizing and route.

Useful DB checks:

```bash
docker compose exec postgres psql -U copytrade -d copytrade -c \
  "select right(leader_address,4), copy_multiplier, fixed_account_value from leader_configs where enabled and deleted_at is null order by id;"

docker compose exec postgres psql -U copytrade -d copytrade -c \
  "select count(*) filter (where last_reconcile_at is null or last_reconcile_at < now() - interval '2 seconds') as stale_over_2s, count(*) as active_allocations from leader_position_allocations where status <> 'CLOSED';"
```

## Implemented

- Hyperliquid info and WebSocket watcher skeleton for `userFills`, `userEvents`, `orderUpdates`, and `clearinghouseState`.
- Snapshot fills are ignored for live copy execution, and fill IDs are deduplicated by leader, hash, tid, oid, time, and coin.
- Binance USD-M REST client with exchange info, account, position mode, positions, and market/limit order support.
- Binance account risk gate requires Hedge Mode, symbol `ISOLATED` margin, and 10x leverage before live opens/adds.
- Hyperliquid execution venue support with default `HYPERLIQUID` preference, optional Binance fallback, per-leader venue settings, and venue-isolated allocation/order fields.
- Hyperliquid market-equivalent orders are aggressive IOC limit orders with `cloid`; no GTC/resting auto orders are intended.
- Hyperliquid risk gate sets isolated `min(10, coin_max_leverage)` before open/add; if max leverage is below 10 the coin is WARNING, not BLOCKED. Close/reduce intent is reduce-only and may proceed when it lowers risk.
- Hedge Mode orders always send `positionSide=LONG` or `SHORT`; `reduceOnly`, `positionSide=BOTH`, and close-all are not used for live orders.
- Automatic copy orders are MARKET-only. The auto executor ignores leader `use_market_order` for AUTO_COPY and never sends LIMIT, price, or timeInForce.
- Automatic copy orders generate Binance `newClientOrderId`, record `PENDING_SUBMIT` before submit, mark unknown network/timeout outcomes as `UNKNOWN`, and never immediately replay an unknown order.
- Startup recovery scans unresolved AUTO_COPY orders and queries Binance by `newClientOrderId` or Hyperliquid by `cloid` before any manual decision to retry.
- Leader-level virtual allocation ledger in `leader_position_allocations` keeps each leader's sub-position separate by venue, symbol, side, and account.
- Copy ratio formula, target-position planning, open/add/reduce/close/flip handling, symbol mapping, quantity rounding, risk checks, dry-run executor, reconciler delta logic.
- PostgreSQL schema and Alembic migration for required tables.
- Background leader-state poller writes Hyperliquid `clearinghouseState` into `latest_leader_states` for dashboard/preflight freshness checks.
- Real-time Hyperliquid account-state cache writes follower and leader balances/positions into `latest_account_states` and `latest_account_positions` every few seconds for Dashboard, Leaders, and Preflight.
- Leader management is database-backed: frontend add/edit/disable/delete updates `leader_configs`, deletion is soft delete, and the poller reads enabled, non-deleted leaders without a restart.
- Auth foundation with argon2 password hashes, TOTP setup/verify, HTTPOnly session cookie, CSRF cookie, IP allowlist, and login rate lock.
- Next.js dashboard, venue settings, preflight, leader management, venue mapping, manual trading, orders, and risk pages.

## Execution Venues

Default policy:

```env
DEFAULT_PREFERRED_VENUE=HYPERLIQUID
ENABLE_HYPERLIQUID_EXECUTION=true
ENABLE_BINANCE_EXECUTION=true
ENABLE_BINANCE_FALLBACK=false
HYPERLIQUID_TRADING_ENABLED=false
BINANCE_TRADING_ENABLED=false
```

Binance mappings are only required for Binance execution. A Hyperliquid coin that is tradable in Hyperliquid meta can still be copied even when Binance has no matching symbol. If Hyperliquid is unavailable and `ENABLE_BINANCE_FALLBACK=true`, Binance is used only when the Binance mapping and Binance risk checks are valid.

Hyperliquid cannot safely represent simultaneous long and short allocations for the same coin on the same follower account. Same-direction allocations from multiple leaders can aggregate on one account, but opposite-direction allocations must be blocked unless separate Hyperliquid account/vault routing is configured.

Leverage does not affect the copy ratio formula. The only supported sizing mode is
`ACCOUNT_RATIO`. Target notional is:

```text
target_notional =
  follower_account_value
  * abs(leader_position_notional / leader_account_value)
  * copy_multiplier
```

`copy_multiplier` scales the leader's account-risk ratio onto your account. It is
not a direct multiplier of the leader's notional position.

If a Hyperliquid coin has `maxLeverage < 10`, the bot uses that max leverage for isolated margin. That increases required margin, so pre-trade checks block open/add if follower withdrawable margin is insufficient.

## Leader Configuration

Leader addresses should normally be added in the frontend `Leaders` page. Production runtime uses the database as the source of truth. `.env` may optionally set `BOOTSTRAP_LEADER_ADDRESSES=0x...,0x...`, but those addresses are imported only once when the database has zero leaders. After import, the frontend and database remain authoritative.

Adding, editing, disabling, or deleting a leader in the frontend does not require restarting the stack. The background poller refreshes enabled leaders from the database and ignores rows where `deleted_at` is set. Deleting a leader sets `enabled=false` and `deleted_at=now`; it does not delete historical orders or allocations and does not automatically close existing follower positions.

Allowed coins are not limited to BTC/ETH/SOL. `allowed_symbols=null` or an empty list means `ALL_COINS`, so every Hyperliquid-tradable coin can be considered unless it is in `blocked_symbols`. A non-empty allowed list means `CUSTOM_LIST`, and only those normalized coins are copied. Binance mappings are checked only for Binance execution or Binance fallback; missing Binance mappings do not block Hyperliquid-primary coins that exist in Hyperliquid meta.

Private keys stay in `.env` or in `HYPERLIQUID_PRIVATE_KEY_FILE`. Do not put private keys in leader API payloads, frontend fields, screenshots, logs, or tickets.

## Preflight

The preflight page is available at:

```text
https://<server>/preflight
```

It blocks live readiness unless required checks are OK. Current checks are split by venue:

Hyperliquid venue:

- API connectivity and wallet/private-key configuration
- enabled coins against Hyperliquid meta
- `exists_in_meta`, `max_leverage`, `target_leverage=min(10,max_leverage)`, isolated margin mode, and OK/WARNING/BLOCKED risk status per coin
- allocation consistency against follower Hyperliquid positions when account state is available
- no unresolved Hyperliquid `PENDING_SUBMIT` / `UNKNOWN` / `SUBMITTED` / `PARTIALLY_FILLED` auto orders

Binance venue:

- Margin mode: `ISOLATED`
- Leverage: `10`
- Position mode: Hedge Mode, `dualSidePosition=true`
- Position sides: `LONG` / `SHORT`, never `BOTH`
- Automatic order type: `MARKET_ONLY`
- No unresolved `PENDING_SUBMIT` / `UNKNOWN` / `SUBMITTED` / `PARTIALLY_FILLED` auto orders
- Recent AUTO_COPY latency summary

`TRADING_ENABLED=false` keeps the system in dry-run. Even if `TRADING_ENABLED=true`, live opens/adds are blocked unless the selected venue's trading flag and readiness are both true. Close/reduce intent orders are bounded by the leader allocation and matching venue position.

The top-level preflight status includes `global_live_ready`, unresolved unknown order count, allocation mismatch status, dry-run/live mode, startup configuration checks, and concrete blocking reasons. If any required item is not OK, the UI shows `实盘未就绪，禁止自动开仓`.

Preflight also shows database enabled leader count, watcher active leader count, leaders in the database that the watcher has not picked up yet, and watcher subscriptions that point to disabled/deleted leaders. Live readiness is blocked when an enabled leader is not active in the watcher or when watcher state is stale.

Account-state readiness is also required before small live copy:

- Follower Hyperliquid account state must be loaded and fresh.
- Each enabled leader account state must be loaded and fresh.
- State is marked stale after `ACCOUNT_STATE_STALE_SECONDS` seconds, default `10`.
- The poller refresh interval is `ACCOUNT_STATE_POLL_SECONDS`, default `5`.
- If follower state is unavailable, Preflight shows `Follower Hyperliquid account state unavailable` and blocks live readiness.
- If an enabled leader has no state or stale state, Preflight shows the leader address and blocks live readiness.

The Preflight page includes a `Small Live Start Checklist` covering live flags, kill switch, follower/leader state freshness, displayed `copy_multiplier`, allowed coins mode, preferred venue, Hyperliquid readiness, unknown orders, allocation mismatch, and effective leverage readiness. Leader max-notional caps are optional insurance rails; missing caps produce warnings, not blockers.

## Account State APIs

```text
GET /account-states/follower
GET /account-states/leaders
GET /account-states/leaders/{leader_id}
GET /dashboard/realtime
```

These APIs return account summaries, positions, update age, stale flags, and copyability metadata. They do not return private keys or raw secrets. Raw account payload fields with secret-like names are masked before storage.

## Local Dry-Run Tests

```bash
cd /home/ubuntu/copytrade-bot
./scripts/dry-run-tests.sh
```

Current tests cover:

- Ratio calculation and `copy_multiplier`.
- Open, add, reduce, close, and flip transitions.
- Symbol mapping and Binance quantity rounding.
- Duplicate fill protection.
- Risk-limit rejection.
- `TRADING_ENABLED=false` dry-run behavior.
- Reconciler tolerance checks.
- Hedge Mode order building, no `reduceOnly`, no `BOTH`, multi-leader allocation isolation, aggregate allocation matching, position-mode switching blocks, Binance 10x/isolated gates, duplicate fills, and snapshots.
- AUTO_COPY MARKET-only behavior, no LIMIT price/timeInForce, Binance-safe client order IDs, timeout UNKNOWN handling, recovery query by client ID, fill-driven lock dispatch, actual executed quantity allocation updates, and latency calculations.
- Hyperliquid-first routing, Binance fallback, venue-isolated allocations, Hyperliquid IOC order construction, isolated effective leverage setup, margin sufficiency checks, cloid recovery, kill-switch and venue trading gates, startup config validation, and manual order source records.

## Backend Dev

Use Python 3.12 for the target runtime. The Dockerfile pins Python 3.12.

```bash
cd backend
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp ../.env.example ../.env
alembic upgrade head
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Frontend Dev

```bash
cd frontend
npm install
NEXT_PUBLIC_API_BASE=http://localhost:8000 npm run dev
```

## Docker Deploy

```bash
cd /home/ubuntu/copytrade-bot
cp .env.example .env
# edit .env before starting
docker compose up -d --build
```

This VPS currently has Docker Compose v2 installed.

## Binance Testnet

Set:

```env
BINANCE_TESTNET=true
BINANCE_HEDGE_MODE=true
TRADING_ENABLED=false
```

The Binance client uses `https://testnet.binancefuture.com` in this mode.

## Hyperliquid Testnet

Set:

```env
HYPERLIQUID_EXECUTION_NETWORK=testnet
HYPERLIQUID_PRIVATE_KEY=
HYPERLIQUID_PRIVATE_KEY_FILE=
HYPERLIQUID_API_WALLET_ADDRESS=
HYPERLIQUID_VAULT_ADDRESS=
HYPERLIQUID_DEFAULT_LEVERAGE=10
HYPERLIQUID_DEFAULT_MARGIN_MODE=ISOLATED
HYPERLIQUID_TRADING_ENABLED=false
```

Private keys stay in backend environment variables only; they are not returned by API responses and are not stored in the database.

Use either `HYPERLIQUID_PRIVATE_KEY` or `HYPERLIQUID_PRIVATE_KEY_FILE`. Do not put private keys in frontend env vars, API payloads, logs, tickets, screenshots, or Git.

## Dry-Run Go-Live Check

1. Fill the Hyperliquid key/account fields, but keep:

```env
TRADING_ENABLED=false
HYPERLIQUID_TRADING_ENABLED=false
BINANCE_TRADING_ENABLED=false
```

2. Keep the kill switch on in the Risk page, or run:

```bash
docker compose exec backend python -m app.scripts.kill_switch_on
```

3. Start or restart the stack:

```bash
docker compose up -d --build
```

4. Open `/preflight` and confirm Hyperliquid API, follower account state, leader state, meta universe, enabled coins, target leverage, and allocation checks. Coins with max leverage below 10 should show WARNING, not BLOCKED.
5. Add one leader in `/leaders`. Leave allowed coins empty for `ALL_COINS`, or choose a custom allowlist only when you want to restrict coins.
6. Open `/dashboard` and confirm `My Hyperliquid Follower Account` shows accountValue, withdrawable, positions, update age, and no stale warning.
7. Open `/leaders` or a leader detail page and confirm the leader accountValue, positions, copyability, route, and allocation status are visible.
8. Watch dry-run orders in `/orders`: venue is correct, Hyperliquid orders are MARKET/IOC intent with cloid, risk checklist has isolated/effective leverage, account-state freshness checks, allocations update, and no live exchange order is submitted.

## Small Live Start

Recommended sequence:

1. Enable one leader only.
2. For a smaller rollout, switch allowed coins to `CUSTOM_LIST` and start with one or two coins such as BTC or ETH.
3. Set `copy_multiplier=0.01` or lower. This scales the account-ratio target, not the leader's raw notional.
4. Optionally set small `max_notional_per_trade` and `max_total_notional` caps as insurance rails.
5. Keep `ENABLE_BINANCE_FALLBACK=false`; test Hyperliquid first.
6. Confirm `/preflight` is OK for the selected venue.
7. Set:

```env
TRADING_ENABLED=true
HYPERLIQUID_TRADING_ENABLED=true
BINANCE_TRADING_ENABLED=false
```

8. Turn the kill switch off in the Risk page only after the above checks are green.
9. Watch the first order: leader fill event time, follower submit time, cloid, filled qty, allocation update, and actual follower position.
10. Increase coins, leaders, and multiplier gradually.

## Emergency Stop

- Frontend Kill Switch immediately blocks new open/add orders.
- Close/reduce intent remains allowed when it reduces risk.
- `TRADING_ENABLED=false` returns the system to dry-run on restart.
- `HYPERLIQUID_TRADING_ENABLED=false` disables only Hyperliquid live execution.
- `BINANCE_TRADING_ENABLED=false` disables only Binance live execution.
- CLI kill switch:

```bash
cd /home/ubuntu/copytrade-bot
docker compose exec backend python -m app.scripts.kill_switch_on
```

## Enabling Live Trading

Live trading should only be enabled after dry-run order records match expectations.

1. Keep Hyperliquid testnet or small production size first and verify coin routing, quantity rounding, cloid, and allocation behavior.
2. Add production Binance keys only if Binance fallback is needed.
3. Set conservative global limits:

```env
GLOBAL_MAX_NOTIONAL=1000
GLOBAL_MAX_DAILY_LOSS=100
```

4. Set venue-specific trading flags only for the venue being tested.
5. Confirm `/preflight` shows `ready_for_live=true` and the chosen venue is ready.
6. Use the Risk page to turn the kill switch off.

If either gate is absent, the executor records `DRY_RUN` and does not submit an exchange order.

## VPS Hardening Notes

- Put the admin UI behind HTTPS. The included Nginx template expects cert files under `deploy/certs/`.
- Add UFW rules for only `22`, `80`, and `443`.
- Add fail2ban for SSH and Nginx auth failures.
- Consider enabling the commented Nginx basic auth layer as an additional perimeter.
- Restrict `IP_ALLOWLIST` in `.env` where possible.
- Restrict VPS SSH and UI access to your own IP where possible.
- Store `.env` outside Git and keep file permissions tight.
- Rotate `ADMIN_PASSWORD_BOOTSTRAP` after first login.
- Keep HTTPS, login, and TOTP enabled for the frontend.
- Use dedicated Binance API keys with futures-only permissions and IP restrictions.
- Start Hyperliquid with small funds first and verify withdrawals/access before scaling.

## API References

- Hyperliquid WebSocket subscriptions: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Hyperliquid info endpoint: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Binance USD-M new order: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order
- Binance USD-M change position mode: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Change-Position-Mode
- Binance USD-M change margin type: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Change-Margin-Type
- Binance USD-M change initial leverage: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Change-Initial-Leverage
- Binance USD-M position information V2: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-Information-V2
- Binance USD-M exchange info: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information
