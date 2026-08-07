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

## New Server Deployment Runbook For The Next AI

This is the authoritative handoff procedure for deploying this bot on a new
server. Read this section before running commands. Also read
[`docs/fill_driven_hot_path.md`](docs/fill_driven_hot_path.md) before changing
execution logic.

### Non-negotiable operating rules

1. **Never expose a secret.** Do not print, `cat`, log, paste, commit, or include
   in a response any `.env` value, signer/private key, Telegram token, API
   secret, admin password, encryption key, TLS private key, session, or database
   dump. Avoid `set -x`, `env`, `printenv`, `docker inspect`, and full
   `docker compose config` output because they can expand secrets. It is safe to
   use `docker compose config --services`.
2. **Only one live automatic-copy writer per execution account.** The main
   `watcher` and explicit `watcher-subaccount` are the automatic-copy
   order-writing processes. Never run an old-server watcher and a new-server
   watcher for the same account at the same time. Database deduplication and
   the writer lease protect processes sharing one database; they cannot protect
   two independent server databases.
3. **Do not consume production fills in dry-run during a migration.** A fill
   processed as `DRY_RUN`, kill-switch-blocked, or otherwise terminal is not
   replayed later just because live trading is enabled. A fresh installation
   should start in dry-run; a production database migration must use the
   dedicated cutover sequence below.
4. **PostgreSQL is the durable source of truth.** Git does not contain leaders,
   multipliers, configured account values, blocked coins, allocations,
   lifecycle ownership, fill outcomes, order history, risk settings, admin
   users, or performance cursors. Redis may start empty; PostgreSQL may not.
5. **Never improvise an active allocation database.** If the follower has open
   positions, restore the production database. A fresh database plus existing
   exchange positions can create wrong ownership, duplicate exposure, or missed
   reductions.
6. **Never use `docker compose down -v`, delete the Postgres volume, retry an
   `UNKNOWN` order, or reset a dirty Git worktree.** Stop and investigate.
7. **Main and subaccount routes are isolated.** `watcher` owns leaders without
   an explicit execution account. `watcher-subaccount` runs with
   `LOW_LATENCY_LEADER_ROUTE_MODE=EXPLICIT` and owns only leaders routed to its
   public subaccount address. They share code and market metadata, but not
   allocations, durable fill scope, orders, positions, or locks.
8. **Only one backend may poll the configured Telegram bot token.** Stop the
   old backend before starting the replacement backend, or disable Telegram on
   one side during staging. Two long-pollers can steal commands from each other.

### What Git deliberately does not contain

- `.env` and any signing/authentication material.
- TLS files under `deploy/certs/` and optional `deploy/htpasswd`.
- Postgres/Redis Docker volumes.
- `backups/*.dump` database backups.

Keep an encrypted/off-server copy of the current `.env`, a recent Postgres
backup, and the ability to reissue TLS certificates. A restored database dump
is sensitive even though it does not contain the Hyperliquid signer key: it
contains account routing, order history, allocation state, admin/auth metadata,
and trading configuration.

### Architecture that must exist after deployment

| Service | Purpose | May submit orders? |
|---|---|---|
| `postgres` | Durable fills, outcomes, orders, allocations and settings | No |
| `redis` | Coordination/cache support | No |
| `backend` | FastAPI, UI API, Telegram control/alerts, migrations | Authorized manual API actions only; embedded auto watcher is disabled by Compose |
| `watcher` | Main-account low-latency fill ingestion and execution | Yes |
| `watcher-subaccount` | Explicit subaccount low-latency execution | Yes |
| `analytics` | Cached leader performance refresh every six hours | No |
| `frontend` | Next.js UI | No |
| `nginx` | HTTPS reverse proxy | No |

The repository currently defines one explicit subaccount worker in
`docker-compose.yml`. Its `HYPERLIQUID_SUBACCOUNT_ADDRESS` override is a public
routing address, not a private key. Before starting it on another installation,
privately verify that this public address is the intended subaccount and that
the leaders assigned to it have the same address in
`leader_configs.hyperliquid_vault_address`. Do not change the signer key just
because the execution target is a subaccount.

### Phase 1: prepare the replacement server without starting the bot

Use a stable, low-latency Linux host with synchronized time. Ubuntu example:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git openssl rsync
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker
timedatectl show -p NTPSynchronized
```

Clone and confirm the exact revision. Do not copy the old working directory or
its build artifacts over the clone.

```bash
sudo mkdir -p /opt/copytrade-bot
sudo chown "$USER":"$USER" /opt/copytrade-bot
git clone git@github.com:Kiyoakiiii/copytrade-bot.git /opt/copytrade-bot
cd /opt/copytrade-bot
git fetch origin
git checkout main
git pull --ff-only origin main
git status --short
git log -1 --oneline
docker compose config --services
```

`git status --short` must be empty before deployment. Build images now, while
the old server is still live, to minimize cutover time. Compose requires its
declared env file even for a build, so create a secret-free temporary copy of
the example; do not start services with it:

```bash
cp .env.example .env
chmod 600 .env
docker compose build --pull
```

The frontend production build is executed inside `docker compose build
frontend`; the final frontend image intentionally contains only the standalone
runtime and cannot rerun `npm` tests. Run the backend suite after the runtime
`.env` has been restored in Phase 2.

Do not run `docker compose up` yet during a production migration.

### Phase 2: recreate runtime-only files securely

Preferred migration method: transfer the existing `.env` over encrypted SSH,
then restrict it immediately. Do not display it to verify the copy.

```bash
scp <old-server>:/home/ubuntu/copytrade-bot/.env /opt/copytrade-bot/.env
chmod 600 /opt/copytrade-bot/.env
```

If the old `.env` is unavailable, rebuild it from a password manager:

```bash
cd /opt/copytrade-bot
cp .env.example .env
chmod 600 .env
```

At minimum, privately verify that these categories are configured without
printing their values:

- PostgreSQL and Redis URLs.
- Main follower public account address.
- Exactly one signer source: preferred `HYPERLIQUID_SIGNER_PRIVATE_KEY`, the
  legacy private-key alias, or a securely mounted private-key file.
- API-wallet public address when an API wallet signs for the main account.
- Mainnet execution and WebSocket/info URLs.
- Public subaccount routing, if the subaccount worker is used.
- `APP_SECRET_KEY` and `ENCRYPTION_MASTER_KEY`. Preserve the old values when
  restoring the old database.
- Admin/TOTP, cookie, IP allowlist and Telegram settings.
- `TRADING_ENABLED` and `HYPERLIQUID_TRADING_ENABLED` according to the fresh
  install or production-cutover path below.

Safe presence check (prints only `set`/`unset`, never values):

```bash
for key in DATABASE_URL HYPERLIQUID_ACCOUNT_ADDRESS APP_SECRET_KEY \
  ENCRYPTION_MASTER_KEY; do
  if grep -q "^${key}=." .env; then echo "${key}=set"; else echo "${key}=unset"; fi
done
```

Run the backend test suite against the image before starting any service:

```bash
docker compose run --rm --no-deps backend pytest -q
```

Generate new application secrets only for a completely fresh database:

```bash
openssl rand -hex 32
openssl rand -hex 32
```

Put the generated values directly into `.env`; never place them in this file,
a shell transcript, a commit, or a chat response.

Nginx requires certificate mount targets before it can start. For an existing
domain, securely install or reissue the certificate:

```bash
cd /opt/copytrade-bot
mkdir -p deploy/certs
sudo install -m 644 /etc/letsencrypt/live/<domain>/fullchain.pem deploy/certs/fullchain.pem
sudo install -m 600 /etc/letsencrypt/live/<domain>/privkey.pem deploy/certs/privkey.pem
touch deploy/htpasswd
chmod 600 deploy/htpasswd
```

For a temporary recovery endpoint only, a self-signed certificate can be used:

```bash
mkdir -p deploy/certs
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout deploy/certs/privkey.pem \
  -out deploy/certs/fullchain.pem \
  -days 30 -subj "/CN=copytrade-recovery"
chmod 600 deploy/certs/privkey.pem
touch deploy/htpasswd
```

Open only SSH, HTTP and HTTPS in the host firewall. Do not expose Postgres,
Redis, FastAPI port 8000, or Next.js port 3000 publicly.

### Phase 3A: completely fresh installation

Use this path only when there are no production allocations or follower
positions to preserve.

1. Set `TRADING_ENABLED=false`, `HYPERLIQUID_TRADING_ENABLED=false`, and
   `BINANCE_TRADING_ENABLED=false` in `.env`.
2. Start the stack and put the database kill switch on:

```bash
cd /opt/copytrade-bot
docker compose up -d
docker compose exec -T backend python -m app.scripts.kill_switch_on
docker compose ps
docker compose exec -T backend curl -fsS http://127.0.0.1:8000/health
```

3. Open `https://<domain>/login`, enroll TOTP, then configure leaders in
   `/leaders`. For every leader, confirm the full public address, execution
   account, multiplier, configured account value, max per-coin position and
   blocked coins.
4. Use `/dashboard` and `/preflight` to verify both follower accounts, watcher
   subscriptions, current positions, market coverage and risk settings.
5. Inspect dry-run outcomes before following the normal go-live procedure later
   in this README.

### Phase 3B: migrate the live production bot without duplicates or lost state

This path preserves existing leader lifecycles. Keep the outage short, but
never overlap order-writing workers.

#### 3B.1 Quiesce and back up the old server

On the old server:

```bash
cd /home/ubuntu/copytrade-bot
docker compose exec -T backend python -m app.scripts.kill_switch_on
docker compose stop watcher watcher-subaccount analytics backend frontend nginx
./scripts/backup-postgres.sh
sha256sum backups/copytrade_*.dump | tail -1
```

Stopping both watchers is the decisive no-duplicate boundary. The kill switch
is written first so the restored database starts in a safe state. Leave the old
watchers stopped until the migration is either completed or deliberately rolled
back.

Transfer the newest dump over encrypted SSH and verify its checksum on the new
server. Never commit it:

```bash
mkdir -p /opt/copytrade-bot/backups
scp <old-server>:/home/ubuntu/copytrade-bot/backups/copytrade_<timestamp>.dump \
  /opt/copytrade-bot/backups/
chmod 600 /opt/copytrade-bot/backups/copytrade_<timestamp>.dump
sha256sum /opt/copytrade-bot/backups/copytrade_<timestamp>.dump
```

If the watcher outage may exceed the configured startup backfill window,
increase `LEADER_FILL_STARTUP_BACKFILL_SECONDS` before first start so it covers
the outage. The durable database cursor and periodic backfill are additional
protection, not permission to run two servers concurrently.

#### 3B.2 Restore with every order writer stopped

On the new server:

```bash
cd /opt/copytrade-bot
docker compose up -d postgres redis
docker compose stop watcher watcher-subaccount analytics backend frontend nginx || true
CONFIRM_RESTORE=1 ./scripts/restore-postgres.sh \
  backups/copytrade_<timestamp>.dump
docker compose up -d backend frontend nginx analytics
docker compose exec -T backend curl -fsS http://127.0.0.1:8000/health
```

The backend automatically runs `alembic upgrade head`. Confirm the database is
on the repository head and the restored kill switch is still on:

```bash
docker compose exec -T postgres psql -U copytrade -d copytrade -Atc \
  "select version_num from alembic_version;"
docker compose exec -T postgres psql -U copytrade -d copytrade -Atc \
  "select coalesce(value->>'kill_switch','missing') from app_settings where key='risk';"
```

Do not start either watcher yet. With only backend/frontend running, privately
verify in the UI:

- all enabled leader addresses, multipliers and configured account values;
- each leader's main/subaccount route;
- blocked coins and max per-coin limits;
- active allocations, including `LIQUIDATION_DETACHED` manual markets;
- main and subaccount public follower identities;
- no unexpected `PENDING_SUBMIT`, `SUBMITTING` or `UNKNOWN` order.

`/preflight` will correctly report the watchers as unavailable at this stage.

#### 3B.3 Live handoff

For an existing production database, the copied `.env` should retain the known
working live venue flags. Do not start a restored production watcher with live
flags off merely to make it “dry-run”; that would consume real leader fills
without executing them.

1. Confirm the old server's two watcher services are still stopped.
2. Confirm both new-server watcher services are stopped.
3. In the new frontend Risk page, turn the kill switch off.
4. Immediately start the new workers:

```bash
cd /opt/copytrade-bot
docker compose up -d watcher watcher-subaccount
```

5. Watch both workers until they report WebSocket and durable-pipeline
   readiness. Fills during the stopped interval are handled by startup/durable
   backfill.

Do not turn the old server back on after this point. Its database is now stale.

### Phase 4: post-start acceptance checks

Service and writer readiness:

```bash
docker compose ps
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select key, value->>'status' as status, value->>'last_heartbeat_at' as heartbeat, value->>'last_error' as error from app_settings where key like 'task_status:low_latency_watcher%' order by key;"
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select key, value->>'websocket_connected' as ws, value->>'ready_for_low_latency_live' as ready, value->>'account_value_ready' as balance_ready, value->>'durable_inbox_pending_count' as pending_fills, value->>'durable_inbox_retrying_count' as retrying_fills, value->>'durable_outbox_pending_submit_count' as pending_orders, value->>'durable_outbox_unknown_count' as unknown_orders from app_settings where key='watcher_status' or key like 'watcher_status:%' order by key;"
```

Expected for both account scopes:

- `websocket_connected=true`;
- `ready_for_low_latency_live=true`;
- `account_value_ready=true` with no account-value blockers;
- pending/retrying/stuck durable inbox counts are zero;
- pending/submitting/unknown durable outbox counts are zero;
- no poll fallback in live mode;
- heartbeats continue updating.

Durable fill/outcome and order checks:

```bash
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select count(*) as unfinished_live_fills from source_fills where is_snapshot=false and processed_at is null;"
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select status, count(*) from execution_orders where source_type='AUTO_COPY' and status in ('PENDING_SUBMIT','SUBMITTING','UNKNOWN') group by status;"
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select right(leader_address,4) as leader, copy_multiplier, fixed_account_value, case when coalesce(hyperliquid_vault_address,'')='' then 'MAIN' else 'SUB' end as route from leader_configs where enabled and deleted_at is null order by id;"
```

After the first real fill, verify all of the following before declaring the
migration complete:

- the exchange leader history contains the same fill as `source_fills`;
- every non-snapshot source fill has exactly one terminal
  `source_fill_outcomes` row;
- all coalesced fill IDs point to one equivalent order, not duplicate orders;
- the order has one unique `cloid` and is `FILLED`;
- actual follower quantity equals the account-scoped allocation quantity;
- main and subaccount orders have the correct `venue_account` scope;
- `leader_event_to_ws_ms`, `ws_to_actual_send_ms`, and exchange fill time are
  plausible. First-time leverage configuration can take seconds; subsequent
  hot-path sends should not inherit it.

Useful recent latency query:

```bash
docker compose exec -T postgres psql -U copytrade -d copytrade -P pager=off -c \
  "select id, right(leader_address,4) as leader, canonical_coin, order_action, leader_event_to_ws_ms, coalesce((latency_trace->'metrics'->>'ws_to_actual_send_ms')::int,ws_to_submit_ms) as ws_to_actual_send_ms, submit_to_ack_ms, event_to_ack_ms from execution_orders where source_type='AUTO_COPY' and created_at >= now()-interval '1 hour' order by id;"
```

### Rollback without creating duplicates

If the new server must be abandoned after its watchers have processed any fill:

1. Turn on the new database kill switch.
2. Stop **both new watcher services**.
3. Back up the new Postgres database.
4. Restore that newest database onto the old server.
5. Verify configuration and only then start the old watchers.

Never simply restart the old watchers against their pre-cutover database. They
do not know which fills/orders the new server processed and can duplicate or
miss trades.

### Routine code deployment on the same server

For a normal code update that keeps the same Postgres volume:

```bash
cd /home/ubuntu/copytrade-bot
git status --short
git fetch origin
git pull --ff-only origin main
docker compose build backend frontend
docker compose up -d backend frontend analytics watcher watcher-subaccount nginx
docker compose ps
```

The watchers use durable source fills, deterministic IDs, unique `cloid`
constraints and startup backfill across a short restart. Do not delete volumes
or start a second server as part of a routine deployment.

### Git safety gate before any future push

Before committing, the next AI must:

```bash
git status --short
git diff --check
git diff --name-only
git ls-files .env
git check-ignore .env
```

`.env` must not be tracked and must be ignored. Stage explicit source files,
never `git add -A` blindly. Inspect staged filenames and scan staged additions
for private keys, Telegram/API tokens, passwords, certificate material and
high-entropy credential literals before pushing.

## Implemented

- Hyperliquid info and WebSocket watcher skeleton for `userFills`, `userEvents`, `orderUpdates`, and `clearinghouseState`.
- Snapshot fills are ignored for live copy execution, and fill IDs are deduplicated by leader, hash, tid, oid, time, and coin.
- Binance USD-M REST client with exchange info, account, position mode, positions, and market/limit order support.
- Binance account risk gate requires Hedge Mode, symbol `ISOLATED` margin, and 10x leverage before live opens/adds.
- Hyperliquid execution venue support with default `HYPERLIQUID` preference, optional Binance fallback, per-leader venue settings, and venue-isolated allocation/order fields.
- Hyperliquid market-equivalent orders are aggressive IOC limit orders with `cloid`; no GTC/resting auto orders are intended.
- Hyperliquid risk policy uses cross margin and the market maximum leverage when cross is supported. Markets that truly require isolated margin use 2x. Risk settings are prewarmed/cached outside the normal fill hot path; close/reduce intent remains reduce-only.
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

Each follower account/market has exactly one active leader lifecycle owner. The
first valid new open from flat wins; fills from competing leaders cannot attach
to that lifecycle. When the actual follower position is flat, ownership is
released immediately, and only a later new open can acquire it. A manually
opened follower position locks that account/market, while manual changes to an
already-owned copy position change the real quantity used by subsequent
proportional reductions. Main and subaccount ownership are independent.

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

For a cross-capable Hyperliquid market, the bot uses cross margin at the market
maximum leverage. A market that the exchange marks as isolated-only uses 2x.
Leverage affects required margin but never changes the copy-ratio formula.

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
- `exists_in_meta`, market maximum leverage, cross/isolated capability, effective policy leverage, and OK/WARNING/BLOCKED risk status per coin
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
- State freshness uses `ACCOUNT_STATE_STALE_SECONDS` and
  `ACCOUNT_STATE_POLL_SECONDS` from the deployed `.env`; do not assume a value
  from an old README or another server.
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
HYPERLIQUID_DEFAULT_LEVERAGE=50
HYPERLIQUID_DEFAULT_MARGIN_MODE=CROSS
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

4. Open `/preflight` and confirm Hyperliquid API, follower account state, leader state, meta universe, enabled coins, cross/isolated policy leverage, and allocation checks.
5. Add one leader in `/leaders`. Leave allowed coins empty for `ALL_COINS`, or choose a custom allowlist only when you want to restrict coins.
6. Open `/dashboard` and confirm `My Hyperliquid Follower Account` shows accountValue, withdrawable, positions, update age, and no stale warning.
7. Open `/leaders` or a leader detail page and confirm the leader accountValue, positions, copyability, route, and allocation status are visible.
8. Watch dry-run orders in `/orders`: venue is correct, Hyperliquid orders are MARKET/IOC intent with cloid, risk checklist has the cached margin mode/effective leverage, account-state freshness checks, allocations update, and no live exchange order is submitted.

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
