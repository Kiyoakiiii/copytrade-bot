# Fill-Driven Hot Path

The low-latency copy path treats the leader fill event as the source of truth for the current market lifecycle.

For the current `leader + dex + canonical_coin` fill:

- Derive the leader post-position from `startPosition`, fill `side`, fill `sz`, and fill price.
- Use that fill-implied post-position for baseline gating, side detection, sizing, reduce, close, and flip planning.
- Do not wait for `latest_account_positions` to include the new fill before making the hot-path decision.
- Do not let a stale snapshot override the fill-implied position.

Snapshots are still used for:

- account value resolution,
- market metadata and risk setting hints,
- post-trade confirmation,
- drift correction,
- allocation mismatch detection.

## Durable inbox/outbox invariants

The live path uses PostgreSQL as the recovery boundary instead of treating an
`asyncio.Queue` as durable state.

1. Every non-snapshot leader fill is inserted into `source_fills` before it is
   handed to a planning worker. `source_fill_id` is the idempotency key.
2. Planning and setting `processed_at` happen in the same transaction as the
   execution-order/allocation transition. An exception rolls all of them back.
3. Unprocessed fills are replayed in durable arrival-id order. A market FIFO
   claim prevents a later fill from passing any earlier unprocessed fill for
   the same `dex + canonical_coin`, even when retry timing differs. They are
   never discarded because an in-memory worker failed.
4. Fragment and lifecycle coalescing stores every covered source-fill id in
   `_copytrade_coalesced_source_fill_ids`; all covered inbox rows are completed
   in the representative plan's transaction.
5. `execution_orders.source_fill_id`, `execution_orders.cloid`, and the submit
   status compare-and-set prevent a fill from creating or claiming two orders.
6. Before the first exchange byte is sent, PostgreSQL atomically allocates the
   signer nonce and commits the exact signed action envelope and its SHA-256
   integrity hash. Recovery first queries the stable `cloid`; if it is absent,
   it may replay only that identical signed envelope/nonce, never sign a second
   action for the same logical order.
   The primary submit transport is a persistent WebSocket. A failure proven to
   occur before `send` may fall back to HTTP with that same envelope. Once a
   WebSocket send starts, timeout/disconnect is classified as ambiguous and
   must use `cloid` recovery; it must never trigger a blind HTTP fallback.
7. Every processed source fill has one `source_fill_outcomes` row. Coalesced
   fragments point to the same logical execution order, while exchange-minimum
   skips are explicitly classified as `MIN_NOTIONAL_EXEMPT`.
8. A session-level PostgreSQL writer lease permits only one live watcher for a
   follower account. Market-scoped transaction locks serialize durable arrival
   insertion and planning, preserving first-arrival-wins ownership when leaders
   trade the same coin.
9. Per-leader REST cursors and a periodic overlapping backfill close reconnect
   and WebSocket delivery gaps. Same-millisecond fills are ordered by their
   `startPosition` causal chain before planning.
10. If leader A's flat fill arrives before leader B's competing open, B remains
    unprocessed in the durable inbox while A's reduce-only close is unresolved.
    A finalized close wakes replay immediately; the earliest waiting fill then
    claims the released market. A restart keeps UNKNOWN/SUBMITTING owner orders
    authoritative, so it cannot create a second owner during recovery.
11. A released market can only be acquired by a high/medium-confidence leader
    fill whose `startPosition=0` and whose derived lifecycle action is a genuine
    new open. Another leader's add, reduce, close, or flip from a position opened
    while the old owner held the market is completed immediately as
    `IGNORED_OLD_LIFECYCLE`; it does not wait on prices/metadata, create an
    allocation, reserve ownership, or submit an exchange order. The final submit
    guard independently rechecks this acquisition invariant.

The order-recovery service must not submit unstarted Hyperliquid outbox rows.
Only the low-latency watcher restores those rows because it reconstructs the
pending-intent ledger and dependency barriers first.

When the durable inbox and outbox are empty, their fallback scans use an idle
backoff instead of polling PostgreSQL on the live-fill cadence. A live fill is
still queued directly, an explicit retry deadline wakes the durable loop at the
exact deadline, and startup performs an immediate recovery pass. Non-critical
status/allocation refresh work yields while an in-memory fill or submit is
active so it cannot compete with the websocket-to-submit path.

Same-direction orders are distributed across a bounded number of submit
shards. Increase/reduce and flip boundaries still wait on the pending-intent
dependency barrier. This avoids making every fill wait for the previous exchange
round trip while preserving market-lifecycle ordering.

Operational counters are published in `watcher_status`:

- `durable_inbox_pending_count`
- `durable_inbox_retrying_count`
- `durable_outbox_pending_submit_count`
- `durable_outbox_submitting_count`
- `in_memory_fill_queue_count`
- `in_memory_submit_queue_count`
- `durable_replay_scan_count`
- `durable_order_resume_scan_count`
- `durable_replay_idle_wait_count`
- `background_cycles_deferred_for_hot_path`

Timestamp rule:

- If `snapshot_updated_at < fill_event_time`, the snapshot is stale for that fill and cannot decide side or size.
- If `snapshot_updated_at >= fill_event_time` and conflicts with the fill-implied post-position, record `POSITION_RECONCILE_MISMATCH` and block unsafe open/increase decisions.

Baseline rule:

- `WAIT_UNTIL_FLAT` plus fill-implied non-flat remains ignored until flat.
- If a copied follower allocation already exists for that leader/market/side, `WAIT_UNTIL_FLAT` must not block that allocation lifecycle; below-min reduce orders are recorded as pending reduce.
- `WAIT_UNTIL_FLAT` plus fill-implied flat clears the baseline but does not copy that flat event.
- `CLEARED` plus fill-implied `startPosition=0` open starts a new `COPY_ALLOWED` lifecycle.
- Unknown fill derivation blocks with `FILL_POSITION_DERIVATION_UNKNOWN`; it must not be written as `IGNORED_BASELINE_POSITION`.
