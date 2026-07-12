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

Timestamp rule:

- If `snapshot_updated_at < fill_event_time`, the snapshot is stale for that fill and cannot decide side or size.
- If `snapshot_updated_at >= fill_event_time` and conflicts with the fill-implied post-position, record `POSITION_RECONCILE_MISMATCH` and block unsafe open/increase decisions.

Baseline rule:

- `WAIT_UNTIL_FLAT` plus fill-implied non-flat remains ignored until flat.
- If a copied follower allocation already exists for that leader/market/side, `WAIT_UNTIL_FLAT` must not block that allocation lifecycle; below-min reduce orders are recorded as pending reduce.
- `WAIT_UNTIL_FLAT` plus fill-implied flat clears the baseline but does not copy that flat event.
- `CLEARED` plus fill-implied `startPosition=0` open starts a new `COPY_ALLOWED` lifecycle.
- Unknown fill derivation blocks with `FILL_POSITION_DERIVATION_UNKNOWN`; it must not be written as `IGNORED_BASELINE_POSITION`.
