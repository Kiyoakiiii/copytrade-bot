"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Activity, ArrowUpRight, Radio, RefreshCw, ShieldAlert, Wallet } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { copyStatusTone, effectiveCopyReason, effectiveCopyStatus } from "@/lib/copyStatus";
import { useDashboardStream, useRealtimeFallbackPolling } from "@/lib/realtime";
import {
  formatAge as formatAgeLabel,
  formatDateTime as formatDateTimeLabel,
  formatDisplayValue,
  formatMs as formatMsLabel,
  formatNotional,
  formatOpenTimeLabel,
  formatPrice,
  formatQuantity,
} from "@/lib/format";

type Position = {
  dex?: string | null;
  dex_display_name?: string | null;
  coin: string;
  canonical_coin?: string | null;
  product_type?: string | null;
  side: string;
  size: string | null;
  notional: string | null;
  entry_px: string | null;
  mark_px: string | null;
  mid_px?: string | null;
  mark_price_stale?: boolean | null;
  open_time?: string | null;
  first_seen_at?: string | null;
  open_time_source?: string | null;
  updated_at?: string | null;
  data_age_ms?: number | null;
  active?: boolean | null;
  status?: string | null;
  unrealized_pnl: string | null;
  leverage: string | null;
  margin_used: string | null;
  margin_mode?: string | null;
  liquidation_px: string | null;
  allocation_matched?: boolean | null;
  copy_status?: string | null;
  copy_reason?: string | null;
  last_copy_order_display_status?: string | null;
  last_copy_order_reason?: string | null;
  baseline_status?: string | null;
  baseline_id?: number | null;
  sizing?: SizingBreakdown | null;
};

type SizingBreakdown = {
  target_notional: string | null;
  delta_notional: string | null;
  error: string | null;
};

type AccountState = {
  role: string | null;
  address: string | null;
  dex?: string | null;
  dex_display_name?: string | null;
  account_label: string | null;
  account_value: string | null;
  account_value_used_for_sizing?: string | null;
  account_value_source?: string | null;
  account_abstraction_mode?: string | null;
  available_collateral_used_for_margin_check?: string | null;
  balance_source?: string | null;
  portfolio_account_value?: string | null;
  spot_usdc_total?: string | null;
  spot_usdc_hold?: string | null;
  withdrawable: string | null;
  total_ntl_pos: string | null;
  total_margin_used: string | null;
  positions: Position[];
  updated_at: string | null;
  data_age_ms: number | null;
  stale: boolean;
  error_message: string | null;
  dex_states?: AccountState[];
  debug?: {
    network: string;
    account_state_query_address_masked: string | null;
    derived_signer_address_masked: string | null;
    signer_type: string;
    spot_usdc_total?: string | null;
    account_value_used_for_sizing?: string | null;
    account_value_source?: string | null;
    account_abstraction_mode?: string | null;
    likely_issue: string;
  };
};

type LeaderAccount = AccountState & {
  watcher_status: string;
  leader: {
    id: number;
    leader_address: string;
    enabled: boolean;
    copy_multiplier: string;
    fixed_account_value: string | null;
    allowed_coins_mode: string;
    preferred_venue: string;
    fallback_venue: string;
    max_notional_per_trade: string | null;
    max_total_notional: string | null;
  };
};

type DashboardRealtime = {
  last_updated_at: string;
  runtime: {
    trading_enabled: boolean;
    hyperliquid_trading_enabled: boolean;
    binance_trading_enabled: boolean;
    dry_run_or_live: string;
    kill_switch: boolean;
    kill_switch_updated_at: string | null;
    live_opens_enabled: boolean;
    hyperliquid_default_leverage: string;
    hyperliquid_default_margin_mode: string;
    follower_migration?: {
      status?: string | null;
      last_action?: string | null;
      active_account_address?: string | null;
      configured_account_address?: string | null;
      previous_account_address?: string | null;
      migration_completed_at?: string | null;
      archived_allocation_ids?: number[];
      waiting_until_flat_count?: number;
      blockers?: string[];
    };
  };
  follower: AccountState & { configured: boolean };
  leaders: LeaderAccount[];
  active_allocations: Array<{
    leader_address: string;
    coin: string;
    symbol: string;
    execution_venue: string;
    execution_account: string;
    dex: string;
    canonical_coin: string | null;
    position_side: string;
    target_notional: string;
    allocated_notional: string;
    allocated_qty: string;
    avg_entry_price: string | null;
    status: string;
  }>;
  recent_orders: Array<{
    id: number;
    allocation_id: number | null;
    leader_address: string;
    source_coin: string;
    execution_venue: string;
    dex: string;
    canonical_coin: string | null;
    venue_symbol: string | null;
    side: string;
    order_action: string | null;
    status: string;
    dry_run: boolean;
    reason: string | null;
    avg_fill_price: string | null;
    leader_entry_px: string | null;
    follower_avg_entry_px: string | null;
    event_to_ack_ms: number | null;
    ws_to_submit_ms: number | null;
    submit_to_ack_ms: number | null;
    total_hot_path_ms: number | null;
    created_at: string | null;
  }>;
  latency: {
    latest_event_to_ack_ms: number | null;
    latest_ws_to_submit_ms: number | null;
    latest_submit_to_ack_ms: number | null;
    latest_total_hot_path_ms: number | null;
    last_10_avg_event_to_ack_ms: number | null;
    last_10_max_event_to_ack_ms: number | null;
    worst_stage: string | null;
    worst_stage_ms: number | null;
  };
  baseline: {
    baseline_ready: boolean;
    ignored_existing_positions_count: number;
    waiting_until_flat_count: number;
    baseline_unknown_count: number;
    waiting_until_flat_positions: Array<{
      id: number;
      leader_address: string;
      dex: string;
      canonical_coin: string;
      side_at_enable: string;
      notional_at_enable: string | null;
      entry_px_at_enable: string | null;
      mark_px_at_enable: string | null;
      copy_status: string;
    }>;
  };
  preflight_blockers: string[];
  small_live_start_checklist: {
    ready: boolean;
    checks: Array<{ name: string; status: "OK" | "WARNING" | "BLOCKED"; message: string }>;
  };
};

export default function DashboardPage() {
  const [data, setData] = useState<DashboardRealtime | null>(null);
  const [error, setError] = useState("");
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);
  const realtime = useDashboardStream({
    onSnapshot: (payload) => {
      setData(payload as DashboardRealtime);
      setLastRefreshedAt(new Date().toISOString());
      setError("");
    },
  });

  async function load() {
    setError("");
    try {
      const payload = await apiFetch<DashboardRealtime>("/dashboard/realtime");
      setData(payload);
      setLastRefreshedAt(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  useEffect(() => {
    load();
  }, []);
  useRealtimeFallbackPolling(realtime, load);

  const liveMode = data
    ? data.runtime.live_opens_enabled
    : null;
  const killSwitchOn = data?.runtime.kill_switch ?? true;
  const liveStatusLabel = data
    ? killSwitchOn
      ? "KILL SWITCH ON"
      : liveMode
        ? "LIVE OPENS ENABLED"
        : "LIVE OPENS NOT ENABLED"
    : "CHECKING LIVE STATUS";
  const liveStatusDetail = data
    ? killSwitchOn
      ? "New live opens and increases are blocked."
      : liveMode
        ? "New live Hyperliquid opens and increases are allowed."
        : "Trading env flags are not fully enabled."
    : "Loading current runtime state.";
  const liveStatusClass = !data
    ? "border-amber-200 bg-amber-50 text-warn"
    : killSwitchOn
      ? "border-red-200 bg-red-50 text-danger"
      : liveMode
        ? "border-green-200 bg-green-50 text-accent"
        : "border-amber-200 bg-amber-50 text-warn";
  const followerPositions = data?.follower.positions ?? [];
  const followerPositionNotional = sumAbsDecimalStrings(followerPositions.map((position) => position.notional));
  const followerUnrealizedPnl = sumDecimalStrings(followerPositions.map((position) => position.unrealized_pnl));
  const blockersCount = data?.preflight_blockers.length ?? 0;
  const followerMigration = data?.runtime.follower_migration;
  const migrationBlocked = Boolean(followerMigration?.status && followerMigration.status !== "READY");
  const migrationCompleted = followerMigration?.last_action === "CUTOVER_COMPLETED_KILL_SWITCH_ON";
  const liquidationManualMarkets = data?.active_allocations.filter(
    (allocation) => allocation.status === "LIQUIDATION_DETACHED"
  ) ?? [];

  return (
    <AppShell>
      <Header
        title="Dashboard"
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slate-500">
              {lastRefreshedAt ? `Refreshed ${formatDateTime(lastRefreshedAt)}` : "Waiting for first refresh"}
            </span>
            <span className={realtime.connected ? "rounded-md border border-green-200 bg-green-50 px-2 py-1 text-xs text-accent" : "rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-warn"}>
              realtime mode: {realtime.mode} / {formatAge(realtime.lastUpdatedAgeMs)}
            </span>
            <button className="btn btn-muted" type="button" onClick={load}>
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}
      {migrationBlocked || migrationCompleted ? (
        <section className={`mb-4 rounded-md border px-4 py-3 ${migrationBlocked ? "border-red-200 bg-red-50 text-danger" : "border-amber-200 bg-amber-50 text-warn"}`}>
          <div className="flex items-start gap-3">
            <ShieldAlert className="mt-0.5 h-5 w-5 shrink-0" />
            <div className="min-w-0">
              <div className="font-semibold">
                {migrationBlocked ? "Follower migration blocked" : "Follower migration completed"}
              </div>
              <div className="mt-1 text-sm">
                {followerMigration?.previous_account_address ?? "--"} → {followerMigration?.configured_account_address ?? followerMigration?.active_account_address ?? "--"}
              </div>
              {migrationBlocked ? (
                <div className="mt-1 text-sm">{(followerMigration?.blockers ?? []).join("; ")}</div>
              ) : (
                <div className="mt-1 text-sm">
                  Old allocations archived; {followerMigration?.waiting_until_flat_count ?? 0} current leader position(s) wait until flat. Kill Switch remains on.
                </div>
              )}
            </div>
          </div>
        </section>
      ) : null}

      {liquidationManualMarkets.length ? (
        <section className="mb-4 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-danger">
          <div className="flex items-start gap-3">
            <ShieldAlert className="mt-0.5 h-5 w-5 shrink-0" />
            <div className="min-w-0">
              <div className="font-semibold">Liquidation markets require manual handling</div>
              <div className="mt-1 text-sm">
                All later copy fills are disabled for these account/market pairs. They release automatically only after the actual follower position is flat.
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {liquidationManualMarkets.map((allocation) => (
                  <span
                    className="rounded border border-red-200 bg-white px-2 py-1 font-mono text-xs"
                    key={`${allocation.execution_account}:${allocation.dex}:${allocation.canonical_coin}:${allocation.leader_address}`}
                  >
                    {allocation.execution_account ? allocation.execution_account.slice(-4) : "MAIN"} · {allocation.canonical_coin ?? allocation.coin} · {allocation.position_side} · qty {allocation.allocated_qty}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      <section className={`mb-4 rounded-md border px-4 py-3 ${liveStatusClass}`}>
        <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
          <div className="flex min-w-0 items-start gap-3">
            <ShieldAlert className="mt-1 h-5 w-5 shrink-0" />
            <div className="min-w-0">
              <div className="text-[11px] font-semibold uppercase tracking-normal">Live open status</div>
              <div className="mt-1 text-xl font-semibold tracking-normal">{liveStatusLabel}</div>
              <div className="mt-1 text-sm">{liveStatusDetail}</div>
            </div>
          </div>
          <div className="text-xs md:text-right">
            Kill switch changed: {formatDateTime(data?.runtime.kill_switch_updated_at)}
          </div>
        </div>
      </section>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-6">
        <Metric icon={<Radio className="h-4 w-4" />} label="Execution" value={liveMode === null ? "Checking" : liveMode ? "Live" : "Dry-run"} tone={liveMode === null ? "warn" : liveMode ? "ok" : "danger"} />
        <Metric icon={<Activity className="h-4 w-4" />} label="Open Positions" value={String(followerPositions.length)} tone={followerPositions.length ? "ok" : undefined} />
        <Metric icon={<Activity className="h-4 w-4" />} label="Position Notional" value={formatNotional(followerPositionNotional)} />
        <Metric icon={<Activity className="h-4 w-4" />} label="Unrealized PnL" value={formatNotional(followerUnrealizedPnl)} tone={pnlTone(followerUnrealizedPnl)} />
        <Metric icon={<Wallet className="h-4 w-4" />} label="Account Value Used" value={data?.follower.account_value_used_for_sizing ?? data?.follower.account_value ?? "--"} />
        <Metric icon={<RefreshCw className="h-4 w-4" />} label="Position Age" value={formatAge(data?.follower.data_age_ms)} tone={data?.follower.stale ? "danger" : "ok"} />
      </div>

      <section className="panel mt-4 overflow-hidden">
        <div className="flex flex-col gap-3 border-b border-line px-4 py-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <div className="text-base font-semibold text-ink">My Real-Time Positions</div>
              <StatusPill tone={liveMode ? "ok" : "danger"}>{liveMode ? "Live" : "Dry-run"}</StatusPill>
              <StatusPill tone={realtime.connected ? "ok" : "warn"}>{realtime.connected ? "Stream Connected" : "Polling"}</StatusPill>
              <StatusPill tone={data?.follower.stale ? "danger" : "ok"}>{data?.follower.stale ? "Stale" : "Fresh"}</StatusPill>
            </div>
            <div className="mt-1 truncate font-mono text-xs text-slate-500">{data?.follower.address ?? "not configured"}</div>
          </div>
          <div className="text-xs text-slate-500">
            API {formatDateTime(data?.last_updated_at)} · local {lastRefreshedAt ? formatDateTime(lastRefreshedAt) : "--"}
          </div>
        </div>
        <LivePositionsTable positions={followerPositions} empty="No open follower positions" />
        <div className="grid gap-x-6 gap-y-3 border-t border-line px-4 py-3 text-sm md:grid-cols-2 xl:grid-cols-5">
          <InlineDatum label="Account mode" value={data?.follower.account_abstraction_mode} />
          <InlineDatum label="Value source" value={data?.follower.account_value_source ?? data?.follower.balance_source} />
          <InlineDatum label="Available collateral" value={formatNotional(data?.follower.available_collateral_used_for_margin_check ?? data?.follower.withdrawable)} />
          <InlineDatum label="Spot USDC" value={formatNotional(data?.follower.spot_usdc_total ?? data?.follower.debug?.spot_usdc_total ?? null)} />
          <InlineDatum label="Total margin used" value={formatNotional(data?.follower.total_margin_used)} />
        </div>
        {data?.follower.debug?.likely_issue && data.follower.debug.likely_issue !== "OK" ? (
          <div className="border-t border-line px-4 py-3 text-sm text-danger">{data.follower.debug.likely_issue}</div>
        ) : null}
        {data?.follower.error_message ? <div className="border-t border-line px-4 py-3 text-sm text-danger">{data.follower.error_message}</div> : null}
      </section>

      <section className="panel mt-4 overflow-hidden">
        <div className="flex flex-col gap-2 border-b border-line px-4 py-3 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-sm font-semibold">Leader Positions Being Watched</div>
            <div className="mt-1 text-xs text-slate-500">Only new copyable leader positions are considered for live copy.</div>
          </div>
          <div className="text-xs font-medium text-slate-500">
            {data?.leaders.length ?? 0} active
          </div>
        </div>
        <div className="space-y-4 p-4">
          {data?.leaders.length ? (
            data.leaders.map((leader) => (
              <LeaderCard key={leader.leader.id} leader={leader} />
            ))
          ) : (
            <div className="text-sm text-slate-500">No enabled leaders</div>
          )}
        </div>
      </section>

      <section className="panel mt-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Live System Status</div>
        <div className="grid gap-x-6 gap-y-3 px-4 py-3 text-sm md:grid-cols-2 xl:grid-cols-6">
          <InlineDatum label="Kill switch" value={data ? data.runtime.kill_switch ? "ON - new opens blocked" : "OFF - new opens allowed" : "Checking"} tone={data?.runtime.kill_switch ? "danger" : data ? "ok" : "warn"} />
          <InlineDatum label="Kill switch changed" value={formatDateTime(data?.runtime.kill_switch_updated_at)} />
          <InlineDatum label="Watcher" value={data?.small_live_start_checklist.ready ? "Ready" : "Checking"} tone={data?.small_live_start_checklist.ready ? "ok" : "warn"} />
          <InlineDatum label="Latest latency" value={formatMs(data?.latency.latest_event_to_ack_ms)} />
          <InlineDatum label="Last 10 avg / max" value={`${formatMs(data?.latency.last_10_avg_event_to_ack_ms)} / ${formatMs(data?.latency.last_10_max_event_to_ack_ms)}`} />
          <InlineDatum label="Ignored existing" value={String(data?.baseline.ignored_existing_positions_count ?? 0)} tone={data?.baseline.baseline_unknown_count ? "danger" : undefined} />
          <InlineDatum label="Blockers" value={String(blockersCount)} tone={blockersCount ? "danger" : "ok"} />
        </div>
        {data?.baseline.waiting_until_flat_positions.length ? (
          <table className="data-table border-t text-xs">
            <thead className="bg-panel text-slate-500">
              <tr>
                <th className="px-4 py-2">Leader</th>
                <th className="px-4 py-2">DEX</th>
                <th className="px-4 py-2">Coin</th>
                <th className="px-4 py-2">Side</th>
                <th className="px-4 py-2">Enable Notional</th>
                <th className="px-4 py-2">Entry Price</th>
                <th className="px-4 py-2">Mark Price</th>
                <th className="px-4 py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {data.baseline.waiting_until_flat_positions.map((item) => (
                <tr key={item.id} className="border-t border-line">
                  <td className="max-w-[180px] truncate px-4 py-2 font-mono">{item.leader_address}</td>
                  <td className="px-4 py-2">{item.dex || "default"}</td>
                  <td className="px-4 py-2 font-mono">{item.canonical_coin}</td>
                  <td className="px-4 py-2">{item.side_at_enable}</td>
                  <td className="px-4 py-2">{formatNotional(item.notional_at_enable)}</td>
                  <td className="px-4 py-2">{formatPrice(item.entry_px_at_enable)}</td>
                  <td className="px-4 py-2">{formatPrice(item.mark_px_at_enable)}</td>
                  <td className="px-4 py-2 text-warn">{item.copy_status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}
      </section>
    </AppShell>
  );
}

function LeaderCard({ leader }: { leader: LeaderAccount }) {
  const defaultPositions = leader.dex_states?.find((item) => (item.dex ?? "") === "")?.positions.length ?? 0;
  const xyzPositions = leader.dex_states?.find((item) => item.dex === "xyz")?.positions.length ?? 0;
  const ignoredExisting = leader.positions.filter((item) => item.copy_status === "IGNORED_EXISTING_POSITION").length;
  const watcherTone = leader.watcher_status === "active" ? "ok" : "danger";
  const freshnessTone = leader.error_message || leader.stale ? "danger" : "ok";

  return (
    <article className="rounded-md border border-line bg-white shadow-sm">
      <div className="border-b border-line px-4 py-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-mono text-sm font-semibold text-ink">{shortAddress(leader.leader.leader_address)}</span>
              <StatusPill tone={watcherTone}>{humanLabel(leader.watcher_status)}</StatusPill>
              <StatusPill tone={freshnessTone}>{leader.stale ? "Stale" : "Fresh"}</StatusPill>
              <StatusPill>{leader.leader.allowed_coins_mode === "ALL_COINS" ? "All coins" : "Custom coins"}</StatusPill>
              <StatusPill>{leader.leader.preferred_venue}</StatusPill>
            </div>
            <div className="mt-1 break-all font-mono text-xs text-slate-500">{leader.leader.leader_address}</div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="rounded-md border border-line bg-slate-50 px-3 py-2">
              <div className="text-[11px] uppercase tracking-normal text-slate-500">Multiplier</div>
              <div className="text-right text-lg font-semibold tabular-nums text-ink">
                {formatDisplayValue(leader.leader.copy_multiplier)}x
              </div>
            </div>
            <Link className="btn btn-muted h-9" href={`/leaders/${leader.leader.id}`}>
              Details
              <ArrowUpRight className="h-4 w-4" />
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 px-4 py-3 md:grid-cols-2 xl:grid-cols-6">
        <LeaderDatum label="Account value used" value={formatNotional(leader.leader.fixed_account_value)} />
        <LeaderDatum label="Value source" value="LEADER_CONFIG_FIXED" />
        <LeaderDatum label="Position notional" value={formatNotional(leader.total_ntl_pos)} />
        <LeaderDatum label="Positions default / xyz" value={`${defaultPositions} / ${xyzPositions}`} />
        <LeaderDatum label="Ignored existing" value={String(ignoredExisting)} tone={ignoredExisting ? "warn" : "ok"} />
        <LeaderDatum label="State age" value={formatAge(leader.data_age_ms)} tone={leader.stale ? "danger" : "ok"} />
      </div>

      {leader.error_message ? (
        <div className="border-t border-line px-4 py-3 text-sm text-danger">{leader.error_message}</div>
      ) : null}

      <div className="border-t border-line">
        {leader.positions.length ? (
          <div className="overflow-x-auto">
            <table className="data-table text-xs">
              <thead>
                <tr>
                  <th>Market</th>
                  <th>Venue</th>
                  <th>Side</th>
                  <th>Size</th>
                  <th>Notional</th>
                  <th>Entry / Mark</th>
                  <th>Opened</th>
                  <th>Target</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {leader.positions.slice(0, 8).map((position) => (
                  <tr key={`${position.dex ?? ""}-${position.canonical_coin ?? position.coin}-${position.side}`}>
                    <td className="font-mono">{position.canonical_coin ?? position.coin}</td>
                    <td>{position.dex_display_name ?? position.dex ?? "Hyperliquid"}</td>
                    <td>{humanLabel(position.side)}</td>
                    <td>{formatQuantity(position.size)}</td>
                    <td>{formatNotional(position.notional)}</td>
                    <td>
                      <div>{formatPrice(position.entry_px)}</div>
                      <div className={position.mark_price_stale ? "text-[11px] text-warn" : "text-[11px] text-slate-500"}>
                        {formatPrice(position.mark_px ?? position.mid_px)}
                      </div>
                    </td>
                    <td>{formatOpenTime(position)}</td>
                    <td>
                      {position.sizing?.target_notional
                        ? formatNotional(position.sizing.target_notional)
                        : position.sizing?.error ?? "--"}
                    </td>
                    <td>
                      <StatusPill tone={copyStatusTone(effectiveCopyStatus(position))}>{humanLabel(effectiveCopyStatus(position))}</StatusPill>
                      {effectiveCopyReason(position) ? (
                        <div className="mt-1 max-w-[220px] truncate text-[11px] text-slate-500">{effectiveCopyReason(position)}</div>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-4 py-4 text-sm text-slate-500">No open leader positions</div>
        )}
      </div>
    </article>
  );
}

function LeaderDatum({ label, value, tone }: { label: string; value: string; tone?: "ok" | "warn" | "danger" }) {
  const toneClass =
    tone === "danger" ? "text-danger" : tone === "warn" ? "text-warn" : tone === "ok" ? "text-accent" : "text-ink";
  return (
    <div className="min-w-0 border-l border-line pl-3">
      <div className="text-[11px] uppercase tracking-normal text-slate-500">{label}</div>
      <div className={`mt-1 truncate text-sm font-semibold tabular-nums ${toneClass}`}>{value}</div>
    </div>
  );
}

function InlineDatum({
  label,
  value,
  tone,
}: {
  label: string;
  value: string | null | undefined;
  tone?: "ok" | "warn" | "danger";
}) {
  const toneClass =
    tone === "danger" ? "text-danger" : tone === "warn" ? "text-warn" : tone === "ok" ? "text-accent" : "text-ink";
  return (
    <div className="min-w-0">
      <div className="text-[11px] uppercase tracking-normal text-slate-500">{label}</div>
      <div className={`mt-1 truncate font-semibold tabular-nums ${toneClass}`}>{formatDisplayValue(value)}</div>
    </div>
  );
}

function LivePositionsTable({ positions, empty }: { positions: Position[]; empty: string }) {
  return (
    <div className="overflow-x-auto">
      <table className="data-table">
        <thead className="bg-panel text-slate-500">
          <tr>
            <th className="px-4 py-3">Market</th>
            <th className="px-4 py-3">DEX</th>
            <th className="px-4 py-3">Side</th>
            <th className="px-4 py-3">Size</th>
            <th className="px-4 py-3">Notional</th>
            <th className="px-4 py-3">Entry</th>
            <th className="px-4 py-3">Mark</th>
            <th className="px-4 py-3">UPnL</th>
            <th className="px-4 py-3">Leverage</th>
            <th className="px-4 py-3">Margin</th>
            <th className="px-4 py-3">Updated</th>
          </tr>
        </thead>
        <tbody>
          {positions.length ? (
            positions.map((item) => (
              <tr key={`${item.dex ?? ""}-${item.canonical_coin ?? item.coin}-${item.side}`} className="border-t border-line">
                <td className="px-4 py-3 font-mono text-xs">{item.canonical_coin ?? item.coin}</td>
                <td className="px-4 py-3">{item.dex_display_name ?? item.dex ?? "Hyperliquid"}</td>
                <td className={item.side === "SHORT" ? "px-4 py-3 font-semibold text-danger" : "px-4 py-3 font-semibold text-accent"}>
                  {item.side}
                </td>
                <td className="px-4 py-3">{formatQuantity(item.size)}</td>
                <td className="px-4 py-3">{formatNotional(absDecimalString(item.notional))}</td>
                <td className="px-4 py-3">{formatPrice(item.entry_px)}</td>
                <td className={item.mark_price_stale ? "px-4 py-3 text-warn" : "px-4 py-3"}>{formatPrice(item.mark_px ?? item.mid_px)}</td>
                <td className={`px-4 py-3 font-semibold ${pnlClass(item.unrealized_pnl)}`}>{formatNotional(item.unrealized_pnl)}</td>
                <td className="px-4 py-3">
                  <div>{formatDisplayValue(item.leverage)}</div>
                  <div className="text-[11px] text-slate-500">{item.margin_mode ?? "--"}</div>
                </td>
                <td className="px-4 py-3">{formatNotional(item.margin_used)}</td>
                <td className="px-4 py-3">
                  <div>{formatAge(item.data_age_ms)}</div>
                  <div className="text-[11px] text-slate-500">{formatDateTime(item.updated_at)}</div>
                </td>
              </tr>
            ))
          ) : (
            <tr className="border-t border-line">
              <td className="px-4 py-6 text-slate-500" colSpan={11}>{empty}</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function Metric({
  icon,
  label,
  value,
  tone
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  tone?: "ok" | "warn" | "danger";
}) {
  const toneClass =
    tone === "danger" ? "text-danger" : tone === "warn" ? "text-warn" : tone === "ok" ? "text-accent" : "text-ink";
  return (
    <section className="metric-card">
      <div className="metric-label">
        {icon}
        <span>{label}</span>
      </div>
      <div className={`metric-value ${toneClass}`}>{formatDisplayValue(value)}</div>
    </section>
  );
}

function StatusPill({
  children,
  tone = "neutral",
}: {
  children: string;
  tone?: "ok" | "warn" | "danger" | "neutral";
}) {
  const toneClass =
    tone === "ok"
      ? "border-teal-200 bg-teal-50 text-teal-800"
      : tone === "warn"
        ? "border-amber-200 bg-amber-50 text-amber-800"
        : tone === "danger"
          ? "border-red-200 bg-red-50 text-red-700"
          : "border-line bg-slate-50 text-slate-600";
  return <span className={`status-pill ${toneClass}`}>{children}</span>;
}

function humanLabel(value: string | null | undefined) {
  if (!value) return "--";
  return value
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function shortAddress(value: string) {
  return value.length > 14 ? `${value.slice(0, 6)}...${value.slice(-4)}` : value;
}

function formatAge(value: number | null | undefined) {
  return formatAgeLabel(value);
}

function formatDateTime(value: string | null | undefined) {
  return formatDateTimeLabel(value);
}

function formatOpenTime(position: Position) {
  return formatOpenTimeLabel(position);
}

function formatMs(value: number | null | undefined) {
  return formatMsLabel(value);
}

function sumDecimalStrings(values: Array<string | null | undefined>): string {
  const total = values.reduce((sum, value) => {
    if (value === null || value === undefined || value === "") return sum;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? sum + parsed : sum;
  }, 0);
  return String(total);
}

function sumAbsDecimalStrings(values: Array<string | null | undefined>): string {
  const total = values.reduce((sum, value) => {
    if (value === null || value === undefined || value === "") return sum;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? sum + Math.abs(parsed) : sum;
  }, 0);
  return String(total);
}

function absDecimalString(value: string | null | undefined): string | null | undefined {
  if (value === null || value === undefined || value === "") return value;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? String(Math.abs(parsed)) : value;
}

function pnlTone(value: string | null | undefined): "ok" | "danger" | undefined {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) < 1e-10) return undefined;
  return parsed > 0 ? "ok" : "danger";
}

function pnlClass(value: string | null | undefined): string {
  const tone = pnlTone(value);
  if (tone === "ok") return "text-accent";
  if (tone === "danger") return "text-danger";
  return "text-ink";
}
