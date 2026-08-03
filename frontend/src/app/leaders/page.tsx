"use client";

import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Plus, Power, RefreshCw, Save, Trash2 } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { LeaderPerformanceOverviewPanel, type LeaderPerformanceOverview } from "@/components/LeaderPerformance";
import { apiFetch } from "@/lib/api";
import { copyStatusTone, effectiveCopyReason, effectiveCopyStatus, effectiveCopyable } from "@/lib/copyStatus";
import { leaderDisplayLabel } from "@/lib/leaderIdentity";
import { useDashboardStream, useRealtimeFallbackPolling } from "@/lib/realtime";
import {
  formatAge as formatAgeLabel,
  formatDateTime as formatDateTimeLabel,
  formatDisplayValue,
  formatNotional,
  formatOpenTimeLabel,
  formatPrice,
  formatQuantity,
} from "@/lib/format";

type AllowedMode = "ALL_COINS" | "CUSTOM_LIST";

type ExecutionAccountOption = {
  route_value: string;
  account_address: string | null;
  account_type: "MAIN" | "SUBACCOUNT";
  label: string;
  watcher_running: boolean;
  watcher_ready: boolean;
  active_leaders: string[];
};

type Leader = {
  id: number;
  enabled: boolean;
  deleted_at: string | null;
  delete_reason: string | null;
  leader_address: string;
  copy_multiplier: string;
  fixed_account_value: string | null;
  allowed_symbols: string[] | null;
  blocked_symbols: string[];
  allowed_coins_mode: AllowedMode;
  max_notional_per_trade: string | null;
  max_total_notional: string | null;
  preferred_venue: string;
  fallback_venue: string;
  enabled_venues: string[];
  hyperliquid_vault_address: string | null;
  watcher_status: string;
  last_state_update: string | null;
  positions_loaded: boolean;
  current_allocations_count: number;
  open_allocations_notional: string;
  baseline_rows_count: number;
  waiting_until_flat_count: number;
};

type LeaderEdit = {
  enabled: boolean;
  copy_multiplier: string;
  fixed_account_value: string;
  allowed_mode: AllowedMode;
  allowed_symbols: string;
  blocked_symbols: string;
  max_notional_per_trade: string;
  max_total_notional: string;
  preferred_venue: string;
  fallback_venue: string;
  hyperliquid_vault_address: string;
};

type LeaderAccountState = {
  address: string | null;
  dex?: string | null;
  dex_display_name?: string | null;
  account_value: string | null;
  account_value_used_for_sizing?: string | null;
  account_value_source?: string | null;
  account_abstraction_mode?: string | null;
  balance_source?: string | null;
  available_collateral_used_for_margin_check?: string | null;
  withdrawable: string | null;
  total_ntl_pos: string | null;
  data_age_ms: number | null;
  stale: boolean;
  error_message: string | null;
  positions: Array<{
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
    unrealized_pnl?: string | null;
    copyable?: boolean;
    coin_allowed?: boolean;
    venue_route?: string | null;
    copy_reason?: string;
    copy_status?: string | null;
    last_copy_order_display_status?: string | null;
    last_copy_order_reason?: string | null;
    baseline_status?: string | null;
    baseline_id?: number | null;
  }>;
  dex_states?: LeaderAccountState[];
  leader: { id: number };
};

export default function LeadersPage() {
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [executionAccounts, setExecutionAccounts] = useState<ExecutionAccountOption[]>([]);
  const [accountStates, setAccountStates] = useState<Record<number, LeaderAccountState>>({});
  const [performance, setPerformance] = useState<LeaderPerformanceOverview | null>(null);
  const [edits, setEdits] = useState<Record<number, LeaderEdit>>({});
  const [address, setAddress] = useState("");
  const [replaceAddress, setReplaceAddress] = useState("");
  const [replaceFixedAccountValue, setReplaceFixedAccountValue] = useState("");
  const [replaceExecutionAccount, setReplaceExecutionAccount] = useState("");
  const [multiplier, setMultiplier] = useState("0.1");
  const [fixedAccountValue, setFixedAccountValue] = useState("");
  const [executionAccount, setExecutionAccount] = useState("");
  const [allowedMode, setAllowedMode] = useState<AllowedMode>("ALL_COINS");
  const [allowedSymbols, setAllowedSymbols] = useState("");
  const [blockedSymbols, setBlockedSymbols] = useState("");
  const [preferredVenue, setPreferredVenue] = useState("HYPERLIQUID");
  const [fallbackVenue, setFallbackVenue] = useState("NONE");
  const [maxPerTrade, setMaxPerTrade] = useState("");
  const [maxTotal, setMaxTotal] = useState("");
  const [showDeleted, setShowDeleted] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<number | "new" | null>(null);
  const [lastSavedLeaderId, setLastSavedLeaderId] = useState<number | null>(null);
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);
  const realtimeRefreshTimerRef = useRef<number | null>(null);
  const realtimeRefreshRunningRef = useRef(false);
  const realtimeRefreshQueuedRef = useRef(false);
  const saveFeedbackTimerRef = useRef<number | null>(null);

  async function load(options?: { preserveEdits?: boolean; includePerformance?: boolean }) {
    setError("");
    const [rows, accounts, executionAccountOptions, performancePayload] = await Promise.all([
      apiFetch<Leader[]>(`/leaders?include_deleted=${showDeleted ? "true" : "false"}`),
      apiFetch<LeaderAccountState[]>("/account-states/leaders").catch(() => []),
      apiFetch<ExecutionAccountOption[]>("/leaders/execution-accounts"),
      options?.includePerformance
        ? apiFetch<LeaderPerformanceOverview>("/leaders/performance").catch(() => null)
        : Promise.resolve(undefined),
    ]);
    setLeaders(rows);
    setExecutionAccounts(executionAccountOptions);
    setAccountStates(Object.fromEntries(accounts.map((item) => [item.leader.id, item])));
    if (performancePayload !== undefined) setPerformance(performancePayload);
    setEdits((current) => {
      const nextDefaults = Object.fromEntries(rows.map((leader) => [leader.id, editFromLeader(leader)]));
      if (!options?.preserveEdits) return nextDefaults;
      return Object.fromEntries(
        rows.map((leader) => [leader.id, current[leader.id] ?? nextDefaults[leader.id]])
      );
    });
    setLastRefreshedAt(new Date().toISOString());
  }

  function scheduleRealtimeRefresh(delayMs = 750) {
    realtimeRefreshQueuedRef.current = true;
    if (realtimeRefreshRunningRef.current || realtimeRefreshTimerRef.current !== null) return;
    realtimeRefreshTimerRef.current = window.setTimeout(async () => {
      realtimeRefreshTimerRef.current = null;
      if (!realtimeRefreshQueuedRef.current || realtimeRefreshRunningRef.current) return;
      realtimeRefreshQueuedRef.current = false;
      realtimeRefreshRunningRef.current = true;
      try {
        await load({ preserveEdits: true });
      } catch (err) {
        setError(errorMessage(err));
      } finally {
        realtimeRefreshRunningRef.current = false;
        // Collapse an event burst into one trailing refresh. The leaders page
        // includes a large account-state payload, so it must never fan out one
        // multi-megabyte request set per SSE event.
        if (realtimeRefreshQueuedRef.current) scheduleRealtimeRefresh(5000);
      }
    }, delayMs);
  }

  const realtime = useDashboardStream({
    onEvent: (event) => {
      if (["leader_state_update", "positions_update", "baseline_status_update", "allocation_status_update"].includes(event.event_type)) {
        scheduleRealtimeRefresh();
      }
    },
  });

  useEffect(() => {
    load({ includePerformance: true }).catch((err) => setError(errorMessage(err)));
  }, [showDeleted]);
  useEffect(() => () => {
    if (realtimeRefreshTimerRef.current !== null) window.clearTimeout(realtimeRefreshTimerRef.current);
    if (saveFeedbackTimerRef.current !== null) window.clearTimeout(saveFeedbackTimerRef.current);
  }, []);
  useRealtimeFallbackPolling(realtime, () => scheduleRealtimeRefresh(0));

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy("new");
    setError("");
    try {
      await apiFetch<Leader>("/leaders", {
        method: "POST",
        body: JSON.stringify({
          leader_address: address,
          copy_multiplier: decimalInputValue(multiplier),
          fixed_account_value: decimalInputValue(fixedAccountValue),
          allowed_symbols: allowedMode === "ALL_COINS" ? null : splitList(allowedSymbols),
          blocked_symbols: splitList(blockedSymbols),
          preferred_venue: preferredVenue,
          fallback_venue: fallbackVenue,
          enabled_venues: enabledVenues(preferredVenue),
          max_notional_per_trade: emptyDecimalToNull(maxPerTrade),
          max_total_notional: emptyDecimalToNull(maxTotal),
          hyperliquid_vault_address: executionAccount.trim() || null
        })
      });
      setAddress("");
      setFixedAccountValue("");
      setExecutionAccount("");
      setAllowedMode("ALL_COINS");
      setAllowedSymbols("");
      setBlockedSymbols("");
      setMaxPerTrade("");
      setMaxTotal("");
      await load();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(null);
    }
  }

  async function replaceActiveLeader(event: FormEvent) {
    event.preventDefault();
    const confirmed = window.confirm("Replace all active leaders in the selected execution account. The other account is untouched, and existing copied positions are not closed automatically.");
    if (!confirmed) return;
    setBusy("new");
    setError("");
    try {
      await apiFetch<Leader>("/leaders/replace-active", {
        method: "POST",
        body: JSON.stringify({
          leader_address: replaceAddress,
          fixed_account_value: decimalInputValue(replaceFixedAccountValue),
          hyperliquid_vault_address: replaceExecutionAccount || null
        })
      });
      setReplaceAddress("");
      setReplaceFixedAccountValue("");
      setReplaceExecutionAccount("");
      await load();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(null);
    }
  }

  async function saveLeader(leader: Leader) {
    const edit = edits[leader.id];
    if (!edit) return;
    setBusy(leader.id);
    setError("");
    try {
      const savedLeader = await apiFetch<Leader>(`/leaders/${leader.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          enabled: edit.enabled,
          copy_multiplier: decimalInputValue(edit.copy_multiplier),
          fixed_account_value: decimalInputValue(edit.fixed_account_value),
          allowed_symbols: edit.allowed_mode === "ALL_COINS" ? null : splitList(edit.allowed_symbols),
          blocked_symbols: splitList(edit.blocked_symbols),
          preferred_venue: edit.preferred_venue,
          fallback_venue: edit.fallback_venue,
          enabled_venues: enabledVenues(edit.preferred_venue),
          max_notional_per_trade: emptyDecimalToNull(edit.max_notional_per_trade),
          max_total_notional: emptyDecimalToNull(edit.max_total_notional),
          hyperliquid_vault_address: edit.hyperliquid_vault_address.trim() || null
        })
      });
      // PATCH already returns the committed server representation. Apply it
      // directly so Save does not wait for the unrelated 1.8MB account-state
      // and performance refreshes before re-enabling the button.
      setLeaders((current) => current.map((item) => item.id === savedLeader.id ? savedLeader : item));
      setEdits((current) => ({ ...current, [savedLeader.id]: editFromLeader(savedLeader) }));
      setLastRefreshedAt(new Date().toISOString());
      setLastSavedLeaderId(savedLeader.id);
      if (saveFeedbackTimerRef.current !== null) window.clearTimeout(saveFeedbackTimerRef.current);
      saveFeedbackTimerRef.current = window.setTimeout(() => {
        setLastSavedLeaderId((current) => current === savedLeader.id ? null : current);
      }, 3000);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(null);
    }
  }

  async function toggleLeader(leader: Leader) {
    setBusy(leader.id);
    setError("");
    try {
      await apiFetch<Leader>(`/leaders/${leader.id}/${leader.enabled && !leader.deleted_at ? "disable" : "enable"}`, {
        method: "POST",
        body: "{}"
      });
      await load();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(null);
    }
  }

  async function deleteLeader(leader: Leader) {
    const confirmed = window.confirm("删除后将停止复制该 leader 的新交易，但不会自动平掉已有仓位。");
    if (!confirmed) return;
    setBusy(leader.id);
    setError("");
    try {
      await apiFetch<Leader>(`/leaders/${leader.id}`, {
        method: "DELETE",
        body: JSON.stringify({ delete_reason: "deleted from Leaders page" })
      });
      await load();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(null);
    }
  }

  const activeCount = useMemo(
    () => leaders.filter((leader) => leader.enabled && !leader.deleted_at).length,
    [leaders]
  );

  return (
    <AppShell>
      <Header
        title="Leaders"
        right={
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">
              {lastRefreshedAt ? `Refreshed ${formatDate(lastRefreshedAt)}` : "Waiting for first refresh"}
            </span>
            <span className={realtime.connected ? "rounded-md border border-green-200 bg-green-50 px-2 py-1 text-xs text-accent" : "rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-warn"}>
              realtime mode: {realtime.mode}
            </span>
            <label className="flex h-10 items-center gap-2 rounded-md border border-line bg-white px-3 text-sm text-slate-600">
              <input type="checkbox" checked={showDeleted} onChange={(e) => setShowDeleted(e.target.checked)} />
              Show deleted
            </label>
            <button className="btn btn-muted" type="button" onClick={() => load({ includePerformance: true }).catch((err) => setError(errorMessage(err)))}>
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}

      <LeaderPerformanceOverviewPanel data={performance} />

      <section className="panel mb-4 p-4 text-sm">
        <div className="grid gap-3 md:grid-cols-4">
          <Metric label="Source of truth" value="Database" />
          <Metric label="Enabled leaders" value={String(activeCount)} />
          <Metric label="Runtime updates" value="Coalesced realtime refresh" />
          <Metric label="Allowed coins" value="ALL_COINS by default" />
        </div>
        <p className="mt-3 text-slate-600">
          Add leader addresses here. The optional .env bootstrap seed is only for first import when the database is empty. All coins means all enabled Hyperliquid DEX markets, including XYZ / TradFi markets.
        </p>
        <p className="mt-2 text-slate-600">
          Account value used is fixed per leader and sizes only a new position lifecycle. Copy multiplier scales that initial account-risk ratio; later increases and reductions follow position-size ratios.
        </p>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {executionAccounts.map((account) => (
            <div key={account.account_type === "MAIN" ? "main" : account.route_value} className="rounded-md border border-line bg-panel px-3 py-2">
              <div className="flex items-center justify-between gap-3">
                <span className="font-medium text-ink">{account.label}</span>
                <span className={account.watcher_running ? "text-accent" : "text-warn"}>
                  worker {account.watcher_running ? "running" : "not running"}
                </span>
              </div>
              <div className="mt-1 break-all font-mono text-xs text-slate-500">{account.account_address ?? "Address unavailable"}</div>
              <div className="mt-1 text-xs text-slate-500">Active leaders: {account.active_leaders.length}</div>
            </div>
          ))}
        </div>
      </section>

      <form onSubmit={replaceActiveLeader} className="panel mb-4 grid gap-3 p-4 md:grid-cols-[minmax(220px,1fr)_220px_minmax(260px,1fr)_auto]">
        <input className="field" value={replaceAddress} onChange={(e) => setReplaceAddress(e.target.value)} placeholder="New leader address 0x..." required />
        <input
          className="field"
          inputMode="decimal"
          value={replaceFixedAccountValue}
          onChange={(e) => setReplaceFixedAccountValue(e.target.value)}
          placeholder="Account value used (USDC)"
          required
        />
        <ExecutionAccountSelect
          value={replaceExecutionAccount}
          options={executionAccounts}
          onChange={setReplaceExecutionAccount}
          ariaLabel="Execution account for replacement"
        />
        <button className="btn btn-danger" type="submit" disabled={busy === "new"}>
          <RefreshCw className="h-4 w-4" />
          Replace in this account
        </button>
      </form>

      <form onSubmit={submit} className="panel mb-4 grid gap-3 p-4 lg:grid-cols-[minmax(220px,1.5fr)_130px_180px_150px_minmax(180px,1fr)]">
        <input className="field" value={address} onChange={(e) => setAddress(e.target.value)} placeholder="Leader address 0x..." required />
        <input className="field" value={multiplier} onChange={(e) => setMultiplier(e.target.value)} placeholder="0.1" />
        <input
          className="field"
          inputMode="decimal"
          value={fixedAccountValue}
          onChange={(e) => setFixedAccountValue(e.target.value)}
          placeholder="Account value used (USDC)"
          required
        />
        <select className="field" value={allowedMode} onChange={(e) => setAllowedMode(e.target.value as AllowedMode)}>
          <option value="ALL_COINS">All coins</option>
          <option value="CUSTOM_LIST">Custom allowlist</option>
        </select>
        <input
          className="field"
          value={allowedSymbols}
          onChange={(e) => setAllowedSymbols(e.target.value)}
          placeholder={allowedMode === "ALL_COINS" ? "All enabled DEX markets" : "canonical coins, e.g. xyz:HYUNDAI"}
          disabled={allowedMode === "ALL_COINS"}
        />
        <input className="field" value={blockedSymbols} onChange={(e) => setBlockedSymbols(e.target.value)} placeholder="blocked coins, optional" />
        <ExecutionAccountSelect
          value={executionAccount}
          options={executionAccounts}
          onChange={setExecutionAccount}
          ariaLabel="Execution account for new leader"
        />
        <select className="field" value={preferredVenue} onChange={(e) => setPreferredVenue(e.target.value)}>
          <option value="HYPERLIQUID">HYPERLIQUID</option>
          <option value="BINANCE">BINANCE</option>
          <option value="AUTO">AUTO</option>
        </select>
        <select className="field" value={fallbackVenue} onChange={(e) => setFallbackVenue(e.target.value)}>
          <option value="NONE">NONE</option>
          <option value="BINANCE">BINANCE</option>
          <option value="HYPERLIQUID">HYPERLIQUID</option>
        </select>
        <input
          className="field"
          inputMode="decimal"
          value={maxPerTrade}
          onChange={(e) => setMaxPerTrade(e.target.value)}
          placeholder="max per-coin position USDC"
        />
        <input
          className="field"
          inputMode="decimal"
          value={maxTotal}
          onChange={(e) => setMaxTotal(e.target.value)}
          placeholder="max total notional USDC"
        />
        <button className="btn btn-primary" type="submit" disabled={busy === "new"}>
          <Plus className="h-4 w-4" />
          Add
        </button>
      </form>

      <div className="space-y-4">
        {leaders.length ? (
          leaders.map((leader) => (
            <LeaderRow
              key={leader.id}
              leader={leader}
              accountState={accountStates[leader.id]}
              edit={edits[leader.id] ?? editFromLeader(leader)}
              executionAccounts={executionAccounts}
              busy={busy === leader.id}
              saved={lastSavedLeaderId === leader.id}
              onEdit={(patch) => setEdits((current) => ({ ...current, [leader.id]: { ...(current[leader.id] ?? editFromLeader(leader)), ...patch } }))}
              onSave={() => saveLeader(leader)}
              onToggle={() => toggleLeader(leader)}
              onDelete={() => deleteLeader(leader)}
            />
          ))
        ) : (
          <section className="panel p-4 text-sm text-slate-500">No leaders configured</section>
        )}
      </div>
    </AppShell>
  );
}

function LeaderRow({
  leader,
  accountState,
  edit,
  executionAccounts,
  busy,
  saved,
  onEdit,
  onSave,
  onToggle,
  onDelete
}: {
  leader: Leader;
  accountState?: LeaderAccountState;
  edit: LeaderEdit;
  executionAccounts: ExecutionAccountOption[];
  busy: boolean;
  saved: boolean;
  onEdit: (patch: Partial<LeaderEdit>) => void;
  onSave: () => void;
  onToggle: () => void;
  onDelete: () => void;
}) {
  const appliedBlockedSymbols = leader.blocked_symbols ?? [];
  const blockedSymbolsHaveUnsavedChanges = !sameSymbolSet(
    appliedBlockedSymbols,
    splitList(edit.blocked_symbols),
  );
  const appliedExecutionAccount = executionAccountForRoute(
    executionAccounts,
    leader.hyperliquid_vault_address ?? "",
  );

  return (
    <section className="panel overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line px-4 py-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-ink">{leaderDisplayLabel(leader.leader_address)}</div>
          <div className="mt-1 break-all font-mono text-xs text-slate-500">{leader.leader_address}</div>
          <div className="mt-1 flex flex-wrap gap-2 text-xs">
            <StatusText label={leader.deleted_at ? "deleted" : leader.enabled ? "enabled" : "disabled"} tone={leader.deleted_at ? "danger" : leader.enabled ? "ok" : "warn"} />
            <StatusText label={`watcher ${leader.watcher_status}`} tone={leader.watcher_status === "active" ? "ok" : leader.watcher_status === "disabled" ? "warn" : "danger"} />
            <StatusText label={appliedExecutionAccount.label} tone={appliedExecutionAccount.account_type === "MAIN" ? "ok" : "warn"} />
            <StatusText label={leader.allowed_coins_mode} tone={leader.allowed_coins_mode === "ALL_COINS" ? "ok" : "warn"} />
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <button className="btn btn-muted" type="button" onClick={onToggle} disabled={busy} title={leader.enabled && !leader.deleted_at ? "Disable" : "Enable"}>
            <Power className="h-4 w-4" />
            {leader.enabled && !leader.deleted_at ? "Disable" : "Enable"}
          </button>
          <Link className="btn btn-muted" href={`/leaders/${leader.id}`}>
            Details
          </Link>
          <button className="btn btn-muted" type="button" onClick={onSave} disabled={busy} title="Save">
            <Save className="h-4 w-4" />
            {busy ? "Saving..." : saved ? "Saved" : "Save"}
          </button>
          <button className="btn btn-danger" type="button" onClick={onDelete} disabled={busy || Boolean(leader.deleted_at)} title="Delete">
            <Trash2 className="h-4 w-4" />
            Delete
          </button>
        </div>
      </div>

      <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
        <Metric label="Last state update" value={formatDate(leader.last_state_update)} />
        <Metric label="Positions loaded" value={String(leader.positions_loaded)} />
        <Metric label="Allocations" value={String(leader.current_allocations_count)} />
        <Metric label="Open notional" value={leader.open_allocations_notional} />
        <Metric label="Ignored existing" value={String(leader.waiting_until_flat_count)} />
        <Metric label="Baseline rows" value={String(leader.baseline_rows_count)} />
        <Metric label="Account value used" value={leader.fixed_account_value ?? "--"} />
        <Metric
          label="Execution account"
          value={`${appliedExecutionAccount.label}${appliedExecutionAccount.account_address ? ` · ${appliedExecutionAccount.account_address}` : ""}`}
        />
        <Metric label="Position notional" value={accountState?.total_ntl_pos ?? "--"} />
        <Metric label="State age" value={formatAge(accountState?.data_age_ms)} />
        <Metric label="Default / xyz positions" value={`${accountState?.dex_states?.find((item) => (item.dex ?? "") === "")?.positions.length ?? 0} / ${accountState?.dex_states?.find((item) => item.dex === "xyz")?.positions.length ?? 0}`} />
      </div>
      {accountState?.error_message ? <div className="border-t border-line px-4 py-3 text-sm text-danger">{accountState.error_message}</div> : null}
      {accountState?.positions.length ? (
        <table className="data-table border-t text-xs">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-2">Coin</th>
              <th className="px-4 py-2">DEX</th>
              <th className="px-4 py-2">Product</th>
              <th className="px-4 py-2">Side</th>
              <th className="px-4 py-2">Size</th>
              <th className="px-4 py-2">Notional</th>
              <th className="px-4 py-2">Entry Price</th>
              <th className="px-4 py-2">Mark Price</th>
              <th className="px-4 py-2">Mid Price</th>
              <th className="px-4 py-2">PnL</th>
              <th className="px-4 py-2">Open Time</th>
              <th className="px-4 py-2">Updated</th>
              <th className="px-4 py-2">Copy</th>
              <th className="px-4 py-2">Copy Status</th>
              <th className="px-4 py-2">Route</th>
            </tr>
          </thead>
          <tbody>
            {accountState.positions.map((position) => (
              <tr key={`${position.dex ?? ""}-${position.canonical_coin ?? position.coin}-${position.side}`} className="border-t border-line">
                <td className="px-4 py-2 font-mono">{position.canonical_coin ?? position.coin}</td>
                <td className="px-4 py-2">{position.dex_display_name ?? position.dex ?? "Hyperliquid"}</td>
                <td className="px-4 py-2">{position.product_type ?? "unknown"}</td>
                <td className="px-4 py-2">{position.side}</td>
                <td className="px-4 py-2">{formatQuantity(position.size)}</td>
                <td className="px-4 py-2">{formatNotional(position.notional)}</td>
                <td className="px-4 py-2">{formatPrice(position.entry_px)}</td>
                <td className={position.mark_price_stale ? "px-4 py-2 text-warn" : "px-4 py-2"}>{formatPrice(position.mark_px)}</td>
                <td className="px-4 py-2">{formatPrice(position.mid_px)}</td>
                <td className="px-4 py-2">{formatNotional(position.unrealized_pnl)}</td>
                <td className="px-4 py-2">{formatOpenTime(position)}</td>
                <td className="px-4 py-2">{formatDate(position.updated_at ?? null)}</td>
                <td className={effectiveCopyable(position) ? "px-4 py-2 text-accent" : "px-4 py-2 text-danger"}>{String(effectiveCopyable(position))}</td>
                <td className="px-4 py-2">
                  <div className={copyStatusTone(effectiveCopyStatus(position)) === "danger" ? "text-danger" : copyStatusTone(effectiveCopyStatus(position)) === "warn" ? "text-warn" : "text-accent"}>{humanLabel(effectiveCopyStatus(position))}</div>
                  {effectiveCopyReason(position) ? <div className="max-w-[220px] truncate text-[11px] text-slate-500">{effectiveCopyReason(position)}</div> : null}
                </td>
                <td className="px-4 py-2">{position.venue_route ?? "--"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}

      <div className="grid gap-3 border-t border-line p-4 text-sm lg:grid-cols-4">
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Enabled</span>
          <select className="field" value={String(edit.enabled)} onChange={(e) => onEdit({ enabled: e.target.value === "true" })} disabled={Boolean(leader.deleted_at)}>
            <option value="true">true</option>
            <option value="false">false</option>
          </select>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Copy multiplier</span>
          <input className="field" value={edit.copy_multiplier} onChange={(e) => onEdit({ copy_multiplier: e.target.value })} />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">Account value used (USDC)</span>
          <input
            className="field"
            inputMode="decimal"
            value={edit.fixed_account_value}
            onChange={(e) => onEdit({ fixed_account_value: e.target.value })}
            required
          />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">preferred_venue</span>
          <select className="field" value={edit.preferred_venue} onChange={(e) => onEdit({ preferred_venue: e.target.value })}>
            <option value="HYPERLIQUID">HYPERLIQUID</option>
            <option value="BINANCE">BINANCE</option>
            <option value="AUTO">AUTO</option>
          </select>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">fallback</span>
          <select className="field" value={edit.fallback_venue} onChange={(e) => onEdit({ fallback_venue: e.target.value })}>
            <option value="NONE">NONE</option>
            <option value="BINANCE">BINANCE</option>
            <option value="HYPERLIQUID">HYPERLIQUID</option>
          </select>
        </label>
        <label className="space-y-1 lg:col-span-2">
          <span className="text-xs text-slate-500">Execution account (disable before changing)</span>
          <ExecutionAccountSelect
            value={edit.hyperliquid_vault_address}
            options={executionAccounts}
            onChange={(value) => onEdit({ hyperliquid_vault_address: value })}
            disabled={leader.enabled && !leader.deleted_at}
            ariaLabel={`Execution account for ${leader.leader_address}`}
          />
          <div className="break-all font-mono text-[11px] text-slate-500">
            {executionAccountForRoute(executionAccounts, edit.hyperliquid_vault_address).account_address ?? "Main account address unavailable"}
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">allowed coins mode</span>
          <select className="field" value={edit.allowed_mode} onChange={(e) => onEdit({ allowed_mode: e.target.value as AllowedMode })}>
            <option value="ALL_COINS">All coins</option>
            <option value="CUSTOM_LIST">Custom allowlist</option>
          </select>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">allowed coins</span>
          <input
            className="field"
            value={edit.allowed_symbols}
            onChange={(e) => onEdit({ allowed_symbols: e.target.value })}
            placeholder={edit.allowed_mode === "ALL_COINS" ? "All enabled DEX markets" : "canonical coins, e.g. xyz:HYUNDAI"}
            disabled={edit.allowed_mode === "ALL_COINS"}
          />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">blocked coins</span>
          <input className="field" value={edit.blocked_symbols} onChange={(e) => onEdit({ blocked_symbols: e.target.value })} placeholder="optional" />
        </label>
        <div className="space-y-1 lg:col-span-4">
          <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
            <span>Currently applied blocked coins ({appliedBlockedSymbols.length})</span>
            <span className={blockedSymbolsHaveUnsavedChanges ? "text-warn" : "text-accent"}>
              {blockedSymbolsHaveUnsavedChanges ? "Unsaved changes" : "Saved and applied"}
            </span>
          </div>
          <div className="min-h-10 rounded-md border border-line bg-panel px-3 py-2">
            {appliedBlockedSymbols.length ? (
              <div className="flex flex-wrap gap-1.5">
                {appliedBlockedSymbols.map((symbol) => (
                  <span key={symbol} className="rounded border border-line bg-surface px-2 py-1 font-mono text-xs text-ink">
                    {symbol}
                  </span>
                ))}
              </div>
            ) : (
              <span className="text-xs text-slate-500">No blocked coins are currently applied.</span>
            )}
          </div>
        </div>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">max per-coin position USDC</span>
          <input
            className="field"
            inputMode="decimal"
            value={edit.max_notional_per_trade}
            onChange={(e) => onEdit({ max_notional_per_trade: e.target.value })}
            placeholder="blank = no cap"
          />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-slate-500">max total notional USDC</span>
          <input
            className="field"
            inputMode="decimal"
            value={edit.max_total_notional}
            onChange={(e) => onEdit({ max_total_notional: e.target.value })}
            placeholder="blank = no cap"
          />
        </label>
        <div className="space-y-1 lg:col-span-2">
          <div className="text-xs text-slate-500">delete status</div>
          <div className="min-h-10 rounded-md border border-line bg-panel px-3 py-2 text-sm text-slate-600">
            {leader.deleted_at ? `${formatDate(leader.deleted_at)} ${leader.delete_reason ?? ""}` : "not deleted"}
          </div>
        </div>
      </div>
    </section>
  );
}

function ExecutionAccountSelect({
  value,
  options,
  onChange,
  disabled = false,
  ariaLabel,
}: {
  value: string;
  options: ExecutionAccountOption[];
  onChange: (value: string) => void;
  disabled?: boolean;
  ariaLabel: string;
}) {
  const normalizedValue = value.trim().toLowerCase();
  const known = options.some((option) => option.route_value === normalizedValue);
  return (
    <select
      className="field font-mono"
      value={normalizedValue}
      onChange={(event) => onChange(event.target.value)}
      disabled={disabled}
      aria-label={ariaLabel}
    >
      {!options.length ? <option value="">Main account</option> : null}
      {options.map((option) => (
        <option key={option.account_type === "MAIN" ? "main" : option.route_value} value={option.route_value}>
          {option.label} · {option.account_address ?? "address unavailable"}
        </option>
      ))}
      {!known && normalizedValue ? (
        <option value={normalizedValue}>Unconfigured route · {normalizedValue}</option>
      ) : null}
    </select>
  );
}

function executionAccountForRoute(
  options: ExecutionAccountOption[],
  route: string,
): ExecutionAccountOption {
  const normalized = route.trim().toLowerCase();
  return options.find((option) => option.route_value === normalized) ?? {
    route_value: normalized,
    account_address: normalized || null,
    account_type: normalized ? "SUBACCOUNT" : "MAIN",
    label: normalized ? `Unconfigured account · ${normalized.slice(-4)}` : "Main account",
    watcher_running: false,
    watcher_ready: false,
    active_leaders: [],
  };
}

function editFromLeader(leader: Leader): LeaderEdit {
  return {
    enabled: leader.enabled,
    // Form values must remain machine-parseable. Display formatting inserts
    // thousands separators (for example 70000 -> "70,000"), which Decimal
    // fields in the API correctly reject when the whole leader card is saved.
    copy_multiplier: numericInputValue(leader.copy_multiplier),
    fixed_account_value: numericInputValue(leader.fixed_account_value),
    allowed_mode: leader.allowed_coins_mode,
    allowed_symbols: (leader.allowed_symbols ?? []).join(","),
    blocked_symbols: (leader.blocked_symbols ?? []).join(","),
    max_notional_per_trade: leader.max_notional_per_trade ?? "",
    max_total_notional: leader.max_total_notional ?? "",
    preferred_venue: leader.preferred_venue,
    fallback_venue: leader.fallback_venue,
    hyperliquid_vault_address: leader.hyperliquid_vault_address ?? ""
  };
}

function numericInputValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return "";
  return decimalInputValue(String(value));
}

function decimalInputValue(value: string): string {
  return value.replaceAll(",", "").trim();
}

function splitList(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function sameSymbolSet(left: string[], right: string[]): boolean {
  const normalize = (values: string[]) =>
    [...new Set(values.map((value) => value.trim().toLowerCase()).filter(Boolean))].sort();
  const normalizedLeft = normalize(left);
  const normalizedRight = normalize(right);
  return normalizedLeft.length === normalizedRight.length
    && normalizedLeft.every((value, index) => value === normalizedRight[index]);
}

function emptyDecimalToNull(value: string): string | null {
  const normalized = decimalInputValue(value);
  return normalized || null;
}

function enabledVenues(preferred: string): string[] {
  return preferred === "AUTO" ? ["HYPERLIQUID", "BINANCE"] : [preferred];
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : "Request failed";
}

function formatDate(value: string | null): string {
  return formatDateTimeLabel(value);
}

function formatAge(value: number | null | undefined): string {
  return formatAgeLabel(value);
}

function formatOpenTime(position: LeaderAccountState["positions"][number]): string {
  return formatOpenTimeLabel(position);
}

function humanLabel(value: string | null | undefined): string {
  if (!value) return "--";
  return value
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function Metric({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="mini-card">
      <div className="mini-label">{label}</div>
      <div className="mini-value">{formatDisplayValue(value)}</div>
    </div>
  );
}

function StatusText({ label, tone }: { label: string; tone: "ok" | "warn" | "danger" }) {
  const color =
    tone === "ok"
      ? "border-teal-200 bg-teal-50 text-accent"
      : tone === "warn"
      ? "border-amber-200 bg-amber-50 text-warn"
      : "border-red-200 bg-red-50 text-danger";
  return <span className={`status-pill ${color}`}>{label}</span>;
}
