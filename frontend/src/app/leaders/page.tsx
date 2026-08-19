"use client";

import Link from "next/link";
import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, Plus, Power, RefreshCw, Save, Settings2, Trash2, UsersRound, WalletCards, X } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { LeaderPerformanceOverviewPanel, type LeaderPerformanceOverview } from "@/components/LeaderPerformance";
import { ResearchLab } from "@/components/ResearchLab";
import { apiFetch } from "@/lib/api";
import { formatAge, formatDateTime, formatNotional } from "@/lib/format";
import { leaderAddressSuffix } from "@/lib/leaderIdentity";

type AllowedMode = "ALL_COINS" | "CUSTOM_LIST";

type ExecutionAccount = {
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
  hyperliquid_vault_address: string | null;
  watcher_status: string;
  last_state_update: string | null;
  positions_loaded: boolean;
  current_allocations_count: number;
  open_allocations_notional: string;
  waiting_until_flat_count: number;
};

type LeaderState = {
  leader: { id: number };
  account_value: string | null;
  total_ntl_pos: string | null;
  data_age_ms: number | null;
  stale: boolean;
  error_message: string | null;
  position_count?: number;
  positions: Array<{ coin: string; canonical_coin?: string | null; side: string; notional: string | null }>;
};

type Edit = {
  copy_multiplier: string;
  fixed_account_value: string;
  route: string;
  allowed_mode: AllowedMode;
  allowed_symbols: string;
  blocked_symbols: string;
  max_notional_per_trade: string;
  max_total_notional: string;
};

type NewLeader = Edit & { address: string };
const EMPTY_NEW: NewLeader = { address: "", copy_multiplier: "1", fixed_account_value: "", route: "", allowed_mode: "ALL_COINS", allowed_symbols: "", blocked_symbols: "", max_notional_per_trade: "", max_total_notional: "" };
const LEADER_OVERVIEW_REFRESH_INTERVAL_MS = 60_000;

export default function LeadersPage() {
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [accounts, setAccounts] = useState<ExecutionAccount[]>([]);
  const [states, setStates] = useState<Record<number, LeaderState>>({});
  const [edits, setEdits] = useState<Record<number, Edit>>({});
  const [performance, setPerformance] = useState<LeaderPerformanceOverview | null>(null);
  const [newLeader, setNewLeader] = useState<NewLeader>(EMPTY_NEW);
  const [showAdd, setShowAdd] = useState(false);
  const [showDeleted, setShowDeleted] = useState(false);
  const [accountFilter, setAccountFilter] = useState("ALL");
  const [busy, setBusy] = useState<number | "new" | null>(null);
  const [saved, setSaved] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [lastRefreshed, setLastRefreshed] = useState<string | null>(null);
  const refreshRunning = useRef(false);

  async function load(options: { preserveEdits?: boolean; performance?: boolean } = {}) {
    if (refreshRunning.current) return;
    refreshRunning.current = true;
    try {
      const [leaderRows, accountRows, stateRows, performancePayload] = await Promise.all([
        apiFetch<Leader[]>(`/leaders?include_deleted=${showDeleted ? "true" : "false"}`),
        apiFetch<ExecutionAccount[]>("/leaders/execution-accounts"),
        apiFetch<LeaderState[]>("/account-states/leaders?compact=true").catch(() => []),
        options.performance ? apiFetch<LeaderPerformanceOverview>("/leaders/performance").catch(() => null) : Promise.resolve(undefined),
      ]);
      setLeaders(leaderRows);
      setAccounts(accountRows);
      setStates(Object.fromEntries(stateRows.map((state) => [state.leader.id, state])));
      setEdits((current) => Object.fromEntries(leaderRows.map((leader) => [leader.id, options.preserveEdits && current[leader.id] ? current[leader.id] : editFrom(leader)])));
      if (performancePayload !== undefined) setPerformance(performancePayload);
      setLastRefreshed(new Date().toISOString());
      setError("");
    } catch (err) {
      setError(message(err));
    } finally {
      refreshRunning.current = false;
    }
  }

  useEffect(() => {
    void load({ performance: true });
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void load({ preserveEdits: true, performance: true });
    }, LEADER_OVERVIEW_REFRESH_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [showDeleted]);

  async function addLeader(event: FormEvent) {
    event.preventDefault();
    setBusy("new"); setError("");
    try {
      await apiFetch("/leaders", { method: "POST", body: JSON.stringify(payloadFromNew(newLeader)) });
      setNewLeader(EMPTY_NEW); setShowAdd(false);
      await load({ performance: true });
    } catch (err) { setError(message(err)); } finally { setBusy(null); }
  }

  async function saveLeader(leader: Leader) {
    const edit = edits[leader.id]; if (!edit) return;
    setBusy(leader.id); setError("");
    try {
      const row = await apiFetch<Leader>(`/leaders/${leader.id}`, { method: "PATCH", body: JSON.stringify(payloadFromEdit(edit)) });
      setLeaders((current) => current.map((item) => item.id === row.id ? row : item));
      setEdits((current) => ({ ...current, [row.id]: editFrom(row) }));
      setSaved(row.id); window.setTimeout(() => setSaved((value) => value === row.id ? null : value), 2_500);
    } catch (err) { setError(message(err)); } finally { setBusy(null); }
  }

  async function toggle(leader: Leader) {
    setBusy(leader.id); setError("");
    try { await apiFetch(`/leaders/${leader.id}/${leader.enabled && !leader.deleted_at ? "disable" : "enable"}`, { method: "POST", body: "{}" }); await load(); }
    catch (err) { setError(message(err)); } finally { setBusy(null); }
  }

  async function remove(leader: Leader) {
    if (!window.confirm("Delete this leader? Existing copied positions are not closed automatically.")) return;
    setBusy(leader.id); setError("");
    try { await apiFetch(`/leaders/${leader.id}`, { method: "DELETE", body: JSON.stringify({ delete_reason: "deleted from Leader Desk" }) }); await load({ performance: true }); }
    catch (err) { setError(message(err)); } finally { setBusy(null); }
  }

  const visible = useMemo(() => leaders.filter((leader) => accountFilter === "ALL" || route(leader.hyperliquid_vault_address) === accountFilter), [leaders, accountFilter]);
  const enabled = leaders.filter((leader) => leader.enabled && !leader.deleted_at).length;

  return (
    <AppShell>
      <Header eyebrow="Configuration" title="Leader Desk" subtitle="Assign leaders to an execution account and change sizing without digging through diagnostic pages." right={<><span className="status-pill border-slate-200 bg-slate-50 text-slate-600">DB snapshot · 60s</span><button className="btn btn-muted" type="button" onClick={() => void load({ performance: true })}><RefreshCw className="h-4 w-4" />Refresh</button><button className="btn btn-primary" type="button" onClick={() => setShowAdd(true)}><Plus className="h-4 w-4" />Add leader</button></>} />
      {error ? <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-danger">{error}</div> : null}

      <div className="mb-6"><LeaderPerformanceOverviewPanel data={performance} /></div>

      <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Summary icon={<UsersRound />} label="Enabled leaders" value={String(enabled)} detail={`${leaders.length} visible configs`} />
        <Summary icon={<WalletCards />} label="Execution routes" value={String(accounts.length)} detail={`${accounts.filter((account) => account.watcher_running).length} watchers online`} />
        <Summary icon={<Settings2 />} label="Sizing model" value="Account ratio" detail="Increases and reductions follow position ratio" />
        <Summary icon={<RefreshCw />} label="Last synchronized" value={formatDateTime(lastRefreshed)} detail="Read-only database snapshot" />
      </div>

      <section className="panel mb-5 overflow-hidden">
        <div className="flex flex-col gap-3 border-b border-line px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
          <div><h2 className="section-title">Account assignment</h2><p className="section-copy">Main and subaccount watchers remain completely isolated.</p></div>
          <div className="flex flex-wrap gap-2"><FilterButton active={accountFilter === "ALL"} onClick={() => setAccountFilter("ALL")}>All · {leaders.length}</FilterButton>{accounts.map((account) => <FilterButton key={account.route_value || "main"} active={accountFilter === route(account.route_value)} onClick={() => setAccountFilter(route(account.route_value))}>{account.account_type === "MAIN" ? "Main" : `Sub · ${account.account_address?.slice(-4)}`} · {leaders.filter((leader) => route(leader.hyperliquid_vault_address) === route(account.route_value)).length}</FilterButton>)}</div>
        </div>
        <div className="grid gap-px bg-line lg:grid-cols-2">
          {accounts.map((account) => <AccountStrip key={account.route_value || "main"} account={account} leaders={leaders.filter((leader) => route(leader.hyperliquid_vault_address) === route(account.route_value))} />)}
        </div>
      </section>

      {showAdd ? <AddLeaderPanel value={newLeader} accounts={accounts} busy={busy === "new"} onChange={(patch) => setNewLeader((current) => ({ ...current, ...patch }))} onClose={() => setShowAdd(false)} onSubmit={addLeader} /> : null}

      <div className="mb-6 space-y-3">
        <div className="flex items-center justify-between"><div><h2 className="section-title">Leader configuration</h2><p className="section-copy">Multiplier and fixed balance are always visible; less common controls stay in Advanced.</p></div><label className="flex items-center gap-2 text-xs text-slate-500"><input type="checkbox" checked={showDeleted} onChange={(event) => setShowDeleted(event.target.checked)} />Show deleted</label></div>
        {visible.map((leader) => <LeaderCard key={leader.id} leader={leader} account={accountFor(accounts, leader.hyperliquid_vault_address)} state={states[leader.id]} edit={edits[leader.id] ?? editFrom(leader)} busy={busy === leader.id} saved={saved === leader.id} accounts={accounts} onEdit={(patch) => setEdits((current) => ({ ...current, [leader.id]: { ...(current[leader.id] ?? editFrom(leader)), ...patch } }))} onSave={() => void saveLeader(leader)} onToggle={() => void toggle(leader)} onDelete={() => void remove(leader)} />)}
        {!visible.length ? <div className="panel p-8 text-center text-sm text-slate-500">No leaders in this account filter.</div> : null}
      </div>

      <ResearchLab />
    </AppShell>
  );
}

function LeaderCard({ leader, account, state, edit, busy, saved, accounts, onEdit, onSave, onToggle, onDelete }: { leader: Leader; account: ExecutionAccount; state?: LeaderState; edit: Edit; busy: boolean; saved: boolean; accounts: ExecutionAccount[]; onEdit: (patch: Partial<Edit>) => void; onSave: () => void; onToggle: () => void; onDelete: () => void }) {
  const dirty = JSON.stringify(edit) !== JSON.stringify(editFrom(leader));
  const active = leader.enabled && !leader.deleted_at;
  return <article className={`panel overflow-hidden ${!active ? "opacity-80" : ""}`}>
    <div className="flex flex-col gap-4 px-5 py-4 xl:flex-row xl:items-center xl:justify-between">
      <div className="min-w-0 xl:w-[32%]"><div className="flex flex-wrap items-center gap-2"><span className="font-mono text-base font-semibold text-ink">{leaderAddressSuffix(leader.leader_address)}</span><Status value={leader.deleted_at ? "DELETED" : leader.enabled ? "ENABLED" : "DISABLED"} /><Status value={leader.watcher_status} /></div><div className="mt-1 break-all font-mono text-[11px] text-slate-400">{leader.leader_address}</div><div className="mt-2 flex items-center gap-2 text-xs text-slate-500"><WalletCards className="h-3.5 w-3.5" />{account.label}<span>·</span><span>{state?.positions?.length ?? 0} positions</span><span>·</span><span className={state?.stale ? "text-warn" : "text-accent"}>{formatAge(state?.data_age_ms)}</span></div></div>
      <div className="grid flex-1 gap-3 sm:grid-cols-2 xl:max-w-[560px]">
        <label className="space-y-1"><span className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Multiplier</span><div className="relative"><input className="field pr-8 font-semibold" inputMode="decimal" value={edit.copy_multiplier} onChange={(event) => onEdit({ copy_multiplier: event.target.value })} /><span className="absolute right-3 top-2.5 text-sm text-slate-400">×</span></div></label>
        <label className="space-y-1"><span className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Fixed leader balance</span><input className="field font-semibold" inputMode="decimal" value={edit.fixed_account_value} onChange={(event) => onEdit({ fixed_account_value: event.target.value })} /></label>
      </div>
      <div className="flex shrink-0 flex-wrap gap-2"><button className="btn btn-muted" type="button" onClick={onToggle} disabled={busy}><Power className="h-4 w-4" />{active ? "Disable" : "Enable"}</button><button className={`btn ${dirty ? "btn-primary" : "btn-muted"}`} type="button" onClick={onSave} disabled={busy || Boolean(leader.deleted_at)}><Save className="h-4 w-4" />{busy ? "Saving" : saved ? "Saved" : dirty ? "Save changes" : "Saved"}</button></div>
    </div>
    <details className="group border-t border-line">
      <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-3 text-xs font-semibold text-slate-600 hover:bg-slate-50"><span className="flex items-center gap-2"><Settings2 className="h-4 w-4" />Advanced routing and market controls</span><ChevronDown className="h-4 w-4 transition group-open:rotate-180" /></summary>
      <div className="grid gap-3 border-t border-line bg-slate-50/60 p-5 md:grid-cols-2 xl:grid-cols-4">
        <Field label="Execution account"><AccountSelect value={edit.route} accounts={accounts} disabled={active} onChange={(value) => onEdit({ route: value })} /></Field>
        <Field label="Allowed coins"><select className="field" value={edit.allowed_mode} onChange={(event) => onEdit({ allowed_mode: event.target.value as AllowedMode })}><option value="ALL_COINS">All markets</option><option value="CUSTOM_LIST">Custom allowlist</option></select></Field>
        <Field label="Allowed symbols"><input className="field" value={edit.allowed_symbols} disabled={edit.allowed_mode === "ALL_COINS"} onChange={(event) => onEdit({ allowed_symbols: event.target.value })} placeholder="BTC,ETH,xyz:…" /></Field>
        <Field label="Blocked symbols"><input className="field" value={edit.blocked_symbols} onChange={(event) => onEdit({ blocked_symbols: event.target.value })} placeholder="ACE,HMSTR,…" /></Field>
        <Field label="Max per-coin position"><input className="field" inputMode="decimal" value={edit.max_notional_per_trade} onChange={(event) => onEdit({ max_notional_per_trade: event.target.value })} placeholder="No cap" /></Field>
        <Field label="Max total notional"><input className="field" inputMode="decimal" value={edit.max_total_notional} onChange={(event) => onEdit({ max_total_notional: event.target.value })} placeholder="No cap" /></Field>
        <div className="md:col-span-2"><div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Applied blocked coins · {leader.blocked_symbols.length}</div><div className="mt-1.5 flex min-h-10 flex-wrap gap-1.5 rounded-lg border border-line bg-white p-2">{leader.blocked_symbols.length ? leader.blocked_symbols.map((coin) => <span key={coin} className="rounded-md border border-line bg-slate-50 px-2 py-1 font-mono text-[11px] text-slate-600">{coin}</span>) : <span className="p-1 text-xs text-slate-400">None</span>}</div></div>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-line px-5 py-3"><div className="flex gap-5 text-xs text-slate-500"><span>Leader equity <strong className="text-ink">${formatNotional(state?.account_value)}</strong></span><span>Open allocation <strong className="text-ink">${formatNotional(leader.open_allocations_notional)}</strong></span><span>Waiting flat <strong className="text-ink">{leader.waiting_until_flat_count}</strong></span></div><div className="flex gap-2"><Link className="btn btn-muted" href={`/leaders/${leader.id}`}>Full details</Link><button className="btn btn-danger" type="button" onClick={onDelete} disabled={busy || Boolean(leader.deleted_at)}><Trash2 className="h-4 w-4" />Delete</button></div></div>
    </details>
  </article>;
}

function AddLeaderPanel({ value, accounts, busy, onChange, onClose, onSubmit }: { value: NewLeader; accounts: ExecutionAccount[]; busy: boolean; onChange: (patch: Partial<NewLeader>) => void; onClose: () => void; onSubmit: (event: FormEvent) => void }) {
  return <section className="panel mb-5 overflow-hidden border-teal-200"><div className="flex items-center justify-between border-b border-line bg-teal-50 px-5 py-4"><div><h2 className="section-title">Add a leader</h2><p className="section-copy">The watcher captures a baseline before the leader becomes active.</p></div><button type="button" onClick={onClose} className="grid h-8 w-8 place-items-center rounded-lg hover:bg-white"><X className="h-4 w-4" /></button></div><form onSubmit={onSubmit} className="p-5"><div className="grid gap-3 lg:grid-cols-[1.6fr_0.65fr_0.9fr_1fr]"><Field label="Public address"><input className="field font-mono" value={value.address} onChange={(event) => onChange({ address: event.target.value })} placeholder="0x…" pattern="0x[0-9a-fA-F]{40}" required /></Field><Field label="Multiplier"><input className="field" inputMode="decimal" value={value.copy_multiplier} onChange={(event) => onChange({ copy_multiplier: event.target.value })} required /></Field><Field label="Fixed balance"><input className="field" inputMode="decimal" value={value.fixed_account_value} onChange={(event) => onChange({ fixed_account_value: event.target.value })} placeholder="USDC" required /></Field><Field label="Execution account"><AccountSelect value={value.route} accounts={accounts} onChange={(route) => onChange({ route })} /></Field></div><details className="group mt-4 rounded-lg border border-line"><summary className="flex cursor-pointer list-none items-center justify-between px-4 py-3 text-xs font-semibold text-slate-600">Optional market controls<ChevronDown className="h-4 w-4 transition group-open:rotate-180" /></summary><div className="grid gap-3 border-t border-line bg-slate-50 p-4 md:grid-cols-2 xl:grid-cols-4"><Field label="Allowed mode"><select className="field" value={value.allowed_mode} onChange={(event) => onChange({ allowed_mode: event.target.value as AllowedMode })}><option value="ALL_COINS">All markets</option><option value="CUSTOM_LIST">Custom allowlist</option></select></Field><Field label="Allowed symbols"><input className="field" value={value.allowed_symbols} disabled={value.allowed_mode === "ALL_COINS"} onChange={(event) => onChange({ allowed_symbols: event.target.value })} /></Field><Field label="Blocked symbols"><input className="field" value={value.blocked_symbols} onChange={(event) => onChange({ blocked_symbols: event.target.value })} /></Field><Field label="Max per coin"><input className="field" value={value.max_notional_per_trade} onChange={(event) => onChange({ max_notional_per_trade: event.target.value })} /></Field></div></details><div className="mt-4 flex justify-end gap-2"><button className="btn btn-muted" type="button" onClick={onClose}>Cancel</button><button className="btn btn-primary" type="submit" disabled={busy}>{busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}{busy ? "Adding" : "Add and enable"}</button></div></form></section>;
}

function AccountStrip({ account, leaders }: { account: ExecutionAccount; leaders: Leader[] }) { return <div className="bg-white px-5 py-4"><div className="flex items-start justify-between"><div><div className="flex items-center gap-2 text-sm font-semibold text-ink">{account.account_type === "MAIN" ? "Main account" : `Subaccount · ${account.account_address?.slice(-4)}`}<Status value={account.watcher_ready ? "ONLINE" : account.watcher_running ? "STARTING" : "OFFLINE"} /></div><div className="mt-1 break-all font-mono text-[10px] text-slate-400">{account.account_address ?? "Not configured"}</div></div><WalletCards className="h-4 w-4 text-accent" /></div><div className="mt-3 flex flex-wrap gap-1.5">{leaders.map((leader) => <span key={leader.id} className="rounded-md border border-line bg-slate-50 px-2 py-1 font-mono text-[11px] font-semibold text-slate-600">{leaderAddressSuffix(leader.leader_address)}</span>)}{!leaders.length ? <span className="text-xs text-slate-400">No leaders assigned</span> : null}</div></div>; }
function Summary({ icon, label, value, detail }: { icon: React.ReactNode; label: string; value: string; detail: string }) { return <div className="metric-card"><div className="mb-3 flex h-8 w-8 items-center justify-center rounded-lg bg-teal-50 text-accent">{icon}</div><div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">{label}</div><div className="mt-1 text-lg font-semibold text-ink">{value}</div><div className="mt-1 text-xs text-slate-500">{detail}</div></div>; }
function Status({ value }: { value: string }) { const upper = value.toUpperCase(); const cls = ["READY", "ONLINE", "ENABLED", "ACTIVE"].some((item) => upper.includes(item)) ? "border-teal-200 bg-teal-50 text-accent" : ["DELETED", "OFFLINE", "ERROR", "NOT_SUBSCRIBED"].some((item) => upper.includes(item)) ? "border-red-200 bg-red-50 text-danger" : "border-amber-200 bg-amber-50 text-warn"; return <span className={`status-pill ${cls}`}>{human(value)}</span>; }
function FilterButton({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) { return <button type="button" onClick={onClick} className={`h-8 rounded-lg border px-3 text-xs font-semibold transition ${active ? "border-accent bg-teal-50 text-accent" : "border-line bg-white text-slate-500 hover:border-slate-300"}`}>{children}</button>; }
function Field({ label, children }: { label: string; children: React.ReactNode }) { return <label className="space-y-1.5"><span className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">{label}</span>{children}</label>; }
function AccountSelect({ value, accounts, onChange, disabled = false }: { value: string; accounts: ExecutionAccount[]; onChange: (value: string) => void; disabled?: boolean }) { return <select className="field" value={route(value)} onChange={(event) => onChange(event.target.value)} disabled={disabled}>{accounts.map((account) => <option key={account.route_value || "main"} value={route(account.route_value)}>{account.label} · {account.account_address ?? "unavailable"}</option>)}</select>; }
function editFrom(leader: Leader): Edit { return { copy_multiplier: clean(leader.copy_multiplier), fixed_account_value: clean(leader.fixed_account_value), route: route(leader.hyperliquid_vault_address), allowed_mode: leader.allowed_coins_mode, allowed_symbols: (leader.allowed_symbols ?? []).join(","), blocked_symbols: (leader.blocked_symbols ?? []).join(","), max_notional_per_trade: clean(leader.max_notional_per_trade), max_total_notional: clean(leader.max_total_notional) }; }
function payloadFromEdit(edit: Edit) { return { copy_multiplier: clean(edit.copy_multiplier), fixed_account_value: clean(edit.fixed_account_value), hyperliquid_vault_address: route(edit.route) || null, allowed_symbols: edit.allowed_mode === "ALL_COINS" ? null : list(edit.allowed_symbols), blocked_symbols: list(edit.blocked_symbols), preferred_venue: "HYPERLIQUID", fallback_venue: "NONE", enabled_venues: ["HYPERLIQUID"], max_notional_per_trade: nullable(edit.max_notional_per_trade), max_total_notional: nullable(edit.max_total_notional) }; }
function payloadFromNew(value: NewLeader) { return { leader_address: value.address.trim(), enabled: true, ...payloadFromEdit(value) }; }
function accountFor(accounts: ExecutionAccount[], value: string | null | undefined): ExecutionAccount { return accounts.find((account) => route(account.route_value) === route(value)) ?? { route_value: route(value), account_address: value || null, account_type: value ? "SUBACCOUNT" : "MAIN", label: value ? `Unconfigured · ${value.slice(-4)}` : "Main account", watcher_running: false, watcher_ready: false, active_leaders: [] }; }
function route(value: string | null | undefined): string { return String(value ?? "").trim().toLowerCase(); }
function clean(value: string | number | null | undefined): string { return String(value ?? "").replaceAll(",", "").trim(); }
function nullable(value: string): string | null { return clean(value) || null; }
function list(value: string): string[] { return value.split(/[,\s]+/).map((item) => item.trim()).filter(Boolean); }
function message(error: unknown): string { return error instanceof Error ? error.message : "Request failed"; }
function human(value: string): string { return value.toLowerCase().replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()); }
