"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  ArrowRight,
  Clock3,
  Gauge,
  Layers3,
  Radio,
  RefreshCw,
  ShieldAlert,
  TrendingUp,
  WalletCards,
} from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { formatAge, formatDateTime, formatExactDecimal, formatMs, formatNotional, formatPrice, formatProfitLossMoney, formatProfitLossPercent, profitLossClass } from "@/lib/format";
import { leaderAddressSuffix } from "@/lib/leaderIdentity";

type Position = {
  dex?: string | null;
  canonical_coin?: string | null;
  coin: string;
  side: string;
  size: string | null;
  notional: string | null;
  entry_px: string | null;
  mark_px: string | null;
  unrealized_pnl: string | null;
  return_on_equity?: string | null;
  funding_since_open?: string | null;
  leverage: string | null;
  margin_used?: string | null;
  margin_mode?: string | null;
  liquidation_px?: string | null;
  data_age_ms?: number | null;
  leader_address?: string | null;
  attribution?: "LEADER" | "MANUAL" | "AMBIGUOUS" | null;
};

type FollowerAccount = {
  execution_scope: string;
  account_type: "MAIN" | "SUBACCOUNT";
  account_label: string;
  address: string | null;
  account_value: string | null;
  account_value_used_for_sizing?: string | null;
  available_collateral_used_for_margin_check?: string | null;
  withdrawable: string | null;
  total_margin_used: string | null;
  data_age_ms: number | null;
  stale: boolean;
  error_message: string | null;
  watcher_running: boolean;
  watcher_ready: boolean;
  active_leaders: string[];
  positions: Position[];
};

type LeaderConfig = {
  id: number;
  enabled: boolean;
  deleted_at: string | null;
  leader_address: string;
  copy_multiplier: string;
  fixed_account_value: string | null;
  hyperliquid_vault_address: string | null;
  watcher_status: string;
};

type Dashboard = {
  last_updated_at: string;
  runtime: {
    kill_switch: boolean;
    kill_switch_updated_at: string | null;
    live_opens_enabled: boolean;
    dry_run_or_live: string;
  };
  accounts: FollowerAccount[];
  leaders: LeaderConfig[];
  recent_orders: Array<{
    id: number;
    leader_address: string;
    source_coin: string;
    execution_account?: string | null;
    order_action: string | null;
    status: string;
    avg_fill_price: string | null;
    total_hot_path_ms: number | null;
    ws_to_submit_ms: number | null;
    submit_to_ack_ms: number | null;
    created_at: string | null;
  }>;
  latency: {
    latest_total_hot_path_ms: number | null;
    latest_ws_to_submit_ms: number | null;
    latest_submit_to_ack_ms: number | null;
    last_10_avg_event_to_ack_ms: number | null;
    last_10_max_event_to_ack_ms: number | null;
  };
};

const DASHBOARD_REFRESH_INTERVAL_MS = 5_000;
const DASHBOARD_NAVIGATION_CACHE_MS = 5_000;
let dashboardNavigationCache: { payload: Dashboard; loadedAt: number } | null = null;

export default function DashboardPage() {
  const [data, setData] = useState<Dashboard | null>(() => dashboardNavigationCache?.payload ?? null);
  const [accounts, setAccounts] = useState<FollowerAccount[]>(() => dashboardNavigationCache?.payload.accounts ?? []);
  const [leaders, setLeaders] = useState<LeaderConfig[]>(() => dashboardNavigationCache?.payload.leaders ?? []);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(() => dashboardNavigationCache?.payload.last_updated_at ?? null);
  const refreshRunning = useRef(false);

  async function load() {
    if (refreshRunning.current) return;
    refreshRunning.current = true;
    setLoading(true);
    try {
      const overview = await apiFetch<Dashboard>("/dashboard/overview");
      const activeLeaders = overview.leaders.filter((leader) => leader.enabled && !leader.deleted_at);
      const payload = { ...overview, leaders: activeLeaders };
      dashboardNavigationCache = { payload, loadedAt: Date.now() };
      setData(payload);
      setAccounts(payload.accounts);
      setLeaders(payload.leaders);
      setLastRefreshedAt(payload.last_updated_at);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load dashboard");
    } finally {
      setLoading(false);
      refreshRunning.current = false;
    }
  }

  useEffect(() => {
    const cacheFresh = dashboardNavigationCache
      && Date.now() - dashboardNavigationCache.loadedAt < DASHBOARD_NAVIGATION_CACHE_MS;
    if (!cacheFresh) void load();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void load();
    }, DASHBOARD_REFRESH_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, []);

  const totals = useMemo(() => {
    const positions = accounts.flatMap((account) => account.positions ?? []);
    return {
      accountValue: sum(accounts.map((account) => account.account_value_used_for_sizing ?? account.account_value)),
      exposure: sumAbs(positions.map((position) => position.notional)),
      pnl: sum(positions.map((position) => position.unrealized_pnl)),
      positionCount: positions.length,
      margin: sum(accounts.map((account) => account.total_margin_used)),
    };
  }, [accounts]);

  const live = Boolean(data?.runtime.live_opens_enabled);
  const killed = data?.runtime.kill_switch ?? true;
  const runningAccounts = accounts.filter((account) => account.watcher_running).length;
  const hasFunding = accounts.some((account) => account.positions.some((position) => position.funding_since_open !== null && position.funding_since_open !== undefined));

  return (
    <AppShell>
      <Header
        eyebrow="Live operations"
        title="Command Center"
        subtitle="Main and subaccount execution, leader ownership, positions and latency in one view."
        right={
          <div className="flex items-center gap-2">
            <span className="status-pill border-slate-200 bg-slate-50 text-slate-600">DB snapshot · 5s</span>
            <button className="btn btn-muted" type="button" onClick={() => void load()} disabled={loading}>
              <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </button>
          </div>
        }
      />

      {error ? <Notice tone="danger">{error}</Notice> : null}

      <section className={`mb-5 overflow-hidden rounded-xl border ${killed ? "border-red-200 bg-red-50" : live ? "border-teal-200 bg-teal-50" : "border-amber-200 bg-amber-50"}`}>
        <div className="flex flex-col gap-4 px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-start gap-3">
            <span className={`mt-0.5 grid h-9 w-9 place-items-center rounded-lg ${killed ? "bg-red-100 text-danger" : live ? "bg-teal-100 text-accent" : "bg-amber-100 text-warn"}`}>
              {killed ? <ShieldAlert className="h-5 w-5" /> : <Radio className="h-5 w-5" />}
            </span>
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Execution state</div>
              <div className="mt-1 text-lg font-semibold text-ink">
                {killed ? "Kill switch engaged" : live ? "Live copy enabled" : "New opens unavailable"}
              </div>
              <div className="mt-0.5 text-sm text-slate-600">
                {killed ? "New opens and increases are blocked; reductions remain available." : live ? "Both account routes are accepting eligible leader fills." : "Live opening is currently unavailable; check System & Risk for the applied state."}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-5 text-sm">
            <div>
              <div className="text-xs text-slate-500">Watchers online</div>
              <div className="mt-1 font-semibold text-ink">{runningAccounts}/{accounts.length}</div>
            </div>
            <div>
              <div className="text-xs text-slate-500">Active leaders</div>
              <div className="mt-1 font-semibold text-ink">{leaders.length}</div>
            </div>
            <Link href="/risk" className="btn btn-muted">
              Risk controls <ArrowRight className="h-4 w-4" />
            </Link>
          </div>
        </div>
      </section>

      <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <Metric icon={<WalletCards />} label="Portfolio value" value={money(totals.accountValue)} detail={`${accounts.length} execution accounts`} />
        <Metric icon={<Layers3 />} label="Gross exposure" value={money(totals.exposure)} detail={`${totals.positionCount} open positions`} />
        <Metric icon={<TrendingUp />} label="Unrealized PnL" value={formatProfitLossMoney(totals.pnl)} tone={profitLossTone(totals.pnl)} detail="Across both accounts" />
        <Metric icon={<Gauge />} label="Margin in use" value={money(totals.margin)} detail="Actual reported leverage" />
        <Metric icon={<Clock3 />} label="Local hot path" value={formatMs(data?.latency.latest_ws_to_submit_ms)} tone={(data?.latency.latest_ws_to_submit_ms ?? 0) > 250 ? "danger" : "ok"} detail={`Last 10 max ${formatMs(data?.latency.last_10_max_event_to_ack_ms)}`} />
      </div>

      <section className="panel mb-5 overflow-hidden">
        <div className="flex items-center justify-between border-b border-line px-5 py-4">
          <div>
            <h2 className="section-title">Open positions</h2>
            <p className="section-copy">Current follower exposure, grouped by the account that owns it.</p>
          </div>
          <span className="status-pill border-slate-200 bg-slate-50 text-slate-600">{totals.positionCount} positions</span>
        </div>
        <div className="overflow-x-auto">
          <table className="data-table">
            <thead><tr><th>Account</th><th>Market</th><th>Position Value</th><th>Entry Price</th><th>Mark Price</th><th>PNL (ROE %)</th>{hasFunding ? <th>Funding</th> : null}<th>Liq. Price</th><th>Margin</th></tr></thead>
            <tbody>
              {accounts.flatMap((account) => account.positions.map((position) => {
                const isLong = position.side.toUpperCase() === "LONG";
                const pnl = nullableNumber(position.unrealized_pnl);
                const roePercent = positionRoePercent(position);
                return (
                  <tr key={`${account.execution_scope}:${position.dex ?? ""}:${position.canonical_coin ?? position.coin}:${position.side}`}>
                    <td><PositionAccountLabel account={account} position={position} /></td>
                    <td>
                      <div className="flex items-center gap-2">
                        <span className="text-[13px] font-semibold tracking-[-0.01em] text-ink">{position.canonical_coin ?? position.coin}</span>
                        {position.leverage ? <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-semibold text-slate-500">{formatNotional(position.leverage)}×</span> : null}
                      </div>
                      <div className={`mt-0.5 text-[10px] font-semibold uppercase ${isLong ? "text-accent" : "text-danger"}`}>{isLong ? "Long" : "Short"}</div>
                    </td>
                    <td>{optionalMoney(position.notional, true)}</td>
                    <td>{formatExactDecimal(position.entry_px)}</td>
                    <td>{formatExactDecimal(position.mark_px)}</td>
                    <td>
                      <div className={`font-semibold ${pnl === null ? "text-slate-500" : profitLossClass(pnl)}`}>{formatProfitLossMoney(pnl)}</div>
                      <div className={`mt-0.5 text-xs ${roePercent === null ? "text-slate-400" : profitLossClass(roePercent)}`}>({formatProfitLossPercent(roePercent)})</div>
                    </td>
                    {hasFunding ? <td className={profitLossClass(position.funding_since_open)}>{formatProfitLossMoney(position.funding_since_open)}</td> : null}
                    <td>{formatPrice(position.liquidation_px)}</td>
                    <td>
                      <div>{optionalMoney(position.margin_used, true)}</div>
                      <div className="mt-0.5 text-[10px] font-medium text-slate-400">{position.margin_mode ? human(position.margin_mode) : "--"}</div>
                    </td>
                  </tr>
                );
              }))}
              {!totals.positionCount ? <tr><td colSpan={hasFunding ? 9 : 8} className="py-10 text-center text-slate-500">No open follower positions</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="mb-5">
        <div className="mb-3 flex items-end justify-between gap-3">
          <div>
            <h2 className="section-title">Execution accounts</h2>
            <p className="section-copy">Each route has an independent watcher, positions and first-arrival market ownership.</p>
          </div>
          <span className="text-xs text-slate-500">Updated {formatDateTime(lastRefreshedAt)}</span>
        </div>
        <div className="grid gap-4 xl:grid-cols-2">
          {accounts.map((account) => (
            <AccountCard
              key={account.execution_scope || "main"}
              account={account}
              leaders={leaders.filter((leader) => normalizeRoute(leader.hyperliquid_vault_address) === normalizeRoute(account.execution_scope))}
            />
          ))}
          {!accounts.length ? <div className="panel p-5 text-sm text-slate-500">Follower accounts are loading.</div> : null}
        </div>
      </section>

      <section className="panel overflow-hidden">
          <div className="flex items-center justify-between border-b border-line px-5 py-4">
            <div>
              <h2 className="section-title">Recent execution</h2>
              <p className="section-copy">The most recent copy outcomes and measured latency.</p>
            </div>
            <Activity className="h-4 w-4 text-accent" />
          </div>
          <div className="overflow-x-auto">
            <table className="data-table">
              <thead><tr><th>Time</th><th>Account</th><th>Leader</th><th>Market</th><th>Action</th><th>Status</th><th>Local</th><th>Total</th></tr></thead>
              <tbody>
                {(data?.recent_orders ?? []).slice(0, 10).map((order) => (
                  <tr key={order.id}>
                    <td>{formatDateTime(order.created_at)}</td>
                    <td>{accountName(accounts, order.execution_account ?? "")}</td>
                    <td className="font-mono">{leaderAddressSuffix(order.leader_address)}</td>
                    <td className="font-mono font-medium">{order.source_coin}</td>
                    <td>{human(order.order_action)}</td>
                    <td><Status status={order.status} /></td>
                    <td>{formatMs(order.ws_to_submit_ms)}</td>
                    <td>{formatMs(order.total_hot_path_ms)}</td>
                  </tr>
                ))}
                {!data?.recent_orders?.length ? <tr><td colSpan={8} className="py-10 text-center text-slate-500">No recent copy orders</td></tr> : null}
              </tbody>
            </table>
          </div>
      </section>
    </AppShell>
  );
}

function AccountCard({ account, leaders }: { account: FollowerAccount; leaders: LeaderConfig[] }) {
  const exposure = sumAbs(account.positions.map((position) => position.notional));
  const pnl = sum(account.positions.map((position) => position.unrealized_pnl));
  const healthy = account.watcher_ready && !account.stale && !account.error_message;
  return (
    <article className="panel overflow-hidden">
      <div className="soft-grid border-b border-line bg-slate-50/80 px-5 py-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2"><AccountLabel account={account} /><Status status={healthy ? "READY" : account.watcher_running ? "STALE" : "OFFLINE"} /></div>
            <div className="mt-2 break-all font-mono text-[11px] text-slate-500">{account.address ?? "Address not configured"}</div>
          </div>
          <WalletCards className="h-5 w-5 text-accent" />
        </div>
      </div>
      <div className="grid grid-cols-2 gap-px bg-line sm:grid-cols-4">
        <AccountMetric label="Account value" value={money(number(account.account_value_used_for_sizing ?? account.account_value))} />
        <AccountMetric label="Exposure" value={money(exposure)} />
        <AccountMetric label="Unrealized" value={formatProfitLossMoney(pnl)} tone={profitLossTone(pnl)} />
        <AccountMetric label="State age" value={formatAge(account.data_age_ms)} tone={account.stale ? "danger" : "ok"} />
      </div>
      <div className="px-5 py-4">
        <div className="mb-3 flex items-center justify-between"><span className="text-xs font-semibold text-slate-600">Assigned leaders</span><Link href="/leaders" className="text-xs font-semibold text-accent hover:underline">Manage</Link></div>
        <div className="space-y-2">
          {leaders.map((leader) => (
            <div key={leader.id} className="flex items-center justify-between gap-3 rounded-lg border border-line bg-slate-50/70 px-3 py-2">
              <div className="min-w-0"><div className="font-mono text-xs font-semibold text-ink">{leaderAddressSuffix(leader.leader_address)}</div><div className="truncate font-mono text-[10px] text-slate-400">{leader.leader_address}</div></div>
              <div className="flex shrink-0 gap-4 text-right text-xs"><div><div className="text-slate-400">Multiplier</div><div className="font-semibold text-ink">{leader.copy_multiplier}×</div></div><div><div className="text-slate-400">Fixed balance</div><div className="font-semibold text-ink">{formatNotional(leader.fixed_account_value)}</div></div></div>
            </div>
          ))}
          {!leaders.length ? <div className="rounded-lg border border-dashed border-line p-3 text-xs text-slate-500">No enabled leaders assigned to this route.</div> : null}
        </div>
      </div>
      {account.error_message ? <div className="border-t border-red-100 bg-red-50 px-5 py-3 text-xs text-danger">{account.error_message}</div> : null}
    </article>
  );
}

function Metric({ icon, label, value, detail, tone }: { icon: React.ReactNode; label: string; value: string; detail: string; tone?: "ok" | "danger" | "profit" | "loss" }) {
  return <div className="metric-card"><div className={`mb-3 flex h-8 w-8 items-center justify-center rounded-lg ${tone === "danger" || tone === "loss" ? "bg-red-50 text-danger" : tone === "profit" ? "bg-emerald-50 text-emerald-600" : "bg-teal-50 text-accent"}`}>{icon}</div><div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">{label}</div><div className={`mt-1 text-xl font-semibold tabular-nums ${tone === "danger" || tone === "loss" ? "text-danger" : tone === "profit" ? "text-emerald-600" : tone === "ok" ? "text-accent" : "text-ink"}`}>{value}</div><div className="mt-1 text-xs text-slate-500">{detail}</div></div>;
}

function AccountMetric({ label, value, tone }: { label: string; value: string; tone?: "ok" | "danger" | "profit" | "loss" }) {
  return <div className="bg-white px-4 py-3"><div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">{label}</div><div className={`mt-1 text-sm font-semibold tabular-nums ${tone === "danger" || tone === "loss" ? "text-danger" : tone === "profit" ? "text-emerald-600" : tone === "ok" ? "text-accent" : "text-ink"}`}>{value}</div></div>;
}

function AccountLabel({ account }: { account: Pick<FollowerAccount, "account_type" | "account_label"> }) {
  return <span className="text-xs font-semibold text-ink">{account.account_type === "MAIN" ? "MAIN" : account.account_label}</span>;
}

function PositionAccountLabel({ account, position }: { account: Pick<FollowerAccount, "account_type">; position: Position }) {
  const route = account.account_type === "MAIN" ? "main" : "sub";
  const owner = position.leader_address
    ? leaderAddressSuffix(position.leader_address).toLowerCase()
    : position.attribution === "AMBIGUOUS"
      ? "review"
      : "manual";
  return (
    <span className="inline-flex h-7 items-center rounded-lg border border-slate-200 bg-slate-50 px-2.5 text-[11px] font-semibold tracking-[0.015em] text-slate-700 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
      <span className={route === "main" ? "text-accent" : "text-indigo-600"}>{route}</span>
      <span className="px-0.5 text-slate-300">-</span>
      <span>{owner}</span>
    </span>
  );
}

function Status({ status }: { status: string }) {
  const upper = status.toUpperCase();
  const tone = ["READY", "FILLED", "SUCCESS", "OK", "ACTIVE"].some((item) => upper.includes(item)) ? "border-teal-200 bg-teal-50 text-accent" : ["FAILED", "REJECT", "ERROR", "OFFLINE", "BLOCKED"].some((item) => upper.includes(item)) ? "border-red-200 bg-red-50 text-danger" : "border-amber-200 bg-amber-50 text-warn";
  return <span className={`status-pill ${tone}`}>{human(status)}</span>;
}

function ToneText({ tone, children }: { tone: "ok" | "warn" | "danger"; children: React.ReactNode }) {
  return <span className={tone === "ok" ? "font-medium text-accent" : tone === "danger" ? "font-medium text-danger" : "font-medium text-warn"}>{children}</span>;
}

function Notice({ tone, children }: { tone: "danger"; children: React.ReactNode }) {
  return <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-danger">{children}</div>;
}

function normalizeRoute(value: string | null | undefined): string { return String(value ?? "").trim().toLowerCase(); }
function number(value: string | number | null | undefined): number { const parsed = Number(value ?? 0); return Number.isFinite(parsed) ? parsed : 0; }
function nullableNumber(value: string | number | null | undefined): number | null { if (value === null || value === undefined || value === "") return null; const parsed = Number(value); return Number.isFinite(parsed) ? parsed : null; }
function absNumber(value: string | number | null | undefined): number { return Math.abs(number(value)); }
function sum(values: Array<string | number | null | undefined>): number { return values.reduce<number>((total, value) => total + number(value), 0); }
function sumAbs(values: Array<string | number | null | undefined>): number { return values.reduce<number>((total, value) => total + absNumber(value), 0); }
function money(value: number): string { return `$${formatNotional(value)}`; }
function optionalMoney(value: string | number | null | undefined, absolute = false): string { const parsed = nullableNumber(value); return parsed === null ? "--" : money(absolute ? Math.abs(parsed) : parsed); }
function profitLossTone(value: number | null): "profit" | "loss" | undefined { return value === null || value === 0 ? undefined : value > 0 ? "profit" : "loss"; }
function positionRoePercent(position: Position): number | null { const reported = nullableNumber(position.return_on_equity); if (reported !== null) return reported * 100; const pnl = nullableNumber(position.unrealized_pnl); const margin = nullableNumber(position.margin_used); return pnl === null || margin === null || margin === 0 ? null : (pnl / Math.abs(margin)) * 100; }
function human(value: string | null | undefined): string { return value ? value.toLowerCase().replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()) : "--"; }
function accountName(accounts: FollowerAccount[], scope: string): string { return accounts.find((account) => normalizeRoute(account.execution_scope) === normalizeRoute(scope))?.account_type === "MAIN" ? "MAIN" : accounts.find((account) => normalizeRoute(account.execution_scope) === normalizeRoute(scope))?.account_label ?? (scope ? `SUB · ${scope.slice(-4)}` : "MAIN"); }
