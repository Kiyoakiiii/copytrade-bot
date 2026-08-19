"use client";

import { useEffect, useState } from "react";
import { Power, RefreshCw, SlidersHorizontal } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { formatDateTime, formatNotional } from "@/lib/format";

type Risk = {
  kill_switch: boolean;
  kill_switch_updated_at: string | null;
  live_opens_enabled: boolean;
  live_status: string;
  live_status_reason: string;
  trading_enabled_env: boolean;
  hyperliquid_trading_enabled_env: boolean;
  global_max_daily_loss: string | number;
  global_max_total_notional: string | number;
  account_value_mode: string;
  low_latency_required_for_live: boolean;
  order_policy: string;
  sizing_policy: string;
};

type ExecutionAccount = {
  route_value: string;
  account_address: string | null;
  account_type: "MAIN" | "SUBACCOUNT";
  label: string;
  watcher_running: boolean;
  watcher_ready: boolean;
  active_leaders: string[];
};

export default function RiskPage() {
  const [risk, setRisk] = useState<Risk | null>(null);
  const [accounts, setAccounts] = useState<ExecutionAccount[]>([]);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);

  async function load() {
    try {
      const [riskPayload, accountPayload] = await Promise.all([
        apiFetch<Risk>("/risk"),
        apiFetch<ExecutionAccount[]>("/leaders/execution-accounts"),
      ]);
      setRisk(riskPayload); setAccounts(accountPayload); setMessage("");
    } catch (err) { setMessage(err instanceof Error ? err.message : "Unable to load settings"); }
  }
  useEffect(() => { void load(); }, []);

  async function changeKillSwitch(next: boolean) {
    const prompt = next ? "Turn the kill switch ON now? New opens and increases will stop." : "Turn the kill switch OFF and allow eligible live opens?";
    if (!window.confirm(prompt)) return;
    setBusy(true); setMessage("");
    try {
      setRisk(next ? await apiFetch<Risk>("/kill-switch", { method: "POST", body: "{}" }) : await apiFetch<Risk>("/risk", { method: "PATCH", body: JSON.stringify({ kill_switch: false }) }));
      await load();
    } catch (err) { setMessage(err instanceof Error ? err.message : "Control update failed"); } finally { setBusy(false); }
  }

  const killed = risk?.kill_switch ?? true;
  const live = Boolean(risk?.live_opens_enabled);
  return (
    <AppShell>
      <Header eyebrow="Safety and policy" title="System & Risk" subtitle="Emergency copy control, execution routes and the policies currently applied by the bot." right={<button className="btn btn-muted" type="button" onClick={() => void load()}><RefreshCw className="h-4 w-4" />Refresh</button>} />
      {message ? <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-danger">{message}</div> : null}

      <section className={`mb-5 overflow-hidden rounded-xl border ${killed ? "border-red-200 bg-red-50" : live ? "border-teal-200 bg-teal-50" : "border-amber-200 bg-amber-50"}`}>
        <div className="flex flex-col gap-5 px-6 py-6 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-start gap-4"><span className={`grid h-12 w-12 shrink-0 place-items-center rounded-xl ${killed ? "bg-red-100 text-danger" : "bg-teal-100 text-accent"}`}><Power className="h-6 w-6" /></span><div><div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Global copy control</div><div className="mt-1 text-2xl font-semibold tracking-tight text-ink">{killed ? "Kill switch engaged" : live ? "Live opening enabled" : "Live opening blocked"}</div><div className="mt-1 max-w-2xl text-sm leading-6 text-slate-600">{risk?.live_status_reason ?? "Loading the current runtime state."}</div><div className="mt-2 text-xs text-slate-500">Last changed {formatDateTime(risk?.kill_switch_updated_at)}</div></div></div>
          <button className={`btn ${killed ? "btn-primary" : "btn-danger"} min-w-[190px]`} type="button" onClick={() => void changeKillSwitch(!killed)} disabled={busy}>{busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Power className="h-4 w-4" />}{killed ? "Resume copy trading" : "Stop copy trading"}</button>
        </div>
      </section>

      <div className="mb-5 grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
        <section className="panel overflow-hidden">
          <div className="border-b border-line px-5 py-4"><h2 className="section-title">Execution routes</h2><p className="section-copy">Main and subaccount copy processes remain isolated from one another.</p></div>
          <div className="divide-y divide-line">
            {accounts.map((account) => <div key={account.route_value || "main"} className="flex flex-col gap-3 px-5 py-4 sm:flex-row sm:items-center sm:justify-between"><div><div className="flex items-center gap-2 text-sm font-semibold text-ink">{account.account_type === "MAIN" ? "Main account" : `Subaccount · ${account.account_address?.slice(-4)}`}<Status ok={account.watcher_ready} label={account.watcher_ready ? "Online" : account.watcher_running ? "Starting" : "Offline"} /></div><div className="mt-1 break-all font-mono text-[10px] text-slate-400">{account.account_address ?? "Not configured"}</div></div><div className="flex gap-6 text-right text-xs"><div><div className="text-slate-400">Watcher</div><div className="mt-1 font-semibold text-ink">{account.watcher_running ? "Running" : "Stopped"}</div></div><div><div className="text-slate-400">Leaders</div><div className="mt-1 font-semibold text-ink">{account.active_leaders.length}</div></div></div></div>)}
          </div>
        </section>

        <section className="panel p-5">
          <div className="flex items-center gap-2"><SlidersHorizontal className="h-4 w-4 text-accent" /><h2 className="section-title">Applied policy</h2></div>
          <dl className="mt-4 grid grid-cols-[1fr_auto] gap-x-5 gap-y-3 text-sm">
            <Policy label="Sizing" value={human(risk?.sizing_policy)} />
            <Policy label="Order policy" value={human(risk?.order_policy)} />
            <Policy label="Account value mode" value={human(risk?.account_value_mode)} />
            <Policy label="Global daily loss guard" value={`$${formatNotional(risk?.global_max_daily_loss)}`} />
            <Policy label="Global notional guard" value={`$${formatNotional(risk?.global_max_total_notional)}`} />
            <Policy label="Low latency required" value={risk?.low_latency_required_for_live ? "Yes" : "No"} />
            <Policy label="Trading environment" value={risk?.trading_enabled_env ? "Enabled" : "Disabled"} />
            <Policy label="Hyperliquid execution" value={risk?.hyperliquid_trading_enabled_env ? "Enabled" : "Disabled"} />
          </dl>
        </section>
      </div>

    </AppShell>
  );
}

function Status({ ok, label }: { ok: boolean; label: string }) { return <span className={`status-pill ${ok ? "border-teal-200 bg-teal-50 text-accent" : "border-amber-200 bg-amber-50 text-warn"}`}>{label}</span>; }
function Policy({ label, value }: { label: string; value: string }) { return <><dt className="text-slate-500">{label}</dt><dd className="text-right font-medium text-ink">{value}</dd></>; }
function human(value: string | null | undefined): string { return value ? value.toLowerCase().replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()) : "--"; }
