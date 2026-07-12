"use client";

import { useEffect, useState } from "react";
import { Power, ShieldAlert } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { formatDateTime } from "@/lib/format";

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

export default function RiskPage() {
  const [risk, setRisk] = useState<Risk | null>(null);
  const [message, setMessage] = useState("");

  async function load() {
    setRisk(await apiFetch<Risk>("/risk"));
  }

  useEffect(() => {
    load().catch((err) => setMessage(err.message));
  }, []);

  async function kill() {
    const confirmed = window.confirm("Turn kill switch ON now? This stops new live auto-copy opens.");
    if (!confirmed) return;
    setMessage("");
    try {
      setRisk(await apiFetch<Risk>("/kill-switch", { method: "POST", body: "{}" }));
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed");
    }
  }

  async function enableTrading() {
    const confirmed = window.confirm("Close kill switch? Only do this after Preflight and Final Live Check are clean.");
    if (!confirmed) return;
    setMessage("");
    try {
      setRisk(
        await apiFetch<Risk>("/risk", {
          method: "PATCH",
          body: JSON.stringify({ kill_switch: false })
        })
      );
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed");
    }
  }

  const killSwitchOn = risk?.kill_switch ?? true;
  const liveOpensEnabled = Boolean(risk?.live_opens_enabled);
  const statusLabel = risk
    ? killSwitchOn
      ? "KILL SWITCH ON"
      : liveOpensEnabled
        ? "LIVE OPENS ENABLED"
        : "LIVE OPENS NOT ENABLED"
    : "CHECKING LIVE STATUS";
  const statusDetail = risk
    ? killSwitchOn
      ? "New live opens and increases are blocked. Reduce and close intents can still run."
      : liveOpensEnabled
        ? "New live Hyperliquid opens and increases are allowed."
        : risk.live_status_reason
    : "Loading current risk state.";
  const statusClass = !risk
    ? "border-amber-200 bg-amber-50 text-warn"
    : killSwitchOn
      ? "border-red-200 bg-red-50 text-danger"
      : liveOpensEnabled
        ? "border-green-200 bg-green-50 text-accent"
        : "border-amber-200 bg-amber-50 text-warn";

  return (
    <AppShell>
      <Header title="Settings / Risk" />
      {message ? <div className="mb-4 text-sm text-danger">{message}</div> : null}
      <section className={`mb-4 rounded-md border px-4 py-4 ${statusClass}`}>
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-normal">Live open status</div>
            <div className="mt-1 text-2xl font-semibold tracking-normal">{statusLabel}</div>
            <div className="mt-1 text-sm">{statusDetail}</div>
            <div className="mt-2 text-xs">
              Last changed: {formatDateTime(risk?.kill_switch_updated_at)}
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {killSwitchOn ? (
              <button className="btn btn-primary" type="button" onClick={enableTrading}>
                <Power className="h-4 w-4" />
                Turn Off Kill Switch
              </button>
            ) : (
              <button className="btn btn-danger" type="button" onClick={kill}>
                <Power className="h-4 w-4" />
                Turn On Kill Switch
              </button>
            )}
            <button className="btn btn-muted" type="button" onClick={load}>
              Refresh Status
            </button>
          </div>
        </div>
      </section>
      <div className="grid gap-4 lg:grid-cols-2">
        <section className="panel panel-pad">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-semibold text-ink">
            <ShieldAlert className="h-4 w-4" />
            Global
          </h2>
          <dl className="grid grid-cols-2 gap-3 text-sm">
            <dt className="text-slate-500">Kill switch</dt>
            <dd className={killSwitchOn ? "text-danger" : "text-accent"}>
              {risk ? (killSwitchOn ? "ON - blocks new opens" : "OFF - new opens allowed") : "--"}
            </dd>
            <dt className="text-slate-500">Last changed</dt>
            <dd>{formatDateTime(risk?.kill_switch_updated_at)}</dd>
            <dt className="text-slate-500">Live opens</dt>
            <dd className={liveOpensEnabled ? "text-accent" : "text-danger"}>
              {risk ? (liveOpensEnabled ? "allowed" : "blocked") : "--"}
            </dd>
            <dt className="text-slate-500">TRADING_ENABLED</dt>
            <dd className={risk?.trading_enabled_env ? "text-accent" : "text-danger"}>
              {risk?.trading_enabled_env ? "true" : "false"}
            </dd>
            <dt className="text-slate-500">HYPERLIQUID_TRADING_ENABLED</dt>
            <dd className={risk?.hyperliquid_trading_enabled_env ? "text-accent" : "text-danger"}>
              {risk?.hyperliquid_trading_enabled_env ? "true" : "false"}
            </dd>
            <dt className="text-slate-500">Daily loss</dt>
            <dd>{risk?.global_max_daily_loss ?? "--"}</dd>
            <dt className="text-slate-500">Total notional</dt>
            <dd>{risk?.global_max_total_notional ?? "--"}</dd>
            <dt className="text-slate-500">Account mode</dt>
            <dd>{risk?.account_value_mode ?? "--"}</dd>
            <dt className="text-slate-500">Low latency required</dt>
            <dd>{String(risk?.low_latency_required_for_live ?? "--")}</dd>
            <dt className="text-slate-500">Order policy</dt>
            <dd>{risk?.order_policy ?? "--"}</dd>
            <dt className="text-slate-500">Sizing policy</dt>
            <dd>{risk?.sizing_policy ?? "--"}</dd>
          </dl>
        </section>
        <section className="panel panel-pad">
          <div className="flex flex-wrap gap-3">
            {killSwitchOn ? (
              <button className="btn btn-primary" type="button" onClick={enableTrading}>
                <Power className="h-4 w-4" />
                Turn Off Kill Switch
              </button>
            ) : (
              <button className="btn btn-danger" type="button" onClick={kill}>
                <Power className="h-4 w-4" />
                Turn On Kill Switch
              </button>
            )}
          </div>
          <div className="mt-4 text-sm text-slate-600">
            Auto-copy policy is FAST_MARKET_ONLY: Hyperliquid uses aggressive IOC, Binance uses MARKET. Copy multiplier scales ACCOUNT_RATIO sizing.
          </div>
        </section>
      </div>
    </AppShell>
  );
}
