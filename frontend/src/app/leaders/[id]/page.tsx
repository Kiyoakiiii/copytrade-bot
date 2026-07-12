"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { RefreshCw } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { copyStatusTone, effectiveCopyReason, effectiveCopyStatus, effectiveCopyable } from "@/lib/copyStatus";
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

type LeaderDetail = {
  address: string | null;
  account_value: string | null;
  account_value_used_for_sizing?: string | null;
  account_value_source?: string | null;
  account_abstraction_mode?: string | null;
  available_collateral_used_for_margin_check?: string | null;
  balance_source?: string | null;
  portfolio_account_value?: string | null;
  spot_usdc_total?: string | null;
  withdrawable: string | null;
  total_ntl_pos: string | null;
  total_margin_used: string | null;
  updated_at: string | null;
  data_age_ms: number | null;
  stale: boolean;
  error_message: string | null;
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
    unrealized_pnl: string | null;
    leverage: string | null;
    margin_used: string | null;
    liquidation_px: string | null;
    copyable: boolean;
    coin_allowed: boolean;
    venue_route: string | null;
    copy_reason: string;
    copy_status?: string | null;
    last_copy_order_display_status?: string | null;
    last_copy_order_reason?: string | null;
    baseline_status?: string | null;
    baseline_id?: number | null;
    sizing: null | SizingBreakdown;
    allocation: null | {
      target_notional: string;
      allocated_notional: string;
      sides: string[];
      statuses: string[];
    };
  }>;
  dex_states?: Array<{
    dex?: string | null;
    dex_display_name?: string | null;
    account_value: string | null;
    account_value_used_for_sizing?: string | null;
    account_value_source?: string | null;
    account_abstraction_mode?: string | null;
    withdrawable: string | null;
    positions: unknown[];
    stale: boolean;
    error_message: string | null;
  }>;
  allocations?: Array<{
    coin: string;
    symbol: string;
    execution_venue: string;
    position_side: string;
    target_notional: string;
    allocated_notional: string;
    allocated_qty: string;
    status: string;
  }>;
};

type SizingBreakdown = {
  sizing_mode: string;
  formula_mode: string;
  leader_account_value: string | null;
  leader_account_value_used_for_sizing?: string | null;
  leader_account_value_source?: string | null;
  leader_account_abstraction_mode?: string | null;
  leader_position_notional: string | null;
  leader_position_ratio: string | null;
  follower_account_value: string | null;
  follower_account_value_used_for_sizing?: string | null;
  follower_account_value_source?: string | null;
  follower_account_abstraction_mode?: string | null;
  copy_multiplier: string;
  target_notional: string | null;
  calculated_target_notional: string | null;
  current_allocation: string;
  current_allocation_notional: string;
  delta_notional: string | null;
  formula?: string;
  error: string | null;
};

export default function LeaderDetailPage() {
  const params = useParams<{ id: string }>();
  const [data, setData] = useState<LeaderDetail | null>(null);
  const [error, setError] = useState("");
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);

  async function load() {
    setError("");
    try {
      setData(await apiFetch<LeaderDetail>(`/account-states/leaders/${params.id}`));
      setLastRefreshedAt(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  const realtime = useDashboardStream({
    onEvent: (event) => {
      if (["leader_state_update", "positions_update", "baseline_status_update", "allocation_status_update"].includes(event.event_type)) {
        load();
      }
    },
  });

  useEffect(() => {
    load();
  }, [params.id]);
  useRealtimeFallbackPolling(realtime, load);

  return (
    <AppShell>
      <Header
        title="Leader Detail"
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slate-500">
              {lastRefreshedAt ? `Refreshed ${formatDateTime(lastRefreshedAt)}` : "Waiting for first refresh"}
            </span>
            <span className={realtime.connected ? "rounded-md border border-green-200 bg-green-50 px-2 py-1 text-xs text-accent" : "rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-warn"}>
              realtime mode: {realtime.mode}
            </span>
            <button className="btn btn-muted" type="button" onClick={load}>
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}
      {data ? (
        <>
          <section className="panel mb-4 overflow-hidden">
            <div className="border-b border-line px-4 py-3">
              <div className="truncate font-mono text-xs">{data.leader.leader_address}</div>
              <div className="mt-1 text-xs text-slate-500">
                {data.leader.enabled ? "enabled" : "disabled"} / {data.watcher_status} / {data.leader.preferred_venue} / {data.leader.allowed_coins_mode}
              </div>
            </div>
            <div className="grid gap-3 p-4 text-sm md:grid-cols-3 xl:grid-cols-6">
              <Metric label="mode" value="FIXED_REFERENCE" />
              <Metric label="account value used" value={data.leader.fixed_account_value} />
              <Metric label="source" value="LEADER_CONFIG_FIXED" />
              <Metric label="position notional" value={data.total_ntl_pos} />
              <Metric label="multiplier" value={data.leader.copy_multiplier} />
              <Metric label="default / xyz positions" value={`${data.dex_states?.find((item) => (item.dex ?? "") === "")?.positions.length ?? 0} / ${data.dex_states?.find((item) => item.dex === "xyz")?.positions.length ?? 0}`} />
              <Metric label="updated" value={formatDateTime(data.updated_at)} intent={data.stale ? "danger" : "ok"} />
            </div>
            <div className="border-t border-line px-4 py-3 text-xs text-slate-600">
              跟单倍率是在按账户比例跟单后的缩放倍数，不是直接乘 leader 的名义仓位。
            </div>
            {data.error_message ? <div className="border-t border-line px-4 py-3 text-sm text-danger">{data.error_message}</div> : null}
          </section>

          <section className="panel mb-4 overflow-hidden">
            <div className="border-b border-line px-4 py-3 text-sm font-semibold">Leader Positions</div>
            <table className="data-table">
              <thead className="bg-panel text-slate-500">
                <tr>
                  <th className="px-4 py-3">Coin</th>
                  <th className="px-4 py-3">DEX</th>
                  <th className="px-4 py-3">Product</th>
                  <th className="px-4 py-3">Side</th>
                  <th className="px-4 py-3">Size</th>
                  <th className="px-4 py-3">Notional</th>
                  <th className="px-4 py-3">Entry Price</th>
                  <th className="px-4 py-3">Mark Price</th>
                  <th className="px-4 py-3">Mid Price</th>
                  <th className="px-4 py-3">PnL</th>
                  <th className="px-4 py-3">Open Time</th>
                  <th className="px-4 py-3">Updated</th>
                  <th className="px-4 py-3">Copy</th>
                  <th className="px-4 py-3">Copy Status</th>
                  <th className="px-4 py-3">ACCOUNT_RATIO target</th>
                  <th className="px-4 py-3">Delta</th>
                  <th className="px-4 py-3">Route</th>
                  <th className="px-4 py-3">Allocation</th>
                </tr>
              </thead>
              <tbody>
                {data.positions.length ? (
                  data.positions.map((item) => (
                    <tr key={`${item.dex ?? ""}-${item.canonical_coin ?? item.coin}-${item.side}`} className="border-t border-line">
                      <td className="px-4 py-3 font-mono text-xs">{item.canonical_coin ?? item.coin}</td>
                      <td className="px-4 py-3">{item.dex_display_name ?? item.dex ?? "Hyperliquid"}</td>
                      <td className="px-4 py-3">{item.product_type ?? "unknown"}</td>
                      <td className="px-4 py-3">{item.side}</td>
                      <td className="px-4 py-3">{formatQuantity(item.size)}</td>
                      <td className="px-4 py-3">{formatNotional(item.notional)}</td>
                      <td className="px-4 py-3">{formatPrice(item.entry_px)}</td>
                      <td className={item.mark_price_stale ? "px-4 py-3 text-warn" : "px-4 py-3"}>{formatPrice(item.mark_px)}</td>
                      <td className="px-4 py-3">{formatPrice(item.mid_px)}</td>
                      <td className="px-4 py-3">{formatNotional(item.unrealized_pnl)}</td>
                      <td className="px-4 py-3">{formatOpenTime(item)}</td>
                      <td className="px-4 py-3">{formatDateTime(item.updated_at)}</td>
                      <td className={effectiveCopyable(item) ? "px-4 py-3 text-accent" : "px-4 py-3 text-danger"}>{String(effectiveCopyable(item))}</td>
                      <td className="px-4 py-3">
                        <div className={copyStatusTone(effectiveCopyStatus(item)) === "danger" ? "text-danger" : copyStatusTone(effectiveCopyStatus(item)) === "warn" ? "text-warn" : "text-accent"}>{humanLabel(effectiveCopyStatus(item))}</div>
                        {effectiveCopyReason(item) ? <div className="max-w-[220px] truncate text-[11px] text-slate-500">{effectiveCopyReason(item)}</div> : null}
                      </td>
                      <td className="px-4 py-3">
                        {item.sizing?.target_notional ? formatNotional(item.sizing.target_notional) : item.sizing?.error ?? "--"}
                        {item.sizing?.leader_position_ratio ? (
                          <div className="text-[11px] text-slate-500">
                            ratio {formatDisplayValue(item.sizing.leader_position_ratio)} / follower {formatNotional(item.sizing.follower_account_value)}
                            <br />
                            leader source {item.sizing.leader_account_value_source ?? "--"} / follower source {item.sizing.follower_account_value_source ?? "--"}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-4 py-3">{formatNotional(item.sizing?.delta_notional)}</td>
                      <td className="px-4 py-3">{item.venue_route ?? "--"}</td>
                      <td className="px-4 py-3">
                        {item.allocation ? `${formatNotional(item.allocation.target_notional)} / ${formatNotional(item.allocation.allocated_notional)}` : "--"}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr className="border-t border-line"><td className="px-4 py-3 text-slate-500" colSpan={18}>No positions</td></tr>
                )}
              </tbody>
            </table>
          </section>
        </>
      ) : null}
    </AppShell>
  );
}

function Metric({ label, value, intent }: { label: string; value: string | null | undefined; intent?: "ok" | "danger" }) {
  return (
    <div className="mini-card">
      <div className="mini-label">{label}</div>
      <div className={`mini-value ${intent === "danger" ? "text-danger" : intent === "ok" ? "text-accent" : "text-ink"}`}>{formatDisplayValue(value)}</div>
    </div>
  );
}

function formatAge(value: number | null | undefined) {
  return formatAgeLabel(value);
}

function formatDateTime(value: string | null | undefined) {
  return formatDateTimeLabel(value);
}

function formatOpenTime(position: LeaderDetail["positions"][number]) {
  return formatOpenTimeLabel(position);
}

function humanLabel(value: string | null | undefined): string {
  if (!value) return "--";
  return value
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}
