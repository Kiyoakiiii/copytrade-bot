"use client";

import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { useDashboardStream, useRealtimeFallbackPolling } from "@/lib/realtime";
import {
  formatDateTime,
  formatDisplayValue,
  formatMs,
  formatNotional,
  formatPrice,
  formatQuantity,
} from "@/lib/format";

type Order = {
  id: number;
  allocation_id: number | null;
  created_at: string;
  leader_address: string;
  source_coin: string;
  source_type: string;
  execution_venue: string;
  dex: string;
  canonical_coin: string | null;
  raw_coin_from_fill: string | null;
  asset_id: number | null;
  venue_symbol: string | null;
  hyperliquid_coin: string | null;
  binance_symbol: string | null;
  side: string;
  position_side: string | null;
  order_action: string | null;
  order_type: string;
  client_order_id: string | null;
  cloid: string | null;
  quantity: string;
  notional: string | null;
  executed_qty: string | null;
  avg_fill_price: string | null;
  leader_entry_px: string | null;
  follower_avg_entry_px: string | null;
  slippage_bps: string | null;
  event_to_ws_ms: number | null;
  ws_to_dedupe_ms: number | null;
  debounce_ms: number | null;
  decision_ms: number | null;
  ws_to_submit_ms: number | null;
  submit_to_ack_ms: number | null;
  event_to_ack_ms: number | null;
  event_to_final_ms: number | null;
  total_hot_path_ms: number | null;
  latency_trace_id: string | null;
  latency_trace: { timestamps?: Record<string, string | null>; metrics?: Record<string, number | null> } | null;
  missing_latency_fields: string[] | null;
  status: string;
  dry_run: boolean;
  error_message: string | null;
  sizing_mode: string | null;
  leader_account_value: string | null;
  leader_account_value_source: string | null;
  leader_account_abstraction_mode: string | null;
  leader_position_notional: string | null;
  follower_account_value: string | null;
  follower_account_value_source: string | null;
  follower_account_abstraction_mode: string | null;
  leader_position_ratio: string | null;
  copy_multiplier: string | null;
  target_notional: string | null;
  delta_notional: string | null;
  pre_trade_checklist?: Record<string, unknown> | null;
  order_validator?: {
    validator_status?: string;
    block_reason?: string | null;
    raw_size?: string;
    rounded_size?: string;
    raw_price?: string;
    raw_limit_price?: string;
    rounded_price?: string;
    estimated_notional?: string;
    min_order_value?: string;
    sz_decimals?: number | null;
    price_decimals?: number | null;
    tick_size?: string | null;
    errors?: string[];
    warnings?: string[];
  } | null;
  validator_status?: string | null;
  error_code?: string | null;
  exchange_submit_attempted?: boolean;
  not_submitted_to_exchange?: boolean;
  required_multiplier_to_pass_min_order?: string | null;
};

type OrdersResponse = {
  data: Order[];
  data_age_ms: number | null;
  stale: boolean;
  refresh_in_progress: boolean;
  last_error: string | null;
};

export default function OrdersPage() {
  const [orders, setOrders] = useState<Order[]>([]);
  const [error, setError] = useState("");
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);
  const realtime = useDashboardStream({
    onEvent: (event) => {
      if (event.event_type === "orders_update" && Array.isArray(event.payload)) {
        setOrders(event.payload as Order[]);
        setLastRefreshedAt(new Date().toISOString());
        setError("");
      }
    },
  });

  async function load() {
    setError("");
    try {
      const payload = await apiFetch<OrdersResponse | Order[]>("/orders");
      setOrders(Array.isArray(payload) ? payload : payload.data);
      setLastRefreshedAt(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  useEffect(() => {
    load();
  }, []);
  useRealtimeFallbackPolling(realtime, load);

  return (
    <AppShell>
      <Header
        title="Orders"
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slate-500">
              {lastRefreshedAt ? `Refreshed ${formatDateTime(lastRefreshedAt)}` : "Waiting for first refresh"}
            </span>
            <span className={realtime.connected ? "rounded-md border border-green-200 bg-green-50 px-2 py-1 text-xs text-accent" : "rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-warn"}>
              realtime mode: {realtime.mode}
            </span>
            <button className="btn btn-muted" type="button" onClick={() => load()}>
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}
      <div className="panel overflow-hidden">
        <table className="data-table">
          <thead className="border-b border-line bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Time</th>
              <th className="px-4 py-3">Leader</th>
              <th className="px-4 py-3">Venue</th>
              <th className="px-4 py-3">DEX</th>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Source</th>
              <th className="px-4 py-3">Allocation</th>
              <th className="px-4 py-3">Side</th>
              <th className="px-4 py-3">Position</th>
              <th className="px-4 py-3">Action</th>
              <th className="px-4 py-3">Type</th>
              <th className="px-4 py-3">Qty</th>
              <th className="px-4 py-3">Sizing</th>
              <th className="px-4 py-3">Target/Delta</th>
              <th className="px-4 py-3">Fill</th>
              <th className="px-4 py-3">Latency</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">Dry</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((order) => (
              <tr key={order.id} className="border-b border-line last:border-0">
                <td className="px-4 py-3 text-slate-500">{formatDateTime(order.created_at)}</td>
                <td className="max-w-xs truncate px-4 py-3 font-mono text-xs">{order.leader_address}</td>
                <td className="px-4 py-3">{order.execution_venue}</td>
                <td className="px-4 py-3">{order.dex || "default"}</td>
                <td className="px-4 py-3 font-mono text-xs">
                  {order.canonical_coin ?? order.venue_symbol ?? order.binance_symbol ?? order.hyperliquid_coin ?? "--"}
                  {order.asset_id !== null ? <div className="text-[11px] text-slate-500">asset {order.asset_id}</div> : null}
                </td>
                <td className="px-4 py-3">{order.source_type}</td>
                <td className="px-4 py-3">{order.allocation_id ?? "--"}</td>
                <td className="px-4 py-3">{order.side}</td>
                <td className="px-4 py-3">{order.position_side ?? "--"}</td>
                <td className="px-4 py-3">{order.order_action ?? "--"}</td>
                <td className="px-4 py-3">{order.order_type}</td>
                <td className="px-4 py-3">{formatQuantity(order.quantity)}</td>
                <td className="px-4 py-3">
                  <details>
                    <summary className="cursor-pointer">{order.sizing_mode ?? "--"}</summary>
                    <div className="mt-1 max-w-[220px] text-[11px] text-slate-500">
                      {order.leader_position_ratio
                        ? `${formatNotional(order.follower_account_value)} * abs(${formatNotional(order.leader_position_notional)} / ${formatNotional(order.leader_account_value)}) * ${formatDisplayValue(order.copy_multiplier)}`
                        : "ACCOUNT_RATIO metadata unavailable"}
                      <div>
                        {order.leader_account_value_source ?? "--"} / {order.leader_account_abstraction_mode ?? "--"} -&gt; {order.follower_account_value_source ?? "--"} / {order.follower_account_abstraction_mode ?? "--"}
                      </div>
                    </div>
                  </details>
                </td>
                <td className="px-4 py-3">
                  <div>{formatNotional(order.target_notional ?? order.notional)}</div>
                  <div className="text-[11px] text-slate-500">delta {formatNotional(order.delta_notional)}</div>
                </td>
                <td className="px-4 py-3">
                  <div>{formatQuantity(order.executed_qty)} @ {formatPrice(order.avg_fill_price)}</div>
                  <div className="text-[11px] text-slate-500">leader entry {formatPrice(order.leader_entry_px)}</div>
                  <div className="text-[11px] text-slate-500">follower avg entry {formatPrice(order.follower_avg_entry_px)}</div>
                </td>
                <td className="px-4 py-3">
                  <div>event-ack {formatMs(order.event_to_ack_ms)}</div>
                  <div className="text-[11px] text-slate-500">ws-submit {formatMs(order.ws_to_submit_ms)}</div>
                  <div className="text-[11px] text-slate-500">submit-ack {formatMs(order.submit_to_ack_ms)}</div>
                  <div className="text-[11px] text-slate-500">total {formatMs(order.total_hot_path_ms)}</div>
                  {order.missing_latency_fields?.length ? (
                    <div className="mt-1 max-w-[220px] text-[11px] text-warn">missing {order.missing_latency_fields.join(", ")}</div>
                  ) : null}
                  <details className="mt-1">
                    <summary className="cursor-pointer text-[11px] text-slate-500">Advanced Latency</summary>
                    <div className="mt-1 max-w-[260px] space-y-1 font-mono text-[11px] text-slate-500">
                      <div>trace {order.latency_trace_id ?? "--"}</div>
                      {Object.entries(order.latency_trace?.metrics ?? {}).map(([key, value]) => (
                        <div key={key}>{key}: {formatMs(value)}</div>
                      ))}
                    </div>
                  </details>
                </td>
                <td className="px-4 py-3">
                  <div>{order.status}</div>
                  {order.error_message ? <div className="max-w-[240px] truncate text-[11px] text-danger">{order.error_message}</div> : null}
                  {order.error_code === "BELOW_MIN_ORDER_VALUE" || order.order_validator?.block_reason === "BLOCKED_TOO_SMALL" ? (
                    <div className="mt-1 max-w-[260px] text-[11px] text-warn">
                      Not submitted to exchange. Delta {formatNotional(order.delta_notional)} / min {formatNotional(order.order_validator?.min_order_value ?? null)}
                      {order.required_multiplier_to_pass_min_order ? <div>required multiplier {formatDisplayValue(order.required_multiplier_to_pass_min_order)}</div> : null}
                    </div>
                  ) : null}
                  {order.order_validator ? (
                    <details className="mt-1">
                      <summary className="cursor-pointer text-[11px] text-slate-500">Validator {order.validator_status ?? order.order_validator.validator_status ?? "--"}</summary>
                      <div className="mt-1 max-w-[280px] space-y-1 text-[11px] text-slate-500">
                        <div>raw size {formatQuantity(order.order_validator.raw_size)} -&gt; {formatQuantity(order.order_validator.rounded_size)}</div>
                        <div>reference price {formatPrice(order.order_validator.raw_price)}</div>
                        <div>IOC taker px {formatPrice(order.order_validator.raw_limit_price)} -&gt; {formatPrice(order.order_validator.rounded_price)}</div>
                        <div>notional {formatNotional(order.order_validator.estimated_notional)} / min {formatNotional(order.order_validator.min_order_value)}</div>
                        <div>szDecimals {order.order_validator.sz_decimals ?? "--"} / priceDecimals {order.order_validator.price_decimals ?? "--"} / tick {order.order_validator.tick_size ?? "--"}</div>
                        {order.exchange_submit_attempted === false ? <div className="text-warn">Not submitted to exchange</div> : null}
                        {order.order_validator.errors?.length ? <div className="text-danger">{order.order_validator.errors.join(", ")}</div> : null}
                      </div>
                    </details>
                  ) : null}
                </td>
                <td className="px-4 py-3">
                  <div>{order.dry_run ? "true" : "false"}</div>
                  <div className="max-w-[180px] truncate font-mono text-[11px] text-slate-500">
                    {order.cloid ?? order.client_order_id ?? "--"}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </AppShell>
  );
}
