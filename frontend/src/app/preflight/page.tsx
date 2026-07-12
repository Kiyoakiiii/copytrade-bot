"use client";

import { useEffect, useState } from "react";
import { ClipboardCheck, RefreshCw, ShieldAlert } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";
import { useDashboardStream, useRealtimeFallbackPolling } from "@/lib/realtime";
import {
  formatDateTime as formatDateTimeLabel,
  formatDisplayValue,
  formatMs as formatMsLabel,
  formatNotional,
  formatOpenTimeLabel,
  formatPrice,
  formatQuantity,
} from "@/lib/format";

type Preflight = {
  last_updated_at: string;
  global: Record<string, boolean | string | number | null>;
  hyperliquid_venue: VenueReadiness;
  binance_venue: VenueReadiness;
  symbols: Array<{
    symbol: string;
    coin: string;
    enabled: boolean;
    current_margin_mode: string;
    expected_margin_mode: string;
    current_leverage: number | null;
    expected_leverage: number;
    current_position_notional: string | null;
    current_position_side: string;
    status: "OK" | "WARNING" | "BLOCKED";
    message: string;
  }>;
  leaders: Array<{
    address: string;
    enabled: boolean;
    deleted_at: string | null;
    websocket_connected: boolean;
    watcher_status: string;
    accountValue: string | null;
    positions_loaded: boolean;
    last_update_age: number | null;
    allowed_symbols: string[] | null;
    blocked_symbols: string[];
    allowed_coins_mode: "ALL_COINS" | "CUSTOM_LIST";
    copy_multiplier: string;
    status: "OK" | "STALE" | "BLOCKED" | "WARNING";
  }>;
  watcher: {
    db_enabled_leaders_count: number;
    watcher_active_leaders_count: number;
    leaders_not_subscribed: string[];
    subscribed_but_disabled_or_deleted: string[];
    mode: string | null;
    source: string | null;
    updated_at: string | null;
    status_age: number | null;
    websocket_connected?: boolean | null;
    low_latency_primary?: boolean | null;
    low_latency_ready?: boolean | null;
    ready_for_low_latency_live?: boolean | null;
    ws_leaders?: string[];
    poll_fallback_leaders?: string[];
    follower_order_updates_subscribed?: boolean | null;
    follower_user_events_subscribed?: boolean | null;
    follower_user_fills_subscribed?: boolean | null;
    leader_user_fills_subscribed_count?: number | null;
    dex_price_cache_status?: Record<string, DexPriceCacheStatus>;
    last_ws_event_at?: string | null;
    last_ws_event_age_ms?: number | null;
    poll_fallback_count?: number | null;
  };
  low_latency: {
    low_latency_watcher_running: boolean;
    watcher_mode: string | null;
    websocket_connected: boolean;
    subscribed_leader_count_by_dex: Record<string, number>;
    subscribed_leaders_count: number;
    ws_leaders: string[];
    poll_fallback_leaders: string[];
    last_event_time_by_dex: Record<string, string | null>;
    poll_fallback_count: number;
    follower_order_updates_subscribed: boolean;
    follower_user_events_subscribed: boolean;
    follower_user_fills_subscribed: boolean;
    leader_user_fills_subscribed_count: number;
    dex_price_cache_status: Record<string, DexPriceCacheStatus>;
    default_dex_price_cache_fresh: boolean;
    xyz_price_cache_fresh: boolean;
    last_ws_event_at: string | null;
    last_ws_event_age_ms: number | null;
    LOW_LATENCY_REQUIRED_FOR_LIVE: boolean;
    ALLOW_POLL_FALLBACK_LIVE: boolean;
    ready_for_low_latency_live: boolean;
  };
  baseline: BaselineSummary;
  risk_settings?: RiskSettings;
  market_coverage?: MarketCoverage;
  account_states: {
    follower: AccountState;
    leaders: AccountState[];
  };
  small_live_start_checklist: {
    ready: boolean;
    checks: Array<{ name: string; status: "OK" | "WARNING" | "BLOCKED"; message: string }>;
  };
  aggregate_positions: Array<{
    venue?: string;
    symbol: string;
    allocated_long_qty: string;
    allocated_short_qty: string;
    binance_long_qty?: string;
    binance_short_qty?: string;
    hyperliquid_long_qty?: string;
    hyperliquid_short_qty?: string;
  }>;
  allocations: Array<{
    leader_address: string;
    coin: string;
    symbol: string;
    execution_venue: string;
    venue_symbol: string | null;
    venue_account: string | null;
    position_side: string;
    target_notional: string;
    allocated_notional: string;
    allocated_qty: string;
    copy_multiplier: string;
    sizing_mode: string;
    leader_account_value: string | null;
    leader_position_notional: string | null;
    leader_position_ratio: string | null;
    follower_account_value: string | null;
    current_allocation: string;
    delta_notional: string;
    status: string;
  }>;
  allocation_mismatches: Array<{
    venue?: string;
    symbol: string;
    allocated_long_qty: string;
    allocated_short_qty: string;
    binance_long_qty?: string;
    binance_short_qty?: string;
    hyperliquid_long_qty?: string;
    hyperliquid_short_qty?: string;
    status: "BLOCKED" | "OK";
    message: string;
  }>;
  pending_unknown_orders_count: number;
  latency: {
    last_auto_order_latency: null | {
      client_order_id: string | null;
      event_to_ack_ms: number | null;
      event_to_final_ms: number | null;
      submit_to_ack_ms: number | null;
      ws_to_submit_ms: number | null;
    };
    recent_avg_latency: number | null;
    recent_max_latency: number | null;
    by_dex: Record<string, LatencyGroup>;
    by_leader: Record<string, LatencyGroup>;
  };
  ready_for_live: boolean;
  blocking_reasons: string[];
  message: string;
};

type FinalLiveCheck = {
  can_live: boolean;
  blockers: string[];
  warnings: string[];
  follower_value: string | null;
  available_collateral: string | null;
  enabled_leaders: number;
  ws_leaders: string[];
  price_cache_status: Record<string, DexPriceCacheStatus>;
  order_policy: string;
  sizing_policy: string;
  invariants: Array<{ name: string; status: "OK" | "WARNING" | "BLOCKED"; message: string }>;
  latency_instrumentation_check: { ok: boolean; message: string };
  allocation_isolation_check: { ok: boolean; message: string };
  baseline_check?: BaselineSummary;
  risk_settings?: RiskSettings;
  exchange_rules?: {
    order_validator_enabled: boolean;
    min_order_value: string;
    precision_rules_loaded_count: number;
    markets_missing_precision: string[];
    markets_missing_asset_id: string[];
    markets_missing_price: string[];
    last_blocked_too_small_count: number;
    last_exchange_rejection_count: number;
    recent_invalid_price_count: number;
    recent_invalid_size_count: number;
    recent_cloid_error_count: number;
  };
  recommended_next_action: string;
};

type BaselineSummary = {
  baseline_tracking_enabled: boolean;
  baseline_ready: boolean;
  baseline_captured_for_all_enabled_leaders: boolean;
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
    baseline_status: string;
    copy_status: string;
    reason: string | null;
  }>;
};

type LatencyGroup = {
  recent_avg_latency: number | null;
  recent_max_latency: number | null;
  count: number;
};

type DexPriceCacheStatus = {
  markets_count: number;
  fresh: boolean;
  stale_markets_count: number;
  last_price_update_age_ms: number | null;
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
  updated_at: string | null;
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
    open_time?: string | null;
    first_seen_at?: string | null;
    open_time_source?: string | null;
    updated_at?: string | null;
    data_age_ms?: number | null;
    unrealized_pnl?: string | null;
    copy_status?: string;
    copy_reason?: string;
    baseline_status?: string | null;
    baseline_id?: number | null;
    sizing?: SizingBreakdown | null;
  }>;
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
  leader?: {
    id: number;
    leader_address: string;
    enabled: boolean;
    copy_multiplier: string;
    allowed_coins_mode: string;
    preferred_venue: string;
    max_notional_per_trade: string | null;
    max_total_notional: string | null;
  };
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

type VenueReadiness = {
  enabled: boolean;
  trading_enabled: boolean;
  api_connected: boolean;
  network?: string;
  wallet_account_configured?: boolean;
  private_key_configured?: boolean;
  current_position_mode?: string;
  accountValue?: string | null;
  withdrawable?: string | null;
  enabled_coins?: string[];
  coin_scope?: string;
  symbols?: Array<{
    coin?: string;
    dex?: string;
    canonical_coin?: string;
    symbol?: string;
    venue_symbol?: string;
    exists_in_meta?: boolean;
    max_leverage?: number | null;
    target_leverage?: number | null;
    margin_mode?: string;
    risk_status?: string;
    warning?: string | null;
    status: "OK" | "WARNING" | "BLOCKED";
    message: string;
  }>;
  unknown_orders_count: number;
  dex_readiness?: DexReadiness[];
  market_coverage?: MarketCoverage;
  ready_for_live_hyperliquid?: boolean;
  ready_for_live_binance?: boolean;
  live_trading_allowed: boolean;
  blocking_reasons: string[];
  message: string;
};

type MarketCoverage = {
  enabled_dexes: string[];
  enabled_dex_count: number;
  markets_loaded_count: number;
  markets_loaded_count_by_dex: Record<string, number>;
  unknown_product_markets_count: number;
  all_coins_mode_includes_enabled_dex_markets: boolean;
  all_coins_mode_includes_hip3_tradfi_unknown: boolean;
  binance_mapping_required_for_hyperliquid: boolean;
  product_type_unknown_hidden: boolean;
  no_static_coin_filter: boolean;
  canonical_scope_keys: string[];
  rows: Array<{
    dex: string;
    display_name: string;
    is_hip3: boolean;
    markets_loaded_count: number;
    meta_universe_count: number;
    mids_markets_count: number;
    unknown_product_markets_count: number;
    asset_id_mapping_ready: boolean;
    status: string;
    message: string;
  }>;
};

type RiskSettings = {
  risk_settings_enabled: boolean;
  margin_mode_setup_enabled?: boolean;
  isolated_setup_enabled: boolean;
  leverage_setup_enabled: boolean;
  desired_margin_mode: string;
  target_default_leverage: number;
  effective_leverage_rule: string;
  ttl_seconds: number;
  markets_confirmed_count: number;
  markets_failed_count: number;
  markets_unknown_count: number;
  failed_markets: RiskSettingRow[];
  unknown_markets: RiskSettingRow[];
  blockers: string[];
  rows: RiskSettingRow[];
};

type RiskSettingRow = {
  dex: string;
  dex_display_name: string;
  canonical_coin: string;
  asset_id: number | null;
  desired_margin_mode: string;
  desired_leverage: number | null;
  market_max_leverage: number | null;
  effective_leverage: number | null;
  actual_margin_mode: string | null;
  actual_leverage: number | null;
  status: string;
  cache_stale: boolean;
  last_confirmed_at: string | null;
  last_checked_at: string | null;
  error: string | null;
  risk_setting_required: boolean;
  source: string;
  baseline_status: string | null;
  reason: string | null;
};

type DexReadiness = {
  dex_name: string;
  display_name: string;
  enabled: boolean;
  is_hip3: boolean;
  meta_loaded: boolean;
  universe_count: number;
  mids_fresh: boolean;
  account_state_loaded_for_follower: boolean;
  accountValue: string | null;
  account_value_used_for_sizing?: string | null;
  account_value_source?: string | null;
  account_abstraction_mode?: string | null;
  available_collateral_used_for_margin_check?: string | null;
  withdrawable: string | null;
  open_positions_count: number;
  unknown_orders_count: number;
  asset_id_mapping_ready: boolean;
  low_latency_watcher_subscribed: boolean;
  ready_for_live_for_dex: boolean;
  message: string;
};

export default function PreflightPage() {
  const [data, setData] = useState<Preflight | null>(null);
  const [finalCheck, setFinalCheck] = useState<FinalLiveCheck | null>(null);
  const [error, setError] = useState("");
  const [lastRefreshedAt, setLastRefreshedAt] = useState<string | null>(null);

  async function load() {
    setError("");
    try {
      const payload = await apiFetch<Preflight>("/preflight");
      setData(payload);
      setLastRefreshedAt(new Date().toISOString());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  const realtime = useDashboardStream();

  async function runFinalLiveCheck() {
    setError("");
    try {
      setFinalCheck(await apiFetch<FinalLiveCheck>("/preflight/final-live-check", { method: "POST", body: "{}" }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  useEffect(() => {
    load();
  }, []);
  useRealtimeFallbackPolling(realtime, load, { reconcileMs: 60000 });

  return (
    <AppShell>
      <Header
        title="Preflight"
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slate-500">
              {lastRefreshedAt ? `Refreshed ${formatDateTime(lastRefreshedAt)}` : "Waiting for first refresh"}
            </span>
            <span className={realtime.connected ? "rounded-md border border-green-200 bg-green-50 px-2 py-1 text-xs text-accent" : "rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-warn"}>
              realtime mode: {realtime.mode}
            </span>
            <button className="btn btn-muted" type="button" onClick={runFinalLiveCheck}>
              <ClipboardCheck className="h-4 w-4" />
              Run Final Live Check
            </button>
            <button className="btn btn-muted" type="button" onClick={load}>
              <RefreshCw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}
      {data && !data.ready_for_live ? (
        <div className="mb-4 flex items-center gap-2 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-danger">
          <ShieldAlert className="h-4 w-4" />
          {data.message}
        </div>
      ) : null}

      {finalCheck ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Final Live Check</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="can_live" value={String(finalCheck.can_live)} intent={finalCheck.can_live ? "ok" : "danger"} />
            <Metric label="order_policy" value={finalCheck.order_policy} />
            <Metric label="sizing_policy" value={finalCheck.sizing_policy} />
            <Metric label="enabled leaders" value={String(finalCheck.enabled_leaders)} />
            <Metric label="follower value" value={finalCheck.follower_value ?? "--"} />
            <Metric label="available collateral" value={finalCheck.available_collateral ?? "--"} />
	            <Metric label="ws leaders" value={String(finalCheck.ws_leaders.length)} />
	            <Metric label="latency instrumentation" value={finalCheck.latency_instrumentation_check.ok ? "OK" : "BLOCKED"} intent={finalCheck.latency_instrumentation_check.ok ? "ok" : "danger"} />
	            <Metric label="allocation isolation" value={finalCheck.allocation_isolation_check.ok ? "OK" : "BLOCKED"} intent={finalCheck.allocation_isolation_check.ok ? "ok" : "danger"} />
	            <Metric label="baseline ready" value={String(finalCheck.baseline_check?.baseline_ready ?? false)} intent={finalCheck.baseline_check?.baseline_ready ? "ok" : "danger"} />
	            <Metric label="ignored existing" value={String(finalCheck.baseline_check?.ignored_existing_positions_count ?? 0)} />
	            <Metric label="risk confirmed" value={String(finalCheck.risk_settings?.markets_confirmed_count ?? 0)} />
	            <Metric label="risk failed" value={String(finalCheck.risk_settings?.markets_failed_count ?? 0)} intent={finalCheck.risk_settings?.markets_failed_count ? "danger" : "ok"} />
	            <Metric label="risk unknown" value={String(finalCheck.risk_settings?.markets_unknown_count ?? 0)} intent={finalCheck.risk_settings?.markets_unknown_count ? "danger" : "ok"} />
	            <Metric label="validator" value={finalCheck.exchange_rules?.order_validator_enabled ? "enabled" : "disabled"} intent={finalCheck.exchange_rules?.order_validator_enabled ? "ok" : "danger"} />
	            <Metric label="min order" value={finalCheck.exchange_rules?.min_order_value ?? "--"} />
	            <Metric label="precision rules" value={String(finalCheck.exchange_rules?.precision_rules_loaded_count ?? 0)} />
	            <Metric label="too small" value={String(finalCheck.exchange_rules?.last_blocked_too_small_count ?? 0)} />
	            <Metric label="exchange rejects" value={String(finalCheck.exchange_rules?.last_exchange_rejection_count ?? 0)} intent={finalCheck.exchange_rules?.last_exchange_rejection_count ? "danger" : "ok"} />
	            <Metric label="invalid price" value={String(finalCheck.exchange_rules?.recent_invalid_price_count ?? 0)} intent={finalCheck.exchange_rules?.recent_invalid_price_count ? "danger" : "ok"} />
	          </div>
          <div className={finalCheck.can_live ? "border-t border-line px-4 py-3 text-sm text-accent" : "border-t border-line px-4 py-3 text-sm text-danger"}>
            {finalCheck.recommended_next_action}
          </div>
          {finalCheck.blockers.length ? <div className="border-t border-line px-4 py-3 text-sm text-danger">{finalCheck.blockers.join("; ")}</div> : null}
	          {finalCheck.warnings.length ? <div className="border-t border-line px-4 py-3 text-sm text-warn">{finalCheck.warnings.join("; ")}</div> : null}
	          {finalCheck.invariants.length ? (
	            <table className="w-full border-t border-line text-left text-xs">
	              <thead className="bg-panel text-slate-500">
	                <tr>
	                  <th className="px-4 py-2">Invariant</th>
	                  <th className="px-4 py-2">Status</th>
	                  <th className="px-4 py-2">Message</th>
	                </tr>
	              </thead>
	              <tbody>
	                {finalCheck.invariants.map((item) => (
	                  <tr key={item.name} className="border-t border-line">
	                    <td className="px-4 py-2">{item.name}</td>
	                    <td className={`px-4 py-2 ${statusClass(item.status)}`}>{item.status}</td>
	                    <td className="px-4 py-2 text-slate-600">{item.message}</td>
	                  </tr>
	                ))}
	              </tbody>
	            </table>
	          ) : null}
	        </section>
      ) : null}

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Global</div>
        <div className="grid gap-3 p-4 text-sm md:grid-cols-3">
          {data
            ? Object.entries(data.global).map(([key, value]) => (
                <div key={key} className="rounded-md border border-line bg-panel p-3">
                  <div className="mb-1 text-xs text-slate-500">{key}</div>
                  <div className="font-medium text-ink">{String(value)}</div>
                </div>
              ))
            : null}
          <Metric label="API refreshed" value={formatDateTime(data?.last_updated_at)} />
        </div>
      </section>

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Watcher</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="DB enabled leaders" value={String(data.watcher.db_enabled_leaders_count)} />
            <Metric label="Watcher active leaders" value={String(data.watcher.watcher_active_leaders_count)} />
            <Metric label="Not subscribed" value={String(data.watcher.leaders_not_subscribed.length)} intent={data.watcher.leaders_not_subscribed.length ? "danger" : "ok"} />
            <Metric label="Disabled still subscribed" value={String(data.watcher.subscribed_but_disabled_or_deleted.length)} intent={data.watcher.subscribed_but_disabled_or_deleted.length ? "danger" : "ok"} />
            <Metric label="Mode" value={data.watcher.mode ?? "--"} />
            <Metric label="Source" value={data.watcher.source ?? "--"} />
            <Metric label="Updated" value={formatDateTime(data.watcher.updated_at)} />
            <Metric label="Age" value={data.watcher.status_age === null ? "--" : `${data.watcher.status_age}s`} />
            <Metric label="WS leaders" value={String(data.watcher.ws_leaders?.length ?? 0)} />
            <Metric label="Poll fallback leaders" value={String(data.watcher.poll_fallback_leaders?.length ?? 0)} intent={data.watcher.poll_fallback_leaders?.length ? "danger" : "ok"} />
            <Metric label="Follower order updates" value={String(data.watcher.follower_order_updates_subscribed ?? false)} intent={data.watcher.follower_order_updates_subscribed ? "ok" : "danger"} />
            <Metric label="Last WS age" value={data.watcher.last_ws_event_age_ms == null ? "--" : `${data.watcher.last_ws_event_age_ms}ms`} />
          </div>
          {data.watcher.leaders_not_subscribed.length || data.watcher.subscribed_but_disabled_or_deleted.length ? (
            <div className="border-t border-line px-4 py-3 text-xs text-danger">
              {[
                ...data.watcher.leaders_not_subscribed.map((address) => `not subscribed: ${address}`),
                ...data.watcher.subscribed_but_disabled_or_deleted.map((address) => `disabled/deleted still subscribed: ${address}`)
              ].join("; ")}
            </div>
          ) : null}
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Low Latency</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="watcher mode" value={data.low_latency.watcher_mode ?? "--"} />
            <Metric label="watcher running" value={String(data.low_latency.low_latency_watcher_running)} intent={data.low_latency.low_latency_watcher_running ? "ok" : "danger"} />
            <Metric label="websocket connected" value={String(data.low_latency.websocket_connected)} intent={data.low_latency.websocket_connected ? "ok" : "danger"} />
            <Metric label="required for live" value={String(data.low_latency.LOW_LATENCY_REQUIRED_FOR_LIVE)} />
            <Metric label="allow poll fallback" value={String(data.low_latency.ALLOW_POLL_FALLBACK_LIVE)} />
            <Metric label="ready for live" value={String(data.low_latency.ready_for_low_latency_live)} intent={data.low_latency.ready_for_low_latency_live ? "ok" : "danger"} />
            <Metric label="ws leaders" value={String(data.low_latency.subscribed_leaders_count)} />
            <Metric label="leader userFills" value={String(data.low_latency.leader_user_fills_subscribed_count)} />
            <Metric label="poll fallback count" value={String(data.low_latency.poll_fallback_count)} />
            <Metric label="follower orderUpdates" value={String(data.low_latency.follower_order_updates_subscribed)} intent={data.low_latency.follower_order_updates_subscribed ? "ok" : "danger"} />
            <Metric label="follower userEvents" value={String(data.low_latency.follower_user_events_subscribed)} />
            <Metric label="follower userFills" value={String(data.low_latency.follower_user_fills_subscribed)} />
            <Metric label="default prices" value={String(data.low_latency.default_dex_price_cache_fresh)} intent={data.low_latency.default_dex_price_cache_fresh ? "ok" : "danger"} />
            <Metric label="xyz prices" value={String(data.low_latency.xyz_price_cache_fresh)} intent={data.low_latency.xyz_price_cache_fresh ? "ok" : "danger"} />
            <Metric label="last WS age" value={data.low_latency.last_ws_event_age_ms == null ? "--" : `${data.low_latency.last_ws_event_age_ms}ms`} />
          </div>
          <DexPriceCache rows={data.low_latency.dex_price_cache_status} />
          {data.low_latency.poll_fallback_leaders.length ? (
            <div className="border-t border-line px-4 py-3 text-xs text-danger">
              poll fallback: {data.low_latency.poll_fallback_leaders.join(", ")}
            </div>
          ) : null}
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Baseline Existing Positions</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="baseline ready" value={String(data.baseline.baseline_ready)} intent={data.baseline.baseline_ready ? "ok" : "danger"} />
            <Metric label="captured leaders" value={String(data.baseline.baseline_captured_for_all_enabled_leaders)} intent={data.baseline.baseline_captured_for_all_enabled_leaders ? "ok" : "danger"} />
            <Metric label="ignored existing" value={String(data.baseline.ignored_existing_positions_count)} />
            <Metric label="baseline unknown" value={String(data.baseline.baseline_unknown_count)} intent={data.baseline.baseline_unknown_count ? "danger" : "ok"} />
          </div>
          {data.baseline.waiting_until_flat_positions.length ? (
            <table className="w-full border-t border-line text-left text-xs">
              <thead className="bg-panel text-slate-500">
                <tr>
                  <th className="px-4 py-2">Leader</th>
                  <th className="px-4 py-2">DEX</th>
                  <th className="px-4 py-2">Coin</th>
                  <th className="px-4 py-2">Side</th>
                  <th className="px-4 py-2">Enable Notional</th>
                  <th className="px-4 py-2">Entry Price</th>
                  <th className="px-4 py-2">Mark Price</th>
                  <th className="px-4 py-2">Copy Status</th>
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
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Follower Account</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="abstraction mode" value={data.account_states.follower.account_abstraction_mode ?? "--"} />
            <Metric label="account value used" value={data.account_states.follower.account_value_used_for_sizing ?? "--"} />
            <Metric label="source" value={data.account_states.follower.account_value_source ?? data.account_states.follower.balance_source ?? "--"} />
            <Metric label="available collateral" value={data.account_states.follower.available_collateral_used_for_margin_check ?? data.account_states.follower.withdrawable ?? "--"} />
            <Metric label="spot USDC" value={data.account_states.follower.spot_usdc_total ?? data.account_states.follower.debug?.spot_usdc_total ?? "--"} />
            <Metric label="portfolio value" value={data.account_states.follower.portfolio_account_value ?? "--"} />
            <Metric label="fresh" value={String(!data.account_states.follower.stale)} intent={data.account_states.follower.stale ? "danger" : "ok"} />
            <Metric label="leader states loaded" value={String(data.account_states.leaders.filter((item) => item.updated_at && !item.error_message).length)} />
          </div>
          {data.account_states.follower.error_message ? (
            <div className="border-t border-line px-4 py-3 text-sm text-danger">{data.account_states.follower.error_message}</div>
          ) : null}
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Dex Readiness</div>
          <DexReadinessTable rows={data.hyperliquid_venue.dex_readiness ?? []} />
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Market Coverage</div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="enabled dexes" value={String(marketCoverage(data).enabled_dex_count ?? 0)} />
            <Metric label="markets loaded" value={String(marketCoverage(data).markets_loaded_count ?? 0)} intent={marketCoverage(data).markets_loaded_count ? "ok" : "danger"} />
            <Metric label="ALL_COINS covers HIP-3" value={String(marketCoverage(data).all_coins_mode_includes_hip3_tradfi_unknown)} intent={marketCoverage(data).all_coins_mode_includes_hip3_tradfi_unknown ? "ok" : "danger"} />
            <Metric label="Binance mapping required" value={String(marketCoverage(data).binance_mapping_required_for_hyperliquid)} intent={marketCoverage(data).binance_mapping_required_for_hyperliquid ? "danger" : "ok"} />
            <Metric label="unknown products hidden" value={String(marketCoverage(data).product_type_unknown_hidden)} intent={marketCoverage(data).product_type_unknown_hidden ? "danger" : "ok"} />
            <Metric label="static coin filter" value={String(!marketCoverage(data).no_static_coin_filter)} intent={marketCoverage(data).no_static_coin_filter ? "ok" : "danger"} />
          </div>
          <MarketCoverageTable rows={marketCoverage(data).rows ?? []} />
        </section>
      ) : null}

      {data?.risk_settings ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3">
            <div className="text-sm font-semibold">Hyperliquid Risk Settings</div>
            <div className="mt-1 text-xs text-slate-500">
              margin mode / effective leverage gate, {data.risk_settings.effective_leverage_rule}, TTL {data.risk_settings.ttl_seconds}s
            </div>
          </div>
          <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
            <Metric label="enabled" value={String(data.risk_settings.risk_settings_enabled)} intent={data.risk_settings.risk_settings_enabled ? "ok" : "danger"} />
            <Metric
              label="margin mode setup"
              value={String(data.risk_settings.margin_mode_setup_enabled ?? data.risk_settings.isolated_setup_enabled)}
              intent={(data.risk_settings.margin_mode_setup_enabled ?? data.risk_settings.isolated_setup_enabled) ? "ok" : "danger"}
            />
            <Metric label="leverage setup" value={String(data.risk_settings.leverage_setup_enabled)} intent={data.risk_settings.leverage_setup_enabled ? "ok" : "danger"} />
            <Metric label="default leverage" value={String(data.risk_settings.target_default_leverage)} />
            <Metric label="confirmed markets" value={String(data.risk_settings.markets_confirmed_count)} />
            <Metric label="failed markets" value={String(data.risk_settings.markets_failed_count)} intent={data.risk_settings.markets_failed_count ? "danger" : "ok"} />
            <Metric label="unknown markets" value={String(data.risk_settings.markets_unknown_count)} intent={data.risk_settings.markets_unknown_count ? "danger" : "ok"} />
            <Metric label="blockers" value={String(data.risk_settings.blockers.length)} intent={data.risk_settings.blockers.length ? "danger" : "ok"} />
          </div>
          <RiskSettingsTable rows={data.risk_settings.rows} />
          {data.risk_settings.blockers.length ? (
            <div className="border-t border-line px-4 py-3 text-xs text-danger">{data.risk_settings.blockers.join("; ")}</div>
          ) : null}
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Small Live Start Checklist</div>
          <table className="w-full text-left text-sm">
            <thead className="bg-panel text-slate-500">
              <tr>
                <th className="px-4 py-3">Check</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Message</th>
              </tr>
            </thead>
            <tbody>
              {data.small_live_start_checklist.checks.map((item) => (
                <tr key={item.name} className="border-t border-line">
                  <td className="px-4 py-3">{item.name}</td>
                  <td className={`px-4 py-3 ${statusClass(item.status)}`}>{item.status}</td>
                  <td className="px-4 py-3 text-slate-600">{item.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}

      {data ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3">
            <div className="text-sm font-semibold">ACCOUNT_RATIO Sizing</div>
            <div className="mt-1 text-xs text-slate-500">
              跟单倍率是在按账户比例跟单后的缩放倍数，不是直接乘 leader 的名义仓位。
            </div>
          </div>
          <table className="w-full text-left text-xs">
            <thead className="bg-panel text-slate-500">
              <tr>
                <th className="px-4 py-3">Leader</th>
                <th className="px-4 py-3">Coin</th>
                <th className="px-4 py-3">Entry Price</th>
                <th className="px-4 py-3">Mark Price</th>
                <th className="px-4 py-3">PnL</th>
                <th className="px-4 py-3">Open Time</th>
                <th className="px-4 py-3">Leader account</th>
                <th className="px-4 py-3">Leader notional</th>
                <th className="px-4 py-3">Ratio</th>
                <th className="px-4 py-3">Follower account</th>
                <th className="px-4 py-3">Multiplier</th>
                <th className="px-4 py-3">Target</th>
                <th className="px-4 py-3">Current</th>
                <th className="px-4 py-3">Delta</th>
              </tr>
            </thead>
            <tbody>
              {sizingRows(data).length ? (
                sizingRows(data).map((row) => (
                  <tr key={`${row.leader}-${row.coin}-${row.side}`} className="border-t border-line">
                    <td className="max-w-[180px] truncate px-4 py-3 font-mono">{row.leader}</td>
                    <td className="px-4 py-3 font-mono">{row.coin} {row.side}</td>
                    <td className="px-4 py-3">{formatPrice(row.entry_px)}</td>
                    <td className="px-4 py-3">{formatPrice(row.mark_px ?? row.mid_px)}</td>
                    <td className="px-4 py-3">{formatNotional(row.unrealized_pnl)}</td>
                    <td className="px-4 py-3">{formatOpenTime(row)}</td>
                    <td className="px-4 py-3">
                      {formatNotional(row.sizing.leader_account_value_used_for_sizing ?? row.sizing.leader_account_value)}
                      <div className="text-[11px] text-slate-500">{row.sizing.leader_account_value_source ?? "--"} / {row.sizing.leader_account_abstraction_mode ?? "--"}</div>
                    </td>
                    <td className="px-4 py-3">{formatNotional(row.sizing.leader_position_notional)}</td>
                    <td className="px-4 py-3">{formatDisplayValue(row.sizing.leader_position_ratio)}</td>
                    <td className="px-4 py-3">
                      {formatNotional(row.sizing.follower_account_value_used_for_sizing ?? row.sizing.follower_account_value)}
                      <div className="text-[11px] text-slate-500">{row.sizing.follower_account_value_source ?? "--"} / {row.sizing.follower_account_abstraction_mode ?? "--"}</div>
                    </td>
                    <td className="px-4 py-3">{formatDisplayValue(row.sizing.copy_multiplier)}</td>
                    <td className="px-4 py-3">{row.sizing.target_notional ? formatNotional(row.sizing.target_notional) : row.sizing.error ?? "--"}</td>
                    <td className="px-4 py-3">{formatNotional(row.sizing.current_allocation)}</td>
                    <td className="px-4 py-3">{formatNotional(row.sizing.delta_notional)}</td>
                  </tr>
                ))
              ) : (
                <tr className="border-t border-line">
                  <td className="px-4 py-3 text-slate-500" colSpan={14}>No leader positions to size</td>
                </tr>
              )}
            </tbody>
          </table>
        </section>
      ) : null}

      {data ? (
        <div className="mb-4 grid gap-4 lg:grid-cols-2">
          <ReadinessBlock title="Hyperliquid Venue Readiness" venue={data.hyperliquid_venue} readyKey="ready_for_live_hyperliquid" />
          <ReadinessBlock title="Binance Venue Readiness" venue={data.binance_venue} readyKey="ready_for_live_binance" />
        </div>
      ) : null}

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Auto Execution</div>
        <div className="grid gap-3 p-4 text-sm md:grid-cols-4">
          <div className="rounded-md border border-line bg-panel p-3">
            <div className="mb-1 text-xs text-slate-500">auto_copy_order_type</div>
            <div className="font-medium text-ink">{String(data?.global.auto_copy_order_type ?? "--")}</div>
          </div>
          <div className="rounded-md border border-line bg-panel p-3">
            <div className="mb-1 text-xs text-slate-500">pending_unknown_orders</div>
            <div className={data?.pending_unknown_orders_count ? "font-medium text-danger" : "font-medium text-ink"}>
              {data?.pending_unknown_orders_count ?? "--"}
            </div>
          </div>
          <div className="rounded-md border border-line bg-panel p-3">
            <div className="mb-1 text-xs text-slate-500">recent_avg_latency</div>
            <div className="font-medium text-ink">{formatMs(data?.latency.recent_avg_latency)}</div>
          </div>
          <div className="rounded-md border border-line bg-panel p-3">
            <div className="mb-1 text-xs text-slate-500">recent_max_latency</div>
            <div className="font-medium text-ink">{formatMs(data?.latency.recent_max_latency)}</div>
          </div>
          <div className="rounded-md border border-line bg-panel p-3">
            <div className="mb-1 text-xs text-slate-500">last_ws_to_submit</div>
            <div className="font-medium text-ink">{formatMs(data?.latency.last_auto_order_latency?.ws_to_submit_ms)}</div>
          </div>
        </div>
      </section>

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Aggregate Positions</div>
        <table className="w-full text-left text-sm">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Allocated Long</th>
              <th className="px-4 py-3">Allocated Short</th>
              <th className="px-4 py-3">Binance Long</th>
              <th className="px-4 py-3">Binance Short</th>
            </tr>
          </thead>
          <tbody>
            {data?.aggregate_positions.length ? (
              data.aggregate_positions.map((item) => (
                <tr key={item.symbol} className="border-t border-line">
                  <td className="px-4 py-3 font-mono text-xs">{item.symbol}</td>
                  <td className="px-4 py-3">{item.allocated_long_qty}</td>
                  <td className="px-4 py-3">{item.allocated_short_qty}</td>
                  <td className="px-4 py-3">{item.binance_long_qty}</td>
                  <td className="px-4 py-3">{item.binance_short_qty}</td>
                </tr>
              ))
            ) : (
              <tr className="border-t border-line">
                <td className="px-4 py-3 text-slate-500" colSpan={5}>No aggregate positions</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Symbols</div>
        <table className="w-full text-left text-sm">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Coin</th>
              <th className="px-4 py-3">Margin</th>
              <th className="px-4 py-3">Leverage</th>
              <th className="px-4 py-3">Position</th>
              <th className="px-4 py-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {data?.symbols.map((item) => (
              <tr key={`${item.coin}-${item.symbol}`} className="border-t border-line">
                <td className="px-4 py-3 font-mono text-xs">{item.symbol}</td>
                <td className="px-4 py-3">{item.coin}</td>
                <td className="px-4 py-3">
                  {item.current_margin_mode} / {item.expected_margin_mode}
                </td>
                <td className="px-4 py-3">
                  {item.current_leverage ?? "--"} / {item.expected_leverage}
                </td>
                <td className="px-4 py-3">
                  {item.current_position_side} {formatNotional(item.current_position_notional)}
                </td>
                <td className={`px-4 py-3 ${statusClass(item.status)}`}>{item.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Leader Allocations</div>
        <table className="w-full text-left text-sm">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Leader</th>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Venue</th>
              <th className="px-4 py-3">Side</th>
              <th className="px-4 py-3">Target</th>
              <th className="px-4 py-3">Allocated</th>
              <th className="px-4 py-3">Qty</th>
              <th className="px-4 py-3">Sizing</th>
              <th className="px-4 py-3">Delta</th>
              <th className="px-4 py-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {data?.allocations.length ? (
              data.allocations.map((item) => (
                <tr key={`${item.leader_address}-${item.symbol}-${item.position_side}`} className="border-t border-line">
                  <td className="max-w-xs truncate px-4 py-3 font-mono text-xs">{item.leader_address}</td>
                  <td className="px-4 py-3 font-mono text-xs">{item.symbol}</td>
                  <td className="px-4 py-3">{item.execution_venue}</td>
                  <td className="px-4 py-3">{item.position_side}</td>
                  <td className="px-4 py-3">{formatNotional(item.target_notional)}</td>
                  <td className="px-4 py-3">{formatNotional(item.allocated_notional)}</td>
                  <td className="px-4 py-3">{formatQuantity(item.allocated_qty)}</td>
                  <td className="px-4 py-3">{item.sizing_mode}</td>
                  <td className="px-4 py-3">{formatNotional(item.delta_notional)}</td>
                  <td className={`px-4 py-3 ${statusClass(item.status)}`}>{item.status}</td>
                </tr>
              ))
            ) : (
              <tr className="border-t border-line">
                <td className="px-4 py-3 text-slate-500" colSpan={10}>No allocations</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      {data?.allocation_mismatches.length ? (
        <section className="panel mb-4 overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">Allocation Mismatches</div>
          <table className="w-full text-left text-sm">
            <thead className="bg-panel text-slate-500">
              <tr>
                <th className="px-4 py-3">Symbol</th>
                <th className="px-4 py-3">Venue</th>
                <th className="px-4 py-3">Allocated L/S</th>
                <th className="px-4 py-3">Follower L/S</th>
                <th className="px-4 py-3">Status</th>
              </tr>
            </thead>
            <tbody>
              {data.allocation_mismatches.map((item) => (
                <tr key={item.symbol} className="border-t border-line">
                  <td className="px-4 py-3 font-mono text-xs">{item.symbol}</td>
                  <td className="px-4 py-3">{item.venue ?? "BINANCE"}</td>
                  <td className="px-4 py-3">{item.allocated_long_qty} / {item.allocated_short_qty}</td>
                  <td className="px-4 py-3">
                    {item.binance_long_qty ?? item.hyperliquid_long_qty ?? "--"} / {item.binance_short_qty ?? item.hyperliquid_short_qty ?? "--"}
                  </td>
                  <td className={`px-4 py-3 ${statusClass(item.status)}`}>{item.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}

      <section className="panel overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Leaders</div>
        <table className="w-full text-left text-sm">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Address</th>
              <th className="px-4 py-3">State</th>
              <th className="px-4 py-3">Watcher</th>
              <th className="px-4 py-3">Account</th>
              <th className="px-4 py-3">Age</th>
              <th className="px-4 py-3">Coins</th>
              <th className="px-4 py-3">Multiplier</th>
              <th className="px-4 py-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {data?.leaders.map((item) => (
              <tr key={item.address} className="border-t border-line">
                <td className="max-w-sm truncate px-4 py-3 font-mono text-xs">{item.address}</td>
                <td className="px-4 py-3">{item.deleted_at ? "deleted" : item.enabled ? "enabled" : "disabled"}</td>
                <td className="px-4 py-3">
                  <div>{item.watcher_status}</div>
                  <div className="text-xs text-slate-500">{item.websocket_connected ? "state connected" : "state offline"}</div>
                </td>
                <td className="px-4 py-3">{formatNotional(item.accountValue)}</td>
                <td className="px-4 py-3">
                  {item.last_update_age === null ? "--" : `${item.last_update_age}s`}
                </td>
                <td className="px-4 py-3">
                  <div>{item.allowed_coins_mode}</div>
                  <div className="text-xs text-slate-500">{item.allowed_symbols?.join(", ") || "all coins"}</div>
                </td>
                <td className="px-4 py-3">{formatDisplayValue(item.copy_multiplier)}</td>
                <td className={`px-4 py-3 ${statusClass(item.status)}`}>{item.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </AppShell>
  );
}

function statusClass(status: string) {
  if (status === "OK") return "text-accent";
  if (status === "WARNING" || status === "STALE") return "text-warn";
  return "text-danger";
}

function formatMs(value: number | null | undefined) {
  return formatMsLabel(value);
}

function formatDateTime(value: string | null | undefined) {
  return formatDateTimeLabel(value);
}

function sizingRows(data: Preflight) {
  return data.account_states.leaders.flatMap((leader) =>
    leader.positions
      .filter((position) => position.sizing)
      .map((position) => ({
        leader: leader.leader?.leader_address ?? leader.address ?? "--",
        coin: position.canonical_coin ?? position.coin,
        side: position.side,
        entry_px: position.entry_px,
        mark_px: position.mark_px,
        mid_px: position.mid_px,
        unrealized_pnl: position.unrealized_pnl,
        open_time: position.open_time,
        first_seen_at: position.first_seen_at,
        open_time_source: position.open_time_source,
        sizing: position.sizing as SizingBreakdown,
      }))
  );
}

function formatOpenTime(position: ReturnType<typeof sizingRows>[number]) {
  return formatOpenTimeLabel(position);
}

function DexPriceCache({ rows }: { rows: Record<string, DexPriceCacheStatus> }) {
  const entries = Object.entries(rows);
  if (!entries.length) return null;
  return (
    <table className="w-full border-t border-line text-left text-xs">
      <thead className="bg-panel text-slate-500">
        <tr>
          <th className="px-4 py-2">DEX</th>
          <th className="px-4 py-2">Fresh</th>
          <th className="px-4 py-2">Markets</th>
          <th className="px-4 py-2">Stale</th>
          <th className="px-4 py-2">Last update age</th>
        </tr>
      </thead>
      <tbody>
        {entries.map(([dex, item]) => (
          <tr key={dex || "default"} className="border-t border-line">
            <td className="px-4 py-2">{dex || "default"}</td>
            <td className={item.fresh ? "px-4 py-2 text-accent" : "px-4 py-2 text-danger"}>{String(item.fresh)}</td>
            <td className="px-4 py-2">{item.markets_count}</td>
            <td className="px-4 py-2">{item.stale_markets_count}</td>
            <td className="px-4 py-2">{formatMs(item.last_price_update_age_ms)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DexStates({ states }: { states: AccountState[] }) {
  if (!states.length) return null;
  return (
    <table className="w-full border-t border-line text-left text-xs">
      <thead className="bg-panel text-slate-500">
        <tr>
          <th className="px-4 py-2">DEX</th>
          <th className="px-4 py-2">accountValue</th>
          <th className="px-4 py-2">withdrawable</th>
          <th className="px-4 py-2">positions</th>
          <th className="px-4 py-2">status</th>
        </tr>
      </thead>
      <tbody>
        {states.map((state) => (
          <tr key={state.dex ?? "default"} className="border-t border-line">
            <td className="px-4 py-2">{state.dex_display_name ?? state.dex ?? "Hyperliquid"}</td>
            <td className="px-4 py-2">{formatNotional(state.account_value)}</td>
            <td className="px-4 py-2">{formatNotional(state.withdrawable)}</td>
            <td className="px-4 py-2">{state.positions.length}</td>
            <td className={state.error_message || state.stale ? "px-4 py-2 text-danger" : "px-4 py-2 text-accent"}>
              {state.error_message ?? (state.stale ? "STALE" : "OK")}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DexReadinessTable({ rows }: { rows: DexReadiness[] }) {
  if (!rows.length) return <div className="p-4 text-sm text-slate-500">No DEX readiness data</div>;
  return (
    <table className="w-full text-left text-xs">
      <thead className="bg-panel text-slate-500">
        <tr>
          <th className="px-4 py-2">DEX</th>
          <th className="px-4 py-2">HIP-3</th>
          <th className="px-4 py-2">Meta</th>
          <th className="px-4 py-2">Mids</th>
          <th className="px-4 py-2">State</th>
          <th className="px-4 py-2">Sizing value</th>
          <th className="px-4 py-2">Positions</th>
          <th className="px-4 py-2">Asset IDs</th>
          <th className="px-4 py-2">Low latency</th>
          <th className="px-4 py-2">Ready</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.dex_name || "default"} className="border-t border-line">
            <td className="px-4 py-2">{row.display_name}</td>
            <td className="px-4 py-2">{String(row.is_hip3)}</td>
            <td className="px-4 py-2">{String(row.meta_loaded)} / {row.universe_count}</td>
            <td className="px-4 py-2">{String(row.mids_fresh)}</td>
            <td className="px-4 py-2">{row.account_state_loaded_for_follower ? "loaded" : "missing"}</td>
            <td className="px-4 py-2">
              {formatNotional(row.account_value_used_for_sizing)}
              <div className="text-[11px] text-slate-500">{row.account_value_source ?? "--"} / {row.account_abstraction_mode ?? "--"}</div>
            </td>
            <td className="px-4 py-2">{row.open_positions_count}</td>
            <td className="px-4 py-2">{String(row.asset_id_mapping_ready)}</td>
            <td className="px-4 py-2">{String(row.low_latency_watcher_subscribed)}</td>
            <td className={row.ready_for_live_for_dex ? "px-4 py-2 text-accent" : "px-4 py-2 text-danger"}>
              {String(row.ready_for_live_for_dex)}
              {!row.ready_for_live_for_dex ? <div className="text-slate-500">{row.message}</div> : null}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function MarketCoverageTable({ rows }: { rows: MarketCoverage["rows"] }) {
  if (!rows.length) return <div className="border-t border-line p-4 text-sm text-slate-500">No market coverage data</div>;
  return (
    <table className="w-full border-t border-line text-left text-xs">
      <thead className="bg-panel text-slate-500">
        <tr>
          <th className="px-4 py-2">DEX</th>
          <th className="px-4 py-2">HIP-3</th>
          <th className="px-4 py-2">Markets</th>
          <th className="px-4 py-2">Meta / Mids</th>
          <th className="px-4 py-2">Unknown Product</th>
          <th className="px-4 py-2">Asset IDs</th>
          <th className="px-4 py-2">Status</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.dex || "default"} className="border-t border-line">
            <td className="px-4 py-2">{row.display_name || row.dex || "Hyperliquid"}</td>
            <td className="px-4 py-2">{String(row.is_hip3)}</td>
            <td className="px-4 py-2">{row.markets_loaded_count}</td>
            <td className="px-4 py-2">{row.meta_universe_count} / {row.mids_markets_count}</td>
            <td className="px-4 py-2">{row.unknown_product_markets_count}</td>
            <td className="px-4 py-2">{String(row.asset_id_mapping_ready)}</td>
            <td className={row.status === "OK" ? "px-4 py-2 text-accent" : "px-4 py-2 text-danger"}>
              {row.status}
              {row.status !== "OK" ? <div className="text-slate-500">{row.message}</div> : null}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RiskSettingsTable({ rows }: { rows: RiskSettingRow[] }) {
  if (!rows.length) return <div className="border-t border-line p-4 text-sm text-slate-500">No risk setting candidates</div>;
  return (
    <table className="w-full border-t border-line text-left text-xs">
      <thead className="bg-panel text-slate-500">
        <tr>
          <th className="px-4 py-2">DEX</th>
          <th className="px-4 py-2">Coin</th>
          <th className="px-4 py-2">Required</th>
          <th className="px-4 py-2">Max / Effective</th>
          <th className="px-4 py-2">Actual</th>
          <th className="px-4 py-2">Status</th>
          <th className="px-4 py-2">Confirmed</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.dex || "default"}-${row.canonical_coin}`} className="border-t border-line">
            <td className="px-4 py-2">{row.dex_display_name || row.dex || "Hyperliquid"}</td>
            <td className="px-4 py-2 font-mono">{row.canonical_coin}</td>
            <td className="px-4 py-2">{row.risk_setting_required ? "yes" : "no"}<div className="text-[11px] text-slate-500">{row.reason ?? "--"}</div></td>
            <td className="px-4 py-2">{row.market_max_leverage ?? "--"} / {row.effective_leverage ?? "--"}x</td>
            <td className="px-4 py-2">{row.actual_margin_mode ?? "--"} / {row.actual_leverage ?? "--"}x</td>
            <td className={`px-4 py-2 ${riskStatusClass(row.status, row.cache_stale)}`}>
              {row.cache_stale ? "NEEDS_REFRESH" : row.status}
              {row.error ? <div className="text-slate-500">{row.error}</div> : null}
            </td>
            <td className="px-4 py-2">{formatDateTime(row.last_confirmed_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function marketCoverage(data: Preflight): MarketCoverage {
  return data.market_coverage ?? data.hyperliquid_venue.market_coverage ?? {
    enabled_dexes: [],
    enabled_dex_count: 0,
    markets_loaded_count: 0,
    markets_loaded_count_by_dex: {},
    unknown_product_markets_count: 0,
    all_coins_mode_includes_enabled_dex_markets: false,
    all_coins_mode_includes_hip3_tradfi_unknown: false,
    binance_mapping_required_for_hyperliquid: true,
    product_type_unknown_hidden: true,
    no_static_coin_filter: false,
    canonical_scope_keys: [],
    rows: []
  };
}

function ReadinessBlock({
  title,
  venue,
  readyKey
}: {
  title: string;
  venue: VenueReadiness;
  readyKey: "ready_for_live_hyperliquid" | "ready_for_live_binance";
}) {
  const ready = Boolean(venue[readyKey]);
  return (
    <section className="panel overflow-hidden">
      <div className="border-b border-line px-4 py-3 text-sm font-semibold">{title}</div>
      <div className="grid gap-3 p-4 text-sm md:grid-cols-2">
        <Metric label="enabled" value={String(venue.enabled)} />
        <Metric label="trading_enabled" value={String(venue.trading_enabled)} />
        <Metric label="api_connected" value={String(venue.api_connected)} />
        <Metric label="ready_for_live" value={String(ready)} intent={ready ? "ok" : "danger"} />
        <Metric label="live_trading_allowed" value={String(venue.live_trading_allowed)} />
        <Metric label="unknown_orders" value={String(venue.unknown_orders_count)} />
        {venue.network ? <Metric label="network" value={venue.network} /> : null}
        {venue.current_position_mode ? <Metric label="position_mode" value={venue.current_position_mode} /> : null}
        {venue.accountValue ? <Metric label="accountValue" value={formatNotional(venue.accountValue)} /> : null}
        {venue.withdrawable ? <Metric label="withdrawable" value={formatNotional(venue.withdrawable)} /> : null}
        {venue.coin_scope ? <Metric label="coin_scope" value={venue.coin_scope} /> : null}
      </div>
      {venue.enabled_coins?.length ? (
        <div className="border-t border-line px-4 py-3 text-xs text-slate-500">
          {venue.enabled_coins.join(", ")}
        </div>
      ) : null}
      {venue.symbols?.length ? (
        <table className="w-full border-t border-line text-left text-xs">
          <thead className="bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-2">Coin</th>
              <th className="px-4 py-2">Venue Symbol</th>
              <th className="px-4 py-2">Meta</th>
              <th className="px-4 py-2">Leverage</th>
              <th className="px-4 py-2">Risk</th>
            </tr>
          </thead>
          <tbody>
            {venue.symbols.map((item) => (
              <tr key={`${item.coin ?? item.symbol}-${item.venue_symbol ?? ""}`} className="border-t border-line">
                <td className="px-4 py-2 font-mono">{item.coin ?? item.symbol ?? "--"}</td>
                <td className="px-4 py-2 font-mono">{item.venue_symbol ?? item.symbol ?? "--"}</td>
                <td className="px-4 py-2">{item.exists_in_meta === undefined ? "--" : String(item.exists_in_meta)}</td>
                <td className="px-4 py-2">{item.target_leverage ?? "--"} / max {item.max_leverage ?? "--"}</td>
                <td className={`px-4 py-2 ${statusClass(item.risk_status ?? item.status)}`}>
                  {item.risk_status ?? item.status}
                  {item.warning ? <div className="mt-1 text-slate-500">{item.warning}</div> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
      {venue.blocking_reasons.length ? (
        <div className="border-t border-line px-4 py-3 text-xs text-danger">
          {venue.blocking_reasons.join("; ")}
        </div>
      ) : null}
    </section>
  );
}

function Metric({ label, value, intent }: { label: string; value: string; intent?: "ok" | "danger" }) {
  return (
    <div className="mini-card">
      <div className="mini-label">{label}</div>
      <div className={`mini-value ${intent === "ok" ? "text-accent" : intent === "danger" ? "text-danger" : "text-ink"}`}>
        {formatDisplayValue(value)}
      </div>
    </div>
  );
}

function riskStatusClass(status: string, cacheStale: boolean) {
  if (!cacheStale && status === "CONFIRMED") return "text-accent";
  if (status === "FAILED" || status === "UNKNOWN" || cacheStale) return "text-danger";
  return "text-warn";
}
