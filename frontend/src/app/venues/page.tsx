"use client";

import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";

type VenueItem = {
  venue: string;
  enabled: boolean;
  trading_enabled: boolean;
  network: string;
  api_connected: boolean;
  status: string;
  wallet_configured: boolean;
  vault_address_configured: boolean;
  private_key_configured: boolean;
  account_address_ambiguous?: boolean;
  default_leverage: number;
  margin_mode: string;
  mapping_count: number;
  dexes?: Array<{
    dex_name: string;
    display_name: string;
    enabled: boolean;
    is_hip3: boolean;
    meta_status: string;
    tradable_markets_count: number;
    low_latency_status: string;
  }>;
};

type VenuesResponse = {
  default_preferred_venue: string;
  fallback_venue: string;
  global_trading_enabled: boolean;
  venues: VenueItem[];
};

export default function VenuesPage() {
  const [data, setData] = useState<VenuesResponse | null>(null);
  const [error, setError] = useState("");

  async function load() {
    setError("");
    try {
      setData(await apiFetch<VenuesResponse>("/venues"));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed");
    }
  }

  useEffect(() => {
    load();
  }, []);

  return (
    <AppShell>
      <Header
        title="Venues"
        right={
          <button className="btn btn-muted" type="button" onClick={load}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}

      <section className="panel mb-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Execution Policy</div>
        <div className="grid gap-3 p-4 text-sm md:grid-cols-3">
          <Metric label="Default Preferred" value={data?.default_preferred_venue ?? "--"} />
          <Metric label="Fallback" value={data?.fallback_venue ?? "--"} />
          <Metric label="Global Trading" value={String(data?.global_trading_enabled ?? false)} />
        </div>
      </section>

      <section className="panel overflow-hidden">
        <table className="w-full text-left text-sm">
          <thead className="border-b border-line bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Venue</th>
              <th className="px-4 py-3">Enabled</th>
              <th className="px-4 py-3">Trading</th>
              <th className="px-4 py-3">Network</th>
              <th className="px-4 py-3">API</th>
              <th className="px-4 py-3">Wallet</th>
              <th className="px-4 py-3">Risk</th>
              <th className="px-4 py-3">Mappings</th>
            </tr>
          </thead>
          <tbody>
            {data?.venues.map((venue) => (
              <tr key={venue.venue} className="border-b border-line last:border-0">
                <td className="px-4 py-3 font-mono text-xs">{venue.venue}</td>
                <td className="px-4 py-3">{String(venue.enabled)}</td>
                <td className="px-4 py-3">{String(venue.trading_enabled)}</td>
                <td className="px-4 py-3">{venue.network}</td>
                <td className={`px-4 py-3 ${venue.api_connected ? "text-accent" : "text-danger"}`}>
                  {venue.api_connected ? "connected" : venue.status}
                </td>
                <td className="px-4 py-3">
                  wallet {String(venue.wallet_configured)} / vault {String(venue.vault_address_configured)}
                  {venue.account_address_ambiguous ? <div className="text-xs text-danger">account address ambiguous</div> : null}
                </td>
                <td className="px-4 py-3">
                  {venue.margin_mode} {venue.default_leverage}x
                </td>
                <td className="px-4 py-3">{venue.mapping_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel mt-4 overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Hyperliquid DEXes</div>
        <table className="w-full text-left text-sm">
          <thead className="border-b border-line bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">DEX</th>
              <th className="px-4 py-3">Enabled</th>
              <th className="px-4 py-3">HIP-3</th>
              <th className="px-4 py-3">Meta</th>
              <th className="px-4 py-3">Markets</th>
              <th className="px-4 py-3">Low latency</th>
            </tr>
          </thead>
          <tbody>
            {data?.venues.find((venue) => venue.venue === "HYPERLIQUID")?.dexes?.map((dex) => (
              <tr key={dex.dex_name || "default"} className="border-b border-line last:border-0">
                <td className="px-4 py-3">{dex.display_name}</td>
                <td className="px-4 py-3">{String(dex.enabled)}</td>
                <td className="px-4 py-3">{String(dex.is_hip3)}</td>
                <td className="px-4 py-3">{dex.meta_status}</td>
                <td className="px-4 py-3">{dex.tradable_markets_count}</td>
                <td className="px-4 py-3">{dex.low_latency_status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </AppShell>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-line bg-panel p-3">
      <div className="mb-1 text-xs text-slate-500">{label}</div>
      <div className="font-medium text-ink">{value}</div>
    </div>
  );
}
