"use client";

import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";

type VenueMappingSide = {
  id: number | null;
  venue_symbol: string | null;
  enabled: boolean;
  tradable: boolean;
  mapping_status: string;
  reason: string | null;
  is_default: boolean;
};

type Mapping = {
  coin: string;
  hyperliquid: VenueMappingSide;
  binance: VenueMappingSide;
  default_venue: string;
  effective_venue: string;
  reason: string;
};

export default function SymbolMappingPage() {
  const [items, setItems] = useState<Mapping[]>([]);
  const [error, setError] = useState("");

  async function load() {
    setError("");
    try {
      setItems(await apiFetch<Mapping[]>("/venue-mappings"));
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
        title="Coins / Routing"
        right={
          <button className="btn btn-muted" type="button" onClick={load}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        }
      />
      {error ? <div className="mb-4 text-sm text-danger">{error}</div> : null}
      <div className="panel overflow-hidden">
        <table className="w-full text-left text-sm">
          <thead className="border-b border-line bg-panel text-slate-500">
            <tr>
              <th className="px-4 py-3">Coin</th>
              <th className="px-4 py-3">Hyperliquid</th>
              <th className="px-4 py-3">Binance</th>
              <th className="px-4 py-3">Default</th>
              <th className="px-4 py-3">Effective</th>
              <th className="px-4 py-3">Reason</th>
            </tr>
          </thead>
          <tbody>
            {items.length ? (
              items.map((item) => (
                <tr key={item.coin} className="border-b border-line last:border-0">
                  <td className="px-4 py-3 font-mono text-xs">{item.coin}</td>
                  <td className="px-4 py-3">
                    <div className="font-mono text-xs">{item.hyperliquid.venue_symbol ?? "--"}</div>
                    <div className={item.hyperliquid.tradable ? "text-xs text-accent" : "text-xs text-danger"}>
                      {item.hyperliquid.mapping_status}
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="font-mono text-xs">{item.binance.venue_symbol ?? "--"}</div>
                    <div className={item.binance.tradable ? "text-xs text-accent" : "text-xs text-danger"}>
                      {item.binance.mapping_status}
                    </div>
                  </td>
                  <td className="px-4 py-3">{item.default_venue}</td>
                  <td className={`px-4 py-3 ${item.effective_venue === "BLOCKED" ? "text-danger" : "text-accent"}`}>
                    {item.effective_venue}
                  </td>
                  <td className="px-4 py-3 text-slate-500">{item.reason}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td className="px-4 py-3 text-slate-500" colSpan={6}>No venue mappings</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </AppShell>
  );
}
