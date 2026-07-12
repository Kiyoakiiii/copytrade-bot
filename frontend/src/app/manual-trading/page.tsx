"use client";

import { FormEvent, useState } from "react";
import { Send } from "lucide-react";
import { AppShell } from "@/components/AppShell";
import { Header } from "@/components/Header";
import { apiFetch } from "@/lib/api";

export default function ManualTradingPage() {
  const [executionVenue, setExecutionVenue] = useState("BINANCE");
  const [symbol, setSymbol] = useState("BTCUSDT");
  const [positionSide, setPositionSide] = useState("LONG");
  const [action, setAction] = useState("OPEN_OR_INCREASE");
  const [orderType, setOrderType] = useState("MARKET");
  const [notional, setNotional] = useState("");
  const [quantity, setQuantity] = useState("");
  const [price, setPrice] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [message, setMessage] = useState("");

  async function submit(event: FormEvent) {
    event.preventDefault();
    const confirmed = window.confirm("Submit this manual order? Manual orders are not part of auto-copy safety flow.");
    if (!confirmed) return;
    setMessage("");
    try {
      const result = await apiFetch<{ status: string }>("/manual-orders", {
        method: "POST",
        body: JSON.stringify({
          symbol,
          execution_venue: executionVenue,
          position_side: positionSide,
          action,
          order_type: orderType,
          notional: notional || null,
          quantity: quantity || null,
          price: price || null,
          confirmation
        })
      });
      setMessage(result.status);
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Order failed");
    }
  }

  return (
    <AppShell>
      <Header title="Manual Trading" />
      <form onSubmit={submit} className="panel grid max-w-3xl gap-4 p-4 md:grid-cols-2">
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Venue</span>
          <select
            className="field"
            value={executionVenue}
            onChange={(e) => {
              const venue = e.target.value;
              setExecutionVenue(venue);
              setOrderType("MARKET");
              setSymbol(venue === "HYPERLIQUID" ? symbol.replace(/USDT$/i, "") || "BTC" : `${symbol.replace(/USDT$/i, "") || "BTC"}USDT`);
            }}
          >
            <option value="BINANCE">BINANCE</option>
            <option value="HYPERLIQUID">HYPERLIQUID</option>
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">{executionVenue === "HYPERLIQUID" ? "Coin" : "Symbol"}</span>
          <input className="field" value={symbol} onChange={(e) => setSymbol(e.target.value.toUpperCase())} />
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Position Side</span>
          <select className="field" value={positionSide} onChange={(e) => setPositionSide(e.target.value)}>
            <option value="LONG">LONG</option>
            <option value="SHORT">SHORT</option>
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Action</span>
          <select className="field" value={action} onChange={(e) => setAction(e.target.value)}>
            <option value="OPEN_OR_INCREASE">Open / Increase</option>
            <option value="CLOSE_OR_REDUCE">Close / Reduce</option>
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Order Type</span>
          <select className="field" value={orderType} disabled={executionVenue === "HYPERLIQUID"} onChange={(e) => setOrderType(e.target.value)}>
            <option value="MARKET">MARKET</option>
            {executionVenue === "BINANCE" ? <option value="LIMIT">LIMIT</option> : null}
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Notional</span>
          <input className="field" inputMode="decimal" value={notional} onChange={(e) => setNotional(e.target.value)} />
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Quantity</span>
          <input className="field" inputMode="decimal" value={quantity} onChange={(e) => setQuantity(e.target.value)} />
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Limit Price</span>
          <input className="field" disabled={orderType === "MARKET"} inputMode="decimal" value={price} onChange={(e) => setPrice(e.target.value)} />
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-slate-500">Confirmation</span>
          <input className="field" value={confirmation} onChange={(e) => setConfirmation(e.target.value)} placeholder="CONFIRM" />
        </label>
        <div className="md:col-span-2 flex items-center gap-3">
          <button className="btn btn-primary" type="submit">
            <Send className="h-4 w-4" />
            Submit
          </button>
          {message ? <span className="text-sm text-slate-600">{message}</span> : null}
        </div>
      </form>
    </AppShell>
  );
}
