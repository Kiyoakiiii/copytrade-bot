"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { API_BASE } from "@/lib/api";

export type DashboardStreamEvent<T = unknown> = {
  event_id: string;
  event_type: string;
  server_time: string;
  data_version: string;
  payload: T;
  stale: boolean;
  stale_flags: Record<string, boolean>;
  data_age_ms: number | null;
};

export type RealtimeStatus = {
  mode: "stream" | "polling fallback";
  connected: boolean;
  lastEventAt: string | null;
  lastUpdatedAgeMs: number | null;
  message: string;
};

type UseDashboardStreamOptions = {
  onEvent?: (event: DashboardStreamEvent) => void;
  onSnapshot?: (payload: any) => void;
};

export function useDashboardStream(options: UseDashboardStreamOptions = {}): RealtimeStatus {
  const optionsRef = useRef(options);
  const [connected, setConnected] = useState(false);
  const [lastEventAt, setLastEventAt] = useState<string | null>(null);
  const [nowTick, setNowTick] = useState(() => Date.now());

  useEffect(() => {
    optionsRef.current = options;
  }, [options]);

  useEffect(() => {
    const timer = window.setInterval(() => setNowTick(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let closed = false;
    const source = new EventSource(`${API_BASE}/stream/dashboard`, { withCredentials: true });

    source.onopen = () => {
      if (!closed) setConnected(true);
    };
    source.onerror = () => {
      if (!closed) setConnected(false);
    };

    const handle = (message: MessageEvent<string>) => {
      if (closed) return;
      try {
        const event = JSON.parse(message.data) as DashboardStreamEvent;
        setConnected(true);
        setLastEventAt(new Date().toISOString());
        optionsRef.current.onEvent?.(event);
        if (event.event_type === "dashboard_snapshot") {
          optionsRef.current.onSnapshot?.(event.payload);
        }
      } catch {
        setConnected(false);
      }
    };

    for (const eventType of [
      "dashboard_snapshot",
      "follower_state_update",
      "leader_state_update",
      "positions_update",
      "orders_update",
      "latency_update",
      "preflight_update",
      "watcher_status_update",
      "task_health_update",
      "baseline_status_update",
      "allocation_status_update",
      "heartbeat",
    ]) {
      source.addEventListener(eventType, handle as EventListener);
    }

    return () => {
      closed = true;
      source.close();
    };
  }, []);

  const lastUpdatedAgeMs = useMemo(() => {
    if (!lastEventAt) return null;
    return Math.max(0, nowTick - new Date(lastEventAt).getTime());
  }, [lastEventAt, nowTick]);

  return {
    mode: connected ? "stream" : "polling fallback",
    connected,
    lastEventAt,
    lastUpdatedAgeMs,
    message: connected ? "Realtime stream connected" : "Realtime stream disconnected, using 1s polling fallback",
  };
}

export function useRealtimeFallbackPolling(
  status: RealtimeStatus,
  load: () => void | Promise<void>,
  options: { fallbackMs?: number; reconcileMs?: number } = {},
) {
  const fallbackMs = options.fallbackMs ?? 1000;
  const reconcileMs = options.reconcileMs ?? 45000;
  const loadRef = useRef(load);
  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    const interval = status.connected ? reconcileMs : fallbackMs;
    const timer = window.setInterval(() => {
      void loadRef.current();
    }, interval);
    return () => window.clearInterval(timer);
  }, [status.connected, fallbackMs, reconcileMs]);

  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState === "visible") {
        void loadRef.current();
      }
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => document.removeEventListener("visibilitychange", onVisible);
  }, []);
}
