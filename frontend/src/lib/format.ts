"use client";

const NUMERIC_PATTERN = /^-?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i;

type DecimalOptions = {
  maximumFractionDigits?: number;
  minimumFractionDigits?: number;
  fallback?: string;
};

export function isNumericLike(value: unknown): value is string | number {
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  return NUMERIC_PATTERN.test(trimmed) && Number.isFinite(Number(trimmed));
}

export function formatDecimal(
  value: string | number | null | undefined,
  options: DecimalOptions = {}
): string {
  const fallback = options.fallback ?? "--";
  if (value === null || value === undefined || value === "") return fallback;
  if (!isNumericLike(value)) return String(value);

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  if (Object.is(numeric, -0) || Math.abs(numeric) < 1e-10) return "0";

  const abs = Math.abs(numeric);
  const maximumFractionDigits =
    options.maximumFractionDigits ??
    (abs >= 1000 ? 2 : abs >= 1 ? 4 : 8);

  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: options.minimumFractionDigits ?? 0,
    maximumFractionDigits,
  }).format(numeric);
}

export function formatDisplayValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "--";
  if (isNumericLike(value)) return formatDecimal(value);
  return String(value);
}

export function formatPrice(value: string | number | null | undefined): string {
  return formatDecimal(value, { maximumFractionDigits: 4 });
}

export function formatQuantity(value: string | number | null | undefined): string {
  return formatDecimal(value, { maximumFractionDigits: 8 });
}

export function formatNotional(value: string | number | null | undefined): string {
  return formatDecimal(value, { maximumFractionDigits: 2 });
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatAge(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  if (value < 1000) return `${Math.max(0, Math.round(value))}ms ago`;
  const seconds = Math.floor(value / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

export function formatSeconds(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return formatAge(value * 1000);
}

export function formatMs(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  if (value < 1000) return `${Math.round(value)}ms`;
  return `${formatDecimal(value / 1000, { maximumFractionDigits: 2 })}s`;
}

export function formatOpenTimeLabel(position: {
  open_time?: string | null;
  first_seen_at?: string | null;
  open_time_source?: string | null;
}): string {
  const value = position.open_time ?? position.first_seen_at;
  if (!value) return "--";
  const suffix = position.open_time_source === "FIRST_SEEN" ? " first seen" : "";
  return `${formatDateTime(value)}${suffix}`;
}

export function statusLabel(value: boolean | null | undefined, trueLabel = "OK", falseLabel = "Blocked"): string {
  if (value === null || value === undefined) return "Checking";
  return value ? trueLabel : falseLabel;
}
