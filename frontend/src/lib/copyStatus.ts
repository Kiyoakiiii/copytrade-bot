export type CopyStatusFields = {
  copyable?: boolean | null;
  copy_status?: string | null;
  copy_reason?: string | null;
  last_copy_order_display_status?: string | null;
  last_copy_order_reason?: string | null;
};

export function effectiveCopyStatus(item: CopyStatusFields): string | null {
  return item.last_copy_order_display_status ?? item.copy_status ?? null;
}

export function effectiveCopyReason(item: CopyStatusFields): string | null {
  const latest = item.last_copy_order_display_status;
  if (latest && latest !== "LAST_ORDER_FILLED") {
    return item.last_copy_order_reason ?? item.copy_reason ?? null;
  }
  return item.copy_reason ?? null;
}

export function effectiveCopyable(item: CopyStatusFields): boolean | null {
  const status = String(effectiveCopyStatus(item) ?? "").toUpperCase();
  if (isProblemCopyStatus(status)) return false;
  return item.copyable ?? null;
}

export function copyStatusTone(value: string | null | undefined): "ok" | "warn" | "danger" | "neutral" {
  const status = String(value ?? "").toUpperCase();
  if (!status) return "neutral";
  if (isProblemCopyStatus(status) || status.includes("ERROR") || status.includes("STALE")) return "danger";
  if (status.includes("IGNORE") || status.includes("WAIT")) return "warn";
  return "ok";
}

function isProblemCopyStatus(status: string): boolean {
  return (
    status.includes("BLOCK")
    || status.includes("REJECT")
    || status.includes("FAIL")
    || status.includes("UNKNOWN")
  );
}
