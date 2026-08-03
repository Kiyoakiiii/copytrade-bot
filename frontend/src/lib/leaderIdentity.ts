export function leaderAddressSuffix(address: string | null | undefined): string {
  const value = String(address ?? "");
  return value.length >= 4 ? value.slice(-4).toUpperCase() : "----";
}

export function leaderDisplayLabel(address: string | null | undefined): string {
  return `Leader · ${leaderAddressSuffix(address)}`;
}
