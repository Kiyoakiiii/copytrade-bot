import type { ReactNode } from "react";

export function Header({
  title,
  right
}: {
  title: string;
  right?: ReactNode;
}) {
  return (
    <div className="mb-5 flex min-h-10 flex-wrap items-center justify-between gap-3">
      <h1 className="text-xl font-semibold tracking-normal text-ink">{title}</h1>
      <div className="flex flex-wrap items-center gap-2">{right}</div>
    </div>
  );
}
