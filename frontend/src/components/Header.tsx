import type { ReactNode } from "react";

export function Header({
  title,
  eyebrow,
  subtitle,
  right
}: {
  title: string;
  eyebrow?: string;
  subtitle?: string;
  right?: ReactNode;
}) {
  return (
    <div className="mb-6 flex min-h-12 flex-wrap items-start justify-between gap-4">
      <div>
        {eyebrow ? <div className="mb-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-accent">{eyebrow}</div> : null}
        <h1 className="text-2xl font-semibold tracking-[-0.02em] text-ink">{title}</h1>
        {subtitle ? <p className="mt-1 max-w-3xl text-sm text-slate-500">{subtitle}</p> : null}
      </div>
      <div className="flex flex-wrap items-center gap-2">{right}</div>
    </div>
  );
}
