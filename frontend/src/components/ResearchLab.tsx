"use client";

import { FormEvent, type ReactNode, useEffect, useState } from "react";
import { BarChart3, Calculator, CheckCircle2, Clock3, FlaskConical, RefreshCw, ShieldAlert } from "lucide-react";
import { apiFetch } from "@/lib/api";
import { formatDateTime, formatLossMoney, formatLossPercent, formatNotional, formatProfitLossMoney, formatProfitLossPercent, profitLossClass } from "@/lib/format";

type Tool = "suitability" | "balance";

type ResearchJob = {
  id: string;
  status: "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | string;
  tool: Tool;
  address: string;
  parameters: Record<string, string>;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  progress: string | null;
  result: any;
  error: string | null;
  cached: boolean;
};

export function ResearchLab() {
  const [tool, setTool] = useState<Tool>("suitability");
  const [address, setAddress] = useState("");
  const [friction, setFriction] = useState("5");
  const [tail, setTail] = useState("7.5");
  const [roundTo, setRoundTo] = useState("10000");
  const [followerBalance, setFollowerBalance] = useState("20000");
  const [job, setJob] = useState<ResearchJob | null>(null);
  const [error, setError] = useState("");
  const active = job?.status === "QUEUED" || job?.status === "RUNNING";

  useEffect(() => {
    apiFetch<ResearchJob[]>("/research/jobs?limit=1")
      .then((rows) => { if (rows[0]) setJob(rows[0]); })
      .catch(() => undefined);
  }, []);

  useEffect(() => {
    if (!active || !job) return;
    const timer = window.setInterval(async () => {
      if (document.visibilityState !== "visible") return;
      try {
        const next = await apiFetch<ResearchJob>(`/research/jobs/${job.id}`);
        setJob(next);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to refresh research job");
      }
    }, 30_000);
    return () => window.clearInterval(timer);
  }, [active, job?.id]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      const next = await apiFetch<ResearchJob>("/research/jobs", {
        method: "POST",
        body: JSON.stringify({
          tool,
          address: address.trim(),
          friction_bps: friction,
          target_tail_pct: tail,
          round_to: roundTo,
          follower_balance: followerBalance,
        }),
      });
      setJob(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to start analysis");
    }
  }

  return (
    <section className="panel overflow-hidden">
      <div className="flex flex-col gap-3 border-b border-line bg-navy px-5 py-5 text-white sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3">
          <span className="grid h-10 w-10 place-items-center rounded-xl bg-white/10 ring-1 ring-white/10"><FlaskConical className="h-5 w-5 text-teal-300" /></span>
          <div><h2 className="font-semibold">Leader Research Lab</h2><p className="mt-0.5 text-xs text-slate-400">Public-history scoring and risk-calibrated balance recommendations</p></div>
        </div>
        <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[10px] uppercase tracking-[0.15em] text-slate-300">Isolated worker · cached</span>
      </div>

      <div className="grid lg:grid-cols-[0.8fr_1.2fr]">
        <form onSubmit={submit} className="border-b border-line p-5 lg:border-b-0 lg:border-r">
          <div className="mb-4 grid grid-cols-2 rounded-lg bg-slate-100 p-1">
            <button type="button" onClick={() => setTool("suitability")} className={`flex h-9 items-center justify-center gap-2 rounded-md text-xs font-semibold transition ${tool === "suitability" ? "bg-white text-ink shadow-sm" : "text-slate-500"}`}><BarChart3 className="h-4 w-4" /> Address score</button>
            <button type="button" onClick={() => setTool("balance")} className={`flex h-9 items-center justify-center gap-2 rounded-md text-xs font-semibold transition ${tool === "balance" ? "bg-white text-ink shadow-sm" : "text-slate-500"}`}><Calculator className="h-4 w-4" /> Balance model</button>
          </div>
          <label className="block space-y-1.5"><span className="text-xs font-medium text-slate-600">Public leader address</span><input className="field font-mono" value={address} onChange={(event) => setAddress(event.target.value)} placeholder="0x…" pattern="0x[0-9a-fA-F]{40}" required /></label>
          <div className="mt-3 grid grid-cols-2 gap-3">
            {tool === "suitability" ? <label className="space-y-1.5"><span className="text-xs text-slate-500">Copy friction (bps)</span><input className="field" inputMode="decimal" value={friction} onChange={(event) => setFriction(event.target.value)} /></label> : <label className="space-y-1.5"><span className="text-xs text-slate-500">Follower basis</span><input className="field" inputMode="decimal" value={followerBalance} onChange={(event) => setFollowerBalance(event.target.value)} /></label>}
            <label className="space-y-1.5"><span className="text-xs text-slate-500">Tail target (%)</span><input className="field" inputMode="decimal" value={tail} onChange={(event) => setTail(event.target.value)} /></label>
            <label className="col-span-2 space-y-1.5">
              <span className="text-xs text-slate-500">Recommendation rounding step (USDC)</span>
              <input className="field" inputMode="decimal" value={roundTo} onChange={(event) => setRoundTo(event.target.value)} />
              <span className="block text-[10px] leading-4 text-slate-400">Rounds the calculated balance upward to a practical increment. Example: 643,200 with a 10,000 step becomes 650,000. Use 1 for no practical rounding.</span>
            </label>
          </div>
          <button className="btn btn-primary mt-4 w-full" type="submit" disabled={active}>
            {active ? <RefreshCw className="h-4 w-4 animate-spin" /> : <FlaskConical className="h-4 w-4" />}
            {active ? "Analysis in progress" : tool === "suitability" ? "Run full score" : "Calculate balance"}
          </button>
          <p className="mt-3 text-[11px] leading-5 text-slate-500">Runs only on demand in an isolated 0.5-vCPU worker. Public-history requests reuse a six-hour cache, are globally spaced by two minutes, and stop at a hard per-job request budget. Long histories may therefore take hours. The worker never reads signer material or the live order path.</p>
          {error ? <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-danger">{error}</div> : null}
        </form>

        <div className="min-h-[360px] p-5">
          {!job ? <EmptyResult /> : <JobResult job={job} />}
        </div>
      </div>
    </section>
  );
}

function JobResult({ job }: { job: ResearchJob }) {
  if (job.status === "QUEUED" || job.status === "RUNNING") {
    return <div className="flex h-full min-h-[320px] flex-col items-center justify-center text-center"><span className="grid h-14 w-14 place-items-center rounded-2xl bg-teal-50"><RefreshCw className="h-6 w-6 animate-spin text-accent" /></span><div className="mt-4 font-semibold text-ink">{job.status === "QUEUED" ? "Queued for analysis" : "Rebuilding public history"}</div><div className="mt-2 max-w-sm text-sm text-slate-500">{job.progress ?? "This can take several minutes for a long or high-frequency history."}</div><div className="mt-3 text-xs text-slate-400">Started {formatDateTime(job.started_at ?? job.created_at)}</div></div>;
  }
  if (job.status === "FAILED") {
    return <div className="rounded-xl border border-red-200 bg-red-50 p-4"><div className="flex items-center gap-2 font-semibold text-danger"><ShieldAlert className="h-5 w-5" />Analysis failed</div><div className="mt-2 whitespace-pre-wrap text-xs leading-5 text-red-700">{job.error}</div></div>;
  }
  const row = job.result?.leaders?.[0];
  if (!row) return <EmptyResult />;
  return job.tool === "suitability" ? <SuitabilityResult job={job} row={row} /> : <BalanceResult job={job} row={row} />;
}

function SuitabilityResult({ job, row }: { job: ResearchJob; row: any }) {
  const metrics = row.metrics ?? {};
  const score = Number(row.score ?? 0);
  return <div>
    <ResultHeader job={job} title="Copy suitability" badge={String(row.verdict ?? "UNKNOWN")} badgeTone={row.verdict === "STRONG" || row.verdict === "ADDABLE" ? "ok" : row.verdict === "REJECT" ? "danger" : "warn"} />
    <div className="mt-5 grid grid-cols-[120px_1fr] gap-5">
      <div className="relative grid h-[120px] w-[120px] place-items-center rounded-full" style={{ background: `conic-gradient(#0f8f83 ${Math.max(0, Math.min(100, score)) * 3.6}deg, #e8edf3 0deg)` }}><div className="grid h-[94px] w-[94px] place-items-center rounded-full bg-white text-center"><div><div className="text-3xl font-semibold text-ink">{formatNumber(score, 1)}</div><div className="text-[10px] uppercase tracking-wider text-slate-400">of 100</div></div></div></div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3"><ResultMetric label="History" value={`${formatNumber(metrics.history_days, 0)}d`} /><ResultMetric label="After friction" value={<PnlMoney value={metrics.friction_net} />} /><ResultMetric label="Annualized" value={<PnlPercent value={metrics.friction_annual_simple_return_pct} />} /><ResultMetric label="Pressure tail" value={<LossPercent value={metrics.pressure_tail_pct} />} /><ResultMetric label="Losing hold P95" value={duration(metrics.p95_losing_hold_hours)} /><ResultMetric label="Break-even" value={`${formatNumber(metrics.breakeven_friction_bps, 1)} bps`} /></div>
    </div>
    <ReasonList title="Hard gates" values={row.hard_failures ?? []} tone="danger" empty="No hard rejection gate triggered." />
    <ReasonList title="Review notes" values={row.warnings ?? []} tone="warn" empty="No material warning from the model." />
  </div>;
}

function BalanceResult({ job, row }: { job: ResearchJob; row: any }) {
  const model = row.exposure_model ?? {};
  return <div>
    <ResultHeader job={job} title="Balance recommendation" badge={`${formatNumber(row.applied_tail_pct, 2)}% tail`} badgeTone={Number(row.applied_tail_pct) <= Number(job.parameters.target_tail_pct ?? 7.5) ? "ok" : "warn"} />
    <div className="mt-5 rounded-xl border border-teal-200 bg-teal-50 p-5"><div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-accent">Recommended fixed balance</div><div className="mt-1 text-3xl font-semibold tracking-tight text-ink">{formatNotional(row.recommended_balance)} <span className="text-sm font-medium text-slate-500">USDC</span></div><div className="mt-2 text-xs text-slate-600">Calibrated from portfolio pressure and observed exposure elasticity; multiplier is intentionally excluded.</div></div>
    <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3"><ResultMetric label="Current account total" value={money(row.current_account_total)} /><ResultMetric label="Projected pressure loss" value={<LossMoney value={row.pressure_drawdown} />} /><ResultMetric label="Projected gross peak" value={money(model.projected_peak_gross)} /><ResultMetric label="Historical gross peak" value={money(model.historical_peak_gross)} /><ResultMetric label="Exposure beta" value={formatNumber(model.beta, 2)} /><ResultMetric label="Observations" value={formatNumber(model.observation_count, 0)} /></div>
    <div className="mt-4 rounded-lg border border-line bg-slate-50 p-3 text-xs leading-5 text-slate-600"><span className="font-semibold text-ink">Model driver:</span> {human(model.projected_component)}. Recommendation is rounded upward to {money(job.parameters.round_to)} and targets a {job.parameters.target_tail_pct}% portfolio pressure tail.</div>
  </div>;
}

function ResultHeader({ job, title, badge, badgeTone }: { job: ResearchJob; title: string; badge: string; badgeTone: "ok" | "warn" | "danger" }) {
  const tone = badgeTone === "ok" ? "border-teal-200 bg-teal-50 text-accent" : badgeTone === "danger" ? "border-red-200 bg-red-50 text-danger" : "border-amber-200 bg-amber-50 text-warn";
  return <div className="flex flex-wrap items-start justify-between gap-3"><div><div className="flex items-center gap-2 text-sm font-semibold text-ink"><CheckCircle2 className="h-4 w-4 text-accent" />{title}</div><div className="mt-1 break-all font-mono text-[11px] text-slate-400">{job.address}</div><div className="mt-1 flex items-center gap-1 text-[10px] text-slate-400"><Clock3 className="h-3 w-3" />{job.cached ? "Cached result" : `Completed ${formatDateTime(job.completed_at)}`}</div></div><span className={`status-pill ${tone}`}>{badge}</span></div>;
}

function ResultMetric({ label, value }: { label: string; value: ReactNode }) { return <div className="rounded-lg border border-line bg-slate-50 p-3"><div className="text-[10px] uppercase tracking-wide text-slate-400">{label}</div><div className="mt-1 text-sm font-semibold tabular-nums text-ink">{value}</div></div>; }
function PnlMoney({ value }: { value: unknown }) { return <span className={profitLossClass(value)}>{formatProfitLossMoney(value)}</span>; }
function PnlPercent({ value }: { value: unknown }) { return <span className={profitLossClass(value)}>{formatProfitLossPercent(value)}</span>; }
function LossMoney({ value }: { value: unknown }) { return <span className={Number(value ?? 0) === 0 ? "text-ink" : "text-danger"}>{formatLossMoney(value)}</span>; }
function LossPercent({ value }: { value: unknown }) { return <span className={Number(value ?? 0) === 0 ? "text-ink" : "text-danger"}>{formatLossPercent(value)}</span>; }
function ReasonList({ title, values, tone, empty }: { title: string; values: string[]; tone: "danger" | "warn"; empty: string }) { return <div className="mt-4"><div className="mb-2 text-xs font-semibold text-slate-600">{title}</div><div className="space-y-2">{values.length ? values.map((value, index) => <div key={`${index}:${value}`} className={`rounded-lg border p-3 text-xs leading-5 ${tone === "danger" ? "border-red-100 bg-red-50 text-red-700" : "border-amber-100 bg-amber-50 text-amber-800"}`}>{value}</div>) : <div className="rounded-lg border border-teal-100 bg-teal-50 p-3 text-xs text-accent">{empty}</div>}</div></div>; }
function EmptyResult() { return <div className="flex min-h-[320px] flex-col items-center justify-center text-center"><span className="grid h-14 w-14 place-items-center rounded-2xl bg-slate-100"><FlaskConical className="h-6 w-6 text-slate-400" /></span><div className="mt-4 font-semibold text-ink">Research a candidate</div><div className="mt-2 max-w-sm text-sm leading-6 text-slate-500">Score copy economics, loss discipline and data quality, or calculate a fixed balance from portfolio-level pressure.</div></div>; }
function formatNumber(value: unknown, digits: number): string { const number = Number(value); return Number.isFinite(number) ? number.toLocaleString(undefined, { maximumFractionDigits: digits }) : "--"; }
function money(value: unknown): string { const number = Number(value); return Number.isFinite(number) ? `$${formatNotional(number)}` : "--"; }
function duration(value: unknown): string { const hours = Number(value); if (!Number.isFinite(hours)) return "--"; return hours < 1 ? `${Math.round(hours * 60)}m` : hours < 48 ? `${formatNumber(hours, 1)}h` : `${formatNumber(hours / 24, 1)}d`; }
function human(value: unknown): string { const text = String(value ?? ""); return text ? text.toLowerCase().replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()) : "--"; }
