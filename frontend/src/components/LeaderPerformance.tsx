"use client";

import Link from "next/link";
import { formatDateTime, formatDecimal, formatMs, formatNotional } from "@/lib/format";

type Numeric = string | number | null;

export type PerformanceFlag = {
  code: string;
  severity: "danger" | "warn" | "info" | string;
  message: string;
};

export type LeaderPerformance = {
  leader_id: number;
  leader_address: string;
  address_suffix: string;
  label: string;
  enabled: boolean;
  joined_at: string;
  observed_days: number;
  window: "SINCE_JOINED";
  scores: {
    overall: number;
    profitability: number;
    risk_control: number;
    copyability: number;
    consistency: number;
    data_confidence: number;
  };
  recommendation: {
    status: string;
    label: string;
    flags: PerformanceFlag[];
  };
  leader_account: {
    portfolio_pnl_since_join: Numeric;
    portfolio_return_pct: Numeric;
    max_drawdown: Numeric;
    max_drawdown_pct: Numeric;
    current_drawdown: Numeric;
    current_drawdown_pct: Numeric;
    start_account_value: Numeric;
    current_account_value: Numeric;
    history_points: number;
    fill_realized_gross: Numeric;
    fill_fees: Numeric;
    funding: Numeric;
    fill_realized_net_including_funding: Numeric;
    current_unrealized_pnl: Numeric;
    known_trading_pnl: Numeric;
    trading_volume: Numeric;
    open_positions_count: number;
  };
  follower_account: {
    realized_gross: Numeric;
    fees: Numeric;
    realized_net_ex_funding: Numeric;
    trading_volume: Numeric;
    realized_edge_bps: Numeric;
    realized_curve_max_drawdown: Numeric;
    matched_exchange_orders: number;
    exchange_fill_fragments: number;
    current_unrealized_pnl: Numeric;
    current_copied_notional: Numeric;
    known_total_pnl_ex_funding: Numeric;
    includes_manual_synced_exposure: boolean;
    peak_allocated_notional: Numeric;
    return_on_peak_allocated_pct: Numeric;
  };
  copyability: {
    matched_priced_orders: number;
    db_filled_orders: number;
    db_filled_exchange_matched_orders: number;
    database_status_disagreement_orders: number;
    exchange_match_coverage_pct: number | null;
    weighted_adverse_slippage_bps: number | null;
    median_slippage_bps: number | null;
    p90_slippage_bps: number | null;
    p95_slippage_bps: number | null;
    max_slippage_bps: number | null;
    adverse_slippage_order_pct: number | null;
    median_event_to_final_ms: number | null;
    p95_event_to_final_ms: number | null;
    realized_edge_bps: number | null;
    minimum_10u_exempt_events: number;
    fcfs_blocked_events: number;
    manual_review_events: number;
  };
  behavior: {
    complete_lifecycles: number;
    winning_lifecycles: number;
    losing_lifecycles: number;
    lifecycle_win_rate_pct: number | null;
    profit_factor: number | null;
    median_lifecycle_return_bps: number | null;
    worst_lifecycle_return_bps: number | null;
    median_hold_hours: number | null;
    winner_median_hold_hours: number | null;
    loser_median_hold_hours: number | null;
    loser_to_winner_hold_ratio: number | null;
    p90_hold_hours: number | null;
    max_hold_hours: number | null;
    current_open_positions: number;
    top_market_volume_pct: number | null;
    top_three_market_volume_pct: number | null;
    top_markets: Array<{ canonical_coin: string; volume: string; share_pct: number | null }>;
  };
  pipeline: {
    logical_events: number;
    executed_events: number;
    minimum_10u_exempt_events: number;
    fcfs_blocked_events: number;
    ignored_old_lifecycle_events: number;
    legacy_outcome_events: number;
    manual_review_events: number;
    other_no_action_events: number;
    missing_outcome_events: number;
  };
  data_quality: {
    source: string;
    caveats: string[];
    portfolio_history_points: number;
    logical_leader_events: number;
  };
  history: {
    leader_pnl: Array<{ time: string; pnl: string }>;
  };
};

export type LeaderPerformanceOverview = {
  schema_version: number;
  status: "ready" | "warming" | string;
  generated_at: string | null;
  window?: "SINCE_JOINED";
  leaders: LeaderPerformance[];
  methodology: {
    window?: string;
    overall_weights?: Record<string, number>;
    principles?: string[];
  };
};

export type SingleLeaderPerformance = Omit<LeaderPerformanceOverview, "leaders"> & {
  leader: LeaderPerformance | null;
};

export function LeaderPerformanceOverviewPanel({ data }: { data: LeaderPerformanceOverview | null }) {
  if (!data || data.status === "warming") {
    return (
      <section className="panel mb-4 p-4">
        <div className="text-sm font-semibold text-ink">Leader 绩效 · 自加入以来</div>
        <div className="mt-2 text-sm text-slate-500">历史数据正在首次计算，页面会在缓存生成后自动显示。</div>
      </section>
    );
  }

  return (
    <section className="panel mb-4 overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line px-4 py-3">
        <div>
          <div className="text-sm font-semibold text-ink">Leader 绩效 · 自加入以来</div>
          <div className="mt-1 text-xs text-slate-500">后台每 6 小时增量更新，页面仅读取缓存，不会因实时持仓事件请求交易所历史接口</div>
        </div>
        <div className="text-xs text-slate-500">统计生成 {formatDateTime(data.generated_at)}</div>
      </div>
      <div className="grid gap-3 p-4 xl:grid-cols-2">
        {data.leaders.map((leader) => (
          <PerformanceCard leader={leader} key={leader.leader_id} />
        ))}
      </div>
      <div className="border-t border-line px-4 py-3 text-xs text-slate-500">
        综合分权重：盈利能力 27% · 风险控制 28% · 可复制性 28% · 稳定性 12% · 数据置信度 5%。评分仅用于比较，不会自动开关 Leader 或改变仓位。
      </div>
    </section>
  );
}

function PerformanceCard({ leader }: { leader: LeaderPerformance }) {
  const contribution = Number(leader.follower_account.known_total_pnl_ex_funding ?? 0);
  const pnl = Number(leader.leader_account.portfolio_pnl_since_join ?? 0);
  const flags = leader.recommendation.flags.slice(0, 3);
  return (
    <article className="rounded-md border border-line bg-slate-50 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <Link className="font-semibold text-ink hover:text-accent" href={`/leaders/${leader.leader_id}`}>
            {leader.label}
          </Link>
          <div className="mt-1 break-all font-mono text-[11px] text-slate-500">{leader.leader_address}</div>
          <div className="mt-1 text-xs text-slate-500">
            加入 {formatDateTime(leader.joined_at)} · {formatDecimal(leader.observed_days, { maximumFractionDigits: 1 })} 天
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className={recommendationClass(leader.recommendation.status)}>{leader.recommendation.label}</span>
          <ScoreRing score={leader.scores.overall} />
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <CompactMetric label="Leader PnL" value={money(leader.leader_account.portfolio_pnl_since_join)} tone={numberTone(pnl)} />
        <CompactMetric label="Leader 回报" value={percent(leader.leader_account.portfolio_return_pct)} tone={numberTone(pnl)} />
        <CompactMetric label="最大回撤" value={`${money(leader.leader_account.max_drawdown)} / ${percent(leader.leader_account.max_drawdown_pct)}`} tone="danger" />
        <CompactMetric label="你的总贡献*" value={money(leader.follower_account.known_total_pnl_ex_funding)} tone={numberTone(contribution)} />
        <CompactMetric label="已实现 / 浮盈亏" value={`${money(leader.follower_account.realized_net_ex_funding)} / ${money(leader.follower_account.current_unrealized_pnl)}`} />
        <CompactMetric label="跟单收益边际" value={bps(leader.follower_account.realized_edge_bps)} />
        <CompactMetric label="加权滑点 / P95" value={`${bps(leader.copyability.weighted_adverse_slippage_bps)} / ${bps(leader.copyability.p95_slippage_bps)}`} />
        <CompactMetric label="延迟中位 / P95" value={`${formatMs(leader.copyability.median_event_to_final_ms)} / ${formatMs(leader.copyability.p95_event_to_final_ms)}`} />
        <CompactMetric label="完整交易 / PF" value={`${leader.behavior.complete_lifecycles} / ${formatDecimal(leader.behavior.profit_factor, { maximumFractionDigits: 2 })}`} />
        <CompactMetric label="胜率（仅展示）" value={percent(leader.behavior.lifecycle_win_rate_pct)} />
        <CompactMetric label="亏损/盈利持仓时长" value={ratio(leader.behavior.loser_to_winner_hold_ratio)} />
        <CompactMetric label="单市场 / Top3 集中度" value={`${percent(leader.behavior.top_market_volume_pct)} / ${percent(leader.behavior.top_three_market_volume_pct)}`} />
      </div>

      {flags.length ? (
        <div className="mt-3 space-y-1">
          {flags.map((flag) => (
            <div className={flagClass(flag.severity)} key={flag.code}>{flag.message}</div>
          ))}
        </div>
      ) : (
        <div className="mt-3 rounded-md border border-green-200 bg-green-50 px-3 py-2 text-xs text-accent">当前没有达到阈值的风险红旗。</div>
      )}
      <div className="mt-3 text-[11px] text-slate-500">* 总贡献 = 交易所可归因的已实现净收益（不含 funding）+ 当前复制仓位浮盈亏。</div>
    </article>
  );
}

export function LeaderPerformanceDetailPanel({ performance }: { performance: LeaderPerformance | null }) {
  if (!performance) {
    return <section className="panel mb-4 p-4 text-sm text-slate-500">绩效缓存正在生成。</section>;
  }
  const leader = performance;
  return (
    <div className="mb-4 space-y-4">
      <section className="panel overflow-hidden">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line px-4 py-3">
          <div>
            <div className="text-sm font-semibold">{leader.label} · 自加入以来绩效</div>
            <div className="mt-1 break-all font-mono text-[11px] text-slate-500">{leader.leader_address}</div>
            <div className="mt-1 text-xs text-slate-500">{formatDateTime(leader.joined_at)} 至最近统计，共 {formatDecimal(leader.observed_days, { maximumFractionDigits: 1 })} 天</div>
          </div>
          <div className="flex items-center gap-2">
            <span className={recommendationClass(leader.recommendation.status)}>{leader.recommendation.label}</span>
            <ScoreRing score={leader.scores.overall} />
          </div>
        </div>
        <div className="grid gap-3 p-4 sm:grid-cols-3 xl:grid-cols-6">
          <ScoreMetric label="综合" score={leader.scores.overall} />
          <ScoreMetric label="盈利能力" score={leader.scores.profitability} />
          <ScoreMetric label="风险控制" score={leader.scores.risk_control} />
          <ScoreMetric label="可复制性" score={leader.scores.copyability} />
          <ScoreMetric label="稳定性" score={leader.scores.consistency} />
          <ScoreMetric label="数据置信度" score={leader.scores.data_confidence} />
        </div>
      </section>

      <section className="panel overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">Leader 原账户</div>
        <div className="grid gap-3 p-4 md:grid-cols-2 xl:grid-cols-4">
          <DetailMetric label="加入后 PnL" value={money(leader.leader_account.portfolio_pnl_since_join)} />
          <DetailMetric label="加入后回报" value={percent(leader.leader_account.portfolio_return_pct)} />
          <DetailMetric label="最大回撤" value={`${money(leader.leader_account.max_drawdown)} / ${percent(leader.leader_account.max_drawdown_pct)}`} />
          <DetailMetric label="当前回撤" value={`${money(leader.leader_account.current_drawdown)} / ${percent(leader.leader_account.current_drawdown_pct)}`} />
          <DetailMetric label="起始 / 当前账户价值" value={`${money(leader.leader_account.start_account_value)} / ${money(leader.leader_account.current_account_value)}`} />
          <DetailMetric label="成交净收益（含 funding）" value={money(leader.leader_account.fill_realized_net_including_funding)} />
          <DetailMetric label="当前浮盈亏" value={money(leader.leader_account.current_unrealized_pnl)} />
          <DetailMetric label="成交量" value={money(leader.leader_account.trading_volume)} />
        </div>
        <div className="border-t border-line p-4">
          <div className="mb-2 text-xs text-slate-500">加入后 Leader perp PnL 曲线</div>
          <PnlSparkline points={leader.history.leader_pnl} />
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <div className="panel overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">你的实际跟单贡献</div>
          <div className="grid gap-3 p-4 sm:grid-cols-2">
            <DetailMetric label="已知总贡献（不含 funding）" value={money(leader.follower_account.known_total_pnl_ex_funding)} />
            <DetailMetric label="已实现净收益（不含 funding）" value={money(leader.follower_account.realized_net_ex_funding)} />
            <DetailMetric label="当前复制仓位浮盈亏" value={money(leader.follower_account.current_unrealized_pnl)} />
            <DetailMetric label="已实现曲线最大回撤" value={money(leader.follower_account.realized_curve_max_drawdown)} />
            <DetailMetric label="峰值分配名义价值" value={money(leader.follower_account.peak_allocated_notional)} />
            <DetailMetric label="总贡献 / 峰值分配" value={percent(leader.follower_account.return_on_peak_allocated_pct)} />
            <DetailMetric label="实际成交量" value={money(leader.follower_account.trading_volume)} />
            <DetailMetric label="扣费后收益边际" value={bps(leader.follower_account.realized_edge_bps)} />
            <DetailMetric label="交易所匹配订单 / fill" value={`${leader.follower_account.matched_exchange_orders} / ${leader.follower_account.exchange_fill_fragments}`} />
            <DetailMetric label="含手动同步仓位" value={leader.follower_account.includes_manual_synced_exposure ? "是" : "否"} />
          </div>
        </div>

        <div className="panel overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">可复制性与延迟</div>
          <div className="grid gap-3 p-4 sm:grid-cols-2">
            <DetailMetric label="方向调整加权滑点" value={bps(leader.copyability.weighted_adverse_slippage_bps)} />
            <DetailMetric label="滑点 中位 / P95 / 最大" value={`${bps(leader.copyability.median_slippage_bps)} / ${bps(leader.copyability.p95_slippage_bps)} / ${bps(leader.copyability.max_slippage_bps)}`} />
            <DetailMetric label="不利滑点订单占比" value={percent(leader.copyability.adverse_slippage_order_pct)} />
            <DetailMetric label="成交延迟 中位 / P95" value={`${formatMs(leader.copyability.median_event_to_final_ms)} / ${formatMs(leader.copyability.p95_event_to_final_ms)}`} />
            <DetailMetric label="旧 FILLED 交易所验证覆盖" value={percent(leader.copyability.exchange_match_coverage_pct)} />
            <DetailMetric label="数据库状态与成交不一致" value={String(leader.copyability.database_status_disagreement_orders)} />
            <DetailMetric label="低于 10U 豁免" value={String(leader.copyability.minimum_10u_exempt_events)} />
            <DetailMetric label="FCFS 冲突阻挡" value={String(leader.copyability.fcfs_blocked_events)} />
          </div>
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <div className="panel overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">交易行为</div>
          <div className="grid gap-3 p-4 sm:grid-cols-2">
            <DetailMetric label="完整生命周期（赢 / 输）" value={`${leader.behavior.complete_lifecycles}（${leader.behavior.winning_lifecycles} / ${leader.behavior.losing_lifecycles}）`} />
            <DetailMetric label="胜率（只展示）" value={percent(leader.behavior.lifecycle_win_rate_pct)} />
            <DetailMetric label="Profit factor" value={formatDecimal(leader.behavior.profit_factor, { maximumFractionDigits: 2 })} />
            <DetailMetric label="生命周期收益中位 / 最差" value={`${bps(leader.behavior.median_lifecycle_return_bps)} / ${bps(leader.behavior.worst_lifecycle_return_bps)}`} />
            <DetailMetric label="持仓时长中位 / P90 / 最大" value={`${hours(leader.behavior.median_hold_hours)} / ${hours(leader.behavior.p90_hold_hours)} / ${hours(leader.behavior.max_hold_hours)}`} />
            <DetailMetric label="盈利 / 亏损持仓中位" value={`${hours(leader.behavior.winner_median_hold_hours)} / ${hours(leader.behavior.loser_median_hold_hours)}`} />
            <DetailMetric label="亏损 / 盈利持仓时长倍数" value={ratio(leader.behavior.loser_to_winner_hold_ratio)} />
            <DetailMetric label="最大单市场 / Top3" value={`${percent(leader.behavior.top_market_volume_pct)} / ${percent(leader.behavior.top_three_market_volume_pct)}`} />
          </div>
          {leader.behavior.top_markets.length ? (
            <div className="border-t border-line px-4 py-3 text-xs text-slate-600">
              主要市场：{leader.behavior.top_markets.map((item) => `${item.canonical_coin} ${percent(item.share_pct)}`).join(" · ")}
            </div>
          ) : null}
        </div>

        <div className="panel overflow-hidden">
          <div className="border-b border-line px-4 py-3 text-sm font-semibold">事件执行口径</div>
          <div className="grid gap-3 p-4 sm:grid-cols-2">
            <DetailMetric label="Leader 逻辑 fill 事件" value={String(leader.pipeline.logical_events)} />
            <DetailMetric label="已执行" value={String(leader.pipeline.executed_events)} />
            <DetailMetric label="低于 10U 豁免" value={String(leader.pipeline.minimum_10u_exempt_events)} />
            <DetailMetric label="FCFS 阻挡" value={String(leader.pipeline.fcfs_blocked_events)} />
            <DetailMetric label="加入前旧生命周期忽略" value={String(leader.pipeline.ignored_old_lifecycle_events)} />
            <DetailMetric label="历史人工复核 / 其他无动作" value={`${leader.pipeline.manual_review_events} / ${leader.pipeline.other_no_action_events}`} />
            <DetailMetric label="旧版结果 / 无结果记录" value={`${leader.pipeline.legacy_outcome_events} / ${leader.pipeline.missing_outcome_events}`} />
            <DetailMetric label="当前 Leader 开仓数" value={String(leader.behavior.current_open_positions)} />
          </div>
        </div>
      </section>

      <section className="panel overflow-hidden">
        <div className="border-b border-line px-4 py-3 text-sm font-semibold">风险红旗与数据边界</div>
        <div className="space-y-2 p-4">
          {leader.recommendation.flags.length ? leader.recommendation.flags.map((flag) => (
            <div className={flagClass(flag.severity)} key={flag.code}>{flag.message}</div>
          )) : <div className="text-sm text-accent">当前没有达到阈值的风险红旗。</div>}
          <div className="pt-2 text-xs font-medium text-slate-600">统计边界</div>
          {leader.data_quality.caveats.map((item) => <div className="text-xs text-slate-500" key={item}>• {item}</div>)}
        </div>
      </section>
    </div>
  );
}

function ScoreRing({ score }: { score: number }) {
  return <div className={`flex h-11 w-11 items-center justify-center rounded-full border-2 text-sm font-bold ${scoreClass(score)}`}>{score}</div>;
}

function ScoreMetric({ label, score }: { label: string; score: number }) {
  return (
    <div className="mini-card">
      <div className="mini-label">{label}</div>
      <div className={`text-xl font-semibold tabular-nums ${score >= 75 ? "text-accent" : score < 50 ? "text-danger" : "text-ink"}`}>{score}<span className="ml-1 text-xs font-normal text-slate-400">/100</span></div>
    </div>
  );
}

function CompactMetric({ label, value, tone }: { label: string; value: string; tone?: "ok" | "danger" }) {
  return (
    <div className="rounded-md border border-line bg-white px-3 py-2">
      <div className="text-[11px] text-slate-500">{label}</div>
      <div className={`mt-1 break-words text-sm font-medium tabular-nums ${tone === "ok" ? "text-accent" : tone === "danger" ? "text-danger" : "text-ink"}`}>{value}</div>
    </div>
  );
}

function DetailMetric({ label, value }: { label: string; value: string }) {
  return <CompactMetric label={label} value={value} />;
}

function PnlSparkline({ points }: { points: Array<{ time: string; pnl: string }> }) {
  if (points.length < 2) return <div className="flex h-28 items-center justify-center rounded-md bg-slate-50 text-sm text-slate-500">历史点不足</div>;
  const values = points.map((item) => Number(item.pnl)).filter(Number.isFinite);
  if (values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1e-9);
  const polyline = values.map((value, index) => `${(index / (values.length - 1)) * 100},${36 - ((value - min) / spread) * 32}`).join(" ");
  const zeroY = min <= 0 && max >= 0 ? 36 - ((0 - min) / spread) * 32 : null;
  return (
    <div>
      <svg aria-label="Leader PnL history" className="h-32 w-full rounded-md bg-slate-50" preserveAspectRatio="none" viewBox="0 0 100 40">
        {zeroY !== null ? <line stroke="#cbd5e1" strokeDasharray="2 2" strokeWidth="0.4" x1="0" x2="100" y1={zeroY} y2={zeroY} /> : null}
        <polyline fill="none" points={polyline} stroke={values.at(-1)! >= 0 ? "#0f766e" : "#b91c1c"} strokeLinejoin="round" strokeWidth="1.2" vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="mt-1 flex justify-between text-[11px] text-slate-500"><span>{formatDateTime(points[0]?.time)}</span><span>{money(values.at(-1) ?? null)}</span><span>{formatDateTime(points.at(-1)?.time)}</span></div>
    </div>
  );
}

function money(value: Numeric): string {
  return value === null || value === undefined ? "--" : `$${formatNotional(value)}`;
}

function percent(value: Numeric): string {
  return value === null || value === undefined ? "--" : `${formatDecimal(value, { maximumFractionDigits: 2 })}%`;
}

function bps(value: Numeric): string {
  return value === null || value === undefined ? "--" : `${formatDecimal(value, { maximumFractionDigits: 2 })}bps`;
}

function ratio(value: Numeric): string {
  return value === null || value === undefined ? "--" : `${formatDecimal(value, { maximumFractionDigits: 2 })}x`;
}

function hours(value: Numeric): string {
  return value === null || value === undefined ? "--" : `${formatDecimal(value, { maximumFractionDigits: 2 })}h`;
}

function numberTone(value: number): "ok" | "danger" | undefined {
  return value > 0 ? "ok" : value < 0 ? "danger" : undefined;
}

function scoreClass(score: number): string {
  if (score >= 75) return "border-teal-600 bg-teal-50 text-accent";
  if (score < 50) return "border-red-500 bg-red-50 text-danger";
  return "border-amber-500 bg-amber-50 text-warn";
}

function recommendationClass(status: string): string {
  if (["HIGH_RISK", "POOR_COPYABILITY"].includes(status)) return "status-pill border-red-200 bg-red-50 text-danger";
  if (["STRONG", "KEEP"].includes(status)) return "status-pill border-green-200 bg-green-50 text-accent";
  return "status-pill border-amber-200 bg-amber-50 text-warn";
}

function flagClass(severity: string): string {
  if (severity === "danger") return "rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-danger";
  if (severity === "warn") return "rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-warn";
  return "rounded-md border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600";
}
