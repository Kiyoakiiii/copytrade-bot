#!/usr/bin/env python3
"""Evaluate fixed leader balances from public Hyperliquid portfolio risk.

This is an offline research tool for the operator/maintainer.  It intentionally
does not import the live backend, environment, follower accounts, signer
material, API secrets, or Telegram configuration.

The fixed methodology is:

1. Rebuild public perp fills into position lifecycles.
2. Apply the bot's economic-dust rule (a follower remainder below 10 USDC is
   flat; a later material add is a new lifecycle).
3. Normalize each lifecycle with the leader Account Total immediately before
   that lifecycle opened.  Adds/reductions retain that denominator.
4. Combine every lifecycle on one time axis and calculate account-level
   portfolio pressure drawdown, including complete 4h candle adverse extremes.
5. Recommend ``current Account Total * historical tail / target tail`` and
   round upward.  Multiplier is deliberately outside this calibration.
6. When several leaders are supplied, validate both net joint drawdown and the
   concurrent sum of each leader's own high-water drawdown.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import leader_loss_risk_report as risk


ZERO = Decimal("0")
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class LeaderInput:
    label: str
    address: str


@dataclass
class PressurePath:
    regular: dict[int, Decimal]
    stress: dict[int, Decimal]


@dataclass
class DrawdownResult:
    amount: Decimal
    peak_ms: int
    trough_ms: int
    peak_value: Decimal
    trough_value: Decimal
    peak_by_leader: dict[str, Decimal]
    trough_by_leader: dict[str, Decimal]


@dataclass
class Evaluation:
    leader: LeaderInput
    current_spot: Decimal
    current_perp: Decimal
    current_total: Decimal
    fill_start_ms: int | None
    fill_end_ms: int | None
    fill_count: int
    logical_fill_count: int
    complete_lifecycles: int
    saturated: bool
    capital_sample_count: int
    max_capital_sample_gap_days: float | None
    drawdown: DrawdownResult
    historical_tail_pct: Decimal
    theoretical_balance: Decimal
    recommended_balance: Decimal
    recommended_tail_pct: Decimal
    contributors: list[dict[str, Any]]
    path: PressurePath


def _decimal(value: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal: {value}") from exc
    if not result.is_finite() or result < ZERO or (result == ZERO and not allow_zero):
        condition = "non-negative" if allow_zero else "greater than zero"
        raise argparse.ArgumentTypeError(f"value must be finite and {condition}")
    return result


def positive_decimal(value: str) -> Decimal:
    return _decimal(value)


def nonnegative_decimal(value: str) -> Decimal:
    return _decimal(value, allow_zero=True)


def parse_leader(value: str) -> LeaderInput:
    if "=" in value:
        label, address = value.split("=", 1)
        label = label.strip()
    else:
        address = value
        label = address[-4:].lower()
    address = address.strip().lower()
    if not label:
        raise argparse.ArgumentTypeError("leader label cannot be empty")
    if not ADDRESS_RE.fullmatch(address):
        raise argparse.ArgumentTypeError(f"invalid Hyperliquid address: {address}")
    return LeaderInput(label=label, address=address)


def parse_balance(value: str) -> tuple[str, Decimal]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("balance must use LABEL=VALUE")
    label, amount = value.split("=", 1)
    if not label.strip():
        raise argparse.ArgumentTypeError("balance label cannot be empty")
    return label.strip(), positive_decimal(amount)


def round_up(value: Decimal, quantum: Decimal) -> Decimal:
    if quantum <= ZERO:
        raise ValueError("rounding quantum must be positive")
    return Decimal(math.ceil(value / quantum)) * quantum


def recommend_balance(
    current_total: Decimal,
    historical_tail_pct: Decimal,
    target_tail_pct: Decimal,
    *,
    round_to: Decimal,
    headroom_pct: Decimal = ZERO,
) -> tuple[Decimal, Decimal, Decimal]:
    """Return exact balance, rounded balance, and tail after rounding."""

    if min(current_total, historical_tail_pct, target_tail_pct, round_to) <= ZERO:
        raise ValueError("balance inputs must be positive")
    if headroom_pct < ZERO:
        raise ValueError("headroom cannot be negative")
    theoretical = current_total * historical_tail_pct / target_tail_pct
    padded = theoretical * (Decimal("1") + headroom_pct / Decimal("100"))
    recommendation = round_up(padded, round_to)
    resulting_tail = historical_tail_pct * current_total / recommendation
    return theoretical, recommendation, resulting_tail


def step_value(points: dict[int, Decimal], timestamp: int) -> Decimal:
    if not points:
        return ZERO
    times = sorted(points)
    index = bisect.bisect_right(times, timestamp) - 1
    return points[times[index]] if index >= 0 else ZERO


def build_pressure_path(lifecycles: list[risk.Lifecycle]) -> PressurePath:
    """Create regular values and instantaneous pressure observations.

    A candle adverse extreme is valid only at its candle timestamp.  Carrying
    it forward to the next fill would create a synthetic hours-long loss.
    """

    groups: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for life in lifecycles:
        if not life.metrics or life.scale is None or not life.complete_start:
            continue
        for point in life.valuations:
            groups[int(point["time_ms"])][life.life_id].append(point)

    latest: dict[str, Decimal] = {}
    regular: dict[int, Decimal] = {}
    stress: dict[int, Decimal] = {}
    for timestamp, life_groups in sorted(groups.items()):
        for life_id, points in life_groups.items():
            ordinary = [point for point in points if not point["extreme"]]
            if ordinary:
                latest[life_id] = risk.dec(ordinary[-1]["value"])
        regular_total = sum(latest.values(), ZERO)
        regular[timestamp] = regular_total
        pressure_total = regular_total
        for life_id, points in life_groups.items():
            extremes = [risk.dec(point["value"]) for point in points if point["extreme"]]
            if extremes:
                pressure_total += min(extremes) - latest.get(life_id, ZERO)
        if pressure_total < regular_total:
            stress[timestamp] = pressure_total
    if not regular:
        raise RuntimeError("no complete followable lifecycle valuations were reconstructed")
    return PressurePath(regular=regular, stress=stress)


def portfolio_drawdown(
    paths: dict[str, PressurePath],
    factors: dict[str, Decimal],
    *,
    start_ms: int | None = None,
) -> DrawdownResult:
    """Maximum combined peak-to-later-pressure-trough drawdown."""

    if not paths or set(paths) != set(factors):
        raise ValueError("paths and factors must contain the same leaders")
    if start_ms is None:
        start_ms = min(min(path.regular) for path in paths.values())
    baselines = {label: step_value(path.regular, start_ms) for label, path in paths.items()}
    timestamps = sorted(
        timestamp
        for timestamp in {
            item
            for path in paths.values()
            for item in set(path.regular) | set(path.stress)
        }
        if timestamp >= start_ms
    )
    latest = dict(baselines)
    peak = ZERO
    peak_ms = start_ms
    peak_parts = {label: ZERO for label in paths}
    best: DrawdownResult | None = None
    for timestamp in timestamps:
        for label, path in paths.items():
            if timestamp in path.regular:
                latest[label] = path.regular[timestamp]
        ordinary = {
            label: (latest[label] - baselines[label]) * factors[label]
            for label in paths
        }
        ordinary_total = sum(ordinary.values(), ZERO)
        if ordinary_total >= peak:
            peak = ordinary_total
            peak_ms = timestamp
            peak_parts = dict(ordinary)
        pressure = dict(ordinary)
        for label, path in paths.items():
            if timestamp in path.stress:
                pressure[label] += (
                    path.stress[timestamp] - path.regular.get(timestamp, latest[label])
                ) * factors[label]
        pressure_total = sum(pressure.values(), ZERO)
        amount = peak - pressure_total
        if best is None or amount > best.amount:
            best = DrawdownResult(
                amount=amount,
                peak_ms=peak_ms,
                trough_ms=timestamp,
                peak_value=peak,
                trough_value=pressure_total,
                peak_by_leader=dict(peak_parts),
                trough_by_leader=pressure,
            )
    if best is None:
        raise RuntimeError("no portfolio drawdown points were available")
    return best


def concurrent_no_offset_drawdown(
    paths: dict[str, PressurePath],
    factors: dict[str, Decimal],
    *,
    start_ms: int,
) -> tuple[Decimal, int, dict[str, Decimal]]:
    """Sum concurrent leader drawdowns from their own high-water marks."""

    baselines = {label: step_value(path.regular, start_ms) for label, path in paths.items()}
    timestamps = sorted(
        timestamp
        for timestamp in {
            item
            for path in paths.values()
            for item in set(path.regular) | set(path.stress)
        }
        if timestamp >= start_ms
    )
    latest = dict(baselines)
    high_water = {label: ZERO for label in paths}
    maximum = ZERO
    maximum_ms = start_ms
    maximum_parts = {label: ZERO for label in paths}
    for timestamp in timestamps:
        for label, path in paths.items():
            if timestamp in path.regular:
                latest[label] = path.regular[timestamp]
        ordinary = {
            label: (latest[label] - baselines[label]) * factors[label]
            for label in paths
        }
        for label in paths:
            high_water[label] = max(high_water[label], ordinary[label])
        pressure = dict(ordinary)
        for label, path in paths.items():
            if timestamp in path.stress:
                pressure[label] += (
                    path.stress[timestamp] - path.regular.get(timestamp, latest[label])
                ) * factors[label]
        parts = {
            label: max(ZERO, high_water[label] - pressure[label])
            for label in paths
        }
        total = sum(parts.values(), ZERO)
        if total > maximum:
            maximum = total
            maximum_ms = timestamp
            maximum_parts = parts
    return maximum, maximum_ms, maximum_parts


def pressure_contributors(
    lifecycles: list[risk.Lifecycle],
    drawdown: DrawdownResult,
) -> list[dict[str, Any]]:
    """Explain the maximum drawdown with raw PnL and opening capital."""

    usable = [
        life
        for life in lifecycles
        if life.metrics and life.scale is not None and life.complete_start
    ]
    life_by_id = {life.life_id: life for life in usable}
    groups: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for life in usable:
        for point in life.valuations:
            groups[int(point["time_ms"])][life.life_id].append(point)

    latest: dict[str, tuple[Decimal, str]] = {}
    peak_state: dict[str, tuple[Decimal, str]] = {}
    trough_state: dict[str, tuple[Decimal, str]] = {}
    for timestamp, life_groups in sorted(groups.items()):
        for life_id, points in life_groups.items():
            ordinary = [point for point in points if not point["extreme"]]
            if ordinary:
                point = ordinary[-1]
                latest[life_id] = (
                    risk.dec(point["value"]),
                    str(point["kind"]),
                )
        if timestamp == drawdown.peak_ms:
            peak_state = dict(latest)
        if timestamp == drawdown.trough_ms:
            trough_state = dict(latest)
            # Match build_pressure_path: each complete-candle adverse extreme
            # is instantaneous at the trough timestamp and replaces that
            # lifecycle's ordinary value only for this pressure observation.
            for life_id, points in life_groups.items():
                extremes = [point for point in points if point["extreme"]]
                if extremes:
                    point = min(extremes, key=lambda item: risk.dec(item["value"]))
                    trough_state[life_id] = (
                        risk.dec(point["value"]),
                        str(point["kind"]),
                    )
            break

    if not peak_state or not trough_state:
        raise RuntimeError("drawdown contributor states could not be reconstructed")

    rows: list[dict[str, Any]] = []
    for life_id in set(peak_state) | set(trough_state):
        before = peak_state.get(life_id, (ZERO, ""))[0]
        after, kind = trough_state.get(life_id, (ZERO, ""))
        normalized_change = after - before
        if abs(normalized_change) < Decimal("0.50"):
            continue
        life = life_by_id[life_id]
        rows.append(
            {
                "market": life.coin,
                "direction": life.side_label,
                "start_ms": life.start_ms,
                "equity_at_open": risk.dec(life.equity_at_open),
                "raw_change": normalized_change / risk.dec(life.scale),
                "normalized_change": normalized_change,
                "trough_kind": kind,
            }
        )
    return sorted(rows, key=lambda row: row["normalized_change"])


def analyze(
    leader: LeaderInput,
    *,
    client: risk.PublicInfoClient,
    end_ms: int,
    follower_balance: Decimal,
    min_order_value: Decimal,
    target_tail_pct: Decimal,
    round_to: Decimal,
    headroom_pct: Decimal,
) -> Evaluation:
    label, address = leader.label, leader.address
    print(f"[{label}] fetching public history", file=sys.stderr)
    fills, saturated = risk.fetch_fills(client, label, address, end_ms)
    events = risk.logical_fills(fills)
    portfolio = risk.fetch_portfolio(client, label, address)
    funding = risk.fetch_funding(client, label, address, end_ms)
    ledger = risk.fetch_ledger(client, label, address, end_ms)
    exact = risk.build_lifecycles(label, events, end_ms)
    risk.assign_equity_and_funding(exact, funding)
    by_coin: dict[str, list[risk.Lifecycle]] = defaultdict(list)
    for life in exact:
        by_coin[life.coin].append(life)
    candles: dict[str, list[dict[str, Any]]] = {}
    for index, (coin, lives) in enumerate(sorted(by_coin.items()), 1):
        start_ms = max(0, min(life.start_ms for life in lives) - risk.FOUR_HOURS_MS)
        candles[coin] = risk.fetch_candles(client, label, coin, start_ms, end_ms)
        if index % 20 == 0:
            print(f"[{label}] candles {index}/{len(by_coin)}", file=sys.stderr)
    for life in exact:
        risk.lifecycle_valuations(life, candles.get(life.coin, []))
    normalizer = risk.CapitalNormalizer(
        address=address,
        portfolio=portfolio,
        ledger=ledger,
        raw_perp_curve=risk.aggregate_lifecycle_curve(exact, include_extremes=False),
    )
    followable = risk.build_followable_lifecycles(
        label,
        events,
        end_ms,
        normalizer,
        follower_base=follower_balance,
        min_order_value=min_order_value,
    )
    risk.assign_equity_and_funding(followable, funding)
    risk.apply_equity_normalization(followable, normalizer)
    # The shared report module normalizes to its 20k constant.  Adjust only if
    # this CLI was explicitly asked to use a different follower base.
    scale_adjustment = follower_balance / risk.FOLLOWER_BASE
    usable: list[risk.Lifecycle] = []
    for life in followable:
        if life.scale is not None:
            life.scale *= scale_adjustment
            risk.lifecycle_valuations(life, candles.get(life.coin, []))
        if life.metrics and life.scale is not None and life.complete_start:
            usable.append(life)
    path = build_pressure_path(usable)
    drawdown = portfolio_drawdown({label: path}, {label: Decimal("1")})
    historical_tail_pct = drawdown.amount / follower_balance * Decimal("100")
    theoretical, recommendation, resulting_tail = recommend_balance(
        normalizer.current_total_account_value,
        historical_tail_pct,
        target_tail_pct,
        round_to=round_to,
        headroom_pct=headroom_pct,
    )
    fill_times = [int(fill.get("time") or 0) for fill in fills]
    gaps = [
        float(life.equity_sample_gap_days)
        for life in usable
        if life.equity_sample_gap_days is not None
    ]
    return Evaluation(
        leader=leader,
        current_spot=normalizer.current_spot_account_value,
        current_perp=normalizer.current_perp_account_value,
        current_total=normalizer.current_total_account_value,
        fill_start_ms=min(fill_times) if fill_times else None,
        fill_end_ms=max(fill_times) if fill_times else None,
        fill_count=len(fills),
        logical_fill_count=len(events),
        complete_lifecycles=sum(1 for life in usable if life.complete_end),
        saturated=saturated,
        capital_sample_count=len(normalizer.samples),
        max_capital_sample_gap_days=max(gaps, default=None),
        drawdown=drawdown,
        historical_tail_pct=historical_tail_pct,
        theoretical_balance=theoretical,
        recommended_balance=recommendation,
        recommended_tail_pct=resulting_tail,
        contributors=pressure_contributors(usable, drawdown),
        path=path,
    )


def money(value: Decimal, digits: int = 2) -> str:
    return f"${value:,.{digits}f}"


def percent(value: Decimal, digits: int = 2) -> str:
    return f"{value:.{digits}f}%"


def render(
    evaluations: list[Evaluation],
    applied_balances: dict[str, Decimal],
    *,
    follower_balance: Decimal,
    target_tail_pct: Decimal,
    joint_limit_pct: Decimal,
) -> tuple[str, dict[str, Any] | None]:
    lines = [
        "# Leader balance evaluation",
        "",
        f"Follower balance basis: **{money(follower_balance, 0)}**  ",
        f"Individual portfolio pressure target: **{percent(target_tail_pct)}**  ",
        f"Joint limit: **{percent(joint_limit_pct)}**  ",
        "Multiplier: **excluded from this calibration**.",
        "",
        "| Leader | Current Account Total | Historical pressure tail | Exact target balance | Applied balance | Applied tail |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in evaluations:
        balance = applied_balances[item.leader.label]
        applied_tail = item.historical_tail_pct * item.current_total / balance
        lines.append(
            f"| {item.leader.label} | {money(item.current_total)} | "
            f"{percent(item.historical_tail_pct)} | {money(item.theoretical_balance, 0)} | "
            f"{money(balance, 0)} | {percent(applied_tail)} |"
        )

    for item in evaluations:
        lines += [
            "",
            f"## {item.leader.label} — `{item.leader.address}`",
            "",
            f"- History: {risk.iso(item.fill_start_ms)} → {risk.iso(item.fill_end_ms)} UTC; "
            f"{item.fill_count:,} fill fragments, {item.logical_fill_count:,} logical fills, "
            f"{item.complete_lifecycles:,} complete followable lifecycles.",
            f"- Current Account Total: Spot {money(item.current_spot)} + Perpetual "
            f"{money(item.current_perp)} = **{money(item.current_total)}**.",
            f"- Maximum portfolio pressure: {risk.iso(item.drawdown.peak_ms)} → "
            f"{risk.iso(item.drawdown.trough_ms)} UTC; normalized PnL "
            f"{money(item.drawdown.peak_value)} → {money(item.drawdown.trough_value)}; "
            f"drawdown **{money(item.drawdown.amount)} "
            f"({percent(item.historical_tail_pct)})**.",
            f"- Recommendation: exact {money(item.theoretical_balance)}, rounded upward to "
            f"**{money(item.recommended_balance, 0)}**, resulting tail "
            f"**{percent(item.recommended_tail_pct)}**.",
        ]
        if item.saturated:
            lines.append(
                "- **History warning:** public fills reached/exceeded the documented 10k boundary; "
                "older history may be incomplete."
            )
        if item.max_capital_sample_gap_days is not None:
            lines.append(
                f"- Account Total samples: {item.capital_sample_count}; maximum nearest-sample "
                f"gap {item.max_capital_sample_gap_days:.1f} days. Older capital values are estimates."
            )
        lines += [
            "",
            "### Auditable pressure contributors",
            "",
            "| Market | Side | Opened UTC | Account Total at open | Raw PnL change | Normalized change | Observation |",
            "|---|---|---|---:|---:|---:|---|",
        ]
        for row in item.contributors:
            lines.append(
                f"| {row['market']} | {row['direction']} | {risk.iso(row['start_ms'])} | "
                f"{money(row['equity_at_open'])} | {money(row['raw_change'])} | "
                f"{money(row['normalized_change'])} | {row['trough_kind']} |"
            )

    joint_json: dict[str, Any] | None = None
    if len(evaluations) > 1:
        paths = {item.leader.label: item.path for item in evaluations}
        factors = {
            item.leader.label: item.current_total / applied_balances[item.leader.label]
            for item in evaluations
        }
        common_start = max(min(path.regular) for path in paths.values())
        joint = portfolio_drawdown(paths, factors, start_ms=common_start)
        no_offset, no_offset_ms, no_offset_parts = concurrent_no_offset_drawdown(
            paths,
            factors,
            start_ms=common_start,
        )
        joint_pct = joint.amount / follower_balance * Decimal("100")
        no_offset_pct = no_offset / follower_balance * Decimal("100")
        lines += [
            "",
            "## Joint validation",
            "",
            f"Common history starts **{risk.iso(common_start)} UTC**.",
            "",
            f"- Net joint maximum: **{money(joint.amount)} ({percent(joint_pct)})**, "
            f"{risk.iso(joint.peak_ms)} → {risk.iso(joint.trough_ms)} UTC.",
            f"- Concurrent no-profit-offset sum: **{money(no_offset)} "
            f"({percent(no_offset_pct)})**, {risk.iso(no_offset_ms)} UTC.",
            f"- Joint limit {percent(joint_limit_pct)}: net "
            f"**{'PASS' if joint_pct <= joint_limit_pct else 'FAIL'}**, no-offset "
            f"**{'PASS' if no_offset_pct <= joint_limit_pct else 'FAIL'}**.",
            "",
            "| Leader | Net-joint peak | Net-joint trough | Change | No-offset contribution |",
            "|---|---:|---:|---:|---:|",
        ]
        for label in paths:
            peak = joint.peak_by_leader[label]
            trough = joint.trough_by_leader[label]
            lines.append(
                f"| {label} | {money(peak)} | {money(trough)} | "
                f"{money(trough - peak)} | {money(no_offset_parts[label])} |"
            )
        joint_json = {
            "common_start_ms": common_start,
            "net_drawdown": joint.amount,
            "net_drawdown_pct": joint_pct,
            "net_peak_ms": joint.peak_ms,
            "net_trough_ms": joint.trough_ms,
            "no_offset_drawdown": no_offset,
            "no_offset_drawdown_pct": no_offset_pct,
            "no_offset_ms": no_offset_ms,
        }

    lines += [
        "",
        "## Limits",
        "",
        "- Historical calibration cannot guarantee that future losses or correlations stay inside the sample.",
        "- Pressure uses real fills and complete 4h adverse highs/lows; shorter intrabar spikes can be missed.",
        "- A hard future joint limit requires runtime account-level enforcement; static balances alone cannot guarantee it.",
        "- This tool reads public leader data only and never reads or changes live bot configuration.",
        "",
    ]
    return "\n".join(lines), joint_json


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate public Hyperliquid leaders and recommend fixed account values."
    )
    parser.add_argument("leaders", nargs="+", type=parse_leader, metavar="[LABEL=]ADDRESS")
    parser.add_argument("--target-tail-pct", type=positive_decimal, default=Decimal("7"))
    parser.add_argument("--joint-limit-pct", type=positive_decimal, default=Decimal("15"))
    parser.add_argument("--follower-balance", type=positive_decimal, default=Decimal("20000"))
    parser.add_argument("--min-order-value", type=positive_decimal, default=Decimal("10"))
    parser.add_argument("--round-to", type=positive_decimal, default=Decimal("10000"))
    parser.add_argument("--headroom-pct", type=nonnegative_decimal, default=ZERO)
    parser.add_argument(
        "--balance",
        action="append",
        type=parse_balance,
        default=[],
        metavar="LABEL=VALUE",
        help="override a recommendation during joint validation",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/copytrade_leader_balance_evaluator"),
    )
    parser.add_argument("--end-ms", type=int, help="fixed cutoff for a reproducible cached run")
    parser.add_argument("--output", type=Path, help="write the Markdown report")
    parser.add_argument("--json-output", type=Path, help="write a machine-readable summary")
    args = parser.parse_args()

    labels = [leader.label for leader in args.leaders]
    if len(labels) != len(set(labels)):
        parser.error("leader labels must be unique")
    overrides = dict(args.balance)
    unknown = set(overrides) - set(labels)
    if unknown:
        parser.error(f"unknown balance label(s): {', '.join(sorted(unknown))}")

    end_ms = args.end_ms or int(time.time() * 1000)
    # The cutoff is part of the cache path, so current Account Total is fresh
    # on every normal invocation.  Passing a fixed cutoff intentionally reuses
    # a reproducible cache.
    cache_root = args.cache_dir / str(end_ms)
    evaluations = []
    for leader in args.leaders:
        # Include the address in the cache namespace.  Reusing a human label
        # such as "candidate" for another address must never return the first
        # candidate's cached public history.
        client = risk.PublicInfoClient(cache_root / f"{leader.label}_{leader.address[2:]}")
        evaluations.append(
            analyze(
                leader,
                client=client,
                end_ms=end_ms,
                follower_balance=args.follower_balance,
                min_order_value=args.min_order_value,
                target_tail_pct=args.target_tail_pct,
                round_to=args.round_to,
                headroom_pct=args.headroom_pct,
            )
        )
    balances = {
        item.leader.label: overrides.get(item.leader.label, item.recommended_balance)
        for item in evaluations
    }
    report, joint = render(
        evaluations,
        balances,
        follower_balance=args.follower_balance,
        target_tail_pct=args.target_tail_pct,
        joint_limit_pct=args.joint_limit_pct,
    )
    print(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    if args.json_output:
        payload = {
            "cutoff_ms": end_ms,
            "follower_balance": args.follower_balance,
            "target_tail_pct": args.target_tail_pct,
            "joint_limit_pct": args.joint_limit_pct,
            "leaders": [
                {
                    "label": item.leader.label,
                    "address": item.leader.address,
                    "current_account_total": item.current_total,
                    "historical_pressure_tail_pct": item.historical_tail_pct,
                    "theoretical_balance": item.theoretical_balance,
                    "recommended_balance": item.recommended_balance,
                    "applied_balance": balances[item.leader.label],
                    "applied_tail_pct": item.historical_tail_pct
                    * item.current_total
                    / balances[item.leader.label],
                    "pressure_drawdown": item.drawdown.amount,
                    "pressure_peak_ms": item.drawdown.peak_ms,
                    "pressure_trough_ms": item.drawdown.trough_ms,
                    "contributors": item.contributors,
                }
                for item in evaluations
            ],
            "joint": joint,
        }
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(jsonable(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
