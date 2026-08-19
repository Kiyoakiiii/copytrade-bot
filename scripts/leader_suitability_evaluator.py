#!/usr/bin/env python3
"""Evaluate whether public Hyperliquid leaders are economically copyable.

The evaluator is deliberately offline and read-only.  It consumes only public
Hyperliquid data and never imports the live backend, signer configuration, bot
secrets, or follower account state.  Reports identify leaders by caller-chosen
labels only; complete wallet addresses are neither rendered nor written to the
JSON output.  Cache namespaces use a one-way digest instead of an address.

The methodology separates four questions which should not be collapsed into a
single PnL curve:

1. Is the public history sufficiently complete to support a decision?
2. Does normalized net PnL remain positive after conservative copy friction?
3. Are drawdown, adverse-excursion duration, and loss management acceptable?
4. Can the strategy still be copied after sizing its historical pressure tail
   to the configured target, including Hyperliquid's 10 USDC minimum order?

High frequency, maker usage, and small-market concentration are observations,
not automatic penalties.  They matter only through measured copy economics.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import leader_loss_risk_report as risk
import leader_balance_evaluator as balance


ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
TEN_THOUSAND = Decimal("10000")
DAY_MS = 86_400_000
FOLLOWER_BASE = Decimal("20000")
MIN_ORDER_VALUE = Decimal("10")
DEFAULT_FRICTION_BPS = Decimal("5")
DEFAULT_TARGET_TAIL_PCT = Decimal("7")
DEFAULT_ROUND_TO = Decimal("10000")


@dataclass(frozen=True)
class LeaderSpec:
    label: str
    address: str
    group: str | None = None

    def __post_init__(self) -> None:
        """Keep programmatic callers on the same validated path as the CLI.

        Historical normalization matches public ledger rows by address.  A
        missing ``0x`` prefix would otherwise make every transfer comparison
        fail silently and can materially distort the reconstructed capital.
        """

        address = self.address.strip().lower()
        if (
            len(address) != 42
            or not address.startswith("0x")
            or any(character not in "0123456789abcdef" for character in address[2:])
        ):
            raise ValueError("invalid public address")
        object.__setattr__(self, "address", address)


@dataclass
class LeaderAnalysis:
    spec: LeaderSpec
    summary: dict[str, Any]
    fills: list[dict[str, Any]]
    logical_events: list[dict[str, Any]]
    lifecycles: list[risk.Lifecycle]
    metrics: dict[str, Any]
    normalizer: risk.CapitalNormalizer | None = None
    raw_lifecycles: list[risk.Lifecycle] | None = None
    raw_pressure_path: balance.PressurePath | None = None
    exposure_model: balance.ExposureRiskModel | None = None
    score: Decimal = ZERO
    verdict: str = ""
    hard_failures: list[str] | None = None
    warnings: list[str] | None = None
    group_conflict_open_pct: Decimal | None = None


def dec(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def bounded(value: Decimal, low: Decimal = ZERO, high: Decimal = ONE) -> Decimal:
    return max(low, min(high, value))


def ascending_score(value: Decimal, zero_at: Decimal, full_at: Decimal) -> Decimal:
    if full_at <= zero_at:
        raise ValueError("full_at must exceed zero_at")
    return bounded((value - zero_at) / (full_at - zero_at))


def descending_score(value: Decimal, full_at: Decimal, zero_at: Decimal) -> Decimal:
    if zero_at <= full_at:
        raise ValueError("zero_at must exceed full_at")
    return bounded((zero_at - value) / (zero_at - full_at))


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def round_up(value: Decimal, quantum: Decimal) -> Decimal:
    if value <= ZERO:
        return quantum
    return Decimal(math.ceil(value / quantum)) * quantum


def safe_label(value: str) -> str:
    label = value.strip()
    if not label or len(label) > 32:
        raise argparse.ArgumentTypeError("leader label must contain 1-32 characters")
    if not all(character.isalnum() or character in "_-" for character in label):
        raise argparse.ArgumentTypeError(
            "leader label may contain only letters, numbers, '_' and '-'"
        )
    return label


def parse_leader(value: str) -> LeaderSpec:
    if "=" not in value:
        raise argparse.ArgumentTypeError("leader must use LABEL=PUBLIC_ADDRESS")
    raw_label, raw_address = value.split("=", 1)
    label = safe_label(raw_label)
    address = raw_address.strip().lower()
    if (
        len(address) != 42
        or not address.startswith("0x")
        or any(character not in "0123456789abcdef" for character in address[2:])
    ):
        # Never echo the rejected value; command-line errors can end up in logs.
        raise argparse.ArgumentTypeError("invalid public address")
    return LeaderSpec(label=label, address=address)


def parse_group(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("group must use LABEL=GROUP")
    raw_label, raw_group = value.split("=", 1)
    label = safe_label(raw_label)
    group = safe_label(raw_group)
    return label, group


def private_cache_namespace(root: Path, cutoff_ms: int, spec: LeaderSpec) -> Path:
    return risk.public_history_cache_namespace(root, cutoff_ms, spec.address)


def build_public_history(
    spec: LeaderSpec,
    *,
    cutoff_ms: int,
    cache_root: Path,
) -> LeaderAnalysis:
    print(f"[{spec.label}] fetching public history", file=sys.stderr, flush=True)
    # Suitability runs can span several leaders and hundreds of candle series.
    # Keep this deliberately slower than the live bot's background clients so
    # an offline research run cannot create a public Info API request burst.
    client = risk.PublicInfoClient(
        private_cache_namespace(cache_root, cutoff_ms, spec),
        pause_seconds=0.25,
    )
    fills, saturated = risk.fetch_fills(client, spec.label, spec.address, cutoff_ms)
    events = risk.logical_fills(fills)
    portfolio = risk.fetch_portfolio(client, spec.label, spec.address)
    funding = risk.fetch_funding(client, spec.label, spec.address, cutoff_ms)
    ledger = risk.fetch_ledger(client, spec.label, spec.address, cutoff_ms)

    exact = risk.build_lifecycles(spec.label, events, cutoff_ms)
    risk.assign_equity_and_funding(exact, funding)
    by_coin: dict[str, list[risk.Lifecycle]] = defaultdict(list)
    for life in exact:
        by_coin[life.coin].append(life)

    candles: dict[str, list[dict[str, Any]]] = {}
    for index, (coin, lives) in enumerate(sorted(by_coin.items()), 1):
        start_ms = max(0, min(life.start_ms for life in lives) - risk.FOUR_HOURS_MS)
        candles[coin] = risk.fetch_candles(
            client,
            spec.label,
            coin,
            start_ms,
            cutoff_ms,
        )
        if index % 20 == 0:
            print(
                f"[{spec.label}] candles {index}/{len(by_coin)}",
                file=sys.stderr,
                flush=True,
            )

    for life in exact:
        risk.lifecycle_valuations(life, candles.get(life.coin, []))
    normalizer = risk.CapitalNormalizer(
        address=spec.address,
        portfolio=portfolio,
        ledger=ledger,
        raw_perp_curve=risk.aggregate_lifecycle_curve(exact, include_extremes=False),
    )
    raw_usable = [
        life
        for life in exact
        if life.metrics and life.scale is not None and life.complete_start
    ]
    raw_pressure_path = balance.build_pressure_path(raw_usable)
    raw_drawdown = balance.portfolio_drawdown(
        {spec.label: raw_pressure_path},
        {spec.label: ONE},
    )
    exposure_model = balance.build_exposure_risk_model(
        lifecycles=raw_usable,
        normalizer=normalizer,
        raw_path=raw_pressure_path,
        raw_drawdown=raw_drawdown,
        end_ms=cutoff_ms,
    )
    followable = risk.build_followable_lifecycles(
        spec.label,
        events,
        cutoff_ms,
        normalizer,
        follower_base=FOLLOWER_BASE,
        min_order_value=MIN_ORDER_VALUE,
    )
    unassigned_count, unassigned_value = risk.assign_equity_and_funding(
        followable,
        funding,
    )
    unavailable = risk.apply_equity_normalization(followable, normalizer)
    for life in followable:
        if life.scale is not None:
            risk.lifecycle_valuations(life, candles.get(life.coin, []))

    summary = risk.leader_summary(
        spec.label,
        spec.address,
        fills,
        saturated,
        followable,
        portfolio,
        normalizer,
        unavailable,
        unassigned_count,
        unassigned_value,
    )
    return LeaderAnalysis(
        spec=spec,
        summary=summary,
        fills=fills,
        logical_events=events,
        lifecycles=followable,
        metrics={},
        normalizer=normalizer,
        raw_lifecycles=raw_usable,
        raw_pressure_path=raw_pressure_path,
        exposure_model=exposure_model,
    )


def lifecycle_turnover(life: risk.Lifecycle) -> Decimal:
    if life.scale is None or not life.complete_start or not life.complete_end:
        return ZERO
    previous = ZERO
    turnover = ZERO
    for snapshot in life.snapshots:
        delta = abs(snapshot.position - previous)
        turnover += delta * snapshot.mark_price * dec(life.scale)
        previous = snapshot.position
    return turnover


def profit_factor(values: Iterable[Decimal]) -> Decimal:
    values = list(values)
    gross_profit = sum((value for value in values if value > ZERO), ZERO)
    gross_loss = abs(sum((value for value in values if value < ZERO), ZERO))
    if gross_loss == ZERO:
        return Decimal("99") if gross_profit > ZERO else ZERO
    return gross_profit / gross_loss


def flow_neutral_path_metrics(
    path: balance.PressurePath,
    normalizer: risk.CapitalNormalizer,
) -> dict[str, Decimal]:
    """Return chain-linked transfer-neutral return and drawdown fractions.

    ``PressurePath`` values are raw cumulative trading PnL. Each PnL increment
    is divided by Account Total immediately before that increment, then returns
    are geometrically chain-linked into a unit NAV. Deposits and withdrawals
    therefore update the next denominator without appearing as profit or loss.
    Directly adding period returns is not a valid drawdown calculation and can
    produce impossible values above 100%.

    Adverse candle observations are one-shot stresses.  They are compared to
    the ordinary mark at the same timestamp and do not mutate the regular
    return path.
    """

    timestamps = sorted(set(path.regular) | set(path.stress))
    wealth = ONE
    previous_raw_value = ZERO
    peak_wealth = ONE
    regular_drawdown = ZERO
    pressure_drawdown = ZERO
    valid_updates = 0
    invalid_updates = 0

    for timestamp in timestamps:
        denominator = normalizer.value_at(max(0, timestamp - 1))
        regular_value = path.regular.get(timestamp, previous_raw_value)
        wealth_before_update = wealth
        period_return = ZERO
        if timestamp in path.regular and denominator > ZERO:
            period_return = (regular_value - previous_raw_value) / denominator
            previous_raw_value = regular_value
            valid_updates += 1
            if period_return <= -ONE:
                # A reconstructed account cannot lose more than all equity in
                # one interval. Preserve a conservative 100% drawdown and make
                # the data-quality failure visible to the caller.
                wealth = ZERO
                invalid_updates += 1
            else:
                wealth *= ONE + period_return

        peak_wealth = max(peak_wealth, wealth)
        regular_drawdown = max(
            regular_drawdown,
            (peak_wealth - wealth) / peak_wealth,
        )

        if timestamp in path.stress and denominator > ZERO:
            stressed_period_return = period_return + (
                path.stress[timestamp] - regular_value
            ) / denominator
            stress_wealth = (
                ZERO
                if stressed_period_return <= -ONE
                else wealth_before_update * (ONE + stressed_period_return)
            )
            pressure_drawdown = max(
                pressure_drawdown,
                (peak_wealth - stress_wealth) / peak_wealth,
            )
        pressure_drawdown = max(pressure_drawdown, regular_drawdown)

    return {
        "final_return": wealth - ONE,
        "regular_drawdown": regular_drawdown,
        "pressure_drawdown": pressure_drawdown,
        "valid_updates": Decimal(valid_updates),
        "invalid_updates": Decimal(invalid_updates),
    }


def raw_lifecycle_path(life: risk.Lifecycle) -> balance.PressurePath:
    """Recover a raw-USDC path from a follower-normalized lifecycle."""

    scale = dec(life.scale)
    if scale <= ZERO:
        return balance.PressurePath(regular={}, stress={})
    regular: dict[int, Decimal] = {}
    stress: dict[int, Decimal] = {}
    for point in life.valuations:
        timestamp = int(point.get("time_ms") or 0)
        raw_value = dec(point.get("value")) / scale
        if bool(point.get("extreme")):
            stress[timestamp] = min(stress.get(timestamp, raw_value), raw_value)
        else:
            regular[timestamp] = raw_value
    return balance.PressurePath(regular=regular, stress=stress)


def flow_neutral_lifecycle_row(
    life: risk.Lifecycle,
    normalizer: risk.CapitalNormalizer,
    *,
    friction_bps: Decimal,
) -> dict[str, Any]:
    """Measure one lifecycle without fixing its opening equity forever."""

    path_metrics = flow_neutral_path_metrics(raw_lifecycle_path(life), normalizer)
    normalized_net = path_metrics["final_return"] * FOLLOWER_BASE

    previous_position = ZERO
    normalized_turnover = ZERO
    for snapshot in life.snapshots:
        denominator = normalizer.value_at(max(0, snapshot.time_ms - 1))
        raw_turnover = abs(snapshot.position - previous_position) * abs(
            snapshot.mark_price
        )
        if denominator > ZERO:
            normalized_turnover += raw_turnover / denominator * FOLLOWER_BASE
        previous_position = snapshot.position

    friction_cost = normalized_turnover * friction_bps / TEN_THOUSAND
    return {
        "life": life,
        "final_net": normalized_net,
        "turnover": normalized_turnover,
        "friction_net": normalized_net - friction_cost,
        "hold_hours": dec(life.metrics.get("hold_hours")),
        "regular_drawdown_pct": path_metrics["regular_drawdown"] * HUNDRED,
        "pressure_drawdown_pct": path_metrics["pressure_drawdown"] * HUNDRED,
    }


def own_liquidation_metrics(
    fills: list[dict[str, Any]],
    address: str,
) -> dict[str, Any]:
    timestamps: set[int] = set()
    market_events: set[tuple[int, str]] = set()
    fragments = 0
    markets: set[str] = set()
    for fill in fills:
        liquidation = fill.get("liquidation")
        if not isinstance(liquidation, dict):
            continue
        liquidated_user = str(liquidation.get("liquidatedUser") or "").lower()
        if liquidated_user != address:
            continue
        fragments += 1
        timestamp = int(fill.get("time") or 0)
        market = str(fill.get("coin") or "")
        timestamps.add(timestamp)
        market_events.add((timestamp, market))
        if market:
            markets.add(market)

    clusters: list[list[int]] = []
    for timestamp in sorted(timestamps):
        if not clusters or timestamp - clusters[-1][-1] > 30 * 60 * 1000:
            clusters.append([timestamp])
        else:
            clusters[-1].append(timestamp)
    return {
        "fragments": fragments,
        "market_time_events": len(market_events),
        "timestamp_count": len(timestamps),
        "cluster_count": len(clusters),
        "latest_ms": max(timestamps, default=None),
        "market_count": len(markets),
    }


def compute_metrics(
    analysis: LeaderAnalysis,
    *,
    cutoff_ms: int,
    friction_bps: Decimal,
    target_tail_pct: Decimal,
    round_to: Decimal,
) -> None:
    summary = analysis.summary
    complete = [
        life
        for life in analysis.lifecycles
        if life.metrics
        and life.scale is not None
        and life.complete_start
        and life.complete_end
    ]
    rows: list[dict[str, Any]] = []
    for life in complete:
        if analysis.normalizer is not None:
            rows.append(
                flow_neutral_lifecycle_row(
                    life,
                    analysis.normalizer,
                    friction_bps=friction_bps,
                )
            )
            continue
        # Programmatic/unit-test fallback for an analysis without public
        # capital history.  Real CLI evaluations always use the flow-neutral
        # branch above.
        final_net = dec(life.metrics.get("final_net"))
        turnover = lifecycle_turnover(life)
        friction_cost = turnover * friction_bps / TEN_THOUSAND
        rows.append(
            {
                "life": life,
                "final_net": final_net,
                "turnover": turnover,
                "friction_net": final_net - friction_cost,
                "hold_hours": dec(life.metrics.get("hold_hours")),
                "regular_drawdown_pct": (
                    abs(min(final_net, ZERO)) / FOLLOWER_BASE * HUNDRED
                ),
                "pressure_drawdown_pct": (
                    abs(min(dec(life.metrics.get("worst_total")), ZERO))
                    / FOLLOWER_BASE
                    * HUNDRED
                ),
            }
        )

    fill_start_ms = summary.get("fill_start_ms")
    fill_end_ms = summary.get("fill_end_ms")
    history_days = (
        Decimal(str((int(fill_end_ms) - int(fill_start_ms)) / DAY_MS))
        if fill_start_ms and fill_end_ms and int(fill_end_ms) > int(fill_start_ms)
        else ZERO
    )
    net = sum((row["final_net"] for row in rows), ZERO)
    friction_net = sum((row["friction_net"] for row in rows), ZERO)
    turnover = sum((row["turnover"] for row in rows), ZERO)
    base_pf = profit_factor(row["final_net"] for row in rows)
    friction_pf = profit_factor(row["friction_net"] for row in rows)
    breakeven_bps = net / turnover * TEN_THOUSAND if turnover > ZERO else ZERO
    annual_return_pct = (
        net / FOLLOWER_BASE * Decimal("365") / history_days * HUNDRED
        if history_days > ZERO
        else ZERO
    )
    friction_annual_return_pct = (
        friction_net / FOLLOWER_BASE * Decimal("365") / history_days * HUNDRED
        if history_days > ZERO
        else ZERO
    )

    monthly: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows:
        end_ms = row["life"].end_ms
        if end_ms is None:
            continue
        month = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m")
        monthly[month] += row["friction_net"]
    profitable_month_pct = (
        Decimal(sum(value > ZERO for value in monthly.values()))
        / Decimal(len(monthly))
        * HUNDRED
        if monthly
        else ZERO
    )

    gross_wins = sorted(
        (row["final_net"] for row in rows if row["final_net"] > ZERO),
        reverse=True,
    )
    gross_profit = sum(gross_wins, ZERO)
    top3_win_concentration_pct = (
        sum(gross_wins[:3], ZERO) / gross_profit * HUNDRED if gross_profit else ZERO
    )
    fast_profit = sum(
        (
            row["final_net"]
            for row in rows
            if row["final_net"] > ZERO and row["hold_hours"] <= Decimal("0.0833333333")
        ),
        ZERO,
    )
    fast_profit_share_pct = fast_profit / gross_profit * HUNDRED if gross_profit else ZERO

    if analysis.normalizer is not None and analysis.raw_pressure_path is not None:
        account_path_metrics = flow_neutral_path_metrics(
            analysis.raw_pressure_path,
            analysis.normalizer,
        )
        pressure_tail_pct = account_path_metrics["pressure_drawdown"] * HUNDRED
        regular_pressure_tail_pct = (
            account_path_metrics["regular_drawdown"] * HUNDRED
        )
        pressure_drawdown = pressure_tail_pct / HUNDRED * FOLLOWER_BASE
        invalid_return_update_count = account_path_metrics["invalid_updates"]
    else:
        pressure_drawdown = dec(summary["extreme_max_drawdown"][0])
        pressure_tail_pct = pressure_drawdown / FOLLOWER_BASE * HUNDRED
        regular_pressure_tail_pct = (
            dec(summary["regular_max_drawdown"][0]) / FOLLOWER_BASE * HUNDRED
        )
        invalid_return_update_count = ZERO
    current_total = dec(summary.get("current_total_account_value"))
    if (
        analysis.exposure_model is not None
        and analysis.exposure_model.projected_raw_drawdown > ZERO
    ):
        theoretical_balance, recommended_balance, _ = (
            balance.recommend_balance_for_projected_loss(
                analysis.exposure_model.projected_raw_drawdown,
                target_tail_pct,
                round_to=round_to,
            )
        )
    else:
        theoretical_balance = (
            current_total * pressure_tail_pct / target_tail_pct
            if current_total > ZERO and pressure_tail_pct > ZERO
            else current_total
        )
        recommended_balance = round_up(theoretical_balance, round_to)

    logical_notionals = [
        abs(dec(event.get("sz")) * dec(event.get("px")))
        for event in analysis.logical_events
    ]
    copied_notionals = [
        notional * FOLLOWER_BASE / recommended_balance
        for notional in logical_notionals
        if recommended_balance > ZERO
    ]
    skipped = [notional for notional in copied_notionals if notional < MIN_ORDER_VALUE]
    skip_count_pct = (
        Decimal(len(skipped)) / Decimal(len(copied_notionals)) * HUNDRED
        if copied_notionals
        else ZERO
    )
    copied_total = sum(copied_notionals, ZERO)
    skip_notional_pct = sum(skipped, ZERO) / copied_total * HUNDRED if copied_total else ZERO

    perp_fills = [fill for fill in analysis.fills if risk.is_perp_fill(fill)]
    raw_notional = sum(
        (abs(dec(fill.get("sz")) * dec(fill.get("px"))) for fill in perp_fills),
        ZERO,
    )
    maker_notional = sum(
        (
            abs(dec(fill.get("sz")) * dec(fill.get("px")))
            for fill in perp_fills
            if fill.get("crossed") is False
        ),
        ZERO,
    )
    maker_notional_pct = maker_notional / raw_notional * HUNDRED if raw_notional else ZERO
    raw_fee = sum((dec(fill.get("fee")) for fill in perp_fills), ZERO)
    raw_fee_bps = raw_fee / raw_notional * TEN_THOUSAND if raw_notional else ZERO

    event_times = [int(event.get("time") or 0) for event in analysis.logical_events]
    intervals = [later - earlier for earlier, later in zip(event_times, event_times[1:])]
    interarrival_le_1s_pct = (
        Decimal(sum(interval <= 1000 for interval in intervals))
        / Decimal(len(intervals))
        * HUNDRED
        if intervals
        else ZERO
    )
    events_per_second = Counter(timestamp // 1000 for timestamp in event_times)
    active_day_counts = Counter(timestamp // DAY_MS for timestamp in event_times)
    p95_events_per_active_day = percentile(list(active_day_counts.values()), 0.95) or 0.0

    losing = [row for row in rows if row["final_net"] < ZERO]
    losing_holds = [float(row["hold_hours"]) for row in losing]
    p95_losing_hold_hours = dec(percentile(losing_holds, 0.95))
    loss_over_72h_pct = (
        Decimal(sum(hours >= 72 for hours in losing_holds))
        / Decimal(len(losing_holds))
        * HUNDRED
        if losing_holds
        else ZERO
    )
    risk_lives = [
        life
        for life in analysis.lifecycles
        if life.metrics and life.scale is not None and life.complete_start
    ]
    if analysis.normalizer is not None:
        risk_rows = [
            flow_neutral_lifecycle_row(
                life,
                analysis.normalizer,
                friction_bps=ZERO,
            )
            for life in risk_lives
        ]
    else:
        risk_rows = [
            {
                "life": life,
                "final_net": dec(life.metrics.get("final_net")),
                "pressure_drawdown_pct": (
                    abs(min(dec(life.metrics.get("worst_total")), ZERO))
                    / FOLLOWER_BASE
                    * HUNDRED
                ),
            }
            for life in risk_lives
        ]
    worst_row = max(
        risk_rows,
        key=lambda row: dec(row["pressure_drawdown_pct"]),
        default=None,
    )
    worst_life = worst_row["life"] if worst_row else None
    worst_trough_pct = (
        dec(worst_row["pressure_drawdown_pct"]) if worst_row else ZERO
    )
    worst_final = min((row["final_net"] for row in rows), default=ZERO)
    worst_final_loss_pct = abs(min(worst_final, ZERO)) / FOLLOWER_BASE * HUNDRED
    worst_lifecycle_hold_hours = (
        dec(worst_life.metrics.get("hold_hours")) if worst_life else ZERO
    )
    worst_lifecycle_adds = dec(worst_life.metrics.get("add_count")) if worst_life else ZERO
    worst_lifecycle_underwater_add_rate_pct = (
        dec(worst_life.metrics.get("underwater_add_count"))
        / worst_lifecycle_adds
        * HUNDRED
        if worst_lifecycle_adds > ZERO
        else ZERO
    )

    current_open_rows = [row for row in risk_rows if row["life"].right_censored]
    current_open_losing_rows = [
        row for row in current_open_rows if dec(row["final_net"]) < ZERO
    ]
    current_open_losing = [row["life"] for row in current_open_losing_rows]
    current_open_losing_hold_hours = [
        dec(life.metrics.get("hold_hours")) for life in current_open_losing
    ]
    current_open_worst_trough_pct = max(
        (
            dec(row["pressure_drawdown_pct"])
            for row in current_open_losing_rows
        ),
        default=ZERO,
    )
    current_open_adds = sum(
        (dec(life.metrics.get("add_count")) for life in current_open_losing),
        ZERO,
    )
    current_open_underwater_add_rate_pct = (
        sum(
            (
                dec(life.metrics.get("underwater_add_count"))
                for life in current_open_losing
            ),
            ZERO,
        )
        / current_open_adds
        * HUNDRED
        if current_open_adds > ZERO
        else ZERO
    )
    portfolio_check = summary.get("portfolio") or {}
    official_portfolio_drawdown_pct = (
        dec(portfolio_check.get("max_drawdown")) / FOLLOWER_BASE * HUNDRED
    )
    mismatch_count = dec(summary.get("position_mismatch_count"))
    mismatch_rate_pct = (
        mismatch_count / Decimal(max(1, int(summary.get("logical_fill_count") or 0))) * HUNDRED
    )
    liquidation = own_liquidation_metrics(analysis.fills, analysis.spec.address)

    analysis.metrics = {
        "history_days": history_days,
        "fill_start_ms": fill_start_ms,
        "fill_end_ms": fill_end_ms,
        "fill_limit_saturated": bool(summary.get("fill_limit_saturated")),
        "perp_fill_count": int(summary.get("perp_fill_count") or 0),
        "logical_fill_count": int(summary.get("logical_fill_count") or 0),
        "complete_lifecycle_count": len(rows),
        "market_count": int(summary.get("coins") or 0),
        "position_mismatch_rate_pct": mismatch_rate_pct,
        "current_account_total": current_total,
        "normalized_net": net,
        "normalized_turnover": turnover,
        "profit_factor": base_pf,
        "friction_bps": friction_bps,
        "friction_net": friction_net,
        "friction_profit_factor": friction_pf,
        "breakeven_friction_bps": breakeven_bps,
        "annual_simple_return_pct": annual_return_pct,
        "friction_annual_simple_return_pct": friction_annual_return_pct,
        "profitable_month_pct": profitable_month_pct,
        "month_count": len(monthly),
        "top3_win_concentration_pct": top3_win_concentration_pct,
        "fast_profit_share_pct": fast_profit_share_pct,
        "pressure_drawdown": pressure_drawdown,
        "pressure_tail_pct": pressure_tail_pct,
        "regular_pressure_tail_pct": regular_pressure_tail_pct,
        "invalid_return_update_count": invalid_return_update_count,
        "official_portfolio_drawdown_pct": official_portfolio_drawdown_pct,
        "worst_lifecycle_trough_pct": worst_trough_pct,
        "worst_lifecycle_hold_hours": worst_lifecycle_hold_hours,
        "worst_lifecycle_underwater_add_rate_pct": (
            worst_lifecycle_underwater_add_rate_pct
        ),
        "worst_closed_loss_pct": worst_final_loss_pct,
        "p95_losing_hold_hours": p95_losing_hold_hours,
        "loss_over_72h_pct": loss_over_72h_pct,
        "current_open_losing_count": len(current_open_losing),
        "max_current_open_losing_hold_hours": max(
            current_open_losing_hold_hours,
            default=ZERO,
        ),
        "current_open_worst_trough_pct": current_open_worst_trough_pct,
        "current_open_underwater_add_rate_pct": (
            current_open_underwater_add_rate_pct
        ),
        "underwater_add_rate_pct": dec(summary.get("underwater_add_rate")),
        "peak_simultaneous_notional": (
            analysis.exposure_model.peak_limit_gross
            * FOLLOWER_BASE
            / recommended_balance
            if analysis.exposure_model is not None and recommended_balance > ZERO
            else dec(summary.get("peak_notional"))
        ),
        "recommended_balance_for_target_tail": recommended_balance,
        "recommended_balance_ratio_to_current": (
            recommended_balance / current_total if current_total > ZERO else ZERO
        ),
        "safe_size_skip_count_pct": skip_count_pct,
        "safe_size_skip_notional_pct": skip_notional_pct,
        "maker_notional_pct": maker_notional_pct,
        "raw_fee_bps": raw_fee_bps,
        "interarrival_le_1s_pct": interarrival_le_1s_pct,
        "max_logical_events_per_second": max(events_per_second.values(), default=0),
        "p95_logical_events_per_active_day": dec(p95_events_per_active_day),
        "liquidation_fragments": liquidation["fragments"],
        "liquidation_event_count": liquidation["market_time_events"],
        "liquidation_cluster_count": liquidation["cluster_count"],
        "latest_liquidation_ms": liquidation["latest_ms"],
        "liquidation_market_count": liquidation["market_count"],
    }


def score_analysis(analysis: LeaderAnalysis, *, cutoff_ms: int) -> None:
    m = analysis.metrics
    history_days = dec(m["history_days"])
    complete = dec(m["complete_lifecycle_count"])
    mismatch = dec(m["position_mismatch_rate_pct"])

    data_score = (
        Decimal("5") * ascending_score(history_days, Decimal("60"), Decimal("180"))
        + Decimal("3") * ascending_score(complete, Decimal("30"), Decimal("150"))
        + Decimal("2") * descending_score(mismatch, Decimal("0.1"), Decimal("2"))
    )
    profit_score = (
        Decimal("8")
        * ascending_score(dec(m["profit_factor"]), Decimal("1"), Decimal("3"))
        + Decimal("8")
        * ascending_score(dec(m["friction_profit_factor"]), Decimal("1"), Decimal("2.5"))
        + Decimal("8")
        * ascending_score(dec(m["annual_simple_return_pct"]), ZERO, Decimal("100"))
        + Decimal("3")
        * ascending_score(dec(m["profitable_month_pct"]), Decimal("40"), Decimal("75"))
        + Decimal("3")
        * descending_score(
            dec(m["top3_win_concentration_pct"]), Decimal("50"), Decimal("90")
        )
    )
    open_profile_score = min(
        descending_score(
            dec(m["max_current_open_losing_hold_hours"]),
            Decimal("24"),
            Decimal("336"),
        ),
        descending_score(
            dec(m["current_open_worst_trough_pct"]),
            Decimal("5"),
            Decimal("20"),
        ),
        descending_score(
            dec(m["current_open_underwater_add_rate_pct"]),
            Decimal("10"),
            Decimal("60"),
        ),
    ) if dec(m["current_open_losing_count"]) > ZERO else ONE
    risk_score = (
        Decimal("9")
        * descending_score(dec(m["pressure_tail_pct"]), Decimal("7"), Decimal("50"))
        + Decimal("5")
        * descending_score(
            dec(m["official_portfolio_drawdown_pct"]),
            Decimal("5"),
            Decimal("25"),
        )
        + Decimal("7")
        * descending_score(
            dec(m["worst_lifecycle_trough_pct"]), Decimal("5"), Decimal("35")
        )
        + Decimal("4")
        * descending_score(dec(m["worst_closed_loss_pct"]), Decimal("3"), Decimal("25"))
        + Decimal("4")
        * descending_score(
            dec(m["p95_losing_hold_hours"]), Decimal("12"), Decimal("168")
        )
        + Decimal("2")
        * descending_score(dec(m["loss_over_72h_pct"]), ZERO, Decimal("30"))
        + Decimal("2")
        * descending_score(
            dec(m["underwater_add_rate_pct"]),
            Decimal("10"),
            Decimal("70"),
        )
        + Decimal("2") * open_profile_score
    )
    copy_score = (
        Decimal("10")
        * ascending_score(dec(m["breakeven_friction_bps"]), Decimal("5"), Decimal("20"))
        + Decimal("7")
        * descending_score(dec(m["safe_size_skip_notional_pct"]), Decimal("1"), Decimal("10"))
        + Decimal("4")
        * descending_score(dec(m["fast_profit_share_pct"]), Decimal("10"), Decimal("60"))
        + Decimal("4")
        * ascending_score(
            dec(m["friction_annual_simple_return_pct"]), ZERO, Decimal("50")
        )
    )
    analysis.score = (data_score + profit_score + risk_score + copy_score).quantize(
        Decimal("0.1")
    )

    hard: list[str] = []
    warnings: list[str] = []
    if history_days < Decimal("45") or complete < Decimal("30"):
        hard.append("公开历史或完整生命周期样本不足，不能形成可靠加入结论")
    if mismatch > Decimal("5"):
        hard.append("公开仓位链跳变超过 5%，重建结果不可靠")
    if dec(m.get("invalid_return_update_count")) > ZERO:
        hard.append("历史资本或盈亏重建出现单区间亏损超过 Account Total，风险数据无效")

    if dec(m["friction_net"]) <= ZERO or dec(m["friction_profit_factor"]) < Decimal("1.05"):
        hard.append("扣除统一复制摩擦后不再具有可靠正收益")
    if (
        dec(m["p95_losing_hold_hours"]) >= Decimal("336")
        and dec(m["worst_closed_loss_pct"]) >= Decimal("25")
    ):
        hard.append("亏损持有 P95 超过 14 天且存在超过 25% 的已平单次亏损")
    if (
        dec(m["p95_losing_hold_hours"]) >= Decimal("720")
        and dec(m["loss_over_72h_pct"]) >= Decimal("50")
        and dec(m["underwater_add_rate_pct"]) >= Decimal("50")
    ):
        hard.append("多数亏损长期持有且主要在浮亏中加仓，存在系统性死扛特征")
    if dec(m["safe_size_skip_notional_pct"]) >= Decimal("10"):
        hard.append("按目标尾部风险缩放后，至少 10% 成交额低于最小订单")
    if (
        dec(m["worst_lifecycle_trough_pct"]) >= Decimal("35")
        and dec(m["worst_lifecycle_hold_hours"]) >= Decimal("168")
    ):
        hard.append("单个生命周期曾回撤至少 35% 且持续至少 7 天，存在严重扛浮亏尾部")
    if (
        dec(m["max_current_open_losing_hold_hours"]) >= Decimal("336")
        and dec(m["current_open_worst_trough_pct"]) >= Decimal("15")
        and dec(m["current_open_underwater_add_rate_pct"]) >= Decimal("50")
    ):
        hard.append("当前未结束亏损仓位已持有至少 14 天，且曾深亏并主要在浮亏中加仓")

    if history_days < Decimal("180"):
        warnings.append("公开成交历史不足 180 天，结论应视为阶段性")
    if bool(m["fill_limit_saturated"]):
        warnings.append("成交超过官方 10k 历史保证边界，更早记录可能不完整")
    if dec(m["pressure_tail_pct"]) > Decimal("40"):
        warnings.append("原始组合压力尾部超过 40%，需要显著放大设定余额")
    if dec(m["p95_losing_hold_hours"]) > Decimal("168"):
        warnings.append("亏损生命周期 P95 超过 7 天")
    if dec(m["safe_size_skip_notional_pct"]) > Decimal("3"):
        warnings.append("安全余额下超过 3% 成交额会落入 10U 最小订单限制")
    if dec(m["fast_profit_share_pct"]) > Decimal("40"):
        warnings.append("超过 40% 毛利润来自五分钟内生命周期，延迟敏感")
    if dec(m["recommended_balance_ratio_to_current"]) > Decimal("10"):
        warnings.append("7% 尾部目标所需设定余额超过当前本金十倍")
    if dec(m["official_portfolio_drawdown_pct"]) > Decimal("20"):
        warnings.append("官方本金归一组合曲线回撤超过 20%")
    if dec(m["max_current_open_losing_hold_hours"]) > Decimal("168"):
        warnings.append("当前仍有亏损仓位持有超过 7 天")

    analysis.hard_failures = hard
    analysis.warnings = warnings
    if hard:
        analysis.verdict = "REJECT"
    elif analysis.score >= Decimal("80"):
        analysis.verdict = "STRONG"
    elif analysis.score >= Decimal("65"):
        analysis.verdict = "ADDABLE"
    elif analysis.score >= Decimal("55"):
        analysis.verdict = "WATCH"
    else:
        analysis.verdict = "REJECT"


def interval_conflict_pct(
    analysis: LeaderAnalysis,
    peers: list[LeaderAnalysis],
) -> Decimal:
    candidate_lives = [
        life
        for life in analysis.lifecycles
        if life.complete_start and life.scale is not None and life.start_ms
    ]
    peer_by_market: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for peer in peers:
        for life in peer.lifecycles:
            if not life.complete_start or life.scale is None or not life.start_ms:
                continue
            peer_by_market[life.coin].append(
                (life.start_ms, int(life.end_ms or peer.metrics.get("fill_end_ms") or life.start_ms))
            )
    conflict = 0
    for life in candidate_lives:
        if any(start <= life.start_ms < end for start, end in peer_by_market[life.coin]):
            conflict += 1
    return (
        Decimal(conflict) / Decimal(len(candidate_lives)) * HUNDRED
        if candidate_lives
        else ZERO
    )


def compute_group_conflicts(analyses: list[LeaderAnalysis]) -> None:
    for analysis in analyses:
        group = analysis.spec.group
        if not group:
            analysis.group_conflict_open_pct = None
            continue
        peers = [
            peer
            for peer in analyses
            if peer is not analysis and peer.spec.group == group
        ]
        analysis.group_conflict_open_pct = interval_conflict_pct(analysis, peers)


def money(value: Any, digits: int = 0) -> str:
    return f"${dec(value):,.{digits}f}"


def pct(value: Any, digits: int = 1) -> str:
    return f"{dec(value):.{digits}f}%"


def duration(hours: Any) -> str:
    value = float(dec(hours))
    if value < 1:
        return f"{value * 60:.0f}m"
    if value < 48:
        return f"{value:.1f}h"
    return f"{value / 24:.1f}d"


def iso(timestamp_ms: int | None) -> str:
    if not timestamp_ms:
        return "--"
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


def render_report(
    analyses: list[LeaderAnalysis],
    *,
    cutoff_ms: int,
    friction_bps: Decimal,
    target_tail_pct: Decimal,
) -> str:
    ranked = sorted(
        analyses,
        key=lambda item: (
            item.verdict == "REJECT",
            -item.score,
        ),
    )
    lines = [
        "# Leader suitability evaluation",
        "",
        f"Cutoff: **{datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC**  ",
        f"Normalized follower basis: **{money(FOLLOWER_BASE)}**  ",
        f"Copy-friction stress: **{friction_bps} bps per normalized traded notional**  ",
        f"Individual pressure-tail target: **{target_tail_pct}%**  ",
        "Capital normalization: **flow-neutral incremental returns; transfers update the next denominator**  ",
        "Complete wallet addresses are intentionally omitted.",
        "",
        "## Decision table",
        "",
        "| Rank | Leader | Verdict | Score | History | Complete lives | Net | Net after friction | PF after friction | Pressure tail | Worst trough | Worst-life hold | Open-loss max hold | Safe-size <10U notional |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, item in enumerate(ranked, 1):
        m = item.metrics
        lines.append(
            f"| {rank} | {item.spec.label} | {item.verdict} | {item.score} | "
            f"{dec(m['history_days']):.0f}d | {m['complete_lifecycle_count']:,} | "
            f"{money(m['normalized_net'])} | {money(m['friction_net'])} | "
            f"{dec(m['friction_profit_factor']):.2f} | {pct(m['pressure_tail_pct'])} | "
            f"{pct(m['worst_lifecycle_trough_pct'])} | {duration(m['worst_lifecycle_hold_hours'])} | "
            f"{duration(m['max_current_open_losing_hold_hours'])} | "
            f"{pct(m['safe_size_skip_notional_pct'])} |"
        )

    lines += [
        "",
        "## Per-leader audit",
    ]
    for item in ranked:
        m = item.metrics
        lines += [
            "",
            f"### {item.spec.label} — {item.verdict} ({item.score}/100)",
            "",
            f"- Coverage: {iso(m['fill_start_ms'])} → {iso(m['fill_end_ms'])}; "
            f"{m['perp_fill_count']:,} perp fragments, {m['logical_fill_count']:,} logical events, "
            f"{m['complete_lifecycle_count']:,} complete followable lifecycles, {m['market_count']} markets.",
            f"- Profit: normalized net {money(m['normalized_net'])}; PF {dec(m['profit_factor']):.2f}; "
            f"after {friction_bps}bps friction {money(m['friction_net'])}, PF "
            f"{dec(m['friction_profit_factor']):.2f}; break-even friction "
            f"{dec(m['breakeven_friction_bps']):.1f}bps.",
            f"- Consistency: {pct(m['profitable_month_pct'])} profitable close months "
            f"({m['month_count']} observed); top-three winning-lifecycle concentration "
            f"{pct(m['top3_win_concentration_pct'])}; five-minute profit dependence "
            f"{pct(m['fast_profit_share_pct'])}.",
            f"- Loss risk: pressure tail {money(m['pressure_drawdown'])} "
            f"({pct(m['pressure_tail_pct'])}); official portfolio drawdown "
            f"{pct(m['official_portfolio_drawdown_pct'])}; worst lifecycle trough "
            f"{pct(m['worst_lifecycle_trough_pct'])}; worst closed loss "
            f"{pct(m['worst_closed_loss_pct'])}; losing hold P95 "
            f"{duration(m['p95_losing_hold_hours'])}; losses over 72h "
            f"{pct(m['loss_over_72h_pct'])}.",
            f"- Loss management: underwater adds {pct(m['underwater_add_rate_pct'])}; "
            f"worst-lifecycle hold {duration(m['worst_lifecycle_hold_hours'])} with "
            f"{pct(m['worst_lifecycle_underwater_add_rate_pct'])} underwater adds; "
            f"currently losing open lives {m['current_open_losing_count']}, max hold "
            f"{duration(m['max_current_open_losing_hold_hours'])}, historical trough "
            f"{pct(m['current_open_worst_trough_pct'])}, underwater adds "
            f"{pct(m['current_open_underwater_add_rate_pct'])}. Own-liquidation rounds "
            f"{m['liquidation_cluster_count']} across {m['liquidation_market_count']} markets "
            "are observation-only and do not affect score or verdict.",
            f"- Safe sizing: target-tail balance {money(m['recommended_balance_for_target_tail'])}; "
            f"balance/current-total ratio {dec(m['recommended_balance_ratio_to_current']):.2f}x; "
            f"below-10U logical events {pct(m['safe_size_skip_count_pct'])}, representing "
            f"{pct(m['safe_size_skip_notional_pct'])} of copied notional.",
            f"- Execution shape (not directly penalized): maker notional "
            f"{pct(m['maker_notional_pct'])}; adjacent events ≤1s "
            f"{pct(m['interarrival_le_1s_pct'])}; max logical events/s "
            f"{m['max_logical_events_per_second']}; active-day event P95 "
            f"{dec(m['p95_logical_events_per_active_day']):.0f}.",
        ]
        if item.group_conflict_open_pct is not None:
            lines.append(
                f"- Current `{item.spec.group}` placement: {pct(item.group_conflict_open_pct)} "
                "of this leader's opens occurred while another assessed leader in the same "
                "account already had that market active. This estimates first-arrival conflict; "
                "it is not part of intrinsic score."
            )
        if item.hard_failures:
            lines.append("- Hard failures: " + "；".join(item.hard_failures) + "。")
        if item.warnings:
            lines.append("- Warnings: " + "；".join(item.warnings) + "。")

    lines += [
        "",
        "## Fixed gates and score",
        "",
        "Hard rejection is evaluated before score: insufficient/reliably broken history; "
        "non-positive economics after friction; a loss P95 of at least 14 days combined "
        "with a closed loss of at least 25%; systematic month-scale losing holds with "
        "predominantly underwater adding; at least 10% copied notional becoming "
        "untradeable after safe sizing; a lifecycle drawdown of at least 35% held for at "
        "least seven days; or a still-open losing position held for at least 14 days after "
        "a deep adverse excursion and predominantly underwater adding.",
        "",
        "The 100-point intrinsic score is auditable: data quality 10, profitability and "
        "consistency 30, drawdown/loss discipline 35, and copy-economics robustness 25. "
        "Frequency, maker share, market size, and own-liquidation history have no automatic "
        "penalty. Liquidations remain visible as an observation only.",
        "",
        "Verdicts: STRONG ≥80; ADDABLE ≥65; WATCH ≥55; otherwise REJECT. Any hard gate "
        "forces REJECT regardless of score. Account-placement conflict is reported separately.",
        "",
        "## Limits",
        "",
        "- Deposits and withdrawals are excluded from trading return and update the capital denominator at their exact ledger timestamp.",
        "- Safe-balance recommendations use raw portfolio pressure and exposure elasticity, not lifecycle-opening equity.",
        "- Public history beyond Hyperliquid's documented fill boundary can be incomplete.",
        "- Four-hour candle extremes can miss shorter intrabar spikes, so tail estimates may be understated.",
        "- The friction stress is a common comparison assumption, not a guarantee of future slippage.",
        "- Historical balance recommendations and scores cannot guarantee future loss limits.",
        "",
    ]
    return "\n".join(lines)


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("leaders", nargs="+", type=parse_leader, metavar="LABEL=ADDRESS")
    parser.add_argument(
        "--group",
        action="append",
        type=parse_group,
        default=[],
        metavar="LABEL=GROUP",
        help="optional follower-account placement used only for conflict analysis",
    )
    parser.add_argument(
        "--friction-bps",
        type=Decimal,
        default=DEFAULT_FRICTION_BPS,
    )
    parser.add_argument(
        "--target-tail-pct",
        type=Decimal,
        default=DEFAULT_TARGET_TAIL_PCT,
    )
    parser.add_argument("--round-to", type=Decimal, default=DEFAULT_ROUND_TO)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/copytrade_leader_suitability"),
    )
    parser.add_argument("--end-ms", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    if args.friction_bps < ZERO:
        parser.error("--friction-bps must be non-negative")
    if args.target_tail_pct <= ZERO or args.round_to <= ZERO:
        parser.error("tail target and rounding quantum must be positive")
    labels = [spec.label for spec in args.leaders]
    if len(labels) != len(set(labels)):
        parser.error("leader labels must be unique")
    groups = dict(args.group)
    unknown_groups = set(groups) - set(labels)
    if unknown_groups:
        parser.error("--group contains an unknown leader label")

    specs = [
        LeaderSpec(spec.label, spec.address, groups.get(spec.label))
        for spec in args.leaders
    ]
    cutoff_ms = args.end_ms or int(time.time() * 1000)
    args.cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        args.cache_dir.chmod(0o700)
    except OSError:
        pass

    analyses: list[LeaderAnalysis] = []
    for spec in specs:
        analysis = build_public_history(
            spec,
            cutoff_ms=cutoff_ms,
            cache_root=args.cache_dir,
        )
        compute_metrics(
            analysis,
            cutoff_ms=cutoff_ms,
            friction_bps=args.friction_bps,
            target_tail_pct=args.target_tail_pct,
            round_to=args.round_to,
        )
        score_analysis(analysis, cutoff_ms=cutoff_ms)
        analyses.append(analysis)
    compute_group_conflicts(analyses)

    report = render_report(
        analyses,
        cutoff_ms=cutoff_ms,
        friction_bps=args.friction_bps,
        target_tail_pct=args.target_tail_pct,
    )
    print(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    if args.json_output:
        payload = {
            "methodology_version": "chain_linked_account_total_v3",
            "cutoff_ms": cutoff_ms,
            "follower_basis": FOLLOWER_BASE,
            "friction_bps": args.friction_bps,
            "target_tail_pct": args.target_tail_pct,
            "leaders": [
                {
                    "label": item.spec.label,
                    "group": item.spec.group,
                    "score": item.score,
                    "verdict": item.verdict,
                    "hard_failures": item.hard_failures,
                    "warnings": item.warnings,
                    "group_conflict_open_pct": item.group_conflict_open_pct,
                    "metrics": item.metrics,
                }
                for item in analyses
            ],
        }
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(jsonable(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
