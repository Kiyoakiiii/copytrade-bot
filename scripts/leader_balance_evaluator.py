#!/usr/bin/env python3
"""Evaluate fixed leader balances from public Hyperliquid portfolio risk.

This is an offline research tool for the operator/maintainer.  It intentionally
does not import the live backend, environment, follower accounts, signer
material, API secrets, or Telegram configuration.

The fixed methodology is:

1. Rebuild public perp fills into position lifecycles.
2. Apply the bot's economic-dust rule (a follower remainder below 10 USDC is
   flat; a later material add is a new lifecycle).
3. Rebuild the raw account-level portfolio pressure path, including complete
   4h candle adverse extremes and simultaneous multi-coin exposure.
4. Estimate position elasticity from daily opening Account Total versus daily
   peak gross exposure, using a robust central slope and conservative upper
   slope instead of assuming fixed-dollar or fully proportional sizing.
5. Stress the recent gross-exposure regime with the historical worst
   peak-to-pressure loss per dollar of block peak exposure. Recommend the
   greater of observed raw drawdown and this elasticity/regime stress divided
   by the target tail, then round upward. Multiplier remains outside calibration.
6. Retain full equity normalization as a diagnostic upper case only. When
   several leaders are supplied, validate time-aligned raw joint drawdown under
   the actually applied fixed balances.
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

    def __post_init__(self) -> None:
        """Reject malformed programmatic inputs before capital reconstruction."""

        address = self.address.strip().lower()
        if not ADDRESS_RE.fullmatch(address):
            raise ValueError("invalid public address")
        object.__setattr__(self, "address", address)


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


@dataclass(frozen=True)
class DailyExposureObservation:
    day_start_ms: int
    opening_equity: Decimal
    peak_gross_notional: Decimal


@dataclass(frozen=True)
class ExposurePosition:
    market: str
    direction: str
    gross_notional: Decimal


@dataclass(frozen=True)
class ExposurePeak:
    time_ms: int
    gross_notional: Decimal
    positions: tuple[ExposurePosition, ...]


@dataclass(frozen=True)
class ExposureRiskModel:
    beta: Decimal
    beta_upper: Decimal
    observation_count: int
    reference_capital: Decimal
    recent_average_gross: Decimal
    prior_average_gross: Decimal
    recent_peak_gross: Decimal
    recent_peak_ms: int
    recent_peak_positions: tuple[ExposurePosition, ...]
    prior_peak_gross: Decimal
    historical_peak_gross: Decimal
    historical_peak_ms: int
    historical_peak_positions: tuple[ExposurePosition, ...]
    historical_p95_gross: Decimal
    fitted_upper_gross: Decimal
    projected_peak_gross: Decimal
    peak_limit_gross: Decimal
    worst_loss_per_peak_gross: Decimal
    regime_scale_factor: Decimal
    observed_raw_drawdown: Decimal
    elasticity_regime_drawdown: Decimal
    projected_raw_drawdown: Decimal
    projected_component: str


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
    equity_normalized_drawdown: DrawdownResult
    equity_normalized_tail_pct: Decimal
    exposure_model: ExposureRiskModel
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


def recommend_balance_for_projected_loss(
    projected_raw_loss: Decimal,
    target_tail_pct: Decimal,
    *,
    round_to: Decimal,
    headroom_pct: Decimal = ZERO,
) -> tuple[Decimal, Decimal, Decimal]:
    """Calibrate a fixed leader denominator from a projected raw loss.

    With multiplier deliberately excluded, a 20k follower and a leader fixed
    balance ``B`` have the same percentage loss as ``raw_loss / B``.  Current
    Account Total therefore does not belong in this final conversion; it is
    used only by the exposure model that projects how large future leader
    positions may become.
    """

    if min(projected_raw_loss, target_tail_pct, round_to) <= ZERO:
        raise ValueError("projected-loss balance inputs must be positive")
    if headroom_pct < ZERO:
        raise ValueError("headroom cannot be negative")
    target_fraction = target_tail_pct / Decimal("100")
    theoretical = projected_raw_loss / target_fraction
    padded = theoretical * (Decimal("1") + headroom_pct / Decimal("100"))
    recommendation = round_up(padded, round_to)
    resulting_tail = projected_raw_loss / recommendation * Decimal("100")
    return theoretical, recommendation, resulting_tail


def step_value(points: dict[int, Decimal], timestamp: int) -> Decimal:
    if not points:
        return ZERO
    times = sorted(points)
    index = bisect.bisect_right(times, timestamp) - 1
    return points[times[index]] if index >= 0 else ZERO


def percentile_decimal(values: list[Decimal], percentile: Decimal) -> Decimal:
    if not values:
        return ZERO
    if percentile < ZERO or percentile > Decimal("1"):
        raise ValueError("percentile must be between zero and one")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = Decimal(len(ordered) - 1) * percentile
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - Decimal(low)
    return ordered[low] * (Decimal("1") - weight) + ordered[high] * weight


def gross_exposure_path(lifecycles: list[risk.Lifecycle]) -> dict[int, Decimal]:
    updates: dict[int, list[tuple[str, Decimal]]] = defaultdict(list)
    for life in lifecycles:
        for point in life.valuations:
            if not point["extreme"]:
                updates[int(point["time_ms"])].append(
                    (life.life_id, risk.dec(point["notional"]))
                )
        # A position-chain mismatch closes this reconstructed lifecycle without
        # manufacturing a PnL close. Its last valuation can consequently be
        # non-zero even though a later lifecycle has taken over the market.
        # Exposure is state, not cumulative PnL: terminate that stale state at
        # the known boundary. A genuinely open cutoff lifecycle is right
        # censored and must remain present.
        if life.end_ms is not None and not life.right_censored:
            updates[int(life.end_ms)].append((life.life_id, ZERO))
    latest: dict[str, Decimal] = {}
    total = ZERO
    result: dict[int, Decimal] = {}
    for timestamp in sorted(updates):
        for life_id, value in updates[timestamp]:
            total += value - latest.get(life_id, ZERO)
            latest[life_id] = value
        result[timestamp] = max(ZERO, total)
    return result


def gross_exposure_peak(
    lifecycles: list[risk.Lifecycle],
    *,
    start_ms: int,
    end_ms: int,
) -> ExposurePeak:
    """Return an auditable simultaneous gross-exposure snapshot.

    The state transition order is identical to :func:`gross_exposure_path`.
    Positions are grouped by market and direction so the evidence adds exactly
    to the reported peak rather than exposing internal lifecycle fragments.
    """

    if end_ms < start_ms:
        raise ValueError("peak-exposure interval must not be negative")
    updates: dict[int, list[tuple[str, str, str, Decimal]]] = defaultdict(list)
    for life in lifecycles:
        for point in life.valuations:
            if not point["extreme"]:
                updates[int(point["time_ms"])].append(
                    (
                        life.life_id,
                        life.coin,
                        life.side_label,
                        risk.dec(point["notional"]),
                    )
                )
        if life.end_ms is not None and not life.right_censored:
            updates[int(life.end_ms)].append(
                (life.life_id, life.coin, life.side_label, ZERO)
            )

    latest: dict[str, tuple[str, str, Decimal]] = {}

    def snapshot(timestamp: int) -> ExposurePeak:
        grouped: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
        for market, direction, notional in latest.values():
            if notional > ZERO:
                grouped[(market, direction)] += notional
        positions = tuple(
            ExposurePosition(market, direction, notional)
            for (market, direction), notional in sorted(
                grouped.items(), key=lambda item: (-item[1], item[0])
            )
        )
        return ExposurePeak(
            time_ms=timestamp,
            gross_notional=sum((item.gross_notional for item in positions), ZERO),
            positions=positions,
        )

    peak = ExposurePeak(start_ms, ZERO, ())
    start_recorded = False
    for timestamp in sorted(updates):
        if timestamp > end_ms:
            break
        if timestamp > start_ms and not start_recorded:
            peak = snapshot(start_ms)
            start_recorded = True
        for life_id, market, direction, notional in updates[timestamp]:
            latest[life_id] = (market, direction, notional)
        if timestamp >= start_ms:
            candidate = snapshot(timestamp)
            start_recorded = True
            if candidate.gross_notional > peak.gross_notional:
                peak = candidate
    if not start_recorded:
        peak = snapshot(start_ms)
    return peak


def time_weighted_average(
    points: dict[int, Decimal],
    *,
    start_ms: int,
    end_ms: int,
) -> Decimal:
    if end_ms <= start_ms:
        raise ValueError("time-weighted interval must be positive")
    current = step_value(points, start_ms)
    previous = start_ms
    area = ZERO
    for timestamp in sorted(
        item for item in points if start_ms < item < end_ms
    ) + [end_ms]:
        area += current * Decimal(timestamp - previous)
        if timestamp != end_ms:
            current = points[timestamp]
        previous = timestamp
    return area / Decimal(end_ms - start_ms)


def daily_exposure_observations(
    exposure: dict[int, Decimal],
    normalizer: risk.CapitalNormalizer,
    *,
    end_ms: int,
    lookback_days: int = 180,
) -> list[DailyExposureObservation]:
    if not exposure:
        return []
    day_ms = 86_400_000
    start_ms = max(min(exposure), end_ms - lookback_days * day_ms)
    day_start = start_ms // day_ms * day_ms
    timestamps = sorted(exposure)
    observations: list[DailyExposureObservation] = []
    while day_start < end_ms:
        day_end = min(day_start + day_ms, end_ms)
        left = bisect.bisect_right(timestamps, day_start)
        right = bisect.bisect_left(timestamps, day_end)
        peak = step_value(exposure, day_start)
        if right > left:
            peak = max(peak, *(exposure[timestamp] for timestamp in timestamps[left:right]))
        equity = normalizer.value_at(max(0, day_start - 1))
        if peak > ZERO and equity > ZERO:
            observations.append(
                DailyExposureObservation(
                    day_start_ms=day_start,
                    opening_equity=equity,
                    peak_gross_notional=peak,
                )
            )
        day_start += day_ms
    return observations


def estimate_exposure_elasticity(
    observations: list[DailyExposureObservation],
) -> tuple[Decimal, Decimal]:
    """Return a robust central and conservative upper position elasticity.

    Daily opening equity is used instead of simultaneous equity, preventing a
    profitable large position from mechanically making both axes rise.  The
    Theil-Sen median resists isolated oversized trades; its 75th-percentile
    pair slope is retained as a conservative upper estimate.  Pairs with less
    than 5% equity separation are discarded because their slopes are noise.
    """

    slopes: list[Decimal] = []
    minimum_log_gap = math.log(1.05)
    for index, left in enumerate(observations):
        left_x = math.log(float(left.opening_equity))
        left_y = math.log(float(left.peak_gross_notional))
        for right in observations[index + 1 :]:
            right_x = math.log(float(right.opening_equity))
            delta_x = right_x - left_x
            if abs(delta_x) < minimum_log_gap:
                continue
            right_y = math.log(float(right.peak_gross_notional))
            slope = Decimal(str((right_y - left_y) / delta_x))
            if slope.is_finite():
                slopes.append(slope)
    if len(observations) < 8 or len(slopes) < 20:
        # Insufficient variation does not prove fixed-dollar sizing. Keep a
        # partial-scaling upper case until more evidence becomes available.
        return ZERO, Decimal("0.5")
    central = max(ZERO, min(Decimal("1"), percentile_decimal(slopes, Decimal("0.5"))))
    upper = max(
        central,
        min(Decimal("1"), percentile_decimal(slopes, Decimal("0.75"))),
    )
    return central, upper


def fitted_upper_exposure(
    observations: list[DailyExposureObservation],
    *,
    reference_capital: Decimal,
    beta: Decimal,
) -> Decimal:
    if not observations or reference_capital <= ZERO:
        return ZERO
    residuals = [
        Decimal(
            str(
                math.log(float(item.peak_gross_notional))
                - float(beta) * math.log(float(item.opening_equity))
            )
        )
        for item in observations
    ]
    upper_residual = percentile_decimal(residuals, Decimal("0.95"))
    projected_log = float(beta) * math.log(float(reference_capital)) + float(upper_residual)
    return Decimal(str(math.exp(projected_log)))


def max_loss_per_peak_gross(
    path: PressurePath,
    exposure: dict[int, Decimal],
) -> Decimal:
    """Worst peak-to-pressure loss divided by peak gross held in that block."""

    timestamps = sorted(set(path.regular) | set(path.stress))
    if not timestamps:
        return ZERO
    peak_value = ZERO
    latest_value = ZERO
    block_peak_gross = step_value(exposure, timestamps[0])
    maximum = ZERO
    for timestamp in timestamps:
        if timestamp in path.regular:
            latest_value = path.regular[timestamp]
        gross = step_value(exposure, timestamp)
        if latest_value >= peak_value:
            peak_value = latest_value
            block_peak_gross = gross
        else:
            block_peak_gross = max(block_peak_gross, gross)
        pressure_value = path.stress.get(timestamp, latest_value)
        drawdown = max(ZERO, peak_value - pressure_value)
        denominator = max(block_peak_gross, gross)
        if denominator > ZERO:
            maximum = max(maximum, drawdown / denominator)
    return maximum


def build_exposure_risk_model(
    *,
    lifecycles: list[risk.Lifecycle],
    normalizer: risk.CapitalNormalizer,
    raw_path: PressurePath,
    raw_drawdown: DrawdownResult,
    end_ms: int,
) -> ExposureRiskModel:
    exposure = gross_exposure_path(lifecycles)
    observations = daily_exposure_observations(
        exposure,
        normalizer,
        end_ms=end_ms,
        lookback_days=180,
    )
    beta, beta_upper = estimate_exposure_elasticity(observations)
    day_ms = 86_400_000
    recent_start = max(min(exposure, default=end_ms), end_ms - 30 * day_ms)
    prior_start = max(min(exposure, default=end_ms), end_ms - 60 * day_ms)
    recent_average = time_weighted_average(
        exposure,
        start_ms=recent_start,
        end_ms=end_ms,
    )
    prior_average = time_weighted_average(
        exposure,
        start_ms=prior_start,
        end_ms=recent_start,
    ) if recent_start > prior_start else ZERO
    recent_peak_snapshot = gross_exposure_peak(
        lifecycles,
        start_ms=recent_start,
        end_ms=end_ms,
    )
    historical_peak_snapshot = gross_exposure_peak(
        lifecycles,
        start_ms=min(exposure, default=end_ms),
        end_ms=end_ms,
    )
    prior_values = [
        step_value(exposure, prior_start),
        *(value for timestamp, value in exposure.items() if prior_start < timestamp <= recent_start),
    ]
    recent_peak = recent_peak_snapshot.gross_notional
    prior_peak = max(prior_values, default=ZERO)
    recent_capitals = [
        item.opening_equity
        for item in observations
        if item.day_start_ms >= end_ms - 30 * day_ms
    ]
    reference_capital = (
        percentile_decimal(recent_capitals, Decimal("0.5"))
        if recent_capitals
        else normalizer.current_total_account_value
    )
    # The central robust beta drives interpolation inside the observed capital
    # range. ``beta_upper`` remains an uncertainty diagnostic; using it for
    # every in-range estimate would silently restore the disproven beta=1
    # assumption whenever noisy pair slopes have a wide upper tail.
    fitted_upper = fitted_upper_exposure(
        observations,
        reference_capital=reference_capital,
        beta=beta,
    )
    projected_peak = max(recent_peak, fitted_upper)
    # A hard peak-notional policy must cover both a fitted future upper regime
    # and every actual peak in the reconstructable history. The tail-loss
    # stress continues to use the recent/fitted regime because the raw
    # drawdown floor already contains older realized pressure episodes.
    peak_limit_gross = max(historical_peak_snapshot.gross_notional, projected_peak)
    historical_p95 = percentile_decimal(
        [item.peak_gross_notional for item in observations],
        Decimal("0.95"),
    )
    severity = max_loss_per_peak_gross(raw_path, exposure)
    observed = raw_drawdown.amount
    # Portfolio drawdowns can span several sequential trades. Dividing such a
    # cumulative loss by one post-peak position can exceed 100% and must not be
    # multiplied into a new single-position forecast. Scale the observed raw
    # portfolio tail only by how far the projected peak-exposure regime exceeds
    # the comparable historical 95th-percentile regime.
    regime_scale_factor = (
        max(Decimal("1"), projected_peak / historical_p95)
        if historical_p95 > ZERO
        else Decimal("1")
    )
    elasticity_regime_drawdown = observed * regime_scale_factor
    if elasticity_regime_drawdown > observed:
        projected = elasticity_regime_drawdown
        component = "ELASTICITY_RECENT_REGIME_STRESS"
    else:
        projected = observed
        component = "OBSERVED_RAW_PORTFOLIO_DRAWDOWN"
    return ExposureRiskModel(
        beta=beta,
        beta_upper=beta_upper,
        observation_count=len(observations),
        reference_capital=reference_capital,
        recent_average_gross=recent_average,
        prior_average_gross=prior_average,
        recent_peak_gross=recent_peak,
        recent_peak_ms=recent_peak_snapshot.time_ms,
        recent_peak_positions=recent_peak_snapshot.positions,
        prior_peak_gross=prior_peak,
        historical_peak_gross=historical_peak_snapshot.gross_notional,
        historical_peak_ms=historical_peak_snapshot.time_ms,
        historical_peak_positions=historical_peak_snapshot.positions,
        historical_p95_gross=historical_p95,
        fitted_upper_gross=fitted_upper,
        projected_peak_gross=projected_peak,
        peak_limit_gross=peak_limit_gross,
        worst_loss_per_peak_gross=severity,
        regime_scale_factor=regime_scale_factor,
        observed_raw_drawdown=observed,
        elasticity_regime_drawdown=elasticity_regime_drawdown,
        projected_raw_drawdown=projected,
        projected_component=component,
    )


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
    raw_usable = [
        life
        for life in exact
        if life.metrics and life.scale is not None and life.complete_start
    ]
    for life in raw_usable:
        life.equity_at_open = normalizer.value_at(max(0, life.start_ms - 1))
        life.equity_sample_gap_days = normalizer.nearest_sample_gap_days(life.start_ms)
    raw_path = build_pressure_path(raw_usable)
    raw_drawdown = portfolio_drawdown(
        {label: raw_path},
        {label: Decimal("1")},
    )
    exposure_model = build_exposure_risk_model(
        lifecycles=raw_usable,
        normalizer=normalizer,
        raw_path=raw_path,
        raw_drawdown=raw_drawdown,
        end_ms=end_ms,
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
    equity_normalized_path = build_pressure_path(usable)
    equity_normalized_drawdown = portfolio_drawdown(
        {label: equity_normalized_path},
        {label: Decimal("1")},
    )
    equity_normalized_tail_pct = (
        equity_normalized_drawdown.amount / follower_balance * Decimal("100")
    )
    theoretical, recommendation, resulting_tail = recommend_balance_for_projected_loss(
        exposure_model.projected_raw_drawdown,
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
        drawdown=raw_drawdown,
        historical_tail_pct=equity_normalized_tail_pct,
        equity_normalized_drawdown=equity_normalized_drawdown,
        equity_normalized_tail_pct=equity_normalized_tail_pct,
        exposure_model=exposure_model,
        theoretical_balance=theoretical,
        recommended_balance=recommendation,
        recommended_tail_pct=resulting_tail,
        contributors=pressure_contributors(raw_usable, raw_drawdown),
        path=raw_path,
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
        "| Leader | 30d median capital | Exposure beta (upper) | Projected raw drawdown | Exact target balance | Applied balance | Applied tail |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in evaluations:
        balance = applied_balances[item.leader.label]
        applied_tail = item.exposure_model.projected_raw_drawdown / balance * Decimal("100")
        lines.append(
            f"| {item.leader.label} | {money(item.exposure_model.reference_capital)} | "
            f"{item.exposure_model.beta:.2f} ({item.exposure_model.beta_upper:.2f}) | "
            f"{money(item.exposure_model.projected_raw_drawdown)} | "
            f"{money(item.theoretical_balance, 0)} | "
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
            f"- Observed raw maximum portfolio pressure: {risk.iso(item.drawdown.peak_ms)} → "
            f"{risk.iso(item.drawdown.trough_ms)} UTC; raw PnL "
            f"{money(item.drawdown.peak_value)} → {money(item.drawdown.trough_value)}; "
            f"drawdown **{money(item.drawdown.amount)}**.",
            f"- Exposure model: {item.exposure_model.observation_count} active daily observations; "
            f"beta {item.exposure_model.beta:.2f}, conservative upper beta "
            f"{item.exposure_model.beta_upper:.2f}; 30d median capital "
            f"{money(item.exposure_model.reference_capital)}.",
            f"- Gross exposure regime: prior 30d average/peak "
            f"{money(item.exposure_model.prior_average_gross)}/"
            f"{money(item.exposure_model.prior_peak_gross)}; recent 30d "
            f"{money(item.exposure_model.recent_average_gross)}/"
            f"{money(item.exposure_model.recent_peak_gross)}; fitted 95% upper "
            f"{money(item.exposure_model.fitted_upper_gross)}; historical daily-peak P95 "
            f"{money(item.exposure_model.historical_p95_gross)}.",
            f"- Recent actual peak evidence: {risk.iso(item.exposure_model.recent_peak_ms)} UTC; "
            + ", ".join(
                f"{position.market} {position.direction} {money(position.gross_notional)}"
                for position in item.exposure_model.recent_peak_positions
            )
            + ".",
            f"- Full-history actual peak evidence: {risk.iso(item.exposure_model.historical_peak_ms)} UTC; "
            + ", ".join(
                f"{position.market} {position.direction} {money(position.gross_notional)}"
                for position in item.exposure_model.historical_peak_positions
            )
            + f"; total {money(item.exposure_model.historical_peak_gross)}. "
            f"Peak-limit input after fitted-upper comparison: "
            f"{money(item.exposure_model.peak_limit_gross)}.",
            f"- Stress components: observed raw {money(item.exposure_model.observed_raw_drawdown)}; "
            f"exposure-regime scale {item.exposure_model.regime_scale_factor:.2f}x; "
            f"elasticity/recent-regime {money(item.exposure_model.elasticity_regime_drawdown)}; "
            f"selected **{money(item.exposure_model.projected_raw_drawdown)}** "
            f"({item.exposure_model.projected_component}).",
            f"- Full proportional-equity normalization is retained only as a diagnostic upper case: "
            f"{money(item.equity_normalized_drawdown.amount)} "
            f"({percent(item.equity_normalized_tail_pct)} on the {money(follower_balance, 0)} basis).",
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
            item.leader.label: follower_balance / applied_balances[item.leader.label]
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
            "Observed raw portfolio paths are mapped through each applied fixed balance; "
            "the joint test does not assume five independent worst cases happen together.",
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
        "- Position elasticity is estimated from daily opening capital and daily peak gross exposure; "
        "a recent structural strategy change can still outpace the fitted history.",
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
    evaluations = []
    for leader in args.leaders:
        # A hashed, address-private namespace is shared with the suitability
        # model so the same public history is never downloaded twice.
        client = risk.PublicInfoClient(
            risk.public_history_cache_namespace(args.cache_dir, end_ms, leader.address)
        )
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
                    "equity_normalized_pressure_tail_pct": item.equity_normalized_tail_pct,
                    "equity_normalized_pressure_drawdown": item.equity_normalized_drawdown.amount,
                    "observed_raw_pressure_drawdown": item.drawdown.amount,
                    "exposure_model": {
                        "beta": item.exposure_model.beta,
                        "beta_upper": item.exposure_model.beta_upper,
                        "observation_count": item.exposure_model.observation_count,
                        "reference_capital": item.exposure_model.reference_capital,
                        "recent_average_gross": item.exposure_model.recent_average_gross,
                        "prior_average_gross": item.exposure_model.prior_average_gross,
                        "recent_peak_gross": item.exposure_model.recent_peak_gross,
                        "recent_peak_ms": item.exposure_model.recent_peak_ms,
                        "recent_peak_positions": [
                            {
                                "market": position.market,
                                "direction": position.direction,
                                "gross_notional": position.gross_notional,
                            }
                            for position in item.exposure_model.recent_peak_positions
                        ],
                        "prior_peak_gross": item.exposure_model.prior_peak_gross,
                        "historical_peak_gross": item.exposure_model.historical_peak_gross,
                        "historical_peak_ms": item.exposure_model.historical_peak_ms,
                        "historical_peak_positions": [
                            {
                                "market": position.market,
                                "direction": position.direction,
                                "gross_notional": position.gross_notional,
                            }
                            for position in item.exposure_model.historical_peak_positions
                        ],
                        "historical_p95_gross": item.exposure_model.historical_p95_gross,
                        "fitted_upper_gross": item.exposure_model.fitted_upper_gross,
                        "projected_peak_gross": item.exposure_model.projected_peak_gross,
                        "peak_limit_gross": item.exposure_model.peak_limit_gross,
                        "worst_loss_per_peak_gross": item.exposure_model.worst_loss_per_peak_gross,
                        "regime_scale_factor": item.exposure_model.regime_scale_factor,
                        "elasticity_regime_drawdown": item.exposure_model.elasticity_regime_drawdown,
                        "projected_raw_drawdown": item.exposure_model.projected_raw_drawdown,
                        "projected_component": item.exposure_model.projected_component,
                    },
                    "theoretical_balance": item.theoretical_balance,
                    "recommended_balance": item.recommended_balance,
                    "applied_balance": balances[item.leader.label],
                    "applied_tail_pct": item.exposure_model.projected_raw_drawdown
                    / balances[item.leader.label]
                    * Decimal("100"),
                    "pressure_drawdown": item.exposure_model.projected_raw_drawdown,
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
