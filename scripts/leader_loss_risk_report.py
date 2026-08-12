#!/usr/bin/env python3
"""Build a loss-holding and drawdown report for selected public HL leaders.

The script intentionally uses only public Hyperliquid info endpoints. It never
loads the bot environment, signer material, API keys, or follower credentials.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Any, Iterable


getcontext().prec = 40

INFO_URL = "https://api.hyperliquid.xyz/info"
FOLLOWER_BASE = Decimal("20000")
ZERO = Decimal("0")
POSITION_EPS = Decimal("0.00000001")
UNDERWATER_USD_THRESHOLD = Decimal("-1")
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
FILL_PAGE_SIZE = 2000
FUNDING_PAGE_SIZE = 500

def dec(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def sign(value: Decimal) -> int:
    if value > POSITION_EPS:
        return 1
    if value < -POSITION_EPS:
        return -1
    return 0


def q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"))


def q8(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"))


def fmt_usd(value: Decimal | float | int | None, digits: int = 0) -> str:
    if value is None:
        return "--"
    amount = dec(value)
    return f"${amount:,.{digits}f}"


def fmt_pct(value: Decimal | float | int | None, digits: int = 2) -> str:
    if value is None:
        return "--"
    return f"{dec(value):.{digits}f}%"


def iso(ms: int | None) -> str:
    if not ms:
        return "--"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def duration_label(hours: float | Decimal | None) -> str:
    if hours is None:
        return "--"
    value = float(hours)
    if value < 1:
        return f"{value * 60:.0f}m"
    if value < 48:
        return f"{value:.1f}h"
    return f"{value / 24:.1f}d"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


class PublicInfoClient:
    def __init__(self, cache_dir: Path, *, pause_seconds: float = 0.10) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.pause_seconds = pause_seconds
        self._last_request = 0.0

    def post(self, payload: dict[str, Any], cache_key: str) -> Any:
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.pause_seconds:
            time.sleep(self.pause_seconds - elapsed)
        data = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(
            INFO_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        error: Exception | None = None
        for attempt in range(6):
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    result = json.loads(response.read().decode())
                self._last_request = time.monotonic()
                cache_path.write_text(
                    json.dumps(result, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8",
                )
                return result
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                error = exc
                time.sleep(min(8.0, 0.5 * (2**attempt)))
        raise RuntimeError(f"public info request failed for {cache_key}: {error}")


def fill_key(fill: dict[str, Any]) -> str:
    return "|".join(
        str(fill.get(key) or "")
        for key in ("hash", "tid", "oid", "time", "coin", "px", "sz", "startPosition", "side")
    )


def funding_key(item: dict[str, Any]) -> str:
    delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
    return "|".join(
        str(value or "")
        for value in (item.get("hash"), item.get("time"), delta.get("coin"), delta.get("usdc"))
    )


def ledger_key(item: dict[str, Any]) -> str:
    return "|".join(
        (
            str(item.get("hash") or ""),
            str(item.get("time") or ""),
            json.dumps(item.get("delta") or {}, sort_keys=True, separators=(",", ":")),
        )
    )


def is_perp_fill(fill: dict[str, Any]) -> bool:
    coin = str(fill.get("coin") or "")
    direction = str(fill.get("dir") or "").lower()
    if not coin or coin.startswith("@") or "/" in coin:
        return False
    return "long" in direction or "short" in direction


def logical_fills(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Combine exchange fragments that share one authoritative position start.

    Hyperliquid can return several non-aggregated fragments for one order with
    the same startPosition. Treating every fragment as a separate position
    transition creates synthetic flips and overlapping lifecycles.
    """

    groups: dict[tuple[str, str, int, str, str], dict[str, Any]] = {}
    for fill in fills:
        if not is_perp_fill(fill):
            continue
        coin = str(fill.get("coin") or "")
        timestamp = int(fill.get("time") or 0)
        direction = str(fill.get("dir") or "")
        side = str(fill.get("side") or "").upper()
        key = (coin, str(fill.get("oid") or fill_key(fill)), timestamp, direction, side)
        size = abs(dec(fill.get("sz")))
        event = groups.setdefault(
            key,
            {
                "coin": coin,
                "time": timestamp,
                "dir": direction,
                "side": side,
                "oid": fill.get("oid"),
                "starts": [],
                "sz": ZERO,
                "quote": ZERO,
                "closedPnl": ZERO,
                "fee": ZERO,
                "fragment_count": 0,
            },
        )
        event["starts"].append(dec(fill.get("startPosition")))
        event["sz"] += size
        event["quote"] += dec(fill.get("px")) * size
        event["closedPnl"] += dec(fill.get("closedPnl"))
        event["fee"] += dec(fill.get("fee"))
        event["fragment_count"] += 1
    result: list[dict[str, Any]] = []
    for event in groups.values():
        starts = event.pop("starts")
        signed_size = event["sz"] if event["side"] == "B" else -event["sz"]
        event["startPosition"] = min(starts) if signed_size >= ZERO else max(starts)
        event["px"] = event.pop("quote") / event["sz"] if event["sz"] > ZERO else ZERO
        result.append(event)

    # Several orders can fill in the same millisecond. Order id is not a
    # causal sequence, while startPosition is: event N's ending position must
    # equal event N+1's startPosition. Reconstruct that chain so high-frequency
    # batches do not create false position jumps or synthetic lifecycles.
    buckets: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for event in result:
        buckets[(str(event["coin"]), int(event["time"]))].append(event)

    def delta_for(event: dict[str, Any]) -> Decimal:
        size = abs(dec(event.get("sz")))
        return size if str(event.get("side") or "").upper() == "B" else -size

    def after_for(event: dict[str, Any]) -> Decimal:
        return dec(event.get("startPosition")) + delta_for(event)

    def stable_key(event: dict[str, Any]) -> tuple[int, str, str]:
        return (
            0 if str(event.get("dir") or "").lower().startswith("close") else 1,
            str(event.get("oid") or ""),
            str(event.get("dir") or ""),
        )

    causal_rank: dict[int, int] = {}
    for coin in sorted({key[0] for key in buckets}):
        cursor: Decimal | None = None
        timestamps = sorted(timestamp for bucket_coin, timestamp in buckets if bucket_coin == coin)
        for timestamp in timestamps:
            remaining = list(buckets[(coin, timestamp)])
            sequence: list[dict[str, Any]] = []
            while remaining:
                exact = (
                    [event for event in remaining if abs(dec(event.get("startPosition")) - cursor) <= POSITION_EPS]
                    if cursor is not None
                    else []
                )
                if exact:
                    selected = min(exact, key=stable_key)
                else:
                    # At the beginning of visible history, or after a genuine
                    # public-data gap, pick the head of the longest visible
                    # startPosition -> afterPosition chain. Prefer zero starts.
                    heads = [
                        event
                        for event in remaining
                        if not any(
                            other is not event
                            and abs(after_for(other) - dec(event.get("startPosition"))) <= POSITION_EPS
                            for other in remaining
                        )
                    ]
                    candidates = heads or remaining
                    if cursor is None:
                        selected = min(
                            candidates,
                            key=lambda event: (
                                0 if abs(dec(event.get("startPosition"))) <= POSITION_EPS else 1,
                                stable_key(event),
                            ),
                        )
                    else:
                        selected = min(
                            candidates,
                            key=lambda event: (
                                abs(dec(event.get("startPosition")) - cursor),
                                stable_key(event),
                            ),
                        )
                remaining.remove(selected)
                sequence.append(selected)
                cursor = after_for(selected)
            for rank, event in enumerate(sequence):
                causal_rank[id(event)] = rank

    return sorted(
        result,
        key=lambda item: (
            int(item["time"]),
            str(item["coin"]),
            causal_rank[id(item)],
        ),
    )


def fetch_fills(client: PublicInfoClient, suffix: str, address: str, end_ms: int) -> tuple[list[dict[str, Any]], bool]:
    unique: dict[str, dict[str, Any]] = {}
    start_ms = 0
    saturated = False
    for page_number in range(12):
        page = client.post(
            {
                "type": "userFillsByTime",
                "user": address,
                "startTime": start_ms,
                "endTime": end_ms,
                "aggregateByTime": False,
            },
            f"fills_{suffix}_{start_ms}_{end_ms}",
        )
        if not isinstance(page, list):
            raise RuntimeError(f"unexpected fills payload for {suffix}")
        before = len(unique)
        for fill in page:
            if isinstance(fill, dict):
                unique[fill_key(fill)] = fill
        if len(page) < FILL_PAGE_SIZE:
            break
        latest = max((int(fill.get("time") or 0) for fill in page), default=start_ms)
        start_ms = latest if latest > start_ms else start_ms + 1
        if len(unique) == before:
            start_ms += 1
        if len(unique) >= 10_000:
            saturated = True
        if page_number == 11:
            saturated = True
    fills = sorted(
        unique.values(),
        key=lambda fill: (
            int(fill.get("time") or 0),
            int(fill.get("tid") or 0),
            int(fill.get("oid") or 0),
            fill_key(fill),
        ),
    )
    if len(fills) >= 9_990:
        saturated = True
    return fills, saturated


def fetch_funding(client: PublicInfoClient, suffix: str, address: str, end_ms: int) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    start_ms = 0
    for page_number in range(40):
        page = client.post(
            {"type": "userFunding", "user": address, "startTime": start_ms, "endTime": end_ms},
            f"funding_{suffix}_{start_ms}_{end_ms}",
        )
        if not isinstance(page, list):
            break
        before = len(unique)
        for item in page:
            if isinstance(item, dict):
                unique[funding_key(item)] = item
        if len(page) < FUNDING_PAGE_SIZE:
            break
        latest = max((int(item.get("time") or 0) for item in page), default=start_ms)
        start_ms = latest if latest > start_ms else start_ms + 1
        if len(unique) == before:
            start_ms += 1
        if page_number == 39:
            print(f"warning: funding pagination cap reached for {suffix}", file=sys.stderr)
    return sorted(unique.values(), key=lambda item: (int(item.get("time") or 0), funding_key(item)))


def fetch_ledger(client: PublicInfoClient, suffix: str, address: str, end_ms: int) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    start_ms = 0
    for page_number in range(40):
        page = client.post(
            {
                "type": "userNonFundingLedgerUpdates",
                "user": address,
                "startTime": start_ms,
                "endTime": end_ms,
            },
            f"ledger_{suffix}_{start_ms}_{end_ms}",
        )
        if not isinstance(page, list):
            break
        before = len(unique)
        for item in page:
            if isinstance(item, dict):
                unique[ledger_key(item)] = item
        if len(page) < FUNDING_PAGE_SIZE:
            break
        latest = max((int(item.get("time") or 0) for item in page), default=start_ms)
        start_ms = latest if latest > start_ms else start_ms + 1
        if len(unique) == before:
            start_ms += 1
        if page_number == 39:
            print(f"warning: ledger pagination cap reached for {suffix}", file=sys.stderr)
    return sorted(unique.values(), key=lambda item: (int(item.get("time") or 0), ledger_key(item)))


def fetch_portfolio(client: PublicInfoClient, suffix: str, address: str) -> dict[str, Any]:
    payload = client.post({"type": "portfolio", "user": address}, f"portfolio_{suffix}")
    if not isinstance(payload, list):
        return {}
    result: dict[str, Any] = {}
    for item in payload:
        if isinstance(item, list) and len(item) == 2 and isinstance(item[1], dict):
            result[str(item[0])] = item[1]
    return result


def fetch_candles(
    client: PublicInfoClient,
    suffix: str,
    coin: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    digest = hashlib.sha256(coin.encode()).hexdigest()[:16]
    payload = client.post(
        {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": "4h", "startTime": start_ms, "endTime": end_ms},
        },
        f"candles_{suffix}_{digest}_{start_ms}_{end_ms}",
    )
    if not isinstance(payload, list):
        return []
    return sorted(
        [item for item in payload if isinstance(item, dict)],
        key=lambda item: int(item.get("T") or item.get("t") or 0),
    )


def perp_all_time(portfolio: dict[str, Any]) -> dict[str, Any]:
    return portfolio.get("perpAllTime") or portfolio.get("allTime") or {}


def total_all_time(portfolio: dict[str, Any]) -> dict[str, Any]:
    return portfolio.get("allTime") or portfolio.get("perpAllTime") or {}


def _perp_period(period: str) -> str:
    return {
        "allTime": "perpAllTime",
        "month": "perpMonth",
        "week": "perpWeek",
        "day": "perpDay",
    }[period]


def _account_value_points(data: dict[str, Any] | None) -> list[tuple[int, Decimal]]:
    return sorted(
        (int(item[0]), dec(item[1]))
        for item in (data or {}).get("accountValueHistory") or []
        if isinstance(item, list) and len(item) >= 2
    )


def _combined_account_value_points(
    spot_points: list[tuple[int, Decimal]],
    perp_points: list[tuple[int, Decimal]],
) -> list[tuple[int, Decimal]]:
    """Recreate the UI Account Total Value series: Spot + Perpetual.

    Corresponding portfolio and perp charts normally share timestamps.  Older
    down-sampled histories can omit a point from one component, so evaluate
    that component on its adjacent chart samples instead of silently dropping
    the other component from the account-total denominator.
    """

    if not spot_points:
        return perp_points
    if not perp_points:
        return spot_points
    timestamps = sorted({time_ms for time_ms, _ in spot_points} | {time_ms for time_ms, _ in perp_points})
    return [
        (
            timestamp,
            linear_point_value(spot_points, timestamp) + linear_point_value(perp_points, timestamp),
        )
        for timestamp in timestamps
    ]


def total_account_samples(portfolio: dict[str, Any]) -> list[tuple[int, Decimal]]:
    """Merge UI Account Total Value history, preferring denser recent periods."""

    merged: dict[int, Decimal] = {}
    for period in ("allTime", "month", "week", "day"):
        spot_points = _account_value_points(portfolio.get(period))
        perp_points = _account_value_points(portfolio.get(_perp_period(period)))
        for timestamp, value in _combined_account_value_points(spot_points, perp_points):
            if value > ZERO:
                merged[timestamp] = value
    return sorted(merged.items())


def ledger_external_flow(delta: dict[str, Any], address: str) -> Decimal:
    """Return the change in total account capital represented by a ledger row."""

    event_type = str(delta.get("type") or "")
    address = address.lower()
    if event_type == "deposit":
        return dec(delta.get("usdc"))
    if event_type == "withdraw":
        return -dec(delta.get("usdc")) - dec(delta.get("fee"))
    if event_type == "rewardsClaim":
        # Hyperliquid's total portfolio curve books claimed rewards as capital,
        # not trading PnL. This classification reconciles accountValue-pnl.
        return dec(delta.get("amount"))
    if event_type in {"send", "spotTransfer", "internalTransfer", "subAccountTransfer"}:
        source = str(delta.get("user") or "").lower()
        destination = str(delta.get("destination") or "").lower()
        if source == address and destination == address:
            return ZERO
        value = dec(delta.get("usdcValue") or delta.get("usdc") or delta.get("amount"))
        if destination == address and source != address:
            return value
        if source == address and destination != address:
            return -value - dec(delta.get("fee"))
    # accountClassTransfer and self DEX/spot moves do not change total equity.
    return ZERO


def step_curve_value(curve: list[tuple[int, Decimal]], at_ms: int) -> Decimal:
    if not curve:
        return ZERO
    index = bisect.bisect_right([item[0] for item in curve], at_ms) - 1
    return curve[index][1] if index >= 0 else ZERO


def linear_point_value(points: list[tuple[int, Decimal]], at_ms: int) -> Decimal:
    if not points:
        return ZERO
    times = [item[0] for item in points]
    index = bisect.bisect_left(times, at_ms)
    if index <= 0:
        return points[0][1]
    if index >= len(points):
        return points[-1][1]
    left_time, left_value = points[index - 1]
    right_time, right_value = points[index]
    if right_time <= left_time:
        return left_value
    fraction = Decimal(at_ms - left_time) / Decimal(right_time - left_time)
    return left_value + (right_value - left_value) * fraction


class CapitalNormalizer:
    """Estimate total leader equity without leaking transfers across time.

    Account Total Value is Spot plus Perpetual. Public ledger rows locate
    deposits/withdrawals/transfers precisely. Raw perp lifecycle PnL supplies
    the high-frequency trading path, while the residual between official chart
    samples accounts for spot PnL and any unmodeled component.
    """

    def __init__(
        self,
        *,
        address: str,
        portfolio: dict[str, Any],
        ledger: list[dict[str, Any]],
        raw_perp_curve: list[tuple[int, Decimal]],
    ) -> None:
        self.address = address.lower()
        raw_samples = total_account_samples(portfolio)
        flow_by_time: dict[int, Decimal] = defaultdict(Decimal)
        for item in ledger:
            delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
            flow_by_time[int(item.get("time") or 0)] += ledger_external_flow(delta, self.address)
        self.flow_events = sorted(flow_by_time.items())
        self.flow_times = [item[0] for item in self.flow_events]
        running = ZERO
        self.flow_prefix: list[Decimal] = []
        for _, amount in self.flow_events:
            running += amount
            self.flow_prefix.append(running)
        self.raw_perp_curve = raw_perp_curve

        self.samples = raw_samples
        self.discarded_account_samples = 0
        self.sample_times = [item[0] for item in self.samples]

        residuals: list[tuple[int, Decimal]] = []
        if self.flow_events and self.samples and self.flow_events[0][0] < self.samples[0][0]:
            residuals.append((self.flow_events[0][0], ZERO))
        for timestamp, account_value in self.samples:
            implied_total_pnl = account_value - self.external_capital_at(timestamp)
            residuals.append(
                (
                    timestamp,
                    implied_total_pnl - step_curve_value(self.raw_perp_curve, timestamp),
                )
            )
        # Later, denser period histories can duplicate an all-time timestamp.
        self.residual_points = sorted(dict(residuals).items())

        spot_latest = _account_value_points(portfolio.get("allTime"))
        perp_latest = _account_value_points(portfolio.get("perpAllTime"))
        self.current_spot_account_value = spot_latest[-1][1] if spot_latest else ZERO
        self.current_perp_account_value = perp_latest[-1][1] if perp_latest else ZERO
        self.current_total_account_value = (
            self.current_spot_account_value + self.current_perp_account_value
        )
        self.capital_reconciliation_error = (
            abs(self.samples[-1][1] - self.current_total_account_value) if self.samples else ZERO
        )
        if self.capital_reconciliation_error > Decimal("0.01"):
            raise RuntimeError(
                "Account Total validation failed: latest history sample does not equal Spot + Perpetual"
            )

    def external_capital_at(self, at_ms: int) -> Decimal:
        index = bisect.bisect_right(self.flow_times, at_ms) - 1
        return self.flow_prefix[index] if index >= 0 else ZERO

    def value_at(self, at_ms: int) -> Decimal:
        return (
            self.external_capital_at(at_ms)
            + step_curve_value(self.raw_perp_curve, at_ms)
            + linear_point_value(self.residual_points, at_ms)
        )

    def nearest_sample_gap_days(self, at_ms: int) -> float | None:
        if not self.sample_times:
            return None
        index = bisect.bisect_left(self.sample_times, at_ms)
        candidates: list[int] = []
        if index < len(self.sample_times):
            candidates.append(self.sample_times[index])
        if index > 0:
            candidates.append(self.sample_times[index - 1])
        return min(abs(timestamp - at_ms) for timestamp in candidates) / 86_400_000


@dataclass
class Snapshot:
    time_ms: int
    position: Decimal
    avg_entry: Decimal | None
    cumulative_closed_pnl: Decimal
    cumulative_fees: Decimal
    mark_price: Decimal
    event: str


@dataclass
class AddEvent:
    time_ms: int
    pre_unrealized: Decimal
    add_ratio: Decimal
    price: Decimal


@dataclass
class Lifecycle:
    life_id: str
    coin: str
    side: int
    start_ms: int
    complete_start: bool
    initial_price: Decimal
    initial_position: Decimal
    end_ms: int | None = None
    complete_end: bool = False
    right_censored: bool = False
    position_mismatch_count: int = 0
    fill_count: int = 0
    snapshots: list[Snapshot] = field(default_factory=list)
    add_events: list[AddEvent] = field(default_factory=list)
    funding_events: list[tuple[int, Decimal]] = field(default_factory=list)
    equity_at_open: Decimal | None = None
    equity_sample_gap_days: float | None = None
    scale: Decimal | None = None
    valuations: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def side_label(self) -> str:
        return "LONG" if self.side > 0 else "SHORT"

    @property
    def last_snapshot(self) -> Snapshot:
        return self.snapshots[-1]


def estimate_unknown_entry(fill: dict[str, Any], start: Decimal, delta: Decimal) -> Decimal:
    price = dec(fill.get("px"))
    closed = min(abs(start), abs(delta)) if sign(start) != sign(delta) else ZERO
    pnl = dec(fill.get("closedPnl"))
    if closed > POSITION_EPS and abs(pnl) > ZERO and sign(start):
        return price - pnl / (Decimal(sign(start)) * closed)
    return price


def new_lifecycle(
    suffix: str,
    sequence: int,
    coin: str,
    side_value: int,
    time_ms: int,
    complete_start: bool,
    initial_price: Decimal,
    initial_position: Decimal,
) -> Lifecycle:
    return Lifecycle(
        life_id=f"{suffix}-{sequence}",
        coin=coin,
        side=side_value,
        start_ms=time_ms,
        complete_start=complete_start,
        initial_price=initial_price,
        initial_position=initial_position,
    )


def append_snapshot(
    life: Lifecycle,
    *,
    time_ms: int,
    position: Decimal,
    avg_entry: Decimal | None,
    closed_pnl: Decimal,
    fees: Decimal,
    mark: Decimal,
    event: str,
) -> None:
    life.snapshots.append(
        Snapshot(
            time_ms=time_ms,
            position=position,
            avg_entry=avg_entry,
            cumulative_closed_pnl=closed_pnl,
            cumulative_fees=fees,
            mark_price=mark,
            event=event,
        )
    )


def build_lifecycles(suffix: str, fills: list[dict[str, Any]], end_ms: int) -> list[Lifecycle]:
    by_coin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        if is_perp_fill(fill):
            by_coin[str(fill.get("coin"))].append(fill)
    result: list[Lifecycle] = []
    sequence = 0
    for coin, coin_fills in sorted(by_coin.items()):
        active: Lifecycle | None = None
        position = ZERO
        avg_entry: Decimal | None = None
        closed_pnl = ZERO
        fees = ZERO
        for fill in coin_fills:
            timestamp = int(fill.get("time") or 0)
            price = dec(fill.get("px"))
            size = abs(dec(fill.get("sz")))
            delta = size if str(fill.get("side") or "").upper() == "B" else -size
            start = dec(fill.get("startPosition"))
            after = start + delta
            fill_fee = dec(fill.get("fee"))
            fill_closed_pnl = dec(fill.get("closedPnl"))

            if active is not None and abs(position - start) > POSITION_EPS:
                active.position_mismatch_count += 1
                if sign(position) != sign(start) and sign(start) != 0:
                    active.end_ms = timestamp
                    active.complete_end = False
                    result.append(active)
                    active = None
                    avg_entry = None
                    closed_pnl = ZERO
                    fees = ZERO
                position = start

            if active is None and sign(start) != 0:
                sequence += 1
                avg_entry = estimate_unknown_entry(fill, start, delta)
                active = new_lifecycle(
                    suffix,
                    sequence,
                    coin,
                    sign(start),
                    timestamp,
                    False,
                    price,
                    start,
                )
                position = start
                closed_pnl = ZERO
                fees = ZERO
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=start,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="LEFT_CENSORED_START",
                )

            if sign(start) == 0:
                if sign(after) == 0:
                    continue
                if active is not None:
                    active.end_ms = timestamp
                    active.complete_end = False
                    result.append(active)
                sequence += 1
                active = new_lifecycle(
                    suffix,
                    sequence,
                    coin,
                    sign(after),
                    timestamp,
                    True,
                    price,
                    after,
                )
                position = after
                avg_entry = price
                closed_pnl = fill_closed_pnl
                fees = fill_fee
                active.fill_count += 1
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="OPEN",
                )
                continue

            if active is None:
                continue

            active.fill_count += 1
            start_side = sign(start)
            delta_side = sign(delta)
            if start_side == delta_side:
                pre_unrealized = (
                    Decimal(start_side) * (price - avg_entry) * abs(start)
                    if avg_entry is not None
                    else ZERO
                )
                active.add_events.append(
                    AddEvent(
                        time_ms=timestamp,
                        pre_unrealized=pre_unrealized,
                        add_ratio=abs(delta) / abs(start) if abs(start) > POSITION_EPS else ZERO,
                        price=price,
                    )
                )
                if avg_entry is None:
                    avg_entry = price
                avg_entry = (avg_entry * abs(start) + price * abs(delta)) / abs(after)
                position = after
                closed_pnl += fill_closed_pnl
                fees += fill_fee
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="ADD",
                )
                continue

            closed_qty = min(abs(start), abs(delta))
            close_fee = fill_fee * (closed_qty / abs(delta)) if abs(delta) > POSITION_EPS else fill_fee
            open_fee = fill_fee - close_fee
            closed_pnl += fill_closed_pnl
            fees += close_fee

            if abs(delta) < abs(start) - POSITION_EPS:
                position = after
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="REDUCE",
                )
                continue

            append_snapshot(
                active,
                time_ms=timestamp,
                position=ZERO,
                avg_entry=None,
                closed_pnl=closed_pnl,
                fees=fees,
                mark=price,
                event="CLOSE" if sign(after) == 0 else "FLIP_CLOSE",
            )
            active.end_ms = timestamp
            active.complete_end = True
            result.append(active)
            active = None
            position = ZERO
            avg_entry = None
            closed_pnl = ZERO
            fees = ZERO

            if sign(after) != 0:
                sequence += 1
                active = new_lifecycle(
                    suffix,
                    sequence,
                    coin,
                    sign(after),
                    timestamp,
                    True,
                    price,
                    after,
                )
                active.fill_count = 1
                position = after
                avg_entry = price
                fees = open_fee
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=ZERO,
                    fees=fees,
                    mark=price,
                    event="FLIP_OPEN",
                )

        if active is not None:
            active.end_ms = end_ms
            active.complete_end = False
            active.right_censored = True
            result.append(active)
    return sorted(result, key=lambda life: (life.start_ms, life.life_id))


def build_followable_lifecycles(
    suffix: str,
    fills: list[dict[str, Any]],
    end_ms: int,
    normalizer: CapitalNormalizer,
    *,
    follower_base: Decimal = FOLLOWER_BASE,
    min_order_value: Decimal = Decimal("10"),
) -> list[Lifecycle]:
    """Rebuild lifecycles using the bot's economic-flat dust semantics.

    A proportional follower remainder below the venue minimum is closed rather
    than retained.  The leader may keep a tiny non-zero position indefinitely;
    a later material increase from that residual is a fresh follower open with
    a new account-ratio scale and entry price.  This prevents residual dust
    from joining several independent trades into one artificial long hold.
    """

    by_coin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        if is_perp_fill(fill):
            by_coin[str(fill.get("coin"))].append(fill)

    result: list[Lifecycle] = []
    sequence = 0

    def scale_at(timestamp: int) -> tuple[Decimal | None, Decimal | None]:
        equity = normalizer.value_at(max(0, timestamp - 1))
        if equity <= ZERO:
            return None, None
        return follower_base / equity, equity

    def projected_notional(position: Decimal, price: Decimal, scale: Decimal | None) -> Decimal:
        if scale is None:
            return ZERO
        return abs(position) * abs(price) * scale

    def fee_for_notional(fill_fee: Decimal, fill_qty: Decimal, price: Decimal, qty: Decimal) -> Decimal:
        if fill_qty <= POSITION_EPS or price <= ZERO:
            return ZERO
        return fill_fee * abs(qty) / fill_qty

    for coin, coin_fills in sorted(by_coin.items()):
        active: Lifecycle | None = None
        position = ZERO
        avg_entry: Decimal | None = None
        closed_pnl = ZERO
        fees = ZERO

        def start_life(
            *,
            timestamp: int,
            price: Decimal,
            new_position: Decimal,
            event: str,
            opening_fee: Decimal,
            complete_start: bool = True,
        ) -> Lifecycle | None:
            nonlocal sequence, position, avg_entry, closed_pnl, fees
            scale, equity = scale_at(timestamp)
            if scale is None or projected_notional(new_position, price, scale) < min_order_value:
                return None
            sequence += 1
            life = new_lifecycle(
                suffix,
                sequence,
                coin,
                sign(new_position),
                timestamp,
                complete_start,
                price,
                new_position,
            )
            life.scale = scale
            life.equity_at_open = equity
            life.equity_sample_gap_days = normalizer.nearest_sample_gap_days(timestamp)
            life.fill_count = 1
            position = new_position
            avg_entry = price
            closed_pnl = ZERO
            fees = opening_fee
            append_snapshot(
                life,
                time_ms=timestamp,
                position=position,
                avg_entry=avg_entry,
                closed_pnl=closed_pnl,
                fees=fees,
                mark=price,
                event=event,
            )
            return life

        for fill in coin_fills:
            timestamp = int(fill.get("time") or 0)
            price = dec(fill.get("px"))
            size = abs(dec(fill.get("sz")))
            delta = size if str(fill.get("side") or "").upper() == "B" else -size
            start = dec(fill.get("startPosition"))
            after = start + delta
            fill_fee = dec(fill.get("fee"))

            if active is None:
                candidate_scale, _ = scale_at(timestamp)
                material_after = (
                    sign(after) != 0
                    and projected_notional(after, price, candidate_scale) >= min_order_value
                )
                is_exact_open = sign(start) == 0 and sign(after) != 0
                is_same_side_increase = sign(start) != 0 and sign(start) == sign(delta) == sign(after)
                is_flip = sign(start) != 0 and sign(after) != 0 and sign(start) != sign(after)
                if material_after and (is_exact_open or is_same_side_increase or is_flip):
                    opening_fee = fee_for_notional(fill_fee, size, price, abs(after))
                    active = start_life(
                        timestamp=timestamp,
                        price=price,
                        new_position=after,
                        event="OPEN" if is_exact_open else "DUST_REOPEN",
                        opening_fee=opening_fee,
                    )
                continue

            if abs(position - start) > POSITION_EPS:
                active.position_mismatch_count += 1
                active.end_ms = timestamp
                active.complete_end = False
                result.append(active)
                active = None
                position = ZERO
                avg_entry = None
                closed_pnl = ZERO
                fees = ZERO
                # Do not let an uncertain transition manufacture a new open.
                continue

            active.fill_count += 1
            start_side = sign(start)
            delta_side = sign(delta)
            if start_side == delta_side:
                pre_unrealized = (
                    Decimal(start_side) * (price - avg_entry) * abs(start)
                    if avg_entry is not None
                    else ZERO
                )
                active.add_events.append(
                    AddEvent(
                        time_ms=timestamp,
                        pre_unrealized=pre_unrealized,
                        add_ratio=abs(delta) / abs(start) if abs(start) > POSITION_EPS else ZERO,
                        price=price,
                    )
                )
                avg_entry = price if avg_entry is None else (
                    (avg_entry * abs(start) + price * abs(delta)) / abs(after)
                )
                position = after
                fees += fill_fee
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="ADD",
                )
                continue

            old_side = sign(start)
            if avg_entry is None:
                avg_entry = estimate_unknown_entry(fill, start, delta)
            follower_remainder = projected_notional(after, price, active.scale)
            same_side_remainder = sign(after) == old_side
            should_economic_close = (
                sign(after) == 0
                or sign(after) != old_side
                or follower_remainder < min_order_value
            )

            if not should_economic_close and same_side_remainder:
                closed_qty = min(abs(start), abs(delta))
                closed_pnl += Decimal(old_side) * (price - avg_entry) * closed_qty
                fees += fee_for_notional(fill_fee, size, price, closed_qty)
                position = after
                append_snapshot(
                    active,
                    time_ms=timestamp,
                    position=position,
                    avg_entry=avg_entry,
                    closed_pnl=closed_pnl,
                    fees=fees,
                    mark=price,
                    event="REDUCE",
                )
                continue

            # The follower exits the entire remaining allocation at this fill
            # price, including a leader residual that is economically dust.
            closed_pnl += Decimal(old_side) * (price - avg_entry) * abs(start)
            fees += fee_for_notional(fill_fee, size, price, abs(start))
            append_snapshot(
                active,
                time_ms=timestamp,
                position=ZERO,
                avg_entry=None,
                closed_pnl=closed_pnl,
                fees=fees,
                mark=price,
                event=(
                    "DUST_CLOSE"
                    if same_side_remainder and sign(after) != 0
                    else "CLOSE"
                    if sign(after) == 0
                    else "FLIP_CLOSE"
                ),
            )
            active.end_ms = timestamp
            active.complete_end = True
            result.append(active)
            active = None
            position = ZERO
            avg_entry = None
            closed_pnl = ZERO
            fees = ZERO

            if sign(after) != 0 and sign(after) != old_side:
                open_qty = abs(after)
                opening_fee = fee_for_notional(fill_fee, size, price, open_qty)
                active = start_life(
                    timestamp=timestamp,
                    price=price,
                    new_position=after,
                    event="FLIP_OPEN",
                    opening_fee=opening_fee,
                )

        if active is not None:
            active.end_ms = end_ms
            active.complete_end = False
            active.right_censored = True
            result.append(active)

    return sorted(result, key=lambda life: (life.start_ms, life.life_id))


def assign_equity_and_funding(
    lifecycles: list[Lifecycle],
    funding: list[dict[str, Any]],
) -> tuple[int, Decimal]:
    by_coin: dict[str, list[Lifecycle]] = defaultdict(list)
    for life in lifecycles:
        # First pass stays raw. A second pass applies 20k/equity-at-open after
        # the raw perp PnL curve has been built for the capital estimator.
        life.equity_at_open = None
        life.equity_sample_gap_days = None
        life.scale = Decimal("1")
        by_coin[life.coin].append(life)
    unassigned = 0
    unassigned_value = ZERO
    for item in funding:
        delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
        coin = str(delta.get("coin") or "")
        timestamp = int(item.get("time") or 0)
        amount = dec(delta.get("usdc"))
        target = next(
            (
                life
                for life in by_coin.get(coin, [])
                if life.start_ms <= timestamp <= int(life.end_ms or timestamp)
            ),
            None,
        )
        if target is None:
            unassigned += 1
            unassigned_value += amount
        else:
            target.funding_events.append((timestamp, amount))
    return unassigned, unassigned_value


def apply_equity_normalization(
    lifecycles: list[Lifecycle],
    normalizer: CapitalNormalizer,
) -> int:
    unavailable = 0
    for life in lifecycles:
        # Use equity immediately before the lifecycle opens. The scale remains
        # fixed for all subsequent adds/reductions, matching the bot's rule
        # that only the opening uses the sizing formula and later fills use
        # position ratios. This also avoids dividing the same loss by an equity
        # denominator already reduced by that loss.
        equity = normalizer.value_at(max(0, life.start_ms - 1))
        if equity <= ZERO:
            life.equity_at_open = None
            life.equity_sample_gap_days = normalizer.nearest_sample_gap_days(life.start_ms)
            life.scale = None
            life.valuations = []
            life.metrics = {}
            unavailable += 1
            continue
        life.equity_at_open = equity
        life.equity_sample_gap_days = normalizer.nearest_sample_gap_days(life.start_ms)
        life.scale = FOLLOWER_BASE / equity
    return unavailable


def valuation_for(
    snapshot: Snapshot,
    mark: Decimal,
    funding_total: Decimal,
    scale: Decimal,
) -> dict[str, Decimal]:
    unrealized = ZERO
    if sign(snapshot.position) and snapshot.avg_entry is not None:
        unrealized = Decimal(sign(snapshot.position)) * (mark - snapshot.avg_entry) * abs(snapshot.position)
    total = snapshot.cumulative_closed_pnl - snapshot.cumulative_fees + funding_total + unrealized
    return {
        "total": total * scale,
        "unrealized": unrealized * scale,
        "notional": abs(snapshot.position) * mark * scale,
    }


def lifecycle_valuations(life: Lifecycle, candles: list[dict[str, Any]]) -> None:
    if not life.snapshots or life.scale is None:
        return
    candle_ends = [int(item.get("T") or item.get("t") or 0) for item in candles]
    funding_sorted = sorted(life.funding_events)
    funding_times = [item[0] for item in funding_sorted]
    funding_prefix: list[Decimal] = []
    running = ZERO
    for _, amount in funding_sorted:
        running += amount
        funding_prefix.append(running)

    def funding_at(timestamp: int) -> Decimal:
        index = bisect.bisect_right(funding_times, timestamp) - 1
        return funding_prefix[index] if index >= 0 else ZERO

    points: list[dict[str, Any]] = []
    for index, snapshot in enumerate(life.snapshots):
        segment_end = (
            life.snapshots[index + 1].time_ms
            if index + 1 < len(life.snapshots)
            else int(life.end_ms or snapshot.time_ms)
        )
        current = valuation_for(snapshot, snapshot.mark_price, funding_at(snapshot.time_ms), life.scale)
        points.append(
            {
                "time_ms": snapshot.time_ms,
                "value": current["total"],
                "unrealized": current["unrealized"],
                "notional": current["notional"],
                "kind": snapshot.event,
                "extreme": False,
            }
        )
        if sign(snapshot.position) == 0 or segment_end <= snapshot.time_ms:
            continue
        left = bisect.bisect_right(candle_ends, snapshot.time_ms)
        right = bisect.bisect_right(candle_ends, segment_end)
        for candle in candles[left:right]:
            candle_start = int(candle.get("t") or 0)
            candle_end = int(candle.get("T") or candle_start)
            close = dec(candle.get("c"))
            close_value = valuation_for(snapshot, close, funding_at(candle_end), life.scale)
            points.append(
                {
                    "time_ms": candle_end,
                    "value": close_value["total"],
                    "unrealized": close_value["unrealized"],
                    "notional": close_value["notional"],
                    "kind": "4H_CLOSE",
                    "extreme": False,
                }
            )
            if candle_start < snapshot.time_ms or candle_end > segment_end:
                continue
            adverse_mark = dec(candle.get("l")) if sign(snapshot.position) > 0 else dec(candle.get("h"))
            extreme_value = valuation_for(snapshot, adverse_mark, funding_at(candle_end), life.scale)
            points.append(
                {
                    "time_ms": candle_end,
                    "value": extreme_value["total"],
                    "unrealized": extreme_value["unrealized"],
                    "notional": extreme_value["notional"],
                    "kind": "4H_ADVERSE_EXTREME",
                    "extreme": True,
                }
            )

    points.sort(key=lambda item: (item["time_ms"], bool(item["extreme"]), item["kind"]))
    life.valuations = points
    regular = [item for item in points if not item["extreme"]]
    if not regular:
        return
    all_points = points
    worst_total = min(all_points, key=lambda item: item["value"])
    worst_unrealized = min(all_points, key=lambda item: item["unrealized"])
    max_notional = max((item["notional"] for item in regular), default=ZERO)
    peak = ZERO
    max_drawdown = ZERO
    max_drawdown_time = life.start_ms
    for item in regular:
        peak = max(peak, item["value"])
        drawdown = peak - item["value"]
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_time = item["time_ms"]

    longest_underwater_ms = 0
    total_underwater_ms = 0
    current_start: int | None = None
    longest_start: int | None = None
    longest_end: int | None = None
    prior: dict[str, Any] | None = None
    for item in regular:
        if prior is not None and prior["value"] < UNDERWATER_USD_THRESHOLD:
            total_underwater_ms += max(0, item["time_ms"] - prior["time_ms"])
        if item["value"] < UNDERWATER_USD_THRESHOLD and current_start is None:
            current_start = item["time_ms"]
        if item["value"] >= ZERO and current_start is not None:
            duration = item["time_ms"] - current_start
            if duration > longest_underwater_ms:
                longest_underwater_ms = duration
                longest_start = current_start
                longest_end = item["time_ms"]
            current_start = None
        prior = item
    if current_start is not None:
        duration = int(life.end_ms or regular[-1]["time_ms"]) - current_start
        if duration > longest_underwater_ms:
            longest_underwater_ms = duration
            longest_start = current_start
            longest_end = int(life.end_ms or regular[-1]["time_ms"])

    final_value = regular[-1]["value"]
    underwater_adds = [event for event in life.add_events if event.pre_unrealized < ZERO]
    life.metrics = {
        "hold_hours": (int(life.end_ms or regular[-1]["time_ms"]) - life.start_ms) / 3_600_000,
        "final_net": final_value,
        "worst_total": worst_total["value"],
        "worst_total_time_ms": worst_total["time_ms"],
        "worst_total_kind": worst_total["kind"],
        "worst_unrealized": worst_unrealized["unrealized"],
        "worst_unrealized_time_ms": worst_unrealized["time_ms"],
        "max_lifecycle_drawdown": max_drawdown,
        "max_lifecycle_drawdown_time_ms": max_drawdown_time,
        "max_notional": max_notional,
        "longest_underwater_hours": longest_underwater_ms / 3_600_000,
        "total_underwater_hours": total_underwater_ms / 3_600_000,
        "underwater_start_ms": longest_start,
        "underwater_end_ms": longest_end,
        "add_count": len(life.add_events),
        "underwater_add_count": len(underwater_adds),
        "max_underwater_add_ratio": max((event.add_ratio for event in underwater_adds), default=ZERO),
        "funding": sum((amount for _, amount in life.funding_events), ZERO) * life.scale,
    }


def curve_max_drawdown(events: list[tuple[int, Decimal]]) -> tuple[Decimal, int | None, int | None, float]:
    if not events:
        return ZERO, None, None, 0.0
    peak = ZERO
    peak_time = events[0][0]
    worst = ZERO
    trough_time = events[0][0]
    drawdown_peak_time = peak_time
    underwater_start: int | None = None
    longest_underwater = 0
    for timestamp, value in events:
        if value >= peak:
            if underwater_start is not None:
                longest_underwater = max(longest_underwater, timestamp - underwater_start)
                underwater_start = None
            peak = value
            peak_time = timestamp
        else:
            if underwater_start is None:
                underwater_start = peak_time
            drawdown = peak - value
            if drawdown > worst:
                worst = drawdown
                drawdown_peak_time = peak_time
                trough_time = timestamp
    if underwater_start is not None:
        longest_underwater = max(longest_underwater, events[-1][0] - underwater_start)
    return worst, drawdown_peak_time, trough_time, longest_underwater / 3_600_000


def aggregate_lifecycle_curve(lifecycles: list[Lifecycle], *, include_extremes: bool) -> list[tuple[int, Decimal]]:
    updates: list[tuple[int, str, Decimal]] = []
    for life in lifecycles:
        for point in life.valuations:
            if point["extreme"] and not include_extremes:
                continue
            updates.append((int(point["time_ms"]), life.life_id, dec(point["value"])))
    updates.sort(key=lambda item: (item[0], item[1]))
    latest: dict[str, Decimal] = {}
    total = ZERO
    curve: list[tuple[int, Decimal]] = []
    for timestamp, life_id, value in updates:
        total += value - latest.get(life_id, ZERO)
        latest[life_id] = value
        curve.append((timestamp, total))
    return curve


def aggregate_exposure(lifecycles: list[Lifecycle]) -> tuple[Decimal, int]:
    updates: list[tuple[int, str, Decimal]] = []
    for life in lifecycles:
        for point in life.valuations:
            if not point["extreme"]:
                updates.append((int(point["time_ms"]), life.life_id, dec(point["notional"])))
    updates.sort(key=lambda item: (item[0], item[1]))
    latest: dict[str, Decimal] = {}
    total = ZERO
    peak_total = ZERO
    peak_count = 0
    for _, life_id, notional in updates:
        total += notional - latest.get(life_id, ZERO)
        latest[life_id] = notional
        if total > peak_total:
            peak_total = total
            peak_count = sum(1 for value in latest.values() if value > ZERO)
    return peak_total, peak_count


def portfolio_crosscheck(portfolio: dict[str, Any], normalizer: CapitalNormalizer) -> dict[str, Any]:
    data = perp_all_time(portfolio)
    pnl_points = sorted(
        (int(item[0]), dec(item[1]))
        for item in data.get("pnlHistory") or []
        if isinstance(item, list) and len(item) >= 2
    )
    if len(pnl_points) < 2:
        return {}
    wealth = Decimal("1")
    peak_wealth = wealth
    max_drawdown_fraction = ZERO
    drawdown_peak_ms: int | None = None
    drawdown_trough_ms: int | None = None
    latest_peak_ms: int | None = None
    valid_intervals = 0
    interval_losses: list[tuple[Decimal, int]] = []
    for previous, current in zip(pnl_points, pnl_points[1:]):
        if normalizer.external_capital_at(previous[0]) != normalizer.external_capital_at(current[0]):
            # A coarse chart interval that contains a deposit/withdrawal has no
            # single scientifically valid return denominator. Skip it rather
            # than divide post-transfer PnL by stale pre-transfer equity.
            continue
        denominator = normalizer.value_at(previous[0])
        if denominator <= ZERO:
            continue
        change = current[1] - previous[1]
        period_return = change / denominator
        if period_return <= Decimal("-1"):
            continue
        wealth *= Decimal("1") + period_return
        valid_intervals += 1
        if wealth >= peak_wealth:
            peak_wealth = wealth
            latest_peak_ms = current[0]
        else:
            drawdown_fraction = (peak_wealth - wealth) / peak_wealth
            if drawdown_fraction > max_drawdown_fraction:
                max_drawdown_fraction = drawdown_fraction
                drawdown_peak_ms = latest_peak_ms
                drawdown_trough_ms = current[0]
        interval_losses.append((period_return * FOLLOWER_BASE, current[0]))
    if valid_intervals < 2:
        return {}
    worst_interval = min(interval_losses, default=(ZERO, 0), key=lambda item: item[0])
    return {
        "start_ms": pnl_points[0][0],
        "end_ms": pnl_points[-1][0],
        "point_count": valid_intervals,
        "normalized_net": (wealth - Decimal("1")) * FOLLOWER_BASE,
        "max_drawdown": max_drawdown_fraction * FOLLOWER_BASE,
        "drawdown_peak_ms": drawdown_peak_ms,
        "drawdown_trough_ms": drawdown_trough_ms,
        "longest_underwater_hours": None,
        "worst_interval": worst_interval[0],
        "worst_interval_ms": worst_interval[1],
    }


def leader_summary(
    suffix: str,
    address: str,
    all_fills: list[dict[str, Any]],
    saturated: bool,
    lifecycles: list[Lifecycle],
    portfolio: dict[str, Any],
    normalizer: CapitalNormalizer,
    normalization_unavailable_count: int,
    funding_unassigned_count: int,
    funding_unassigned_value: Decimal,
) -> dict[str, Any]:
    usable = [life for life in lifecycles if life.metrics and life.scale is not None]
    complete = [life for life in usable if life.complete_start and life.complete_end]
    closed = [life for life in usable if life.complete_end]
    winners = [life for life in complete if dec(life.metrics["final_net"]) > ZERO]
    losers = [life for life in complete if dec(life.metrics["final_net"]) < ZERO]
    losing_holds = [float(life.metrics["hold_hours"]) for life in losers]
    winning_holds = [float(life.metrics["hold_hours"]) for life in winners]
    all_adds = sum(int(life.metrics.get("add_count") or 0) for life in usable)
    underwater_adds = sum(int(life.metrics.get("underwater_add_count") or 0) for life in usable)
    reliable_curve_lives = [life for life in usable if life.complete_start]
    regular_curve = aggregate_lifecycle_curve(reliable_curve_lives, include_extremes=False)
    extreme_curve = aggregate_lifecycle_curve(reliable_curve_lives, include_extremes=True)
    regular_dd = curve_max_drawdown(regular_curve)
    extreme_dd = curve_max_drawdown(extreme_curve)
    peak_notional, peak_positions = aggregate_exposure(usable)
    gross_losses = sorted(
        [abs(dec(life.metrics["final_net"])) for life in complete if dec(life.metrics["final_net"]) < ZERO],
        reverse=True,
    )
    total_gross_loss = sum(gross_losses, ZERO)
    worst = sorted(usable, key=lambda life: dec(life.metrics["worst_total"]))
    longest = sorted(usable, key=lambda life: float(life.metrics["longest_underwater_hours"]), reverse=True)
    worst_closed = sorted(losers, key=lambda life: dec(life.metrics["final_net"]))
    current_open = [life for life in usable if life.right_censored]
    fill_times = [int(fill.get("time") or 0) for fill in all_fills]
    opening_equities = [dec(life.equity_at_open) for life in usable if life.equity_at_open is not None]
    opening_scales = [dec(life.scale) for life in usable if life.scale is not None]
    sample_gaps = [float(life.equity_sample_gap_days) for life in usable if life.equity_sample_gap_days is not None]
    return {
        "suffix": suffix,
        "address": address,
        "fill_count_all": len(all_fills),
        "perp_fill_count": sum(1 for fill in all_fills if is_perp_fill(fill)),
        "logical_fill_count": len(logical_fills(all_fills)),
        "fill_start_ms": min(fill_times) if fill_times else None,
        "fill_end_ms": max(fill_times) if fill_times else None,
        "fill_limit_saturated": saturated,
        "coins": len({life.coin for life in lifecycles}),
        "lifecycles": usable,
        "lifecycle_count": len(usable),
        "complete_count": len(complete),
        "left_censored_count": sum(1 for life in usable if not life.complete_start),
        "right_censored_count": len(current_open),
        "position_mismatch_life_count": sum(1 for life in usable if life.position_mismatch_count > 0),
        "position_mismatch_count": sum(life.position_mismatch_count for life in usable),
        "normalization_unavailable_count": normalization_unavailable_count,
        "opening_equity_min": min(opening_equities, default=None),
        "opening_equity_median": Decimal(str(statistics.median(opening_equities))) if opening_equities else None,
        "opening_equity_max": max(opening_equities, default=None),
        "opening_scale_min": min(opening_scales, default=None),
        "opening_scale_max": max(opening_scales, default=None),
        "opening_equity_sample_gap_max_days": max(sample_gaps, default=None),
        "capital_sample_count": len(normalizer.samples),
        "capital_sample_discarded_count": normalizer.discarded_account_samples,
        "capital_sample_start_ms": normalizer.samples[0][0] if normalizer.samples else None,
        "capital_sample_end_ms": normalizer.samples[-1][0] if normalizer.samples else None,
        "capital_reconciliation_error": normalizer.capital_reconciliation_error,
        "current_spot_account_value": normalizer.current_spot_account_value,
        "current_perp_account_value": normalizer.current_perp_account_value,
        "current_total_account_value": normalizer.current_total_account_value,
        "wins": len(winners),
        "losses": len(losers),
        "win_rate": (Decimal(len(winners)) / Decimal(len(complete)) * 100) if complete else None,
        "median_losing_hold": statistics.median(losing_holds) if losing_holds else None,
        "p90_losing_hold": percentile(losing_holds, 0.90),
        "p95_losing_hold": percentile(losing_holds, 0.95),
        "max_losing_hold": max(losing_holds, default=None),
        "median_winning_hold": statistics.median(winning_holds) if winning_holds else None,
        "loss_over_24h_pct": (sum(value >= 24 for value in losing_holds) / len(losing_holds) * 100) if losing_holds else None,
        "loss_over_72h_pct": (sum(value >= 72 for value in losing_holds) / len(losing_holds) * 100) if losing_holds else None,
        "loss_over_7d_pct": (sum(value >= 168 for value in losing_holds) / len(losing_holds) * 100) if losing_holds else None,
        "underwater_add_rate": (Decimal(underwater_adds) / Decimal(all_adds) * 100) if all_adds else None,
        "underwater_add_life_pct": (
            Decimal(sum(1 for life in usable if int(life.metrics.get("underwater_add_count") or 0) > 0))
            / Decimal(len(usable))
            * 100
        ) if usable else None,
        "all_adds": all_adds,
        "underwater_adds": underwater_adds,
        "worst": worst,
        "longest": longest,
        "worst_closed": worst_closed,
        "current_open": current_open,
        "regular_max_drawdown": regular_dd,
        "extreme_max_drawdown": extreme_dd,
        "peak_notional": peak_notional,
        "peak_positions": peak_positions,
        "top1_loss_concentration": (gross_losses[0] / total_gross_loss * 100) if gross_losses and total_gross_loss else None,
        "top3_loss_concentration": (sum(gross_losses[:3], ZERO) / total_gross_loss * 100) if gross_losses and total_gross_loss else None,
        "portfolio": portfolio_crosscheck(portfolio, normalizer),
        "funding_unassigned_count": funding_unassigned_count,
        "funding_unassigned_value": funding_unassigned_value,
    }


def risk_score(summary: dict[str, Any]) -> Decimal:
    worst = abs(dec(summary["worst"][0].metrics["worst_total"])) if summary["worst"] else ZERO
    drawdown = dec(summary["extreme_max_drawdown"][0])
    longest_hours = max(
        (float(life.metrics["longest_underwater_hours"]) for life in summary["lifecycles"]),
        default=0.0,
    )
    add_rate = dec(summary.get("underwater_add_rate")) / Decimal("100")
    p95_hours = float(summary.get("p95_losing_hold") or 0)
    return (
        Decimal("35") * min(Decimal("1"), worst / FOLLOWER_BASE)
        + Decimal("25") * min(Decimal("1"), drawdown / FOLLOWER_BASE)
        + Decimal("20") * min(Decimal("1"), Decimal(str(longest_hours / (30 * 24))))
        + Decimal("10") * min(Decimal("1"), add_rate)
        + Decimal("10") * min(Decimal("1"), Decimal(str(p95_hours / (7 * 24))))
    )


def life_row(life: Lifecycle, *, final_column: bool = True) -> str:
    metrics = life.metrics
    quality = (
        "完整"
        if life.complete_start and life.complete_end
        else "当前未平"
        if life.right_censored
        else "左截断"
        if not life.complete_start
        else "边界不连续"
    )
    final = fmt_usd(metrics.get("final_net"), 0) if final_column else quality
    return (
        f"| {life.coin} | {life.side_label} | {iso(life.start_ms)} | {duration_label(metrics.get('hold_hours'))} "
        f"| {fmt_usd(life.equity_at_open, 0)} | {dec(life.scale):.3f}x "
        f"| {fmt_usd(metrics.get('max_notional'), 0)} | {fmt_usd(metrics.get('worst_unrealized'), 0)} "
        f"| {fmt_usd(metrics.get('worst_total'), 0)} | {duration_label(metrics.get('longest_underwater_hours'))} "
        f"| {metrics.get('underwater_add_count', 0)}/{metrics.get('add_count', 0)} | {final} | {quality} |"
    )


def risk_interpretation(summary: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    suffix = summary["suffix"]
    worst = summary["worst"][0] if summary["worst"] else None
    longest = summary["longest"][0] if summary["longest"] else None
    add_rate = dec(summary.get("underwater_add_rate"))
    p95 = float(summary.get("p95_losing_hold") or 0)
    dd = dec(summary["extreme_max_drawdown"][0])
    if worst:
        loss_pct = abs(dec(worst.metrics["worst_total"])) / FOLLOWER_BASE * 100
        lines.append(
            f"- 最深单生命周期亏损出现在 **{worst.coin} {worst.side_label}**，标准化谷底为 "
            f"**{fmt_usd(worst.metrics['worst_total'], 0)}（{loss_pct:.1f}%）**。"
        )
    if longest:
        lines.append(
            f"- 最长连续扛亏约 **{duration_label(longest.metrics['longest_underwater_hours'])}**，"
            f"对应 {longest.coin} {longest.side_label}。"
        )
    if p95 >= 168:
        lines.append(f"- 亏损单持有时间 P95 为 **{duration_label(p95)}**，存在明显长尾持仓。")
    elif p95 >= 72:
        lines.append(f"- 亏损单持有时间 P95 为 **{duration_label(p95)}**，有中等程度的延迟止损长尾。")
    else:
        lines.append(f"- 亏损单持有时间 P95 为 **{duration_label(p95)}**，多数亏损生命周期处理较快。")
    if add_rate >= Decimal("50"):
        lines.append(f"- {suffix} 有 **{add_rate:.1f}%** 的加仓发生在当时浮亏状态，逆势加仓倾向偏强。")
    elif add_rate >= Decimal("20"):
        lines.append(f"- {suffix} 有 **{add_rate:.1f}%** 的加仓发生在浮亏状态，存在但不是绝对主导。")
    else:
        lines.append(f"- {suffix} 浮亏时加仓占全部加仓 **{add_rate:.1f}%**，逆势加仓倾向相对低。")
    lines.append(f"- 组合级标准化压力曲线最大回撤约 **{fmt_usd(dd, 0)}（{dd / FOLLOWER_BASE * 100:.1f}%）**。")
    return lines


def render_report(
    summaries: list[dict[str, Any]],
    generated_ms: int,
    lifecycle_detail_filename: str,
) -> str:
    ranked = sorted(summaries, key=risk_score)
    worst_amount_rank = sorted(
        summaries,
        key=lambda summary: abs(dec(summary["worst"][0].metrics["worst_total"])) if summary["worst"] else ZERO,
    )
    drawdown_rank = sorted(summaries, key=lambda summary: dec(summary["extreme_max_drawdown"][0]))
    duration_rank = sorted(
        summaries,
        key=lambda summary: max(
            (float(life.metrics["longest_underwater_hours"]) for life in summary["lifecycles"]),
            default=0.0,
        ),
    )
    total_lifecycles = sum(summary["lifecycle_count"] for summary in summaries)
    total_position_mismatches = sum(summary["position_mismatch_count"] for summary in summaries)
    lines = [
        "# Hyperliquid Leader 等本金扛亏与回撤研究",
        "",
        f"生成时间：{iso(generated_ms)} UTC  ",
        "统一口径：假设每次生命周期开始时 follower 本金为 **20,000 USDC**。用 leader 开仓前当时的 **Account Total Value（Spot + Perpetual）** 作分母，将该生命周期的数量、手续费、funding、浮亏和最终盈亏按同一比例缩放；模拟仓位低于 **10 USDC** 时按机器人规则立即平仓并结束生命周期，后续从 leader 灰尘仓位重新加到可交易规模时按当时本金视为新开仓；不使用各 leader 当前配置的余额或 multiplier。",
        "",
        "## 结论摘要",
        "",
        "这份报告的目标不是评价谁赚得最多，而是回答三个风险问题：如果同样投入 20,000，单个可实际复制的生命周期最坏能扛到多亏、亏损会扛多久，以及亏损时是否继续加仓。leader 自身本金差异已经被消除；leader 未精确归零的灰尘不会把多个独立交易错误串成长生命周期。",
        "",
        "### 报告截止时本金分项核对",
        "",
        "| Leader | Spot | Perpetual | Account Total | 恒等式 |",
        "|---|---:|---:|---:|---|",
    ]
    for summary in summaries:
        lines.append(
            f"| {summary['suffix']} | {fmt_usd(summary['current_spot_account_value'], 2)} | "
            f"{fmt_usd(summary['current_perp_account_value'], 2)} | "
            f"{fmt_usd(summary['current_total_account_value'], 2)} | "
            "Spot + Perpetual = Total |"
        )
    lines += [
        "",
        "### 风险排序（低风险在前）",
        "",
        "| 排名 | Leader | 筛查分 | 最深单生命周期亏损 | 组合最大回撤 | 最长连续扛亏 | 浮亏加仓率 |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for index, summary in enumerate(ranked, 1):
        worst = summary["worst"][0] if summary["worst"] else None
        longest = summary["longest"][0] if summary["longest"] else None
        lines.append(
            f"| {index} | {summary['suffix']} | {risk_score(summary):.1f}/100 | "
            f"{fmt_usd(worst.metrics['worst_total'] if worst else None, 0)} | "
            f"{fmt_usd(summary['extreme_max_drawdown'][0], 0)} | "
            f"{duration_label(longest.metrics['longest_underwater_hours'] if longest else None)} | "
            f"{fmt_pct(summary.get('underwater_add_rate'), 1)} |"
        )
    lines += [
        "",
        "> 筛查分只用于统一排序，不是收益预测。权重为：最深单生命周期亏损 35%、组合最大回撤 25%、最长扛亏 20%、浮亏加仓率 10%、亏损持有 P95 10%。原始指标比总分更重要。",
        "",
        "### 按你最在意的维度拆开看（风险从低到高）",
        "",
        f"- 最深单生命周期亏损：{' → '.join(summary['suffix'] for summary in worst_amount_rank)}。",
        f"- 组合压力最大回撤：{' → '.join(summary['suffix'] for summary in drawdown_rank)}。",
        f"- 最长连续扛亏：{' → '.join(summary['suffix'] for summary in duration_rank)}。",
        "",
        "## 总表",
        "",
        "| Leader | 可用历史 | Perp fills | 完整生命周期 | 最差已平亏损 | 最深持仓期谷底 | 亏损持有 P50 / P95 | 亏损单 >3天 | 单笔亏损集中度 Top1 / Top3 | 峰值同时名义仓位 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for summary in summaries:
        worst_closed = summary["worst_closed"][0] if summary["worst_closed"] else None
        worst = summary["worst"][0] if summary["worst"] else None
        lines.append(
            f"| {summary['suffix']} | {iso(summary['fill_start_ms'])} → {iso(summary['fill_end_ms'])}"
            f"{'（官方 10k 历史边界）' if summary['fill_limit_saturated'] else ''} | {summary['perp_fill_count']:,} | "
            f"{summary['complete_count']:,} | {fmt_usd(worst_closed.metrics['final_net'] if worst_closed else None, 0)} | "
            f"{fmt_usd(worst.metrics['worst_total'] if worst else None, 0)} | "
            f"{duration_label(summary['median_losing_hold'])} / {duration_label(summary['p95_losing_hold'])} | "
            f"{fmt_pct(summary['loss_over_72h_pct'], 1)} | {fmt_pct(summary['top1_loss_concentration'], 1)} / "
            f"{fmt_pct(summary['top3_loss_concentration'], 1)} | {fmt_usd(summary['peak_notional'], 0)} |"
        )

    for summary in summaries:
        lines += [
            "",
            f"## {summary['suffix']} — {summary['address']}",
            "",
            "### 数据覆盖与完整度",
            "",
            f"- 官方 fill 覆盖：**{iso(summary['fill_start_ms'])} → {iso(summary['fill_end_ms'])} UTC**。",
            f"- API 返回 fill：{summary['fill_count_all']:,} 条；其中永续合约 fragment：{summary['perp_fill_count']:,} 条，合并为 {summary['logical_fill_count']:,} 个逻辑事件；涉及 {summary['coins']} 个市场。",
            f"- 按 follower 10U 经济归零规则重建可跟生命周期：{summary['lifecycle_count']:,} 个；完整开平：{summary['complete_count']:,} 个；左截断：{summary['left_censored_count']:,} 个；尚未结束/右截断：{summary['right_censored_count']:,} 个。",
            f"- 仓位链完整性：{summary['position_mismatch_count']:,} 个无法由相邻 `startPosition` 完全衔接的跳变，分布于 {summary['position_mismatch_life_count']:,} 个生命周期。",
            f"- Account Total（Spot + Perpetual）本金采样：{summary['capital_sample_count']:,} 个，覆盖 {iso(summary['capital_sample_start_ms'])} → {iso(summary['capital_sample_end_ms'])} UTC；当前总值分项恒等式核对误差 {fmt_usd(summary['capital_reconciliation_error'], 2)}。",
            f"- 生命周期开仓权益：最小 {fmt_usd(summary['opening_equity_min'], 0)}，中位 {fmt_usd(summary['opening_equity_median'], 0)}，最大 {fmt_usd(summary['opening_equity_max'], 0)}；对应 20k 缩放范围 {dec(summary['opening_scale_min']):.3f}x → {dec(summary['opening_scale_max']):.3f}x。",
            f"- 无法取得正数开仓权益而排除的生命周期：{summary['normalization_unavailable_count']:,} 个；开仓距最近官方权益采样最远 {float(summary['opening_equity_sample_gap_max_days'] or 0):.1f} 天。",
            (
                "- 官方文档的 10,000 fill 历史边界：**已达到/超过**。分页目前实际返回了更多记录，"
                "但最早历史是否绝对完整仍不作保证。"
                if summary["fill_limit_saturated"]
                else "- 官方文档的 10,000 fill 历史边界：**未达到**。结合起点仓位与左截断检查，当前可见历史完整性较高。"
            ),
            "",
            "### 关键风险指标（统一投入 20,000）",
            "",
            f"- 完整生命周期胜/负：{summary['wins']} / {summary['losses']}；胜率 {fmt_pct(summary['win_rate'], 1)}（仅作样本描述，不用于风险结论）。",
            f"- 亏损生命周期持有时间：P50 {duration_label(summary['median_losing_hold'])}，P90 {duration_label(summary['p90_losing_hold'])}，P95 {duration_label(summary['p95_losing_hold'])}，最大 {duration_label(summary['max_losing_hold'])}。",
            f"- 亏损单持有超过 24h / 72h / 7d：{fmt_pct(summary['loss_over_24h_pct'], 1)} / {fmt_pct(summary['loss_over_72h_pct'], 1)} / {fmt_pct(summary['loss_over_7d_pct'], 1)}。",
            f"- 浮亏加仓：{summary['underwater_adds']:,}/{summary['all_adds']:,} 次（{fmt_pct(summary['underwater_add_rate'], 1)}）；出现浮亏加仓的生命周期占 {fmt_pct(summary['underwater_add_life_pct'], 1)}。",
            f"- 逐笔+4h 收盘组合曲线最大回撤：{fmt_usd(summary['regular_max_drawdown'][0], 0)}；加入完整 4h K 线极端价后的压力回撤：{fmt_usd(summary['extreme_max_drawdown'][0], 0)}。",
            f"- 压力回撤区间：{iso(summary['extreme_max_drawdown'][1])} → {iso(summary['extreme_max_drawdown'][2])}；最长未创新高约 {duration_label(summary['regular_max_drawdown'][3])}。",
            f"- 峰值同时名义仓位：{fmt_usd(summary['peak_notional'], 0)}，当时约 {summary['peak_positions']} 个并存仓位。",
            f"- 已平亏损集中度：最大一笔占总亏损 {fmt_pct(summary['top1_loss_concentration'], 1)}，前三笔占 {fmt_pct(summary['top3_loss_concentration'], 1)}。",
        ]
        portfolio = summary.get("portfolio") or {}
        if portfolio:
            lines += [
                f"- 官方 `perpAllTime.pnlHistory`、总账户权益分母交叉验证：{iso(portfolio['start_ms'])} → {iso(portfolio['end_ms'])}，{portfolio['point_count']} 个无划转区间；20k 归一最大回撤 {fmt_usd(portfolio['max_drawdown'], 0)}，最差单采样周期 {fmt_usd(portfolio['worst_interval'], 0)}。",
            ]
        lines += ["", "### 风险解读", "", *risk_interpretation(summary)]

        lines += [
            "",
            "### 最深的 10 个持仓期亏损",
            "",
            "| 市场 | 方向 | 开始时间 UTC | 持有 | Leader开仓权益 | 20k缩放 | 峰值名义仓位 | 最大浮亏 | 生命周期谷底 | 最长连续亏损 | 浮亏加仓/全部加仓 | 最终净值 | 完整性 |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
        for life in summary["worst"][:10]:
            lines.append(life_row(life))

        lines += [
            "",
            "### 最长的 10 个连续扛亏生命周期",
            "",
            "| 市场 | 方向 | 开始时间 UTC | 持有 | Leader开仓权益 | 20k缩放 | 峰值名义仓位 | 最大浮亏 | 生命周期谷底 | 最长连续亏损 | 浮亏加仓/全部加仓 | 最终净值 | 完整性 |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
        for life in summary["longest"][:10]:
            lines.append(life_row(life))

        lines += [
            "",
            "### 最差的 10 个已平亏损生命周期",
            "",
            "| 市场 | 方向 | 开始时间 UTC | 持有 | Leader开仓权益 | 20k缩放 | 峰值名义仓位 | 最大浮亏 | 生命周期谷底 | 最长连续亏损 | 浮亏加仓/全部加仓 | 最终净值 | 完整性 |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
        for life in summary["worst_closed"][:10]:
            lines.append(life_row(life))

        if summary["current_open"]:
            lines += [
                "",
                "### 当前仍未结束的可见生命周期",
                "",
                "| 市场 | 方向 | 开始时间 UTC | 持有 | Leader开仓权益 | 20k缩放 | 峰值名义仓位 | 最大浮亏 | 生命周期谷底 | 最长连续亏损 | 浮亏加仓/全部加仓 | 当前估值 | 完整性 |",
                "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
            for life in sorted(summary["current_open"], key=lambda item: dec(item.metrics["worst_total"])):
                lines.append(life_row(life))

    lines += [
        "",
        "## 方法与口径",
        "",
        "1. 从 Hyperliquid 官方 `userFillsByTime` 拉取非聚合成交；先按 coin、order id、时间、方向与 side 合并同一订单在同一时刻的 exchange fragments，再使用权威 `startPosition` 重建仓位链。资本估值使用 leader 精确归零链；风险统计另按机器人的实际可跟语义重建：模拟 follower 剩余名义仓位低于 10U 时在该次减仓价格全平，leader 灰尘期间保持空仓，后续从灰尘增加到至少10U时按当时Account Total作为全新生命周期。反手同样拆为旧方向关闭和新方向开启。",
        "2. Spot fill（`@index`、`TOKEN/USDC` 及没有 Long/Short 方向语义的成交）全部排除，只研究机器人会复制的永续市场。",
        "3. 本金严格使用 Hyperliquid 页面显示的 Account Total Value 口径：同一时点的 Spot `allTime/month/week/day.accountValueHistory` 加上 Perpetual `perpAllTime/perpMonth/perpWeek/perpDay.accountValueHistory`。绝不再把 Spot 分项单独当作总本金。近期 day/week/month 采样覆盖 allTime 的粗采样。",
        "4. `userNonFundingLedgerUpdates` 用于把入金、出金、外部转账和奖励领取定位到精确时间；账户内部 spot/perp/DEX 转移不改变 Account Total。每次生成还会校验最新 `Account Total = Spot + Perpetual`。",
        "5. 每个生命周期使用开仓前一毫秒估算的 leader 总账户权益，固定缩放系数为 `20,000 / leader_equity_at_open`。后续加仓、减仓和平仓沿用该系数，只看仓位比例；不会因同一笔浮亏降低了 leader 权益而把该亏损二次放大。",
        "6. fill 自带的 `closedPnl` 与 `fee` 逐笔计入；`userFunding` 按 coin 和持仓时间归入对应生命周期。maker rebate 若为负 fee，会自然增加净值。",
        "7. 持仓期间估值使用官方 4h K 线。完整落在同一持仓状态内的 K 线使用 high/low 计算最大不利波动，使用 close 计算连续扛亏时间；生命周期边界所在的不完整 K 线只使用真实 fill 价格，避免把开仓前或平仓后的极端价算进去。",
        "8. “连续扛亏”定义为20k归一生命周期净值低于 -1 USDC，直到重新回到不亏或生命周期结束。开仓手续费造成的不足 1 USDC 小波动不计。",
        "9. “组合最大回撤”把同一 leader 的所有等本金生命周期按时间合并；已结束生命周期保留最终已实现净值，未结束生命周期使用当时 mark-to-market。压力回撤额外纳入完整 4h K 线内的 adverse high/low。",
        "",
        "## 限制与应如何解读",
        "",
        "- Hyperliquid 官方文档说明历史查询只保证最近 10,000 个 fill。本次分页对部分地址实际返回超过 10,000 条，报告仍采取保守口径：标记“官方 10k 历史边界”的地址，其“最大”只表示**本次可取得历史内最大**，不能证明更早没有更严重事件。",
        "- 4h K 线最多 5,000 根，约覆盖 833 天。本报告四个地址的可见成交均落在这一范围内。小于一个完整 4h 区间的持仓主要依靠真实 fill 价格，因此可能漏掉两次 fill 之间的秒级/分钟级插针。",
        "- 旧历史的 Account Total 分项主要是周级采样。估值器用逐笔永续 PnL 和精确资金流水恢复高频变化，无法由永续成交解释的 spot PnL/资产价格变化在相邻官方采样之间线性插值。因此归一金额是高质量历史估算，不应理解为分毫不差的交易所逐秒权益快照。",
        "- 若要换算成其他统一测试本金，可把报告中的金额和峰值仓位乘以 `目标本金 / 20,000`；扛亏时间、加仓比例和风险百分比不变。",
        "- 报告研究的是 leader 自身的亏损管理风格，没有加入你实际跟单的延迟滑点。费用已按 leader 成交名义金额同比例缩放；真实跟单仍可能因滑点比这里更差。",
        "- 左截断生命周期的真实开仓时间、原始均价可能不可见。这类记录保留用于压力提示，但完整生命周期统计优先使用开仓和平仓都可见的样本。",
        f"- 同毫秒高并发成交按 `startPosition → afterPosition` 因果链重新排序。本次 {total_lifecycles:,} 个可用生命周期剩余 {total_position_mismatches:,} 个公开数据无法衔接跳变；最坏样本会单独核对 mismatch。",
        "- 风险排序不能替代实时风控。它更适合回答“谁更可能逆势加仓、谁的亏损尾部更长”，不应用来保证未来最大亏损不会突破历史值。",
        f"- 正文展示各类 Top 10；全部生命周期的逐单计算保存在 `{lifecycle_detail_filename}`，可按 leader、市场、开始时间或亏损排序复核。",
        "",
        "## 数据来源",
        "",
        "- Hyperliquid 官方 Info API：`userFillsByTime`、`userFunding`、`userNonFundingLedgerUpdates`、`portfolio`、`candleSnapshot`。",
        "- 本地机器人数据库仅用于确认当前研究对象；没有读取私钥、Token、API secret 或 follower 签名材料。",
        "",
    ]
    return "\n".join(lines)


def write_lifecycle_details(path: Path, summaries: list[dict[str, Any]]) -> None:
    fields = [
        "leader",
        "address",
        "lifecycle_id",
        "market",
        "direction",
        "start_utc",
        "end_utc",
        "complete_start",
        "complete_end",
        "right_censored",
        "logical_fill_count",
        "position_mismatch_count",
        "leader_equity_at_open_usdc",
        "normalization_scale_to_20000",
        "nearest_equity_sample_gap_days",
        "hold_hours",
        "peak_notional_usdc",
        "worst_unrealized_usdc",
        "lifecycle_trough_usdc",
        "trough_time_utc",
        "longest_underwater_hours",
        "total_underwater_hours",
        "add_count",
        "underwater_add_count",
        "final_net_usdc",
        "funding_usdc",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for summary in summaries:
            for life in summary["lifecycles"]:
                metrics = life.metrics
                writer.writerow(
                    {
                        "leader": summary["suffix"],
                        "address": summary["address"],
                        "lifecycle_id": life.life_id,
                        "market": life.coin,
                        "direction": life.side_label,
                        "start_utc": iso(life.start_ms),
                        "end_utc": iso(life.end_ms),
                        "complete_start": life.complete_start,
                        "complete_end": life.complete_end,
                        "right_censored": life.right_censored,
                        "logical_fill_count": life.fill_count,
                        "position_mismatch_count": life.position_mismatch_count,
                        "leader_equity_at_open_usdc": q(dec(life.equity_at_open)),
                        "normalization_scale_to_20000": q8(dec(life.scale)),
                        "nearest_equity_sample_gap_days": q(dec(life.equity_sample_gap_days)),
                        "hold_hours": q(dec(metrics.get("hold_hours"))),
                        "peak_notional_usdc": q(dec(metrics.get("max_notional"))),
                        "worst_unrealized_usdc": q(dec(metrics.get("worst_unrealized"))),
                        "lifecycle_trough_usdc": q(dec(metrics.get("worst_total"))),
                        "trough_time_utc": iso(metrics.get("worst_total_time_ms")),
                        "longest_underwater_hours": q(dec(metrics.get("longest_underwater_hours"))),
                        "total_underwater_hours": q(dec(metrics.get("total_underwater_hours"))),
                        "add_count": metrics.get("add_count", 0),
                        "underwater_add_count": metrics.get("underwater_add_count", 0),
                        "final_net_usdc": q(dec(metrics.get("final_net"))),
                        "funding_usdc": q(dec(metrics.get("funding"))),
                    }
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--leader",
        action="append",
        required=True,
        metavar="LABEL=PUBLIC_ADDRESS",
        help="public leader address; repeat for multiple leaders",
    )
    parser.add_argument("--output", default="docs/leader_loss_risk_report.md")
    parser.add_argument("--details", default="docs/leader_loss_risk_lifecycle_details.csv")
    parser.add_argument("--cache", default="/tmp/copytrade_leader_loss_risk")
    parser.add_argument("--end-ms", type=int, help="fixed report cutoff for a reproducible cached run")
    args = parser.parse_args()
    leaders: dict[str, str] = {}
    for value in args.leader:
        if "=" not in value:
            parser.error("--leader must use LABEL=PUBLIC_ADDRESS")
        label, address = (part.strip() for part in value.split("=", 1))
        if not label or not address.startswith("0x") or len(address) != 42:
            parser.error("--leader requires a label and a 42-character public address")
        if label in leaders:
            parser.error(f"duplicate leader label: {label}")
        leaders[label] = address.lower()
    now_ms = args.end_ms or int(time.time() * 1000)
    client = PublicInfoClient(Path(args.cache))
    summaries = []
    for suffix, address in leaders.items():
        print(f"[{suffix}] fetching public fills/portfolio/funding/ledger", flush=True)
        fills, saturated = fetch_fills(client, suffix, address, now_ms)
        events = logical_fills(fills)
        portfolio = fetch_portfolio(client, suffix, address)
        funding = fetch_funding(client, suffix, address, now_ms)
        ledger = fetch_ledger(client, suffix, address, now_ms)
        exact_lifecycles = build_lifecycles(suffix, events, now_ms)
        assign_equity_and_funding(exact_lifecycles, funding)
        by_coin: dict[str, list[Lifecycle]] = defaultdict(list)
        for life in exact_lifecycles:
            by_coin[life.coin].append(life)
        print(
            f"[{suffix}] {len(fills)} fragments, {len(events)} logical fills, "
            f"{len(exact_lifecycles)} exact-flat lifecycles, {len(by_coin)} perp markets",
            flush=True,
        )
        candles_by_coin: dict[str, list[dict[str, Any]]] = {}
        for coin_index, (coin, coin_lives) in enumerate(sorted(by_coin.items()), 1):
            start_ms = max(0, min(life.start_ms for life in coin_lives) - FOUR_HOURS_MS)
            candles_by_coin[coin] = fetch_candles(client, suffix, coin, start_ms, now_ms)
            if coin_index % 20 == 0:
                print(f"[{suffix}] candles {coin_index}/{len(by_coin)}", flush=True)
        for life in exact_lifecycles:
            lifecycle_valuations(life, candles_by_coin.get(life.coin, []))
        raw_perp_curve = aggregate_lifecycle_curve(exact_lifecycles, include_extremes=False)
        normalizer = CapitalNormalizer(
            address=address,
            portfolio=portfolio,
            ledger=ledger,
            raw_perp_curve=raw_perp_curve,
        )
        lifecycles = build_followable_lifecycles(
            suffix,
            events,
            now_ms,
            normalizer,
            follower_base=FOLLOWER_BASE,
            min_order_value=Decimal("10"),
        )
        unassigned_count, unassigned_value = assign_equity_and_funding(lifecycles, funding)
        normalization_unavailable = apply_equity_normalization(lifecycles, normalizer)
        for life in lifecycles:
            if life.scale is not None:
                lifecycle_valuations(life, candles_by_coin.get(life.coin, []))
        summaries.append(
            leader_summary(
                suffix,
                address,
                fills,
                saturated,
                lifecycles,
                portfolio,
                normalizer,
                normalization_unavailable,
                unassigned_count,
                unassigned_value,
            )
        )
    output = Path(args.output)
    details = Path(args.details)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_lifecycle_details(details, summaries)
    output.write_text(render_report(summaries, now_ms, details.name), encoding="utf-8")
    print(f"report written to {output}")
    print(f"lifecycle details written to {details}")
    for summary in sorted(summaries, key=risk_score):
        worst = summary["worst"][0] if summary["worst"] else None
        print(
            f"{summary['suffix']}: score={risk_score(summary):.1f}, "
            f"worst={q(dec(worst.metrics['worst_total'])) if worst else '--'}, "
            f"maxdd={q(dec(summary['extreme_max_drawdown'][0]))}",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
