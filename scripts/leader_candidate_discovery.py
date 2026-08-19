#!/usr/bin/env python3
"""Discover copy-trading candidates without exposing wallet identifiers.

The discovery funnel is intentionally separate from the live bot.  It reads
public Hyperdash and Hyperliquid data, never imports signer settings, and never
changes leader configuration.  Complete addresses are written only to a
caller-owned directory with mode 0700/0600.  Console and public reports use an
address suffix, with a short digest added only when suffixes collide.

This is a broad first pass, not the final suitability decision.  The output is
designed to feed ``leader_suitability_evaluator.py`` for candle- and
capital-normalized analysis of a small shortlist.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


HYPERDASH_GRAPHQL = "https://api.hyperdash.com/graphql"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"
DAY_MS = 86_400_000
ZERO = Decimal("0")
TEN_THOUSAND = Decimal("10000")
DEFAULT_FRICTION_BPS = Decimal("5")

EXPLORE_QUERY = """
query ExploreTraders(
  $page: Int
  $pageSize: Int
  $timeframe: TraderTimeframe!
  $sortBy: TraderSortInput
  $filters: TraderFilterInput
) {
  exploreTraders(
    page: $page
    pageSize: $pageSize
    timeframe: $timeframe
    sortBy: $sortBy
    filters: $filters
  ) {
    data {
      address
      tag
      pnl
      perpsEquity
      winrate
      sharpe
      drawdown
      copyScore
      totalTrades
      portfolioGraph { timestamp value }
    }
    pagination { page pageSize totalItems totalPages }
  }
}
"""


def dec(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return result if result.is_finite() else default


def clamp(value: Decimal, low: Decimal = ZERO, high: Decimal = Decimal("1")) -> Decimal:
    return max(low, min(high, value))


def address_is_valid(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return (
        len(text) == 42
        and text.startswith("0x")
        and all(character in "0123456789abcdef" for character in text[2:])
    )


def private_label(address: str) -> str:
    return address.lower()[-4:]


def assign_unique_labels(candidates: list[Candidate]) -> None:
    by_suffix: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        by_suffix.setdefault(private_label(candidate.address), []).append(candidate)
    for suffix, matches in by_suffix.items():
        if len(matches) == 1:
            matches[0].label = suffix
            continue
        for candidate in matches:
            digest = hashlib.sha256(candidate.address.encode("ascii")).hexdigest()[:4]
            candidate.label = f"{suffix}-{digest}"


def graph_span_days(graph: Any) -> Decimal:
    timestamps = [
        int(item.get("timestamp") or 0)
        for item in graph or []
        if isinstance(item, dict) and item.get("timestamp")
    ]
    if len(timestamps) < 2:
        return ZERO
    divisor = 1000 if max(timestamps) > 10**12 else 1
    return dec(max(timestamps) - min(timestamps)) / dec(divisor * 86_400)


def fill_key(fill: dict[str, Any]) -> str:
    return "|".join(
        str(fill.get(key) or "")
        for key in ("hash", "tid", "oid", "time", "coin", "px", "sz", "side")
    )


def is_perp_fill(fill: dict[str, Any]) -> bool:
    coin = str(fill.get("coin") or "")
    direction = str(fill.get("dir") or "").lower()
    return bool(coin and not coin.startswith("@") and "/" not in coin) and (
        "long" in direction or "short" in direction
    )


class JsonPostClient:
    def __init__(
        self,
        *,
        url: str,
        cache_dir: Path,
        pause_seconds: float,
        user_agent: str,
    ) -> None:
        self.url = url
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.cache_dir.chmod(0o700)
        self.pause_seconds = pause_seconds
        self.user_agent = user_agent
        self._last_request = 0.0

    def post(self, payload: dict[str, Any], cache_key: str) -> Any:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        cache_path = self.cache_dir / f"{digest}.json"
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.pause_seconds:
            time.sleep(self.pause_seconds - elapsed)
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": self.user_agent,
            },
            method="POST",
        )
        error: Exception | None = None
        for attempt in range(6):
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    result = json.loads(response.read().decode("utf-8"))
                self._last_request = time.monotonic()
                cache_path.write_text(
                    json.dumps(result, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8",
                )
                cache_path.chmod(0o600)
                return result
            except urllib.error.HTTPError as exc:
                error = exc
                retryable = exc.code in {408, 425, 429} or 500 <= exc.code < 600
                if not retryable:
                    break
                retry_after = dec(exc.headers.get("Retry-After"))
                time.sleep(float(max(retry_after, min(20, 1 * (2**attempt)))))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                error = exc
                time.sleep(min(20.0, 1.0 * (2**attempt)))
        detail = (
            f"HTTP {error.code}"
            if isinstance(error, urllib.error.HTTPError)
            else type(error).__name__
        )
        raise RuntimeError(f"public data request failed ({detail})")


@dataclass
class Candidate:
    address: str
    label: str
    tag: str = "unclassified"
    sources: set[str] = field(default_factory=set)
    views: dict[str, dict[str, Any]] = field(default_factory=dict)
    preliminary_score: Decimal = ZERO
    quick_score: Decimal | None = None
    quick_metrics: dict[str, Any] = field(default_factory=dict)
    quick_rejections: list[str] = field(default_factory=list)


def current_or_historical_leaders() -> set[str]:
    """Read only public leader identifiers from the local database.

    Deleted rows remain excluded so discovery does not repeatedly return a
    previously evaluated or removed leader.  Nothing is printed if Docker or
    the database is unavailable.
    """

    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "copytrade",
        "-d",
        "copytrade",
        "-At",
        "-c",
        "SELECT DISTINCT lower(leader_address) FROM leader_configs",
    ]
    try:
        output = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=20,
        ).stdout
    except (FileNotFoundError, subprocess.SubprocessError):
        return set()
    return {line.strip().lower() for line in output.splitlines() if address_is_valid(line)}


def equity_bands() -> list[tuple[int, int | None]]:
    return [
        (10_000, 50_000),
        (50_000, 200_000),
        (200_000, 1_000_000),
        (1_000_000, 5_000_000),
        (5_000_000, None),
    ]


def collect_hyperdash(
    client: JsonPostClient,
    *,
    excluded: set[str],
    page_size: int,
) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    searches = (
        ("thirty_days", "pnl"),
        ("all", "pnl"),
        ("all", "copyScore"),
    )
    completed = 0
    total = len(equity_bands()) * len(searches)
    for minimum, maximum in equity_bands():
        filters: dict[str, Any] = {
            "minPerpsEquity": minimum,
            "minPnl30d": 100,
            "minPnlAllTime": 1000,
        }
        if maximum is not None:
            filters["maxPerpsEquity"] = maximum
        for timeframe, sort_field in searches:
            variables = {
                "page": 1,
                "pageSize": page_size,
                "timeframe": timeframe,
                "sortBy": {"field": sort_field, "order": "desc"},
                "filters": filters,
            }
            payload = client.post(
                {"query": EXPLORE_QUERY, "variables": variables},
                json.dumps(variables, sort_keys=True, separators=(",", ":")),
            )
            if payload.get("errors"):
                raise RuntimeError("Hyperdash returned a GraphQL error")
            result = ((payload.get("data") or {}).get("exploreTraders") or {})
            source = f"{timeframe}:{sort_field}:{minimum}:{maximum or 'up'}"
            for row in result.get("data") or []:
                address = str(row.get("address") or "").strip().lower()
                if not address_is_valid(address) or address in excluded:
                    continue
                candidate = candidates.setdefault(
                    address,
                    Candidate(address=address, label=private_label(address)),
                )
                candidate.sources.add(source)
                candidate.tag = str(row.get("tag") or candidate.tag or "unclassified")
                # Multiple searches can return the same timeframe.  They carry
                # identical metrics, so keep the latest copy without addresses
                # in any report.
                candidate.views[timeframe] = {
                    "pnl": row.get("pnl"),
                    "perps_equity": row.get("perpsEquity"),
                    "winrate": row.get("winrate"),
                    "sharpe": row.get("sharpe"),
                    "drawdown": row.get("drawdown"),
                    "copy_score": row.get("copyScore"),
                    "total_trades": row.get("totalTrades"),
                    "history_span_days": str(graph_span_days(row.get("portfolioGraph"))),
                }
            completed += 1
            print(
                f"Hyperdash strata {completed}/{total}; unique candidates {len(candidates)}",
                file=sys.stderr,
                flush=True,
            )
    return candidates


def view(candidate: Candidate, timeframe: str) -> dict[str, Any]:
    return candidate.views.get(timeframe) or {}


def preliminary_score(candidate: Candidate) -> Decimal:
    recent = view(candidate, "thirty_days")
    lifetime = view(candidate, "all")
    common = recent or lifetime
    history = dec(lifetime.get("history_span_days"))
    copy_score = dec(common.get("copy_score"))
    recent_drawdown = max(ZERO, dec(recent.get("drawdown"), Decimal("50")))
    all_drawdown = max(ZERO, dec(lifetime.get("drawdown"), Decimal("50")))
    sharpe = dec(recent.get("sharpe", lifetime.get("sharpe")))
    equity = max(dec(common.get("perps_equity")), Decimal("1"))
    recent_roi_proxy = dec(recent.get("pnl")) / equity
    all_roi_proxy = dec(lifetime.get("pnl")) / equity
    trades = max(dec(common.get("total_trades")), ZERO)

    score = (
        Decimal("18") * clamp(history / Decimal("180"))
        + Decimal("20") * clamp(copy_score / Decimal("80"))
        + Decimal("12") * clamp((Decimal("20") - recent_drawdown) / Decimal("20"))
        + Decimal("10") * clamp((Decimal("35") - all_drawdown) / Decimal("35"))
        + Decimal("12") * clamp((sharpe + Decimal("1")) / Decimal("4"))
        + Decimal("14") * clamp(recent_roi_proxy / Decimal("0.25"))
        + Decimal("9") * clamp(all_roi_proxy / Decimal("1"))
        + Decimal("5") * clamp(dec(math.log10(float(trades + Decimal("1")))) / Decimal("3"))
    )
    candidate.preliminary_score = score.quantize(Decimal("0.1"))
    return candidate.preliminary_score


def diversified_selection(candidates: list[Candidate], limit: int) -> list[Candidate]:
    ranked = sorted(candidates, key=lambda item: item.preliminary_score, reverse=True)
    if len(ranked) <= limit:
        return ranked
    # Most slots follow score.  The remainder preserve style diversity so the
    # search does not silently redefine maker/scalp or concentrated strategies
    # as undesirable.
    core_size = max(1, int(limit * 0.75))
    chosen = ranked[:core_size]
    chosen_addresses = {item.address for item in chosen}
    by_tag: dict[str, list[Candidate]] = {}
    for item in ranked[core_size:]:
        by_tag.setdefault(item.tag, []).append(item)
    while len(chosen) < limit and by_tag:
        chosen_tag_counts = Counter(item.tag for item in chosen)
        for tag in sorted(
            list(by_tag),
            key=lambda value: (chosen_tag_counts[value], value),
        ):
            bucket = by_tag[tag]
            while bucket and bucket[0].address in chosen_addresses:
                bucket.pop(0)
            if bucket and len(chosen) < limit:
                item = bucket.pop(0)
                chosen.append(item)
                chosen_addresses.add(item.address)
            if not bucket:
                by_tag.pop(tag, None)
    return chosen


def own_liquidation_clusters(fills: list[dict[str, Any]], address: str) -> int:
    timestamps = sorted(
        {
            int(fill.get("time") or 0)
            for fill in fills
            if isinstance(fill.get("liquidation"), dict)
            and str(fill["liquidation"].get("liquidatedUser") or "").lower() == address
        }
    )
    clusters: list[list[int]] = []
    for timestamp in timestamps:
        if not clusters or timestamp - clusters[-1][-1] > 30 * 60 * 1000:
            clusters.append([timestamp])
        else:
            clusters[-1].append(timestamp)
    return len(clusters)


def quick_fill_metrics(
    fills: list[dict[str, Any]],
    *,
    address: str,
    cutoff_ms: int,
    friction_bps: Decimal,
) -> dict[str, Any]:
    perps = [fill for fill in fills if is_perp_fill(fill)]
    turnover = sum(
        (abs(dec(fill.get("sz")) * dec(fill.get("px"))) for fill in perps),
        ZERO,
    )
    closed_pnl = sum((dec(fill.get("closedPnl")) for fill in perps), ZERO)
    fees = sum((dec(fill.get("fee")) for fill in perps), ZERO)
    net_before_funding = closed_pnl - fees
    friction_net = net_before_funding - turnover * friction_bps / TEN_THOUSAND
    breakeven_bps = net_before_funding / turnover * TEN_THOUSAND if turnover else ZERO
    maker_notional = sum(
        (
            abs(dec(fill.get("sz")) * dec(fill.get("px")))
            for fill in perps
            if fill.get("crossed") is False
        ),
        ZERO,
    )
    times = [int(fill.get("time") or 0) for fill in perps if fill.get("time")]
    return {
        "fill_count": len(perps),
        "market_count": len({str(fill.get("coin") or "") for fill in perps}),
        "oldest_age_days": str(dec(cutoff_ms - min(times)) / dec(DAY_MS)) if times else None,
        "newest_age_days": str(dec(cutoff_ms - max(times)) / dec(DAY_MS)) if times else None,
        "turnover": str(turnover),
        "net_before_funding": str(net_before_funding),
        "friction_net_before_funding": str(friction_net),
        "breakeven_bps_before_funding": str(breakeven_bps),
        "maker_notional_pct": str(maker_notional / turnover * Decimal("100")) if turnover else "0",
        "own_liquidation_clusters_in_samples": own_liquidation_clusters(perps, address),
    }


def quick_screen_candidate(
    candidate: Candidate,
    *,
    client: JsonPostClient,
    cutoff_ms: int,
    friction_bps: Decimal,
) -> None:
    digest = hashlib.sha256(candidate.address.encode("ascii")).hexdigest()
    unique: dict[str, dict[str, Any]] = {}
    for days in (180, 30, 1):
        start_ms = cutoff_ms - days * DAY_MS
        payload = client.post(
            {
                "type": "userFillsByTime",
                "user": candidate.address,
                "startTime": start_ms,
                "endTime": cutoff_ms,
                "aggregateByTime": False,
            },
            f"fills:{digest}:{start_ms}:{cutoff_ms}",
        )
        if not isinstance(payload, list):
            continue
        for fill in payload:
            if isinstance(fill, dict):
                unique[fill_key(fill)] = fill
    fills = list(unique.values())
    metrics = quick_fill_metrics(
        fills,
        address=candidate.address,
        cutoff_ms=cutoff_ms,
        friction_bps=friction_bps,
    )
    candidate.quick_metrics = metrics

    history = dec(view(candidate, "all").get("history_span_days"))
    newest = dec(metrics.get("newest_age_days"), Decimal("999"))
    breakeven = dec(metrics.get("breakeven_bps_before_funding"))
    score = (
        candidate.preliminary_score * Decimal("0.55")
        + Decimal("19") * clamp(history / Decimal("180"))
        + Decimal("19") * clamp((breakeven - friction_bps) / Decimal("20"))
        + Decimal("7") * clamp((Decimal("30") - newest) / Decimal("30"))
    )
    candidate.quick_score = score.quantize(Decimal("0.1"))
    rejections: list[str] = []
    if history < Decimal("90"):
        rejections.append("portfolio history under 90 days")
    if int(metrics.get("fill_count") or 0) < 20:
        rejections.append("too few sampled perp fills")
    if newest > Decimal("30"):
        rejections.append("no sampled fill in 30 days")
    candidate.quick_rejections = rejections


def jsonable_candidate(candidate: Candidate, *, include_address: bool) -> dict[str, Any]:
    result = {
        "label": candidate.label,
        "tag": candidate.tag,
        "sources": sorted(candidate.sources),
        "views": candidate.views,
        "preliminary_score": str(candidate.preliminary_score),
        "quick_score": str(candidate.quick_score) if candidate.quick_score is not None else None,
        "quick_metrics": candidate.quick_metrics,
        "quick_rejections": candidate.quick_rejections,
    }
    if include_address:
        result["address"] = candidate.address
    return result


def write_private(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def render_public_report(candidates: list[Candidate], cutoff_ms: int) -> str:
    ranked = sorted(
        candidates,
        key=lambda item: item.quick_score or item.preliminary_score,
        reverse=True,
    )
    tags = Counter(item.tag for item in candidates)
    lines = [
        "# Candidate discovery pre-screen",
        "",
        f"Cutoff: {datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
        f"Candidates screened: {len(candidates)}",
        "Complete addresses are intentionally omitted from this report.",
        "",
        "Style mix: " + ", ".join(f"{tag}={count}" for tag, count in sorted(tags.items())),
        "",
        "| Rank | Label | Style | Pre-score | Quick score | History | Sample break-even | Sample liquidation rounds (observation only) | Status |",
        "|---:|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for rank, item in enumerate(ranked, 1):
        metrics = item.quick_metrics
        status = "PASS" if not item.quick_rejections else "DROP: " + "; ".join(item.quick_rejections)
        lines.append(
            f"| {rank} | {item.label} | {item.tag} | {item.preliminary_score} | "
            f"{item.quick_score if item.quick_score is not None else '--'} | "
            f"{dec(view(item, 'all').get('history_span_days')):.0f}d | "
            f"{dec(metrics.get('breakeven_bps_before_funding')):.1f}bps | "
            f"{metrics.get('own_liquidation_clusters_in_samples', '--')} | {status} |"
        )
    return "\n".join(lines) + "\n"


def render_private_shortlist(candidates: list[Candidate], limit: int) -> str:
    ranked = [
        item
        for item in sorted(
            candidates,
            key=lambda candidate: candidate.quick_score or ZERO,
            reverse=True,
        )
        if not item.quick_rejections
    ][:limit]
    lines = [
        "# Private candidate shortlist",
        "",
        "This file contains public wallet identifiers and must stay outside Git.",
        "",
    ]
    for rank, item in enumerate(ranked, 1):
        lines.append(
            f"{rank}. {item.label} {item.address} quick={item.quick_score} style={item.tag}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick-limit", type=int, default=100)
    parser.add_argument("--full-shortlist", type=int, default=15)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--friction-bps", type=Decimal, default=DEFAULT_FRICTION_BPS)
    parser.add_argument("--end-ms", type=int)
    parser.add_argument(
        "--private-dir",
        type=Path,
        default=Path.home() / ".copytrade-leader-research",
    )
    parser.add_argument("--include-configured", action="store_true")
    args = parser.parse_args()
    if args.quick_limit <= 0 or args.full_shortlist <= 0:
        parser.error("limits must be positive")
    if not 1 <= args.page_size <= 100:
        parser.error("--page-size must be 1-100")
    if args.friction_bps < ZERO:
        parser.error("--friction-bps must be non-negative")

    cutoff_ms = args.end_ms or int(time.time() * 1000)
    run_id = datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    run_dir = args.private_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_dir.chmod(0o700)
    write_private(
        run_dir / "run.json",
        json.dumps(
            {
                "cutoff_ms": cutoff_ms,
                "friction_bps": str(args.friction_bps),
                "quick_limit": args.quick_limit,
                "full_shortlist": args.full_shortlist,
            },
            indent=2,
        ),
    )
    cache_dir = run_dir / "cache"
    hyperdash = JsonPostClient(
        url=HYPERDASH_GRAPHQL,
        cache_dir=cache_dir / "hyperdash",
        pause_seconds=0.40,
        user_agent="copytrade-candidate-research/1.0",
    )
    hyperliquid = JsonPostClient(
        url=HYPERLIQUID_INFO,
        cache_dir=cache_dir / "hyperliquid",
        pause_seconds=0.25,
        user_agent="copytrade-candidate-research/1.0",
    )
    excluded = set() if args.include_configured else current_or_historical_leaders()
    candidates = collect_hyperdash(
        hyperdash,
        excluded=excluded,
        page_size=args.page_size,
    )
    assign_unique_labels(list(candidates.values()))
    for candidate in candidates.values():
        preliminary_score(candidate)
    selected = diversified_selection(list(candidates.values()), args.quick_limit)
    print(
        f"Official quick screen: {len(selected)} candidates; configured exclusions {len(excluded)}",
        file=sys.stderr,
        flush=True,
    )
    for index, candidate in enumerate(selected, 1):
        try:
            quick_screen_candidate(
                candidate,
                client=hyperliquid,
                cutoff_ms=cutoff_ms,
                friction_bps=args.friction_bps,
            )
        except RuntimeError as exc:
            # A public-data gap for one wallet must not abort a broad research
            # run.  The label is one-way and safe to emit; the address remains
            # confined to the private result file.
            candidate.quick_score = ZERO
            candidate.quick_rejections = [str(exc)]
            print(
                f"Official quick screen {candidate.label}: {exc}; continuing",
                file=sys.stderr,
                flush=True,
            )
        if index == 1 or index % 10 == 0 or index == len(selected):
            print(
                f"Official quick screen {index}/{len(selected)}",
                file=sys.stderr,
                flush=True,
            )

    private_payload = {
        "cutoff_ms": cutoff_ms,
        "friction_bps": str(args.friction_bps),
        "configured_exclusion_count": len(excluded),
        "pool_count": len(candidates),
        "screened_count": len(selected),
        "candidates": [
            jsonable_candidate(candidate, include_address=True)
            for candidate in sorted(
                selected,
                key=lambda item: item.quick_score or ZERO,
                reverse=True,
            )
        ],
    }
    write_private(
        run_dir / "candidate_pool.private.json",
        json.dumps(private_payload, ensure_ascii=False, indent=2),
    )
    public_report = render_public_report(selected, cutoff_ms)
    write_private(run_dir / "prescreen.public.md", public_report)
    write_private(
        run_dir / "shortlist.private.md",
        render_private_shortlist(selected, args.full_shortlist),
    )
    print(public_report)
    print(f"Private research directory: {run_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
