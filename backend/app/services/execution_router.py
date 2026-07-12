from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from app.services.target_position import PositionSide


class ExecutionVenue(str, Enum):
    HYPERLIQUID = "HYPERLIQUID"
    BINANCE = "BINANCE"


class VenuePreference(str, Enum):
    HYPERLIQUID = "HYPERLIQUID"
    BINANCE = "BINANCE"
    AUTO = "AUTO"


class FallbackVenue(str, Enum):
    NONE = "NONE"
    BINANCE = "BINANCE"
    HYPERLIQUID = "HYPERLIQUID"


class VenueRouteStatus(str, Enum):
    OK = "OK"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class VenueAvailability:
    available: bool
    reason: str
    venue_symbol: str | None = None


@dataclass(frozen=True)
class VenueRouteResult:
    status: VenueRouteStatus
    execution_venue: ExecutionVenue | None
    venue_symbol: str | None
    reason: str
    warnings: list[str] = field(default_factory=list)


class ExecutionRouter:
    def route_order(
        self,
        *,
        leader_id: int,
        hyperliquid_coin: str,
        preferred_venue: VenuePreference,
        fallback_venue: FallbackVenue,
        hyperliquid: VenueAvailability,
        binance: VenueAvailability,
        warnings: list[str] | None = None,
    ) -> VenueRouteResult:
        warnings = warnings or []
        coin = hyperliquid_coin.upper()

        if preferred_venue in {VenuePreference.HYPERLIQUID, VenuePreference.AUTO}:
            if hyperliquid.available:
                return VenueRouteResult(
                    status=VenueRouteStatus.OK,
                    execution_venue=ExecutionVenue.HYPERLIQUID,
                    venue_symbol=hyperliquid.venue_symbol or coin,
                    reason="HYPERLIQUID_PRIMARY",
                    warnings=warnings,
                )
            if fallback_venue == FallbackVenue.BINANCE and binance.available:
                return VenueRouteResult(
                    status=VenueRouteStatus.OK,
                    execution_venue=ExecutionVenue.BINANCE,
                    venue_symbol=binance.venue_symbol,
                    reason="BINANCE_FALLBACK",
                    warnings=warnings,
                )

        if preferred_venue == VenuePreference.BINANCE:
            if binance.available:
                return VenueRouteResult(
                    status=VenueRouteStatus.OK,
                    execution_venue=ExecutionVenue.BINANCE,
                    venue_symbol=binance.venue_symbol,
                    reason="BINANCE_PRIMARY",
                    warnings=warnings,
                )
            if fallback_venue == FallbackVenue.HYPERLIQUID and hyperliquid.available:
                return VenueRouteResult(
                    status=VenueRouteStatus.OK,
                    execution_venue=ExecutionVenue.HYPERLIQUID,
                    venue_symbol=hyperliquid.venue_symbol or coin,
                    reason="HYPERLIQUID_FALLBACK",
                    warnings=warnings,
                )

        return VenueRouteResult(
            status=VenueRouteStatus.BLOCKED,
            execution_venue=None,
            venue_symbol=None,
            reason=f"both venues unavailable: hyperliquid={hyperliquid.reason}; binance={binance.reason}",
            warnings=warnings,
        )

    def validate_hyperliquid_netting_constraint(
        self,
        allocations: list[object],
        *,
        venue_account: str,
        coin: str,
    ) -> VenueRouteResult:
        active_sides = set()
        for item in allocations:
            venue = getattr(item, "execution_venue", None)
            venue_value = venue.value if isinstance(venue, ExecutionVenue) else str(venue or "").upper()
            if venue_value != ExecutionVenue.HYPERLIQUID.value:
                continue
            if getattr(item, "venue_account", None) != venue_account:
                continue
            if getattr(item, "hyperliquid_coin", "").upper() != coin.upper():
                continue
            if getattr(item, "allocated_qty", Decimal("0")) <= 0:
                continue
            if str(getattr(item, "status", "")).upper() == "CLOSED":
                continue
            raw_side = getattr(item, "position_side", "")
            side_value = raw_side.value if isinstance(raw_side, PositionSide) else str(raw_side).upper()
            try:
                active_sides.add(PositionSide(side_value))
            except ValueError:
                continue
        if PositionSide.LONG in active_sides and PositionSide.SHORT in active_sides:
            return VenueRouteResult(
                status=VenueRouteStatus.BLOCKED,
                execution_venue=ExecutionVenue.HYPERLIQUID,
                venue_symbol=coin.upper(),
                reason="opposite directions on same Hyperliquid follower account require separate account/vault",
            )
        return VenueRouteResult(
            status=VenueRouteStatus.OK,
            execution_venue=ExecutionVenue.HYPERLIQUID,
            venue_symbol=coin.upper(),
            reason="OK",
        )

    def venue_readiness(self, *, hyperliquid_ready: bool, binance_ready: bool) -> dict[str, bool]:
        return {
            "ready_for_live_hyperliquid": hyperliquid_ready,
            "ready_for_live_binance": binance_ready,
        }

    def manual_order_source(self, venue: ExecutionVenue) -> dict[str, str]:
        return {"execution_venue": venue.value, "source_type": "MANUAL"}
