from __future__ import annotations

from dataclasses import dataclass

from app.services.execution_router import FallbackVenue, VenuePreference


@dataclass(frozen=True)
class VenuePolicy:
    preferred_venue: VenuePreference = VenuePreference.HYPERLIQUID
    fallback_venue: FallbackVenue = FallbackVenue.NONE


def default_venue_policy() -> VenuePolicy:
    return VenuePolicy()


def venue_live_allowed(
    *,
    global_trading_enabled: bool,
    venue_trading_enabled: bool,
    kill_switch_active: bool,
    venue_ready: bool,
) -> bool:
    return (
        global_trading_enabled
        and venue_trading_enabled
        and not kill_switch_active
        and venue_ready
    )
