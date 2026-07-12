from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from app.core.config import Settings
from app.models import AppSetting, LeaderConfig
from app.services.account_abstraction import AccountAbstractionService, resolve_account_value_for_sizing
from app.services.binance_client import BinanceFuturesClient
from app.services.execution_router import ExecutionVenue, VenuePreference
from app.services.follower_migration import (
    FOLLOWER_MIGRATION_READY,
    FOLLOWER_RUNTIME_IDENTITY_KEY,
)
from app.services.hyperliquid_execution import HyperliquidExecutionClient
from app.services.hyperliquid_dex import HyperliquidDexRegistry
from app.services.leader_config import ADDRESS_RE, active_leaders_statement, normalize_leader_address

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class StartupCheck:
    name: str
    status: str
    message: str

    @property
    def blocks_live(self) -> bool:
        return self.status == "BLOCKED"


@dataclass(frozen=True)
class StartupValidationResult:
    ready_for_live: bool
    checks: list[StartupCheck]

    @property
    def blocking_reasons(self) -> list[str]:
        return [f"{check.name}: {check.message}" for check in self.checks if check.blocks_live]

    @property
    def warnings(self) -> list[str]:
        return [f"{check.name}: {check.message}" for check in self.checks if check.status == "WARNING"]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready_for_live": self.ready_for_live,
            "blocking_reasons": self.blocking_reasons,
            "warnings": self.warnings,
            "checks": [
                {"name": check.name, "status": check.status, "message": check.message}
                for check in self.checks
            ],
        }


async def ensure_startup_defaults(db: Any) -> None:
    risk = await db.get(AppSetting, "risk")
    if risk is None:
        stmt = insert(AppSetting).values(key="risk", value={"kill_switch": True})
        await db.execute(stmt)
        await db.commit()


async def bootstrap_leaders_from_settings(db: Any, settings: Settings) -> dict[str, Any]:
    addresses = settings.bootstrap_leader_address_list()
    if not addresses:
        return {"configured": 0, "imported": 0, "skipped": []}

    existing_count = await db.scalar(select(func.count()).select_from(LeaderConfig))
    if existing_count:
        return {"configured": len(addresses), "imported": 0, "skipped": ["database already has leaders"]}

    skipped: list[str] = []
    imported = 0
    for raw_address in addresses:
        address = normalize_leader_address(raw_address)
        if not ADDRESS_RE.fullmatch(address):
            skipped.append(raw_address)
            continue
        preferred = settings.default_preferred_venue.upper()
        enabled_venues = ["HYPERLIQUID", "BINANCE"] if preferred == "AUTO" else [preferred]
        db.add(
            LeaderConfig(
                leader_address=address,
                enabled=True,
                copy_multiplier=Decimal(str(settings.default_copy_multiplier)),
                allowed_symbols=None,
                blocked_symbols=[],
                preferred_venue=preferred,
                fallback_venue="BINANCE" if settings.enable_binance_fallback else "NONE",
                enabled_venues=enabled_venues,
            )
        )
        imported += 1
    await db.commit()
    return {"configured": len(addresses), "imported": imported, "skipped": skipped}


async def validate_startup_config(
    *,
    settings: Settings,
    db: Any,
    check_external: bool = True,
) -> StartupValidationResult:
    checks: list[StartupCheck] = []
    checks.extend(_global_config_checks(settings))
    checks.extend(await _database_checks(db))
    checks.extend(await _redis_checks(settings))
    checks.extend(await _leader_checks(db, settings))
    checks.extend(await _follower_migration_checks(db))
    checks.extend(await _hyperliquid_checks(settings, check_external=check_external))
    checks.extend(await _binance_checks(settings, check_external=check_external))
    result = StartupValidationResult(
        ready_for_live=not any(check.blocks_live for check in checks),
        checks=checks,
    )
    return result


async def _follower_migration_checks(db: Any) -> list[StartupCheck]:
    row = await db.get(AppSetting, FOLLOWER_RUNTIME_IDENTITY_KEY)
    if row is None:
        return [
            StartupCheck(
                "follower.runtime_identity",
                "WARNING",
                "runtime identity has not been initialized yet",
            )
        ]
    payload = dict(row.value or {})
    status = str(payload.get("status") or "").upper()
    blockers = [str(item) for item in payload.get("blockers") or []]
    if status != FOLLOWER_MIGRATION_READY:
        return [
            StartupCheck(
                "follower.runtime_identity",
                "BLOCKED",
                "; ".join(blockers) or f"follower migration status is {status or 'UNKNOWN'}",
            )
        ]
    return [StartupCheck("follower.runtime_identity", "OK", "configured follower identity is active")]


async def store_startup_validation(db: Any, result: StartupValidationResult) -> None:
    payload = result.as_dict()
    stmt = (
        insert(AppSetting)
        .values(key="startup_config", value=payload)
        .on_conflict_do_update(index_elements=[AppSetting.key], set_={"value": payload})
    )
    await db.execute(stmt)
    await db.commit()
    if result.ready_for_live:
        log.info("startup_config_ready", ready_for_live=True)
    else:
        log.warning("startup_config_not_ready", blocking_count=len(result.blocking_reasons))


def _global_config_checks(settings: Settings) -> list[StartupCheck]:
    checks = [
        StartupCheck("TRADING_ENABLED", "OK", f"set to {settings.trading_enabled}"),
        _secret_check(
            name="APP_SECRET_KEY",
            value=settings.app_secret_key.get_secret_value(),
            forbidden={"change-me", ""},
        ),
        _secret_check(
            name="ENCRYPTION_MASTER_KEY",
            value=settings.encryption_master_key.get_secret_value(),
            forbidden={"change-me-32-bytes", "change-me", ""},
        ),
    ]
    if settings.default_preferred_venue.upper() not in {item.value for item in VenuePreference}:
        checks.append(
            StartupCheck(
                "DEFAULT_PREFERRED_VENUE",
                "BLOCKED",
                "must be HYPERLIQUID, BINANCE, or AUTO",
            )
        )
    return checks


async def _database_checks(db: Any) -> list[StartupCheck]:
    try:
        await db.execute(text("select 1"))
        return [StartupCheck("database", "OK", "connected")]
    except Exception as exc:
        return [StartupCheck("database", "BLOCKED", f"not connected: {exc}")]


async def _redis_checks(settings: Settings) -> list[StartupCheck]:
    try:
        import redis.asyncio as redis

        client = redis.from_url(settings.redis_url, socket_connect_timeout=1)
        await client.ping()
        await client.aclose()
        return [StartupCheck("redis", "OK", "connected")]
    except Exception as exc:
        return [StartupCheck("redis", "BLOCKED", f"not connected: {exc}")]


async def _leader_checks(db: Any, settings: Settings) -> list[StartupCheck]:
    leaders = (await db.execute(active_leaders_statement())).scalars().all()
    if not leaders:
        return [StartupCheck("leaders", "BLOCKED", "no enabled leader configured")]
    checks: list[StartupCheck] = [StartupCheck("leaders", "OK", f"{len(leaders)} enabled leader(s)")]
    for leader in leaders:
        label = f"leader:{leader.id}"
        if not ADDRESS_RE.fullmatch(leader.leader_address):
            checks.append(StartupCheck(label, "BLOCKED", "leader address format is invalid"))
        if leader.copy_multiplier <= 0:
            checks.append(StartupCheck(label, "BLOCKED", "copy_multiplier must be > 0"))
        if leader.fixed_account_value is None or leader.fixed_account_value <= 0:
            checks.append(StartupCheck(label, "BLOCKED", "fixed_account_value must be > 0"))
        if leader.max_notional_per_trade is None or leader.max_notional_per_trade <= 0:
            checks.append(StartupCheck(label, "WARNING", "No max notional cap set."))
        if leader.max_total_notional is None or leader.max_total_notional <= 0:
            checks.append(StartupCheck(label, "WARNING", "No max total notional cap set."))
        preferred = str(leader.preferred_venue or settings.default_preferred_venue).upper()
        if preferred == ExecutionVenue.HYPERLIQUID.value and not settings.enable_hyperliquid_execution:
            checks.append(StartupCheck(label, "BLOCKED", "preferred Hyperliquid venue is disabled"))
        if preferred == ExecutionVenue.BINANCE.value and not settings.enable_binance_execution:
            checks.append(StartupCheck(label, "BLOCKED", "preferred Binance venue is disabled"))
    return checks


async def _hyperliquid_checks(settings: Settings, *, check_external: bool) -> list[StartupCheck]:
    if not settings.enable_hyperliquid_execution:
        return [StartupCheck("hyperliquid", "WARNING", "execution disabled")]
    checks: list[StartupCheck] = []
    if settings.hyperliquid_execution_network.lower() not in {"testnet", "mainnet"}:
        checks.append(StartupCheck("hyperliquid.network", "BLOCKED", "must be testnet or mainnet"))
    if not settings.hyperliquid_private_key_value():
        checks.append(StartupCheck("hyperliquid.signer_private_key", "BLOCKED", "not configured"))
    follower_account = settings.hyperliquid_follower_account_address()
    if not follower_account:
        checks.append(StartupCheck("hyperliquid.account", "BLOCKED", "follower account not configured"))
    if settings.hyperliquid_follower_address_ambiguous():
        checks.append(
            StartupCheck(
                "hyperliquid.account",
                "BLOCKED",
                "Follower account address is ambiguous. Set HYPERLIQUID_ACCOUNT_ADDRESS explicitly.",
            )
        )
    if not check_external:
        checks.append(StartupCheck("hyperliquid.external", "WARNING", "external checks not run"))
        return checks
    if settings.low_latency_required_for_live:
        checks.append(
            StartupCheck(
                "hyperliquid.low_latency",
                "WARNING",
                "LOW_LATENCY_REQUIRED_FOR_LIVE=true; runtime Preflight verifies fill-driven WebSocket readiness after startup",
            )
        )

    client = HyperliquidExecutionClient(
        info_url=f"{settings.hyperliquid_execution_base_url()}/info",
        private_key=settings.hyperliquid_private_key_value(),
        account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
        vault_address=settings.hyperliquid_vault_address,
        network=settings.hyperliquid_execution_network,
        timeout=3.0,
    )
    try:
        enabled_dexes = HyperliquidDexRegistry(settings).enabled_dexes()
        for dex in enabled_dexes:
            label = dex.dex_name or "default"
            meta = await client.meta(dex.dex_name)
            if meta.get("universe"):
                checks.append(StartupCheck(f"hyperliquid.{label}.meta", "OK", "universe loaded"))
            else:
                checks.append(StartupCheck(f"hyperliquid.{label}.meta", "BLOCKED", "universe missing"))
            if follower_account:
                state = await client.account_state(follower_account, dex=dex.dex_name)
                if state.get("marginSummary"):
                    checks.append(StartupCheck(f"hyperliquid.{label}.account_state", "OK", "loaded"))
                else:
                    checks.append(StartupCheck(f"hyperliquid.{label}.account_state", "BLOCKED", "marginSummary missing"))
        if follower_account:
            snapshot = await AccountAbstractionService(client, settings).fetch_snapshot(
                role="FOLLOWER",
                address=follower_account,
                dexes=[dex.dex_name for dex in enabled_dexes],
            )
            default_result = resolve_account_value_for_sizing(snapshot, "", settings)
            status = "OK"
            message = (
                f"{default_result.mode} using {default_result.source}; "
                f"account_value_used={default_result.account_value}; "
                f"available_collateral={default_result.withdrawable_or_available}"
            )
            if default_result.blockers:
                status = "BLOCKED"
                message = "; ".join(default_result.blockers)
            elif default_result.warnings:
                status = "WARNING"
                message = "; ".join(default_result.warnings)
            checks.append(StartupCheck("hyperliquid.account_abstraction", status, message))
    except Exception as exc:
        checks.append(StartupCheck("hyperliquid.external", "BLOCKED", str(exc)[:160]))
    finally:
        await client.close()
    return checks


async def _binance_checks(settings: Settings, *, check_external: bool) -> list[StartupCheck]:
    if not settings.enable_binance_execution:
        return [StartupCheck("binance", "WARNING", "execution disabled")]
    binance_required = (
        settings.default_preferred_venue.upper() in {ExecutionVenue.BINANCE.value, "AUTO"}
        or settings.enable_binance_fallback
    )
    if not binance_required:
        return [StartupCheck("binance", "WARNING", "not required for Hyperliquid primary without fallback")]
    checks: list[StartupCheck] = []
    if not settings.binance_api_key or not settings.binance_api_secret:
        checks.append(StartupCheck("binance.credentials", "BLOCKED", "not configured"))
        return checks
    if not check_external:
        checks.append(StartupCheck("binance.external", "WARNING", "external checks not run"))
        return checks
    client = BinanceFuturesClient(settings, timeout=3.0)
    try:
        hedge = await client.position_mode_dual_side()
        checks.append(
            StartupCheck(
                "binance.hedge_mode",
                "OK" if hedge else "BLOCKED",
                "HEDGE" if hedge else "not Hedge Mode",
            )
        )
    except Exception as exc:
        checks.append(StartupCheck("binance.external", "BLOCKED", str(exc)[:160]))
    finally:
        await client.close()
    return checks


def _secret_check(*, name: str, value: str, forbidden: set[str]) -> StartupCheck:
    if value in forbidden or len(value) < 16:
        return StartupCheck(name, "BLOCKED", "must be set to a non-default secret")
    return StartupCheck(name, "OK", "configured")
