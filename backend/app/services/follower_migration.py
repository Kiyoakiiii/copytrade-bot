from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from app.models import AppSetting
from app.services.hyperliquid_dex import mask_address
from app.services.leader_config import normalize_leader_address

FOLLOWER_RUNTIME_IDENTITY_KEY = "follower_runtime_identity"
FOLLOWER_MIGRATION_READY = "READY"
FOLLOWER_MIGRATION_BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class FollowerMigrationResult:
    ready: bool
    changed: bool
    status: str
    blockers: list[str]
    payload: dict[str, Any]


async def prepare_follower_runtime_identity(
    db: Any,
    *,
    settings: Any,
    info_client: Any | None = None,
    risk_settings_client: Any | None = None,
) -> FollowerMigrationResult:
    now = datetime.now(timezone.utc)
    configured_address = normalize_leader_address(
        settings.hyperliquid_follower_account_address() or ""
    )
    configured_network = str(settings.hyperliquid_execution_network or "").lower()
    existing_row = await db.get(AppSetting, FOLLOWER_RUNTIME_IDENTITY_KEY)
    existing = dict(existing_row.value or {}) if existing_row else {}
    active_address = normalize_leader_address(existing.get("active_account_address") or "")
    active_network = str(existing.get("active_network") or configured_network).lower()

    config_blockers = _follower_configuration_blockers(settings, configured_address)
    if not existing:
        if config_blockers:
            payload = _identity_payload(
                status=FOLLOWER_MIGRATION_BLOCKED,
                active_address=None,
                configured_address=configured_address or None,
                active_network=None,
                configured_network=configured_network,
                blockers=config_blockers,
                now=now,
                last_action="INITIALIZATION_BLOCKED",
            )
            await _store_identity(db, payload)
            return FollowerMigrationResult(False, False, FOLLOWER_MIGRATION_BLOCKED, config_blockers, payload)
        payload = _identity_payload(
            status=FOLLOWER_MIGRATION_READY,
            active_address=configured_address,
            configured_address=configured_address,
            active_network=configured_network,
            configured_network=configured_network,
            blockers=[],
            now=now,
            last_action="INITIALIZED_CURRENT_ACCOUNT",
        )
        await _store_identity(db, payload)
        return FollowerMigrationResult(True, False, FOLLOWER_MIGRATION_READY, [], payload)

    if active_address == configured_address and active_network == configured_network:
        blockers = config_blockers
        status = FOLLOWER_MIGRATION_BLOCKED if blockers else FOLLOWER_MIGRATION_READY
        payload = {
            **existing,
            "status": status,
            "migration_required": False,
            "configured_account_address": configured_address or None,
            "configured_network": configured_network,
            "blockers": blockers,
            "last_checked_at": now.isoformat(),
            "last_action": "CONFIGURATION_BLOCKED" if blockers else "IDENTITY_UNCHANGED",
        }
        await _store_identity(db, payload)
        return FollowerMigrationResult(not blockers, False, status, blockers, payload)

    await _force_kill_switch(db)
    blockers = list(config_blockers)
    if active_network != configured_network:
        blockers.append("follower network changed; manual follower migration is required")
    blockers.append(
        "automatic follower migration is disabled; manual follower carryover is required"
    )
    payload = {
        **existing,
        "status": FOLLOWER_MIGRATION_BLOCKED,
        "migration_required": True,
        "active_account_address": active_address or None,
        "previous_account_address": active_address or None,
        "configured_account_address": configured_address or None,
        "active_network": active_network,
        "previous_network": active_network,
        "configured_network": configured_network,
        "blockers": blockers,
        "automatic_migration_disabled": True,
        "kill_switch_forced_on": True,
        "last_checked_at": now.isoformat(),
        "last_action": "MANUAL_FOLLOWER_MIGRATION_REQUIRED",
    }
    await _store_identity(db, payload)
    return FollowerMigrationResult(False, True, FOLLOWER_MIGRATION_BLOCKED, blockers, payload)


def public_follower_migration_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    value = dict(payload or {})
    for key in (
        "active_account_address",
        "configured_account_address",
        "previous_account_address",
    ):
        if value.get(key):
            value[key] = mask_address(str(value[key]))
    return value


def _follower_configuration_blockers(settings: Any, configured_address: str) -> list[str]:
    blockers: list[str] = []
    if not configured_address:
        blockers.append("configured follower account address is missing")
        return blockers
    signer = normalize_leader_address(settings.hyperliquid_signer_address() or "")
    api_wallet = normalize_leader_address(settings.hyperliquid_api_wallet_address or "")
    is_vault_or_subaccount = bool(
        settings.hyperliquid_vault_address or settings.hyperliquid_subaccount_address
    )
    if not signer:
        blockers.append("Hyperliquid signer private key is missing or invalid")
    if api_wallet and signer != api_wallet:
        blockers.append("HYPERLIQUID_API_WALLET_ADDRESS does not match the configured signer private key")
    if not api_wallet and not is_vault_or_subaccount and signer and signer != configured_address:
        blockers.append(
            "follower account differs from signer; configure its approved API wallet explicitly"
        )
    return blockers


async def _force_kill_switch(db: Any) -> None:
    row = await db.get(AppSetting, "risk")
    value = dict(row.value or {}) if row else {}
    value["kill_switch"] = True
    stmt = (
        insert(AppSetting)
        .values(key="risk", value=value)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": value, "updated_at": datetime.now(timezone.utc)},
        )
    )
    await db.execute(stmt)
    await db.commit()


async def _store_identity(db: Any, payload: dict[str, Any]) -> None:
    stmt = (
        insert(AppSetting)
        .values(key=FOLLOWER_RUNTIME_IDENTITY_KEY, value=payload)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": datetime.now(timezone.utc)},
        )
    )
    await db.execute(stmt)
    await db.commit()


def _identity_payload(
    *,
    status: str,
    active_address: str | None,
    configured_address: str | None,
    active_network: str | None,
    configured_network: str,
    blockers: list[str],
    now: datetime,
    last_action: str,
    previous_address: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": status,
        "migration_required": status != FOLLOWER_MIGRATION_READY,
        "active_account_address": active_address,
        "configured_account_address": configured_address,
        "previous_account_address": previous_address,
        "active_network": active_network,
        "configured_network": configured_network,
        "blockers": blockers,
        "last_action": last_action,
        "last_checked_at": now.isoformat(),
    }
