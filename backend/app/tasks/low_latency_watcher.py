from __future__ import annotations

from typing import Iterable

from sqlalchemy import select

from app.core.config import Settings
from app.db.session import (
    SessionLocal,
    create_low_latency_submit_session_factory,
    prewarm_database_engine,
)
from app.services.hyperliquid import HyperliquidInfoClient
from app.services.hyperliquid_execution import HyperliquidExecutionClient
from app.services.low_latency_watcher import HyperliquidLowLatencyWatcher
from app.models import LeaderConfig


def _settings_with_explicit_execution_route(
    settings: Settings,
    route_addresses: Iterable[str],
) -> Settings:
    """Resolve the single explicit worker account from durable leader routes.

    Docker Compose interpolation can lose a shell-only sub-account variable on
    a later recreate. Leader routing is already persisted by the admin UI, so
    it is the durable source of truth and avoids leaving the sub-account worker
    in a restart loop after an otherwise safe deploy.
    """

    if not settings.low_latency_uses_explicit_leader_route():
        return settings
    routes = sorted(
        {
            str(address or "").strip().lower()
            for address in route_addresses
            if str(address or "").strip()
        }
    )
    if len(routes) != 1:
        raise RuntimeError(
            "explicit leader route requires exactly one active execution account"
        )
    route = routes[0]
    configured_route = str(
        settings.hyperliquid_vault_address
        or settings.hyperliquid_subaccount_address
        or ""
    ).strip().lower()
    if configured_route and configured_route != route:
        raise RuntimeError(
            "configured explicit execution account conflicts with active leader routes"
        )
    if configured_route == route:
        return settings
    return settings.model_copy(update={"hyperliquid_subaccount_address": route})


async def _resolve_explicit_execution_route(settings: Settings) -> Settings:
    if not settings.low_latency_uses_explicit_leader_route():
        return settings
    async with SessionLocal() as db:
        routes = (
            await db.execute(
                select(LeaderConfig.hyperliquid_vault_address)
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .where(LeaderConfig.hyperliquid_vault_address.is_not(None))
                .where(LeaderConfig.hyperliquid_vault_address != "")
                .distinct()
            )
        ).scalars().all()
    return _settings_with_explicit_execution_route(settings, routes)


async def run_low_latency_watcher(settings: Settings) -> None:
    settings = await _resolve_explicit_execution_route(settings)
    info_client = HyperliquidInfoClient(f"{settings.hyperliquid_execution_base_url()}/info")
    if settings.low_latency_uses_explicit_leader_route():
        execution_account = settings.hyperliquid_follower_account_address()
        master_account = settings.hyperliquid_account_address or settings.hyperliquid_signer_address()
        if not execution_account or not master_account:
            await info_client.close()
            raise RuntimeError("explicit leader route requires master and execution account addresses")
        subaccounts = await info_client.sub_accounts(master_account)
        known_subaccounts = {
            str(item.get("subAccountUser") or "").lower()
            for item in subaccounts
            if isinstance(item, dict)
        }
        if execution_account.lower() not in known_subaccounts:
            await info_client.close()
            raise RuntimeError("configured execution account is not a subaccount of the master account")
    execution_client = HyperliquidExecutionClient(
        info_url=f"{settings.hyperliquid_execution_base_url()}/info",
        private_key=settings.hyperliquid_private_key_value(),
        account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
        vault_address=settings.hyperliquid_execution_vault_address(),
        network=settings.hyperliquid_execution_network,
        order_submit_transport=settings.order_submit_transport,
    )
    # Account-value polling is deliberately isolated from market metadata,
    # leader backfill and recovery reads used by the fill path.
    account_info_client = HyperliquidInfoClient(
        f"{settings.hyperliquid_execution_base_url()}/info"
    )
    submit_engine, SubmitSessionLocal = create_low_latency_submit_session_factory(settings)
    watcher = None
    try:
        await prewarm_database_engine(
            submit_engine,
            connection_count=settings.low_latency_submit_database_pool_size,
        )
        watcher = HyperliquidLowLatencyWatcher(
            settings=settings,
            info_client=info_client,
            account_info_client=account_info_client,
            execution_client=execution_client,
            db_session_factory=SessionLocal,
            submit_db_session_factory=SubmitSessionLocal,
        )
        await watcher.run()
    finally:
        if watcher is not None:
            await watcher.stop()
        await account_info_client.close()
        await info_client.close()
        await execution_client.close()
        await submit_engine.dispose()
