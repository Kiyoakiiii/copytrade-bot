from __future__ import annotations

from app.core.config import Settings
from app.db.session import SessionLocal
from app.services.hyperliquid import HyperliquidInfoClient
from app.services.hyperliquid_execution import HyperliquidExecutionClient
from app.services.low_latency_watcher import HyperliquidLowLatencyWatcher


async def run_low_latency_watcher(settings: Settings) -> None:
    info_client = HyperliquidInfoClient(f"{settings.hyperliquid_execution_base_url()}/info")
    execution_client = HyperliquidExecutionClient(
        info_url=f"{settings.hyperliquid_execution_base_url()}/info",
        private_key=settings.hyperliquid_private_key_value(),
        account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
        vault_address=settings.hyperliquid_vault_address,
        network=settings.hyperliquid_execution_network,
        order_submit_transport=settings.order_submit_transport,
    )
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings,
        info_client=info_client,
        execution_client=execution_client,
        db_session_factory=SessionLocal,
    )
    try:
        await watcher.run()
    finally:
        await watcher.stop()
        await info_client.close()
        await execution_client.close()
