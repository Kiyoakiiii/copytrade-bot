from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_name: str = "copytrade-bot"
    app_env: str = "production"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres/copytrade"
    redis_url: str = "redis://redis:6379/0"

    binance_api_key: SecretStr | None = None
    binance_api_secret: SecretStr | None = None
    binance_testnet: bool = True
    binance_hedge_mode: bool = True
    binance_trading_enabled: bool = False
    binance_expected_leverage: int = 10
    binance_expected_margin_type: str = "ISOLATED"

    default_preferred_venue: str = "HYPERLIQUID"
    enable_hyperliquid_execution: bool = True
    enable_binance_execution: bool = False
    enable_binance_fallback: bool = False

    hyperliquid_execution_network: str = "testnet"
    hyperliquid_account_address: str | None = None
    hyperliquid_signer_private_key: SecretStr | None = None
    hyperliquid_private_key: SecretStr | None = None
    hyperliquid_private_key_file: str | None = None
    hyperliquid_api_wallet_address: str | None = None
    hyperliquid_vault_address: str | None = None
    hyperliquid_subaccount_address: str | None = None
    hyperliquid_default_leverage: int = 10
    hyperliquid_default_margin_mode: str = "CROSS"
    hyperliquid_min_order_value_usd: float = 10.0
    hyperliquid_trading_enabled: bool = False
    enabled_hyperliquid_dexes: str = ",xyz"
    default_hyperliquid_dex: str = ""
    enable_xyz_dex: bool = True
    xyz_dex_name: str = "xyz"
    allow_hip3_markets: bool = True
    low_latency_required_for_live: bool = True
    allow_poll_fallback_live: bool = False
    max_fill_debounce_ms: int = 0
    leader_fill_startup_backfill_seconds: float = 60.0
    price_cache_stale_ms: int = 2000
    price_cache_poll_seconds: float = 1.0
    low_latency_leader_refresh_seconds: float = 0.1
    allocation_sync_poll_seconds: float = 1.0
    allocation_post_fill_snapshot_lag_guard_seconds: float = 30.0
    follower_active_position_refresh_seconds: float = 0.5
    order_recovery_interval_seconds: float = 2.0
    order_recovery_stale_pending_submit_seconds: float = 10.0
    order_recovery_unknown_oid_resubmit_seconds: float = 30.0
    hyperliquid_risk_settings_ttl_seconds: int = 300
    hyperliquid_prewarm_common_coins: str = (
        "BTC,ETH,SOL,HYPE,BNB,XRP,DOGE,LINK,ADA,AVAX,SUI,LTC,BCH,TRX,TON,"
        "APT,ARB,OP,ENA,WIF,NEAR,AAVE,UNI,PENDLE,TAO,FET,SEI,INJ,JUP,PUMP,ZEC,XMR"
    )
    order_submit_transport: str = "http"
    hyperliquid_ws_leader_subscription_limit: int = 0
    account_value_mode: str = "auto"
    account_value_reference_dexes: str = ",xyz"
    require_confirmed_account_abstraction_for_live: bool = True
    allow_unified_account_for_live: bool = True
    allow_portfolio_margin_for_live: bool = False
    unified_account_collateral_source: str = "spot_or_portfolio"
    default_collateral_token: str = "USDC"

    trading_enabled: bool = False
    default_copy_multiplier: float = 0.1
    global_max_notional: float = 1000
    global_max_daily_loss: float = 100
    leader_state_stale_seconds: int = 10
    account_state_stale_seconds: int = 2
    account_state_poll_seconds: int = 5
    bootstrap_leader_addresses: str | None = None

    app_secret_key: SecretStr = Field(default=SecretStr("change-me"))
    encryption_master_key: SecretStr = Field(default=SecretStr("change-me-32-bytes"))
    admin_email: str = "operator@example.invalid"
    admin_password_bootstrap: SecretStr | None = None
    require_totp: bool = True
    ip_allowlist: str | None = None
    cookie_secure: bool = True
    session_ttl_seconds: int = 60 * 60 * 8
    csrf_cookie_name: str = "copytrade_csrf"
    session_cookie_name: str = "copytrade_session"

    hyperliquid_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hyperliquid_info_url: str = "https://api.hyperliquid.xyz/info"

    def binance_base_url(self) -> str:
        if self.binance_testnet:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"

    def hyperliquid_execution_base_url(self) -> str:
        if self.hyperliquid_execution_network.lower() == "testnet":
            return "https://api.hyperliquid-testnet.xyz"
        return "https://api.hyperliquid.xyz"

    def hyperliquid_private_key_value(self) -> str | None:
        if self.hyperliquid_signer_private_key:
            value = self.hyperliquid_signer_private_key.get_secret_value().strip()
            if value:
                return value
        if self.hyperliquid_private_key_file:
            try:
                value = Path(self.hyperliquid_private_key_file).read_text(encoding="utf-8").strip()
                return value or None
            except OSError:
                return None
        if self.hyperliquid_private_key:
            value = self.hyperliquid_private_key.get_secret_value().strip()
            if value:
                return value
        return None

    def hyperliquid_signer_address(self) -> str | None:
        private_key = self.hyperliquid_private_key_value()
        if not private_key:
            return None
        try:
            from eth_account import Account

            return Account.from_key(private_key).address.lower()
        except Exception:
            return None

    def hyperliquid_follower_account_address(self) -> str | None:
        if self.hyperliquid_vault_address:
            return self.hyperliquid_vault_address.lower()
        if self.hyperliquid_subaccount_address:
            return self.hyperliquid_subaccount_address.lower()
        if self.hyperliquid_account_address:
            return self.hyperliquid_account_address.lower()
        return self.hyperliquid_signer_address()

    def hyperliquid_account_address_is_explicit(self) -> bool:
        return bool(
            self.hyperliquid_account_address
            or self.hyperliquid_vault_address
            or self.hyperliquid_subaccount_address
        )

    def hyperliquid_signer_type(self) -> str:
        signer = self.hyperliquid_signer_address()
        api_wallet = self.hyperliquid_api_wallet_address.lower() if self.hyperliquid_api_wallet_address else None
        configured_account = self.hyperliquid_account_address.lower() if self.hyperliquid_account_address else None
        if signer and configured_account and signer == configured_account:
            return "MAIN_WALLET"
        if signer and api_wallet and signer == api_wallet:
            return "API_WALLET"
        if signer and not configured_account and not self.hyperliquid_vault_address and not self.hyperliquid_subaccount_address:
            return "MAIN_WALLET"
        if api_wallet:
            return "API_WALLET"
        return "UNKNOWN"

    def hyperliquid_follower_address_ambiguous(self) -> bool:
        if not self.hyperliquid_private_key_value():
            return True
        if self.hyperliquid_account_address_is_explicit():
            return False
        if self.hyperliquid_api_wallet_address:
            return True
        return False

    def enabled_hyperliquid_dex_list(self) -> list[str]:
        raw = self.enabled_hyperliquid_dexes
        names: list[str] = []
        for part in raw.split(","):
            name = part.strip().lower()
            if name not in names:
                names.append(name)
        default = self.default_hyperliquid_dex.strip().lower()
        if default not in names:
            names.insert(0, default)
        xyz = self.xyz_dex_name.strip().lower()
        if self.enable_xyz_dex and xyz not in names:
            names.append(xyz)
        if not self.enable_xyz_dex:
            names = [name for name in names if name != xyz]
        if "" not in names:
            names.insert(0, "")
        return names

    def allowed_ips(self) -> set[str]:
        if not self.ip_allowlist:
            return set()
        return {ip.strip() for ip in self.ip_allowlist.split(",") if ip.strip()}

    def bootstrap_leader_address_list(self) -> list[str]:
        if not self.bootstrap_leader_addresses:
            return []
        return [
            address.strip()
            for address in self.bootstrap_leader_addresses.split(",")
            if address.strip()
        ]

    def hyperliquid_prewarm_common_coin_list(self) -> list[str]:
        coins: list[str] = []
        for part in self.hyperliquid_prewarm_common_coins.split(","):
            coin = part.strip().upper()
            if coin and coin not in coins:
                coins.append(coin)
        return coins


@lru_cache
def get_settings() -> Settings:
    return Settings()
