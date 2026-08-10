import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.core.config import Settings
from app.models import MarketRiskSetting
from app.services.hyperliquid_risk_settings import (
    DESIRED_MARGIN_MODE,
    FALLBACK_MARGIN_MODE,
    REASON_LEVERAGE_NOT_CONFIRMED,
    REASON_MAX_LEVERAGE_UNKNOWN,
    STATUS_CONFIRMED,
    STATUS_FAILED,
    STATUS_NEEDS_REFRESH,
    effective_leverage_for_market,
    ensure_hyperliquid_market_risk_settings,
    isolated_margin_excess_for_target_leverage,
    prepare_migrated_hyperliquid_risk_settings,
    remove_excess_isolated_margin_for_target_leverage,
    seed_market_risk_settings_for_account_migration,
    _merge_existing_risk_rows_as_prepare_candidates,
    _merge_common_market_prewarm_candidates,
)


def settings() -> Settings:
    return Settings(_env_file=None, hyperliquid_account_address="0x" + "5" * 40)


class FakeDb:
    def __init__(self, row: MarketRiskSetting | None = None) -> None:
        self.row = row
        self.added = []
        self.flushes = 0
        self.statements = []

    async def scalar(self, stmt):
        self.statements.append(stmt)
        return self.row

    def add(self, item):
        self.added.append(item)
        if isinstance(item, MarketRiskSetting):
            self.row = item

    async def flush(self):
        self.flushes += 1


class FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class FakeRiskRowsDb:
    def __init__(self, *, source_rows=None, existing_target_rows=None) -> None:
        self.source_rows = list(source_rows or [])
        self.existing_target_rows = list(existing_target_rows or [])
        self.added = []
        self.flushes = 0
        self.execute_calls = 0
        self.scalar_calls = 0

    async def execute(self, stmt):
        self.execute_calls += 1
        if self.execute_calls == 1:
            return FakeRowsResult(self.source_rows)
        if self.execute_calls == 2:
            return FakeRowsResult(self.existing_target_rows)
        return FakeRowsResult(self.existing_target_rows + self.added)

    async def scalar(self, stmt):
        rows = self.existing_target_rows + self.added
        row = rows[self.scalar_calls]
        self.scalar_calls += 1
        return row

    def add(self, item):
        self.added.append(item)

    async def flush(self):
        self.flushes += 1


class FakeClient:
    def __init__(self, *, universe=None, update_response=None, update_error: Exception | None = None) -> None:
        self.universe = universe if universe is not None else [{"name": "BTC", "maxLeverage": 50}]
        self.update_response = update_response if update_response is not None else {"status": "ok"}
        self.update_error = update_error
        self.updates = []

    async def meta(self, dex=""):
        return {"universe": self.universe}

    async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool):
        self.updates.append({"coin": coin, "leverage": leverage, "is_cross": is_cross})
        if self.update_error:
            raise self.update_error
        return self.update_response


class SequenceUpdateClient(FakeClient):
    def __init__(self, *, responses, universe=None) -> None:
        super().__init__(universe=universe)
        self.responses = list(responses)

    async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool):
        self.updates.append({"coin": coin, "leverage": leverage, "is_cross": is_cross})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class AssetAwareClient(FakeClient):
    async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool, asset_id: int | None = None):
        self.updates.append({"coin": coin, "leverage": leverage, "is_cross": is_cross, "asset_id": asset_id})
        if self.update_error:
            raise self.update_error
        return self.update_response


def test_effective_leverage_rule_uses_min_default_and_market_max() -> None:
    assert effective_leverage_for_market(50, desired_default_leverage=10) == 10
    assert effective_leverage_for_market(10, desired_default_leverage=10) == 10
    assert effective_leverage_for_market(5, desired_default_leverage=10) == 5
    assert effective_leverage_for_market(3, desired_default_leverage=10) == 3
    assert effective_leverage_for_market(None, desired_default_leverage=10) is None


def test_ensure_sets_cross_effective_leverage_before_open() -> None:
    db = FakeDb()
    client = FakeClient(universe=[{"name": "BTC", "maxLeverage": 5}])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="BTC",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.status == STATUS_CONFIRMED
    assert result.actual_margin_mode == DESIRED_MARGIN_MODE
    assert result.effective_leverage == 5
    assert client.updates == [{"coin": "BTC", "leverage": 5, "is_cross": True}]
    assert db.row.status == STATUS_CONFIRMED


def test_only_isolated_market_metadata_skips_cross_and_sets_three_x_before_open() -> None:
    db = FakeDb()
    client = FakeClient(
        universe=[
            {
                "name": "xyz:GIGADEV",
                "maxLeverage": 10,
                "onlyIsolated": True,
                "marginMode": "noCross",
            }
        ]
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:GIGADEV",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.effective_leverage == 3
    assert client.updates == [{"coin": "xyz:GIGADEV", "leverage": 3, "is_cross": False}]
    assert db.row.desired_margin_mode == FALLBACK_MARGIN_MODE
    assert db.row.desired_leverage == 3


def test_ensure_hydrates_missing_asset_id_even_when_max_leverage_cached() -> None:
    universe = [{"name": f"DUMMY{i}", "maxLeverage": 10} for i in range(231)]
    universe.append({"name": "CASHCAT", "maxLeverage": 3})
    row = MarketRiskSetting(
        account_address=("0x" + "5" * 40).lower(),
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="CASHCAT",
        asset_id=None,
        market_max_leverage=3,
        effective_leverage=3,
        desired_margin_mode="CROSS",
        desired_leverage=10,
        status=STATUS_FAILED,
    )
    db = FakeDb(row)
    client = AssetAwareClient(universe=universe)

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="CASHCAT",
            market_max_leverage=3,
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.asset_id == 231
    assert db.row.asset_id == 231
    assert result.desired_leverage == 1
    assert result.effective_leverage == 1
    assert client.updates == [{"coin": "CASHCAT", "leverage": 1, "is_cross": True, "asset_id": 231}]


def test_cross_unsupported_falls_back_to_isolated_before_open() -> None:
    db = FakeDb()
    client = SequenceUpdateClient(
        universe=[{"name": "CL", "maxLeverage": 20}],
        responses=[
            {"status": "err", "response": "Cross margin is not allowed for this asset."},
            {"status": "ok"},
        ],
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:CL",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.status == STATUS_CONFIRMED
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.desired_margin_mode == FALLBACK_MARGIN_MODE
    assert result.desired_leverage == 3
    assert result.effective_leverage == 3
    assert result.actual_leverage == 3
    assert result.warning == "cross margin unsupported; using isolated margin for this market"
    assert client.updates == [
        {"coin": "xyz:CL", "leverage": 10, "is_cross": True},
        {"coin": "xyz:CL", "leverage": 3, "is_cross": False},
    ]
    assert db.row.status == STATUS_CONFIRMED


def test_invalid_cross_leverage_falls_back_to_isolated_before_open() -> None:
    db = FakeDb()
    client = SequenceUpdateClient(
        universe=[{"name": "SKHY", "maxLeverage": 10}],
        responses=[
            {"status": "err", "response": "Invalid leverage value"},
            {"status": "ok"},
        ],
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:SKHY",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.status == STATUS_CONFIRMED
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.desired_margin_mode == FALLBACK_MARGIN_MODE
    assert result.desired_leverage == 3
    assert result.effective_leverage == 3
    assert result.warning == "cross leverage rejected; using isolated margin for this market"
    assert client.updates == [
        {"coin": "xyz:SKHY", "leverage": 10, "is_cross": True},
        {"coin": "xyz:SKHY", "leverage": 3, "is_cross": False},
    ]


def test_isolated_fallback_is_three_x_when_market_max_allows_it() -> None:
    db = FakeDb()
    client = SequenceUpdateClient(
        universe=[{"name": "VIX", "maxLeverage": 3}],
        responses=[
            {"status": "err", "response": "Cross margin is not allowed for this asset."},
            {"status": "ok"},
        ],
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:VIX",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.desired_leverage == 3
    assert result.effective_leverage == 3
    assert result.actual_leverage == 3
    assert client.updates == [
        {"coin": "xyz:VIX", "leverage": 3, "is_cross": True},
        {"coin": "xyz:VIX", "leverage": 3, "is_cross": False},
    ]


def test_cxmt_is_forced_to_three_x_even_when_cross_margin_is_supported() -> None:
    db = FakeDb()
    client = FakeClient(universe=[{"name": "CXMT", "maxLeverage": 5}])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:CXMT",
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.actual_margin_mode == DESIRED_MARGIN_MODE
    assert result.desired_leverage == 3
    assert result.effective_leverage == 3
    assert result.actual_leverage == 3
    assert client.updates == [
        {"coin": "xyz:CXMT", "leverage": 3, "is_cross": True},
    ]


def test_success_response_does_not_confirm_wrong_active_position_leverage() -> None:
    class ActivePositionClient(SequenceUpdateClient):
        async def account_state(self, address=None, dex=""):
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "CL",
                            "szi": "1",
                            "leverage": {"type": "isolated", "value": 4},
                        }
                    }
                ]
            }

    db = FakeDb()
    client = ActivePositionClient(
        universe=[{"name": "CL", "maxLeverage": 20}],
        responses=[
            {"status": "err", "response": "Cross margin is not allowed for this asset."},
            {"status": "ok"},
        ],
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:CL",
            action_type="OPEN",
        )
    )

    assert result.is_ok is False
    assert result.status == STATUS_FAILED
    assert result.reason_code == REASON_LEVERAGE_NOT_CONFIRMED


def test_active_isolated_only_position_is_adjusted_to_three_x() -> None:
    class ActiveIsolatedOnlyClient(AssetAwareClient):
        def __init__(self):
            super().__init__(
                universe=[
                    {
                        "name": "EWY",
                        "maxLeverage": 20,
                        "marginMode": "noCross",
                        "onlyIsolated": True,
                    }
                ]
            )
            self.topped_up = False
            self.top_ups = []

        async def update_leverage(self, *, coin, leverage, is_cross, asset_id=None):
            self.updates.append(
                {"coin": coin, "leverage": leverage, "is_cross": is_cross, "asset_id": asset_id}
            )
            return {
                "status": "err",
                "response": (
                    "Isolated position does not have sufficient margin available to decrease leverage. "
                    "To decrease leverage, add margin to the position."
                ),
            }

        async def top_up_isolated_only_margin(self, *, coin, leverage, asset_id=None):
            self.top_ups.append({"coin": coin, "leverage": leverage, "asset_id": asset_id})
            self.topped_up = True
            return {"status": "ok", "response": {"type": "default"}}

        async def account_state(self, address=None, dex=""):
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "EWY",
                            "szi": "8.061",
                            "leverage": {
                                "type": "isolated",
                                "value": 3 if self.topped_up else 4,
                            },
                        }
                    }
                ]
            }

    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:EWY",
        asset_id=47,
        desired_margin_mode=FALLBACK_MARGIN_MODE,
        desired_leverage=4,
        market_max_leverage=20,
        status=STATUS_NEEDS_REFRESH,
    )
    db = FakeDb(row)
    client = ActiveIsolatedOnlyClient()

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:EWY",
            action_type="OPEN",
            force_refresh=True,
        )
    )

    assert result.is_ok is True
    assert result.status == STATUS_CONFIRMED
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.actual_leverage == 3
    assert client.updates == [
        {"coin": "xyz:EWY", "leverage": 3, "is_cross": False, "asset_id": 47}
    ]
    assert client.top_ups == [{"coin": "xyz:EWY", "leverage": 3, "asset_id": 47}]


def test_active_no_cross_position_adds_margin_then_sets_three_x() -> None:
    class ActiveNoCrossClient(AssetAwareClient):
        def __init__(self):
            super().__init__(
                universe=[
                    {
                        "name": "EWY",
                        "maxLeverage": 20,
                        "marginMode": "noCross",
                        "onlyIsolated": True,
                    }
                ]
            )
            self.update_count = 0
            self.top_ups = []
            self.margin_additions = []

        async def update_leverage(self, *, coin, leverage, is_cross, asset_id=None):
            self.update_count += 1
            self.updates.append(
                {"coin": coin, "leverage": leverage, "is_cross": is_cross, "asset_id": asset_id}
            )
            if self.update_count == 1:
                return {
                    "status": "err",
                    "response": (
                        "Isolated position does not have sufficient margin available to decrease leverage. "
                        "To decrease leverage, add margin to the position."
                    ),
                }
            return {"status": "ok", "response": {"type": "default"}}

        async def top_up_isolated_only_margin(self, *, coin, leverage, asset_id=None):
            self.top_ups.append({"coin": coin, "leverage": leverage, "asset_id": asset_id})
            return {"status": "err", "response": "Asset not strict isolated"}

        async def add_isolated_margin(self, *, coin, amount, asset_id=None):
            self.margin_additions.append({"coin": coin, "amount": amount, "asset_id": asset_id})
            return {"status": "ok", "response": {"type": "default"}}

        async def account_state(self, address=None, dex=""):
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "EWY",
                            "szi": "8",
                            "positionValue": "1400",
                            "marginUsed": "350",
                            "leverage": {
                                "type": "isolated",
                                "value": 3 if self.update_count > 1 else 4,
                            },
                        }
                    }
                ]
            }

    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:EWY",
        asset_id=47,
        desired_margin_mode=FALLBACK_MARGIN_MODE,
        desired_leverage=4,
        market_max_leverage=20,
        status=STATUS_NEEDS_REFRESH,
    )
    db = FakeDb(row)
    client = ActiveNoCrossClient()

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:EWY",
            action_type="OPEN",
            force_refresh=True,
        )
    )

    assert result.is_ok is True
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.actual_leverage == 3
    assert len(client.updates) == 2
    assert client.top_ups == [{"coin": "xyz:EWY", "leverage": 3, "asset_id": 47}]
    assert client.margin_additions == [
        {"coin": "xyz:EWY", "amount": Decimal("119"), "asset_id": 47}
    ]


def test_isolated_margin_excess_uses_larger_notional_and_keeps_buffer() -> None:
    removable = isolated_margin_excess_for_target_leverage(
        {
            "szi": "10",
            "positionValue": "1000",
            "entryPx": "105",
            "marginUsed": "600",
        },
        target_leverage=2,
    )

    # The entry-price notional (1050) is larger than the current positionValue,
    # so the calculation retains 525 target margin plus a 2.625 USDC buffer.
    assert removable == Decimal("72.375000")


def test_remove_excess_isolated_margin_uses_fresh_exchange_position() -> None:
    class MarginClient:
        def __init__(self):
            self.removals = []

        async def account_state(self, address=None, dex=""):
            return {
                "assetPositions": [
                    {
                        "position": {
                            "coin": "CL",
                            "szi": "10",
                            "positionValue": "1000",
                            "entryPx": "100",
                            "marginUsed": "600",
                        }
                    }
                ]
            }

        async def remove_isolated_margin(self, *, coin, amount, asset_id=None):
            self.removals.append(
                {"coin": coin, "amount": amount, "asset_id": asset_id}
            )
            return {"status": "ok", "response": {"type": "default"}}

    client = MarginClient()
    removed = asyncio.run(
        remove_excess_isolated_margin_for_target_leverage(
            db=FakeDb(),
            client=client,
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:CL",
            asset_id=47,
            target_leverage=2,
        )
    )

    assert removed == Decimal("97.500000")
    assert client.removals == [
        {"coin": "xyz:CL", "amount": Decimal("97.500000"), "asset_id": 47}
    ]


def test_missing_market_max_leverage_blocks_open() -> None:
    db = FakeDb()
    client = FakeClient(universe=[])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:HYUNDAI",
            action_type="OPEN",
        )
    )

    assert result.is_ok is False
    assert result.status == STATUS_FAILED
    assert result.reason_code == REASON_MAX_LEVERAGE_UNKNOWN
    assert client.updates == []


def test_missing_market_max_leverage_allows_reduce_with_warning() -> None:
    db = FakeDb()
    client = FakeClient(universe=[])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:HYUNDAI",
            action_type="CLOSE",
            reduce_only=True,
        )
    )

    assert result.is_ok is True
    assert result.status == STATUS_FAILED
    assert result.warning
    assert client.updates == []


def test_fresh_confirmed_cache_avoids_repeated_update() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="BTC",
        desired_margin_mode=DESIRED_MARGIN_MODE,
        desired_leverage=10,
        market_max_leverage=50,
        effective_leverage=10,
        actual_margin_mode=DESIRED_MARGIN_MODE,
        actual_leverage=10,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc),
    )
    db = FakeDb(row)
    client = FakeClient()

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="BTC",
            market_max_leverage=50,
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.cache_used is True
    assert client.updates == []


def test_market_risk_row_lookup_is_case_insensitive_for_dex_prefixed_coin() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:HYUNDAI",
        desired_margin_mode=DESIRED_MARGIN_MODE,
        desired_leverage=10,
        market_max_leverage=5,
        effective_leverage=5,
        actual_margin_mode=DESIRED_MARGIN_MODE,
        actual_leverage=5,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc),
    )
    db = FakeDb(row)
    client = FakeClient(universe=[{"name": "HYUNDAI", "maxLeverage": 5}])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="XYZ:HYUNDAI",
            market_max_leverage=5,
            action_type="OPEN",
        )
    )

    sql = str(db.statements[0].compile(compile_kwargs={"literal_binds": True})).lower()
    assert "upper(market_risk_settings.canonical_coin)" in sql
    assert result.is_ok is True
    assert result.cache_used is True
    assert db.added == []
    assert client.updates == []


def test_stale_confirmed_cache_refreshes_before_open() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="BTC",
        desired_margin_mode=DESIRED_MARGIN_MODE,
        desired_leverage=10,
        market_max_leverage=50,
        effective_leverage=10,
        actual_margin_mode=DESIRED_MARGIN_MODE,
        actual_leverage=10,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc) - timedelta(seconds=600),
    )
    db = FakeDb(row)
    client = FakeClient()

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="BTC",
            market_max_leverage=50,
            action_type="OPEN",
        )
    )

    assert result.is_ok is True
    assert result.cache_used is False
    assert client.updates == [{"coin": "BTC", "leverage": 10, "is_cross": True}]


def test_legacy_isolated_market_override_is_changed_to_three_x_on_refresh() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:CL",
        desired_margin_mode=FALLBACK_MARGIN_MODE,
        desired_leverage=4,
        market_max_leverage=20,
        effective_leverage=10,
        actual_margin_mode=FALLBACK_MARGIN_MODE,
        actual_leverage=10,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc),
    )
    db = FakeDb(row)
    client = FakeClient(
        universe=[
            {
                "name": "CL",
                "maxLeverage": 20,
                "marginMode": "noCross",
                "onlyIsolated": True,
            }
        ]
    )

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="xyz",
            canonical_coin_value="xyz:CL",
            market_max_leverage=20,
            desired_default_leverage=10,
            action_type="PREPARE_OPEN",
        )
    )

    assert result.is_ok is True
    assert result.desired_leverage == 3
    assert result.effective_leverage == 3
    assert result.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert result.actual_leverage == 3
    assert db.row.desired_leverage == 3
    assert client.updates == [{"coin": "xyz:CL", "leverage": 3, "is_cross": False}]


def test_stale_isolated_row_migrates_to_cross_market_maximum_when_meta_allows_cross() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="SOL",
        desired_margin_mode=FALLBACK_MARGIN_MODE,
        desired_leverage=1,
        market_max_leverage=20,
        effective_leverage=1,
        actual_margin_mode=FALLBACK_MARGIN_MODE,
        actual_leverage=1,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc),
    )
    db = FakeDb(row)
    client = FakeClient(universe=[{"name": "SOL", "maxLeverage": 20}])

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="SOL",
            market_max_leverage=20,
            market_only_isolated=False,
            desired_default_leverage=20,
            action_type="PREPARE_OPEN",
        )
    )

    assert result.is_ok is True
    assert result.desired_margin_mode == DESIRED_MARGIN_MODE
    assert result.desired_leverage == 20
    assert result.effective_leverage == 20
    assert result.actual_margin_mode == DESIRED_MARGIN_MODE
    assert result.actual_leverage == 20
    assert client.updates == [{"coin": "SOL", "leverage": 20, "is_cross": True}]


def test_low_latency_submit_can_reuse_stale_confirmed_cache() -> None:
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="BTC",
        desired_margin_mode=DESIRED_MARGIN_MODE,
        desired_leverage=10,
        market_max_leverage=50,
        effective_leverage=10,
        actual_margin_mode=DESIRED_MARGIN_MODE,
        actual_leverage=10,
        status=STATUS_CONFIRMED,
        last_confirmed_at=datetime.now(timezone.utc) - timedelta(seconds=600),
    )
    db = FakeDb(row)
    client = FakeClient()

    result = asyncio.run(
        ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings(),
            account_address="0x" + "5" * 40,
            dex="",
            canonical_coin_value="BTC",
            market_max_leverage=50,
            action_type="OPEN",
            allow_stale_confirmed_cache=True,
        )
    )

    assert result.is_ok is True
    assert result.cache_used is True
    assert client.updates == []


def test_existing_risk_rows_are_prepare_candidates_without_live_blocking() -> None:
    rows = [
        MarketRiskSetting(
            execution_venue="HYPERLIQUID",
            account_address=("0x" + "5" * 40).lower(),
            dex="xyz",
            canonical_coin="xyz:GME",
            asset_id=1,
            market_max_leverage=10,
            status=STATUS_NEEDS_REFRESH,
        )
    ]

    candidates = _merge_existing_risk_rows_as_prepare_candidates([], rows)

    assert candidates == [
        {
            "dex": "xyz",
            "dex_display_name": "XYZ",
            "canonical_coin": "xyz:GME",
            "asset_id": 1,
            "market_max_leverage": 10,
            "source": "stored_risk_setting",
            "baseline_status": None,
            "risk_setting_required": False,
            "reason": "known market risk setting prewarm",
            "prepare_risk_setting": True,
        }
    ]


def test_common_market_prewarm_candidates_use_default_meta_only() -> None:
    client = FakeClient(
        universe=[
            {"name": "BTC", "maxLeverage": 50},
            {"name": "ETH", "maxLeverage": 25},
            {"name": "NOTCOMMON", "maxLeverage": 10},
        ]
    )
    cfg = Settings(
        _env_file=None,
        hyperliquid_account_address="0x" + "5" * 40,
        hyperliquid_prewarm_common_coins="BTC,ETH,SOL",
    )

    candidates = asyncio.run(_merge_common_market_prewarm_candidates([], settings=cfg, client=client))

    assert candidates == [
        {
            "dex": "",
            "dex_display_name": "Hyperliquid",
            "canonical_coin": "BTC",
            "asset_id": 0,
            "market_max_leverage": 50,
            "source": "common_market_prewarm",
            "baseline_status": None,
            "risk_setting_required": False,
            "reason": "default Hyperliquid common market prewarm",
            "prepare_risk_setting": True,
        },
        {
            "dex": "",
            "dex_display_name": "Hyperliquid",
            "canonical_coin": "ETH",
            "asset_id": 1,
            "market_max_leverage": 25,
            "source": "common_market_prewarm",
            "baseline_status": None,
            "risk_setting_required": False,
            "reason": "default Hyperliquid common market prewarm",
            "prepare_risk_setting": True,
        },
    ]


def test_follower_migration_seeds_risk_templates_without_reusing_old_confirmation() -> None:
    old_account = "0x" + "1" * 40
    new_account = "0x" + "2" * 40
    source_rows = [
        MarketRiskSetting(
            id=1,
            execution_venue="HYPERLIQUID",
            account_address=old_account,
            dex="",
            canonical_coin="BTC",
            asset_id=0,
            desired_margin_mode=DESIRED_MARGIN_MODE,
            desired_leverage=10,
            market_max_leverage=50,
            effective_leverage=10,
            actual_margin_mode=DESIRED_MARGIN_MODE,
            actual_leverage=10,
            status=STATUS_CONFIRMED,
            last_confirmed_at=datetime.now(timezone.utc),
        ),
        MarketRiskSetting(
            id=2,
            execution_venue="HYPERLIQUID",
            account_address=old_account,
            dex="xyz",
            canonical_coin="xyz:CL",
            asset_id=99,
            desired_margin_mode=FALLBACK_MARGIN_MODE,
            desired_leverage=4,
            market_max_leverage=20,
            effective_leverage=4,
            actual_margin_mode=FALLBACK_MARGIN_MODE,
            actual_leverage=4,
            status=STATUS_CONFIRMED,
            last_confirmed_at=datetime.now(timezone.utc),
        ),
    ]
    db = FakeRiskRowsDb(source_rows=source_rows)

    payload = asyncio.run(
        seed_market_risk_settings_for_account_migration(
            db=db,
            previous_account_address=old_account,
            new_account_address=new_account,
            desired_default_leverage=10,
        )
    )

    assert payload["source_count"] == 2
    assert payload["seeded_count"] == 2
    assert len(db.added) == 2
    btc, cl = db.added
    assert btc.account_address == new_account
    assert btc.status == STATUS_NEEDS_REFRESH
    assert btc.actual_margin_mode is None
    assert btc.actual_leverage is None
    assert cl.desired_margin_mode == FALLBACK_MARGIN_MODE
    assert cl.desired_leverage == 3
    assert cl.effective_leverage == 3
    assert cl.status == STATUS_NEEDS_REFRESH
    assert cl.actual_margin_mode is None


def test_follower_migration_confirms_seeded_risk_templates_for_new_account() -> None:
    old_account = "0x" + "1" * 40
    new_account = "0x" + "2" * 40
    source_rows = [
        MarketRiskSetting(
            id=1,
            execution_venue="HYPERLIQUID",
            account_address=old_account,
            dex="",
            canonical_coin="BTC",
            asset_id=0,
            desired_margin_mode=DESIRED_MARGIN_MODE,
            desired_leverage=10,
            market_max_leverage=50,
            effective_leverage=10,
            actual_margin_mode=DESIRED_MARGIN_MODE,
            actual_leverage=10,
            status=STATUS_CONFIRMED,
            last_confirmed_at=datetime.now(timezone.utc),
        ),
        MarketRiskSetting(
            id=2,
            execution_venue="HYPERLIQUID",
            account_address=old_account,
            dex="xyz",
            canonical_coin="xyz:CL",
            asset_id=99,
            desired_margin_mode=FALLBACK_MARGIN_MODE,
            desired_leverage=4,
            market_max_leverage=20,
            effective_leverage=4,
            actual_margin_mode=FALLBACK_MARGIN_MODE,
            actual_leverage=4,
            status=STATUS_CONFIRMED,
            last_confirmed_at=datetime.now(timezone.utc),
        ),
    ]
    db = FakeRiskRowsDb(source_rows=source_rows)
    client = FakeClient()

    payload = asyncio.run(
        prepare_migrated_hyperliquid_risk_settings(
            db=db,
            settings=settings(),
            client=client,
            previous_account_address=old_account,
            new_account_address=new_account,
        )
    )

    assert payload["seeded_count"] == 2
    assert payload["confirmed_count"] == 2
    assert payload["blockers"] == []
    assert client.updates == [
        {"coin": "BTC", "leverage": 10, "is_cross": True},
        {"coin": "xyz:CL", "leverage": 3, "is_cross": False},
    ]
    btc, cl = db.added
    assert btc.status == STATUS_CONFIRMED
    assert btc.actual_margin_mode == DESIRED_MARGIN_MODE
    assert btc.actual_leverage == 10
    assert cl.status == STATUS_CONFIRMED
    assert cl.actual_margin_mode == FALLBACK_MARGIN_MODE
    assert cl.actual_leverage == 3
