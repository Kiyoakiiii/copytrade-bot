from datetime import datetime, timezone
from decimal import Decimal

from app.services.leader_state import parse_leader_state
from app.services.target_position import PositionSide


def test_parse_leader_state_positions_and_leverage_display_only() -> None:
    state = parse_leader_state(
        "0xLeader",
        {
            "marginSummary": {
                "accountValue": "80000",
                "totalNtlPos": "40000",
                "totalMarginUsed": "1000",
            },
            "withdrawable": "79000",
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1",
                        "positionValue": "40000",
                        "entryPx": "39000",
                        "unrealizedPnl": "1000",
                        "leverage": {"type": "isolated", "value": 30},
                    }
                }
            ],
        },
        updated_at=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert state.leader_address == "0xleader"
    assert state.account_value == Decimal("80000")
    assert state.withdrawable == Decimal("79000")
    assert state.positions[0].side == PositionSide.LONG
    assert state.positions[0].notional == Decimal("40000")
    assert state.positions[0].mark_price == Decimal("40000.00000000")
    assert state.positions[0].leverage == Decimal("30")

