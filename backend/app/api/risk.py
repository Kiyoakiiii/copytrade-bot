from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.models import AppSetting, AuditLog
from app.schemas.api import RiskPatch
from app.services.calculator import SIZING_MODE_ACCOUNT_RATIO
from app.services.order_policy import AUTO_COPY_ORDER_POLICY
from app.services.runtime_control import acquire_copy_trading_control_lock

router = APIRouter(tags=["risk"])


async def get_risk_setting(db: DbSession) -> dict:
    row = await db.get(AppSetting, "risk")
    if row:
        return {
            **(row.value or {}),
            "kill_switch_updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
    return {"kill_switch": True, "kill_switch_updated_at": None}


def _stored_risk_value(payload: dict) -> dict:
    return {"kill_switch": bool(payload.get("kill_switch", True))}


def _risk_response(stored: dict, *, settings: AppSettings) -> dict:
    kill_switch = bool(stored.get("kill_switch", True))
    live_opens_enabled = bool(
        settings.trading_enabled
        and settings.hyperliquid_trading_enabled
        and not kill_switch
    )
    if kill_switch:
        live_status = "KILL_SWITCH_ON"
        live_status_reason = "New live opens and increases are blocked."
    elif not settings.trading_enabled:
        live_status = "TRADING_DISABLED"
        live_status_reason = "TRADING_ENABLED is false."
    elif not settings.hyperliquid_trading_enabled:
        live_status = "HYPERLIQUID_DISABLED"
        live_status_reason = "HYPERLIQUID_TRADING_ENABLED is false."
    else:
        live_status = "LIVE_OPENS_ENABLED"
        live_status_reason = "New live Hyperliquid opens and increases are allowed."
    return {
        **stored,
        "live_opens_enabled": live_opens_enabled,
        "live_status": live_status,
        "live_status_reason": live_status_reason,
    }


@router.get("/risk")
async def read_risk(_: CurrentUser, db: DbSession, settings: AppSettings):
    stored = await get_risk_setting(db)
    return {
        **_risk_response(stored, settings=settings),
        "trading_enabled_env": settings.trading_enabled,
        "hyperliquid_trading_enabled_env": settings.hyperliquid_trading_enabled,
        "global_max_daily_loss": settings.global_max_daily_loss,
        "global_max_total_notional": settings.global_max_notional,
        "account_value_mode": settings.account_value_mode,
        "low_latency_required_for_live": settings.low_latency_required_for_live,
        "order_policy": AUTO_COPY_ORDER_POLICY,
        "sizing_policy": SIZING_MODE_ACCOUNT_RATIO,
    }


@router.patch("/risk")
async def patch_risk(user: CurrentUser, payload: RiskPatch, db: DbSession, settings: AppSettings):
    await acquire_copy_trading_control_lock(db)
    previous = _stored_risk_value(await get_risk_setting(db))
    current = dict(previous)
    current.update(payload.model_dump(exclude_unset=True))
    current = _stored_risk_value(current)
    stmt = (
        insert(AppSetting)
        .values(key="risk", value=current)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": current, "updated_at": func.now()},
        )
    )
    await db.execute(stmt)
    db.add(
        AuditLog(
            user_id=user.id,
            action="risk.patch",
            metadata_json={"previous": previous, "current": current},
        )
    )
    await db.commit()
    return _risk_response(await get_risk_setting(db), settings=settings)


@router.post("/kill-switch")
async def kill_switch(user: CurrentUser, db: DbSession, settings: AppSettings):
    await acquire_copy_trading_control_lock(db)
    previous = _stored_risk_value(await get_risk_setting(db))
    current = dict(previous)
    current["kill_switch"] = True
    stmt = (
        insert(AppSetting)
        .values(key="risk", value=current)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": current, "updated_at": func.now()},
        )
    )
    await db.execute(stmt)
    db.add(
        AuditLog(
            user_id=user.id,
            action="risk.kill_switch_on",
            metadata_json={"previous": previous, "current": current},
        )
    )
    await db.commit()
    return _risk_response(await get_risk_setting(db), settings=settings)
