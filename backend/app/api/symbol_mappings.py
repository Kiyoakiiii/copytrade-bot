from __future__ import annotations

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.api.deps import CurrentUser, DbSession
from app.models import SymbolMapping
from app.schemas.api import SymbolMappingPatch

router = APIRouter(prefix="/symbol-mappings", tags=["symbol-mappings"])


@router.get("")
async def list_symbol_mappings(_: CurrentUser, db: DbSession):
    result = await db.execute(select(SymbolMapping).order_by(SymbolMapping.hyperliquid_coin))
    return result.scalars().all()


@router.patch("/{mapping_id}")
async def patch_symbol_mapping(
    mapping_id: int, _: CurrentUser, payload: SymbolMappingPatch, db: DbSession
):
    mapping = await db.get(SymbolMapping, mapping_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="mapping not found")
    for key, value in payload.model_dump(exclude_unset=True).items():
        setattr(mapping, key, value)
    await db.commit()
    await db.refresh(mapping)
    return mapping

