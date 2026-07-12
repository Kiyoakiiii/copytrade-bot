from __future__ import annotations

from typing import Annotated

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.core.security import sha256_hex, unsign_session_token, utcnow
from app.db.session import get_session
from app.models import Session as SessionModel
from app.models import User


DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]


async def current_user(
    request: Request,
    db: DbSession,
    settings: AppSettings,
    session_cookie: str | None = Cookie(default=None, alias="copytrade_session"),
) -> User:
    cookie_name = settings.session_cookie_name
    signed_token = session_cookie or request.cookies.get(cookie_name)
    if not signed_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="not authenticated")
    raw_token = unsign_session_token(
        signed_token,
        settings.app_secret_key.get_secret_value(),
        settings.session_ttl_seconds,
    )
    if not raw_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid session")

    query = (
        select(SessionModel, User)
        .join(User, User.id == SessionModel.user_id)
        .where(SessionModel.token_hash == sha256_hex(raw_token))
        .where(SessionModel.revoked_at.is_(None))
        .where(SessionModel.expires_at > utcnow())
        .where(User.is_active.is_(True))
    )
    row = (await db.execute(query)).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="session expired")
    return row[1]


CurrentUser = Annotated[User, Depends(current_user)]

