from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.core.crypto import decrypt_text, encrypt_text
from app.core.security import (
    expires_at,
    generate_totp_secret,
    hash_password,
    new_token,
    sha256_hex,
    sign_session_token,
    totp_uri,
    unsign_session_token,
    utcnow,
    verify_password,
    verify_totp,
)
from app.models import Session as SessionModel
from app.models import User
from app.schemas.api import LoginRequest, TotpVerifyRequest

router = APIRouter(prefix="/auth", tags=["auth"])


async def bootstrap_admin(db: AsyncSession, settings: AppSettings) -> None:
    result = await db.execute(select(User).where(User.email == settings.admin_email))
    if result.scalar_one_or_none():
        return
    bootstrap = settings.admin_password_bootstrap
    if not bootstrap:
        return
    user = User(
        email=settings.admin_email,
        password_hash=hash_password(bootstrap.get_secret_value()),
        is_active=True,
        is_admin=True,
    )
    db.add(user)
    await db.commit()


@router.post("/login")
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    db: DbSession,
    settings: AppSettings,
):
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        if user:
            user.failed_login_count += 1
            if user.failed_login_count >= 5:
                user.locked_until = utcnow() + timedelta(minutes=15)
            await db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="bad credentials")

    if user.locked_until and user.locked_until > utcnow():
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="login locked")

    if user.totp_enabled:
        if not payload.totp_code or not user.totp_secret_encrypted:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="totp required")
        secret = decrypt_text(
            user.totp_secret_encrypted,
            settings.encryption_master_key.get_secret_value(),
        )
        if not verify_totp(secret, payload.totp_code):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="bad totp")

    user.failed_login_count = 0
    user.locked_until = None
    raw_token = new_token()
    db.add(
        SessionModel(
            user_id=user.id,
            token_hash=sha256_hex(raw_token),
            expires_at=expires_at(settings.session_ttl_seconds),
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
    )
    await db.commit()

    csrf_token = new_token()
    signed = sign_session_token(raw_token, settings.app_secret_key.get_secret_value())
    response.set_cookie(
        settings.session_cookie_name,
        signed,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="strict",
        max_age=settings.session_ttl_seconds,
    )
    response.set_cookie(
        settings.csrf_cookie_name,
        csrf_token,
        httponly=False,
        secure=settings.cookie_secure,
        samesite="strict",
        max_age=settings.session_ttl_seconds,
    )
    return {"ok": True, "totp_enabled": user.totp_enabled}


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    db: DbSession,
    settings: AppSettings,
):
    signed_token = request.cookies.get(settings.session_cookie_name)
    if signed_token:
        raw = unsign_session_token(
            signed_token,
            settings.app_secret_key.get_secret_value(),
            settings.session_ttl_seconds,
        )
        if raw:
            result = await db.execute(
                select(SessionModel).where(SessionModel.token_hash == sha256_hex(raw))
            )
            session = result.scalar_one_or_none()
            if session:
                session.revoked_at = utcnow()
                await db.commit()
    response.delete_cookie(settings.session_cookie_name)
    response.delete_cookie(settings.csrf_cookie_name)
    return {"ok": True}


@router.post("/totp/setup")
async def setup_totp(user: CurrentUser, db: DbSession, settings: AppSettings):
    secret = generate_totp_secret()
    user.totp_secret_encrypted = encrypt_text(
        secret, settings.encryption_master_key.get_secret_value()
    )
    user.totp_enabled = False
    await db.commit()
    return {"secret": secret, "uri": totp_uri(user.email, secret)}


@router.post("/totp/verify")
async def verify_totp_setup(
    payload: TotpVerifyRequest,
    user: CurrentUser,
    db: DbSession,
    settings: AppSettings,
):
    if not user.totp_secret_encrypted:
        raise HTTPException(status_code=400, detail="totp setup required")
    secret = decrypt_text(
        user.totp_secret_encrypted, settings.encryption_master_key.get_secret_value()
    )
    if not verify_totp(secret, payload.code):
        raise HTTPException(status_code=400, detail="bad totp")
    user.totp_enabled = True
    await db.commit()
    return {"ok": True}
