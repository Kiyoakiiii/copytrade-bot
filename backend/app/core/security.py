from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

import pyotp
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except VerifyMismatchError:
        return False


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def new_token() -> str:
    return secrets.token_urlsafe(32)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def expires_at(ttl_seconds: int) -> datetime:
    return utcnow() + timedelta(seconds=ttl_seconds)


def generate_totp_secret() -> str:
    return pyotp.random_base32()


def totp_uri(email: str, secret: str, issuer: str = "copytrade-bot") -> str:
    return pyotp.TOTP(secret).provisioning_uri(name=email, issuer_name=issuer)


def verify_totp(secret: str, code: str) -> bool:
    return pyotp.TOTP(secret).verify(code, valid_window=1)


def make_serializer(secret_key: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=secret_key, salt="copytrade-session")


def sign_session_token(raw_token: str, secret_key: str) -> str:
    return make_serializer(secret_key).dumps(raw_token)


def unsign_session_token(
    signed_token: str, secret_key: str, max_age_seconds: int
) -> str | None:
    try:
        return make_serializer(secret_key).loads(signed_token, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None

