from __future__ import annotations

import base64
import hashlib
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _key(master_key: str) -> bytes:
    return hashlib.sha256(master_key.encode("utf-8")).digest()


def encrypt_text(plaintext: str, master_key: str) -> str:
    nonce = os.urandom(12)
    aes = AESGCM(_key(master_key))
    ciphertext = aes.encrypt(nonce, plaintext.encode("utf-8"), None)
    return base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")


def decrypt_text(token: str, master_key: str) -> str:
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    nonce, ciphertext = raw[:12], raw[12:]
    aes = AESGCM(_key(master_key))
    return aes.decrypt(nonce, ciphertext, None).decode("utf-8")

