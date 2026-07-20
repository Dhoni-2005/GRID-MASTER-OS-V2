"""
security/encryption.py — Grid Master OS Phase 6
Symmetric encryption for sensitive config values at rest.
Uses Fernet (AES-128-CBC + HMAC-SHA256) from the cryptography library.
"""
import logging
from cryptography.fernet import Fernet, InvalidToken
from security.config import FERNET_KEY

log = logging.getLogger("gridmaster.security.encryption")
_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is not None:
        return _fernet
    key = FERNET_KEY.encode() if FERNET_KEY else None
    if not key:
        log.warning(
            "GRIDMASTER_FERNET_KEY not set — generating ephemeral key. "
            "Encrypted values will not survive restart."
        )
        key = Fernet.generate_key()
    _fernet = Fernet(key)
    return _fernet


def generate_key() -> str:
    """Generate a new Fernet key for use as GRIDMASTER_FERNET_KEY."""
    return Fernet.generate_key().decode()


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext string. Returns a URL-safe base64 token."""
    if not isinstance(plaintext, str):
        raise TypeError("plaintext must be a string")
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(token: str) -> str:
    """Decrypt a token from encrypt(). Raises ValueError on invalid token."""
    if not isinstance(token, str):
        raise TypeError("token must be a string")
    try:
        return _get_fernet().decrypt(token.encode()).decode()
    except InvalidToken:
        raise ValueError("Invalid or tampered encryption token")


def encrypt_dict(data: dict) -> dict:
    """Encrypt all string values in a dict. Non-strings passed through."""
    return {k: (encrypt(v) if isinstance(v, str) else v) for k, v in data.items()}


def decrypt_dict(data: dict) -> dict:
    """Decrypt all string values in a dict produced by encrypt_dict()."""
    result = {}
    for k, v in data.items():
        if isinstance(v, str):
            try:
                result[k] = decrypt(v)
            except Exception:
                result[k] = v
        else:
            result[k] = v
    return result
