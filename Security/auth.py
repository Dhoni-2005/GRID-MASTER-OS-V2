"""
security/auth.py — Grid Master OS Phase 6
Authentication: API Key and Bearer Token validation.
Extensible design — add new methods without changing the verify() interface.
"""
import hashlib
import hmac
import logging
import secrets
import time
from datetime import datetime, timezone

from security.config    import SECRET_KEY, TOKEN_TTL_SECONDS
from security.api_keys  import validate_api_key

log = logging.getLogger("gridmaster.security.auth")

# In-memory token store: token → metadata
# Phase 7+: replace with Redis or database-backed store
_TOKEN_STORE: dict[str, dict] = {}


# ── BEARER TOKEN ──────────────────────────────────────────────

def issue_token(role: str = "viewer",
                owner: str = "unknown",
                ttl: int | None = None) -> dict:
    """
    Issue a signed Bearer token.
    Returns {"token": str, "expires_at": str, "role": str}.
    """
    ttl        = ttl if ttl is not None else TOKEN_TTL_SECONDS
    raw        = secrets.token_hex(32)
    expires_at = int(time.time()) + ttl
    sig        = hmac.new(
        SECRET_KEY.encode(), f"{raw}:{expires_at}:{role}".encode(),
        hashlib.sha256
    ).hexdigest()
    signed = f"{raw}.{expires_at}.{sig}"

    _TOKEN_STORE[raw] = {
        "role":       role,
        "owner":      owner,
        "expires_at": expires_at,
        "revoked":    False,
    }
    return {
        "token":      signed,
        "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
        "role":       role,
    }


def validate_token(signed_token: str) -> dict | None:
    """
    Validate a Bearer token. Returns metadata on success, None on failure.
    Checks signature, expiry, and revocation.
    """
    try:
        parts = signed_token.split(".")
        if len(parts) != 3:
            return None
        raw, expires_str, provided_sig = parts
        expires_at = int(expires_str)
    except (ValueError, AttributeError):
        return None

    # Verify HMAC
    meta = _TOKEN_STORE.get(raw)
    if meta is None or meta.get("revoked"):
        return None

    role = meta["role"]
    expected_sig = hmac.new(
        SECRET_KEY.encode(), f"{raw}:{expires_at}:{role}".encode(),
        hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(provided_sig, expected_sig):
        return None

    # Check expiry
    if time.time() > expires_at:
        return None

    return meta


def revoke_token(signed_token: str) -> bool:
    """Revoke a Bearer token. Returns True if found."""
    try:
        raw = signed_token.split(".")[0]
    except (IndexError, AttributeError):
        return False
    meta = _TOKEN_STORE.get(raw)
    if meta is None:
        return False
    meta["revoked"] = True
    return True


# ── UNIFIED VERIFY ────────────────────────────────────────────

def verify(request) -> dict | None:
    """
    Attempt to authenticate a Flask request using any supported method.
    Tries (in order): API Key header, Bearer token.
    Returns identity metadata dict on success, None on failure.

    Metadata dict always contains "role" and "owner" keys.

    Extension point: add new authentication methods here
    (e.g. session cookies, client certificates) without
    changing how the rest of the system calls verify().
    """
    # ── Method 1: X-API-Key header ────────────────────────────
    api_key = request.headers.get("X-API-Key", "").strip()
    if api_key:
        meta = validate_api_key(api_key)
        if meta:
            log.debug("Authenticated via API key: key_id=%s role=%s",
                      meta.get("key_id"), meta.get("role"))
            return {**meta, "auth_method": "api_key"}
        log.warning("Invalid API key presented")
        return None

    # ── Method 2: Authorization: Bearer <token> header ────────
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
        meta  = validate_token(token)
        if meta:
            log.debug("Authenticated via Bearer token: role=%s", meta.get("role"))
            return {**meta, "auth_method": "bearer"}
        log.warning("Invalid or expired Bearer token presented")
        return None

    # No credentials provided
    return None
