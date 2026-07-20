"""
security/api_keys.py — Grid Master OS Phase 6
API key generation, storage, and validation.
Keys are stored as SHA-256 hashes — the raw key is shown once at creation.
"""
import hashlib
import secrets
import logging
from datetime import datetime, timezone

from security.config import API_KEY_PREFIX, API_KEY_LENGTH

log = logging.getLogger("gridmaster.security.api_keys")

# In-memory store: hashed_key → metadata dict
# Phase 7+: replace with database-backed store
_KEY_STORE: dict[str, dict] = {}


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def create_api_key(role: str = "viewer",
                   description: str = "",
                   owner: str = "unknown") -> dict:
    """
    Generate a new API key. Returns the raw key ONCE — it is not stored.
    Subsequent lookups use the hashed form only.

    Returns:
        {
          "key":         "gm_<hex>",   ← show to user, never stored
          "key_id":      str,          ← short prefix for identification
          "role":        str,
          "description": str,
          "owner":       str,
          "created_at":  str,
        }
    """
    raw     = API_KEY_PREFIX + secrets.token_hex(API_KEY_LENGTH)
    hashed  = _hash_key(raw)
    key_id  = raw[:12]   # "gm_" + 9 chars — safe to log
    now     = datetime.now(timezone.utc).isoformat()

    _KEY_STORE[hashed] = {
        "key_id":      key_id,
        "role":        role,
        "description": description,
        "owner":       owner,
        "created_at":  now,
        "last_used":   None,
        "revoked":     False,
    }
    log.info("API key created: key_id=%s role=%s owner=%s", key_id, role, owner)
    return {
        "key":         raw,
        "key_id":      key_id,
        "role":        role,
        "description": description,
        "owner":       owner,
        "created_at":  now,
    }


def validate_api_key(raw_key: str) -> dict | None:
    """
    Validate an API key. Returns the metadata dict on success, None on failure.
    Updates last_used timestamp on success.
    """
    if not raw_key or not raw_key.startswith(API_KEY_PREFIX):
        return None
    hashed = _hash_key(raw_key)
    meta   = _KEY_STORE.get(hashed)
    if meta is None or meta.get("revoked"):
        return None
    meta["last_used"] = datetime.now(timezone.utc).isoformat()
    return meta


def revoke_api_key(raw_key: str) -> bool:
    """Revoke an API key by raw value. Returns True if found and revoked."""
    hashed = _hash_key(raw_key)
    meta   = _KEY_STORE.get(hashed)
    if meta is None:
        return False
    meta["revoked"] = True
    log.info("API key revoked: key_id=%s", meta.get("key_id"))
    return True


def list_api_keys() -> list[dict]:
    """Return metadata for all non-revoked keys (raw keys never included)."""
    return [
        {k: v for k, v in meta.items() if k != "revoked"}
        for meta in _KEY_STORE.values()
        if not meta.get("revoked")
    ]


def seed_default_keys() -> dict:
    """
    Create default admin and node keys for bootstrap / testing.
    Returns the raw keys — call once at startup in non-production.
    """
    admin = create_api_key(role="admin",    description="Default admin key",    owner="system")
    node  = create_api_key(role="node",     description="Default node key",     owner="system")
    op    = create_api_key(role="operator", description="Default operator key", owner="system")
    return {"admin": admin["key"], "node": node["key"], "operator": op["key"]}
