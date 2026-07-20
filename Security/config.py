"""
security/config.py — Grid Master OS Phase 6
Security configuration loaded from environment variables.
Never stores secrets in plain text or source code.
"""
import os
import secrets
import logging

log = logging.getLogger("gridmaster.security.config")

# ── MASTER SECRET KEY ─────────────────────────────────────────
SECRET_KEY: str = os.environ.get("GRIDMASTER_SECRET_KEY", "")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    log.warning(
        "GRIDMASTER_SECRET_KEY not set — using ephemeral key. "
        "All tokens will be invalidated on restart. "
        "Set this environment variable in production."
    )

# ── TOKEN SETTINGS ────────────────────────────────────────────
TOKEN_TTL_SECONDS: int = int(os.environ.get("GRIDMASTER_TOKEN_TTL", 3600))
TOKEN_ALGORITHM:   str = "HS256"

# ── RATE LIMITING ─────────────────────────────────────────────
RATE_LIMIT_DEFAULT_RPM: int = int(
    os.environ.get("GRIDMASTER_RATE_LIMIT_RPM", 60)
)

# ── API KEY SETTINGS ──────────────────────────────────────────
API_KEY_PREFIX: str = "gm_"
API_KEY_LENGTH: int = 32   # bytes; hex-encoded → 64 chars

# ── ENCRYPTION ────────────────────────────────────────────────
FERNET_KEY: str = os.environ.get("GRIDMASTER_FERNET_KEY", "")

# ── AUDIT SETTINGS ────────────────────────────────────────────
AUDIT_LOG_FILE: str = os.environ.get("GRIDMASTER_AUDIT_LOG", "gridmaster_audit.log")
AUDIT_ENABLED:  bool = os.environ.get("GRIDMASTER_AUDIT", "1") != "0"

# ── ROLES ─────────────────────────────────────────────────────
ROLES = ["admin", "operator", "viewer", "node"]

# ── ENVIRONMENT ───────────────────────────────────────────────
IS_PRODUCTION: bool = os.environ.get("GRIDMASTER_ENV", "dev") == "production"
