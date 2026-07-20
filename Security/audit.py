"""
security/audit.py — Grid Master OS Phase 6
Structured audit logging for security events.
Records: auth attempts, API requests, permission denials, admin actions.
"""
import json
import logging
import logging.handlers
from datetime import datetime, timezone

from security.config import AUDIT_LOG_FILE, AUDIT_ENABLED

# ── AUDIT EVENT TYPES ─────────────────────────────────────────

class AuditEvent:
    AUTH_SUCCESS      = "auth_success"
    AUTH_FAILURE      = "auth_failure"
    API_REQUEST       = "api_request"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMITED      = "rate_limited"
    KEY_CREATED       = "key_created"
    KEY_REVOKED       = "key_revoked"
    TOKEN_ISSUED      = "token_issued"
    TOKEN_REVOKED     = "token_revoked"
    ADMIN_ACTION      = "admin_action"
    TASK_SUBMITTED    = "task_submitted"
    CONFIG_CHANGE     = "config_change"


# ── AUDIT LOGGER SETUP ────────────────────────────────────────

_audit_log: logging.Logger = logging.getLogger("gridmaster.audit")
_configured = False


def _setup_audit_logger() -> None:
    global _configured
    if _configured:
        return
    _audit_log.setLevel(logging.INFO)
    _audit_log.propagate = False   # audit log is separate from app log

    # File handler — rotates at 10 MB, keeps 5 backups
    try:
        fh = logging.handlers.RotatingFileHandler(
            AUDIT_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        fh.setFormatter(logging.Formatter("%(message)s"))
        _audit_log.addHandler(fh)
    except (OSError, PermissionError) as e:
        logging.getLogger("gridmaster.security.audit").warning(
            "Could not open audit log file %s: %s — audit to stderr only",
            AUDIT_LOG_FILE, e,
        )

    # Always mirror to stderr at DEBUG level so tests can capture output
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(logging.Formatter("[AUDIT] %(message)s"))
    _audit_log.addHandler(sh)
    _configured = True


def log_event(event_type: str,
              detail:     str  = "",
              ip:         str  = "unknown",
              user:       str  = "unknown",
              role:       str  = "",
              **extra) -> None:
    """
    Record a structured audit event.
    Each event is a single JSON line in the audit log.
    """
    if not AUDIT_ENABLED:
        return
    _setup_audit_logger()
    record = {
        "ts":    datetime.now(timezone.utc).isoformat(),
        "event": event_type,
        "user":  user,
        "ip":    ip,
        "role":  role,
        "detail": detail,
    }
    record.update(extra)
    _audit_log.info(json.dumps(record, default=str))


def get_recent_events(limit: int = 50) -> list[dict]:
    """
    Read the last N events from the audit log file.
    Returns parsed dicts; skips unparseable lines.
    """
    events: list[dict] = []
    try:
        with open(AUDIT_LOG_FILE) as f:
            lines = f.readlines()
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(events) >= limit:
                break
    except FileNotFoundError:
        pass
    return list(reversed(events))
