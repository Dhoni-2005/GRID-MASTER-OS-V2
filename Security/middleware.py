"""
security/middleware.py — Grid Master OS Phase 6
Flask middleware: rate limiting, input sanitisation, security headers.
Applied globally via create_app() in interface/api.py.
"""
import time
import re
import logging
from collections import defaultdict
from flask import request, jsonify, g

from security.config import RATE_LIMIT_DEFAULT_RPM
from security.audit  import log_event, AuditEvent

log = logging.getLogger("gridmaster.security.middleware")

# ── RATE LIMITER ──────────────────────────────────────────────
# In-memory sliding window (per IP + per API key).
# Phase 7+: replace with Redis for distributed rate limiting.

_rate_windows: dict[str, list[float]] = defaultdict(list)


def check_rate_limit(identifier: str,
                     rpm: int = RATE_LIMIT_DEFAULT_RPM) -> bool:
    """
    Sliding-window rate limit. Returns True if request is allowed.
    identifier: IP address or API key prefix.
    rpm: allowed requests per minute.
    """
    now    = time.monotonic()
    window = _rate_windows[identifier]

    # Evict entries older than 60 seconds
    cutoff = now - 60.0
    _rate_windows[identifier] = [t for t in window if t > cutoff]

    if len(_rate_windows[identifier]) >= rpm:
        return False  # rate limit exceeded

    _rate_windows[identifier].append(now)
    return True


# ── INPUT SANITISATION ────────────────────────────────────────

# Characters / patterns not allowed in task titles and inputs
_DANGEROUS_PATTERNS = [
    re.compile(r"<script.*?>.*?</script>", re.IGNORECASE | re.DOTALL),
    re.compile(r"javascript:", re.IGNORECASE),
    re.compile(r"on\w+\s*=",  re.IGNORECASE),       # onclick=, etc.
    re.compile(r"--\s*$",     re.MULTILINE),         # SQL comment
    re.compile(r";\s*drop\s+table", re.IGNORECASE),  # SQL injection
]


def sanitise(value: str | None) -> str:
    """
    Sanitise a user-supplied string.
    - Strips leading/trailing whitespace.
    - Rejects strings containing dangerous patterns.
    Returns the cleaned string or raises ValueError.
    """
    if value is None:
        return ""
    value = str(value).strip()
    for pattern in _DANGEROUS_PATTERNS:
        if pattern.search(value):
            raise ValueError(
                f"Input contains a disallowed pattern: {pattern.pattern[:40]}"
            )
    return value


def sanitise_request_body(body: dict,
                           text_fields: list[str] | None = None) -> dict:
    """
    Sanitise specified text fields in a request body dict.
    Returns cleaned dict. Raises ValueError on dangerous input.
    """
    text_fields = text_fields or ["title", "input_data", "description"]
    cleaned = dict(body)
    for field in text_fields:
        if field in cleaned and isinstance(cleaned[field], str):
            cleaned[field] = sanitise(cleaned[field])
    return cleaned


# ── SECURITY HEADERS ──────────────────────────────────────────

def apply_security_headers(response):
    """
    Attach security headers to every outgoing HTTP response.
    Called via app.after_request in create_app().
    """
    response.headers["X-Content-Type-Options"]  = "nosniff"
    response.headers["X-Frame-Options"]          = "DENY"
    response.headers["X-XSS-Protection"]         = "1; mode=block"
    response.headers["Referrer-Policy"]           = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"]             = "no-store, no-cache, must-revalidate"
    response.headers["Content-Security-Policy"]   = "default-src 'self'"
    return response


# ── MIDDLEWARE REGISTRATION ───────────────────────────────────

def register_middleware(app) -> None:
    """
    Register all middleware hooks on the Flask app.
    Call this once inside create_app() after route registration.
    """

    @app.before_request
    def _rate_limit_check():
        # Identify caller by API key prefix or IP
        api_key    = request.headers.get("X-API-Key", "")
        identifier = api_key[:12] if api_key else (request.remote_addr or "unknown")

        if not check_rate_limit(identifier):
            log_event(AuditEvent.RATE_LIMITED,
                      detail=f"{request.method} {request.path}",
                      ip=request.remote_addr or "unknown")
            return jsonify({
                "status": "error",
                "error":  "Rate limit exceeded. Try again in 60 seconds.",
                "code":   429,
            }), 429

    @app.after_request
    def _security_headers(response):
        return apply_security_headers(response)
