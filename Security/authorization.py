"""
security/authorization.py — Grid Master OS Phase 6
Role-Based Access Control (RBAC) enforcement.
Provides require_permission() decorator for Flask routes.
"""
import functools
import logging
from flask import request, jsonify, g

from security.auth        import verify
from security.permissions import has_permission
from security.audit       import log_event, AuditEvent

log = logging.getLogger("gridmaster.security.authorization")


def require_permission(action: str):
    """
    Flask route decorator. Verifies authentication and checks RBAC permission.

    Usage:
        @app.route("/run", methods=["POST"])
        @require_permission(Action.RUN_TASK)
        def run_task_route():
            identity = g.identity   # available after auth
            ...

    Returns 401 if no valid credentials, 403 if role lacks permission.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            identity = verify(request)

            if identity is None:
                log_event(AuditEvent.AUTH_FAILURE,
                          detail=f"No valid credentials for {request.path}",
                          ip=_client_ip())
                return jsonify({
                    "status": "error",
                    "error":  "Authentication required",
                    "code":   401,
                }), 401

            role = identity.get("role", "viewer")
            if not has_permission(role, action):
                log_event(AuditEvent.PERMISSION_DENIED,
                          detail=f"role={role} action={action} path={request.path}",
                          ip=_client_ip(),
                          user=identity.get("owner", "unknown"))
                return jsonify({
                    "status": "error",
                    "error":  f"Role '{role}' does not have permission for '{action}'",
                    "code":   403,
                }), 403

            # Store identity in Flask's request context for handler use
            g.identity = identity
            log_event(AuditEvent.API_REQUEST,
                      detail=f"{request.method} {request.path}",
                      ip=_client_ip(),
                      user=identity.get("owner", "unknown"),
                      role=role)
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def check_permission(identity: dict, action: str) -> bool:
    """Non-decorator helper for programmatic permission checks."""
    role = identity.get("role", "viewer") if identity else "viewer"
    return has_permission(role, action)


def _client_ip() -> str:
    """Extract client IP from request, respecting proxy headers."""
    return (request.headers.get("X-Forwarded-For", "")
            .split(",")[0].strip()
            or request.remote_addr
            or "unknown")
