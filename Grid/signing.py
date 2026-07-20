"""
grid/signing.py — Grid Master OS Phase 7  [RC-1]
HMAC-SHA256 signing and verification for all inter-node payloads.
Single responsibility: payload integrity. No networking, no models, no DB.

Shared-secret architecture: master and worker both hold NODE_SECRET.
Master signs TaskAssignment payloads; worker verifies before executing.
Worker signs ResultPayload; master verifies before recording.
"""
import hashlib
import hmac
import logging

log = logging.getLogger("gridmaster.grid.signing")

# Field key-order for deterministic canonical strings
_ASSIGNMENT_FIELDS: tuple[str, ...] = (
    "task_id", "project_id", "title", "input_data", "priority", "assigned_at"
)
_RESULT_FIELDS: tuple[str, ...] = (
    "task_id", "node_id", "status", "output", "completed_at"
)


# ── PRIVATE HELPERS ───────────────────────────────────────────

def _canonical_string(data: dict, key_order: tuple[str, ...]) -> str:
    """
    Build a deterministic string from specified dict fields for HMAC input.
    Fields are joined as 'key=value' pairs separated by '|'.
    Only the keys listed in key_order are included; order is fixed.
    Missing keys produce an empty string value — callers should ensure
    all keys are present by validating the payload before signing.
    """
    parts = [f"{k}={data.get(k, '')}" for k in key_order]
    return "|".join(parts)


def _compute_hmac(canonical: str, secret: str) -> str:
    """Compute HMAC-SHA256 hex digest of a canonical string."""
    return hmac.new(
        secret.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


# ── PUBLIC API ────────────────────────────────────────────────

def sign_assignment(assignment: dict, secret: str) -> str:
    """
    Compute HMAC-SHA256 signature for a TaskAssignment payload dict.

    Parameters
    ----------
    assignment : dict produced by TaskAssignment.to_dict() (signature field
                 may be absent or empty — it is excluded from the input)
    secret     : NODE_SECRET shared between master and worker

    Returns
    -------
    Hex digest string (64 characters)

    Raises
    ------
    ValueError  if secret is empty
    """
    if not secret:
        raise ValueError("Cannot sign assignment: NODE_SECRET is empty")
    canonical = _canonical_string(assignment, _ASSIGNMENT_FIELDS)
    return _compute_hmac(canonical, secret)


def verify_assignment(assignment: dict, signature: str, secret: str) -> bool:
    """
    Verify an HMAC-SHA256 signature for a TaskAssignment payload.
    Uses hmac.compare_digest() for timing-safe comparison.

    Returns True if signature is valid, False otherwise.
    Never raises — invalid input returns False and logs a warning.
    """
    if not secret or not signature:
        log.warning("verify_assignment: empty secret or signature")
        return False
    try:
        expected = sign_assignment(assignment, secret)
        return hmac.compare_digest(expected, signature)
    except Exception as exc:
        log.warning("verify_assignment: unexpected error: %s", exc)
        return False


def sign_result(result: dict, secret: str) -> str:
    """
    Compute HMAC-SHA256 signature for a ResultPayload dict.

    Parameters
    ----------
    result : dict produced by ResultPayload.to_dict() (signature field excluded)
    secret : NODE_SECRET

    Returns
    -------
    Hex digest string (64 characters)

    Raises
    ------
    ValueError  if secret is empty
    """
    if not secret:
        raise ValueError("Cannot sign result: NODE_SECRET is empty")
    canonical = _canonical_string(result, _RESULT_FIELDS)
    return _compute_hmac(canonical, secret)


def verify_result(result: dict, signature: str, secret: str) -> bool:
    """
    Verify an HMAC-SHA256 signature for a ResultPayload dict.
    Uses hmac.compare_digest() for timing-safe comparison.

    Returns True if valid, False otherwise. Never raises.
    """
    if not secret or not signature:
        log.warning("verify_result: empty secret or signature")
        return False
    try:
        expected = sign_result(result, secret)
        return hmac.compare_digest(expected, signature)
    except Exception as exc:
        log.warning("verify_result: unexpected error: %s", exc)
        return False
