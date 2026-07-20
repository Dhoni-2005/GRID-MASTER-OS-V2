"""
grid/client.py — Grid Master OS Phase 7
HTTP client for all distributed worker → master communication.
Uses stdlib urllib only — no third-party HTTP libraries.

Responsibilities:
  • Worker registration
  • Heartbeat transmission
  • Poll master for work
  • Submit completed task results
  • Sync memory entries to master
  • Query task status for reconciliation
  • Exponential backoff retry policy
  • HMAC signing via grid.signing
  • Structured error handling
  • No business logic

All methods raise GridClientError subclasses on unrecoverable failure.
Transient failures are retried with exponential backoff before raising.
"""
import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any
from datetime import datetime, timezone

from grid.config import DISPATCH_TIMEOUT_SECONDS, MASTER_URL
from grid.models import (
    HeartbeatPayload,
    NodeInfo,
    PollResponse,
    RegistrationResponse,
    ResultPayload,
)

log = logging.getLogger("gridmaster.grid.client")

# ── EXCEPTIONS ────────────────────────────────────────────────

class GridClientError(Exception):
    """Base exception for all HTTP client errors."""
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class GridAuthError(GridClientError):
    """Raised when the server returns 401 Unauthorized."""


class GridForbiddenError(GridClientError):
    """Raised when the server returns 403 Forbidden."""


class GridNotFoundError(GridClientError):
    """Raised when the server returns 404 Not Found."""


class GridRateLimitError(GridClientError):
    """Raised when the server returns 429 Too Many Requests."""


class GridTimeoutError(GridClientError):
    """Raised when the HTTP request times out after all retries."""


class GridServerError(GridClientError):
    """Raised when the server returns 5xx."""


# ── RETRY POLICY ─────────────────────────────────────────────

_RETRY_DELAYS = (1.0, 2.0, 4.0)          # seconds between attempts (3 retries)
_NON_RETRYABLE = {400, 401, 403, 404, 429}  # status codes that must not be retried


# ── PRIVATE HTTP HELPERS ──────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _build_headers(api_key: str,
                   extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build standard request headers for all Grid API calls."""
    headers = {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-API-Key":     api_key,
        "X-Grid-Client": "gridmaster-worker/1.0",
    }
    if extra:
        headers.update(extra)
    return headers


def _parse_response(response) -> dict[str, Any]:
    """
    Read and parse a JSON response body.
    Raises GridClientError if the body is not valid JSON.
    """
    try:
        raw = response.read()
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise GridClientError(f"Server returned non-JSON response: {exc}") from exc


def _raise_for_status(status: int, body: dict[str, Any], url: str) -> None:
    """Map HTTP status codes to typed exceptions."""
    if status == 401:
        raise GridAuthError(
            f"Authentication failed for {url}: {body.get('error', 'Unauthorized')}",
            status_code=401,
        )
    if status == 403:
        raise GridForbiddenError(
            f"Permission denied for {url}: {body.get('error', 'Forbidden')}",
            status_code=403,
        )
    if status == 404:
        raise GridNotFoundError(
            f"Resource not found at {url}: {body.get('error', 'Not Found')}",
            status_code=404,
        )
    if status == 429:
        raise GridRateLimitError(
            f"Rate limit exceeded for {url}", status_code=429
        )
    if status >= 500:
        raise GridServerError(
            f"Server error {status} from {url}: {body.get('error', '')}",
            status_code=status,
        )
    if status >= 400:
        raise GridClientError(
            f"Client error {status} from {url}: {body.get('error', '')}",
            status_code=status,
        )


def _http_post(url: str,
               payload: dict[str, Any],
               api_key: str,
               timeout: int = DISPATCH_TIMEOUT_SECONDS,
               max_retries: int = 3) -> dict[str, Any]:
    """
    POST JSON payload to url with retry and exponential backoff.
    Non-retryable status codes (4xx) raise immediately on first attempt.

    Returns parsed response dict.
    Raises appropriate GridClientError subclass on failure.
    """
    data = json.dumps(payload, default=str).encode("utf-8")
    headers = _build_headers(api_key)
    last_exc: Exception | None = None

    for attempt, delay in enumerate([0.0] + list(_RETRY_DELAYS), start=1):
        if delay:
            log.debug("POST %s — retry %d after %.1fs", url, attempt - 1, delay)
            time.sleep(delay)
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = _parse_response(resp)
                return body
        except urllib.error.HTTPError as exc:
            try:
                raw = exc.read()
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                body = {"error": str(exc)}

            if exc.code in _NON_RETRYABLE:
                _raise_for_status(exc.code, body, url)   # raises immediately, no retry
            # 5xx: store and retry
            log.warning("POST %s HTTP %d (attempt %d): %s", url, exc.code, attempt, body.get("error",""))
            last_exc = GridServerError(f"HTTP {exc.code}: {body.get('error','')}", status_code=exc.code)
        except urllib.error.URLError as exc:
            log.warning("POST %s network error (attempt %d): %s", url, attempt, exc.reason)
            last_exc = GridClientError(f"Network error: {exc.reason}")
        except TimeoutError as exc:
            log.warning("POST %s timed out (attempt %d)", url, attempt)
            last_exc = GridTimeoutError(f"Request timed out after {timeout}s")

    raise last_exc or GridClientError(f"POST {url} failed after {max_retries} retries")


def _http_get(url: str,
              api_key: str,
              timeout: int = DISPATCH_TIMEOUT_SECONDS,
              max_retries: int = 3) -> dict[str, Any]:
    """
    GET url with retry and exponential backoff.
    Returns parsed response dict.
    Raises appropriate GridClientError subclass on failure.
    """
    headers = _build_headers(api_key)
    last_exc: Exception | None = None

    for attempt, delay in enumerate([0.0] + list(_RETRY_DELAYS), start=1):
        if delay:
            log.debug("GET %s — retry %d after %.1fs", url, attempt - 1, delay)
            time.sleep(delay)
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return _parse_response(resp)
        except urllib.error.HTTPError as exc:
            try:
                raw = exc.read()
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                body = {"error": str(exc)}

            if exc.code in _NON_RETRYABLE:
                _raise_for_status(exc.code, body, url)
            log.warning("GET %s HTTP %d (attempt %d)", url, exc.code, attempt)
            last_exc = GridServerError(f"HTTP {exc.code}: {body.get('error','')}", status_code=exc.code)
        except urllib.error.URLError as exc:
            log.warning("GET %s network error (attempt %d): %s", url, attempt, exc.reason)
            last_exc = GridClientError(f"Network error: {exc.reason}")
        except TimeoutError as exc:
            log.warning("GET %s timed out (attempt %d)", url, attempt)
            last_exc = GridTimeoutError(f"Request timed out after {timeout}s")

    raise last_exc or GridClientError(f"GET {url} failed after {max_retries} retries")


# ── PUBLIC API ────────────────────────────────────────────────

def register(master_url: str,
             info: NodeInfo,
             api_key: str,
             timeout: int = DISPATCH_TIMEOUT_SECONDS) -> RegistrationResponse:
    """
    Register this worker node with the master.

    Parameters
    ----------
    master_url : base HTTPS URL of the master node
    info       : NodeInfo dataclass describing this worker
    api_key    : node-role API key for authentication
    timeout    : HTTP timeout in seconds

    Returns
    -------
    RegistrationResponse containing registration_token and expiry

    Raises
    ------
    GridAuthError       on 401 (invalid API key)
    GridClientError     on network failure after retries
    """
    url = f"{master_url.rstrip('/')}/grid/register"
    payload = info.to_dict()
    log.info("Registering node %s with master at %s", info.node_id, url)
    body = _http_post(url, payload, api_key, timeout=timeout)
    return RegistrationResponse.from_dict(body)


def heartbeat(master_url: str,
              payload: HeartbeatPayload,
              api_key: str,
              timeout: int = 10) -> dict[str, Any]:
    """
    Send a heartbeat to the master.

    Parameters
    ----------
    master_url : base HTTPS URL of the master
    payload    : HeartbeatPayload with active task ids and metrics
    api_key    : node API key
    timeout    : HTTP timeout (short — heartbeats must be fast)

    Returns
    -------
    Dict with "status" and "server_ts" keys

    Raises
    ------
    GridAuthError     on 401 — caller should trigger re-registration
    GridNotFoundError on 404 — node not registered; caller should re-register
    GridClientError   on network failure (after retries)
    """
    url = f"{master_url.rstrip('/')}/grid/heartbeat"
    body = _http_post(url, payload.to_dict(), api_key, timeout=timeout, max_retries=1)
    log.debug("Heartbeat sent: server_ts=%s", body.get("server_ts", ""))
    return body


def poll(master_url: str,
         node_id: str,
         api_key: str,
         timeout: int = DISPATCH_TIMEOUT_SECONDS) -> PollResponse:
    """
    Poll the master for available work.

    Parameters
    ----------
    master_url : base HTTPS URL of the master
    node_id    : this worker's node_id
    api_key    : node API key
    timeout    : HTTP timeout

    Returns
    -------
    PollResponse — has_work=True with TaskAssignment, or has_work=False with wait_seconds

    Raises
    ------
    GridAuthError     on 401
    GridNotFoundError on 404 — node must re-register
    GridClientError   on network failure
    """
    url = f"{master_url.rstrip('/')}/grid/poll?node_id={node_id}"
    body = _http_get(url, api_key, timeout=timeout)
    return PollResponse.from_dict(body)


def report_result(master_url: str,
                  payload: ResultPayload,
                  api_key: str,
                  timeout: int = DISPATCH_TIMEOUT_SECONDS) -> dict[str, Any]:
    """
    Submit a completed task result to the master.

    Parameters
    ----------
    master_url : base HTTPS URL of the master
    payload    : ResultPayload — must have signature set by grid.signing
    api_key    : node API key
    timeout    : HTTP timeout

    Returns
    -------
    Dict with "status", "task_id", "recorded" keys

    Raises
    ------
    GridClientError on invalid signature (400), network failure, or server error
    """
    url = f"{master_url.rstrip('/')}/grid/result"
    body = _http_post(url, payload.to_dict(), api_key, timeout=timeout)
    log.info(
        "Result reported: task_id=%s recorded=%s",
        payload.task_id, body.get("recorded", "?"),
    )
    return body


def sync_memory(master_url: str,
                entries: list[dict[str, Any]],
                node_id: str,
                api_key: str,
                timeout: int = DISPATCH_TIMEOUT_SECONDS) -> dict[str, Any]:
    """
    Sync a batch of memory entries from this worker to the master database.
    Called by grid.memory_sync.flush_outbox() in batches of OUTBOX_FLUSH_BATCH.

    Parameters
    ----------
    master_url : base HTTPS URL of the master
    entries    : list of memory entry dicts (from outbox payloads)
    node_id    : this worker's node_id (for audit)
    api_key    : node API key
    timeout    : HTTP timeout

    Returns
    -------
    Dict with "status", "stored", "failed" keys
    """
    url = f"{master_url.rstrip('/')}/grid/memory"
    payload = {"node_id": node_id, "entries": entries}
    body = _http_post(url, payload, api_key, timeout=timeout)
    log.debug(
        "Memory sync: stored=%s failed=%s",
        body.get("stored", 0), body.get("failed", 0),
    )
    return body


def get_task_status(master_url: str,
                    task_id: int,
                    api_key: str,
                    timeout: int = DISPATCH_TIMEOUT_SECONDS) -> dict[str, Any]:
    """
    Query the master for the current status of a specific task.
    Used by grid.reconciler after reconnect to identify reassigned tasks.

    Parameters
    ----------
    master_url : base HTTPS URL of the master
    task_id    : integer task id to query
    api_key    : node API key
    timeout    : HTTP timeout

    Returns
    -------
    Dict with "status", "task_id", "task_status", "assigned_node_id" keys
    """
    url = f"{master_url.rstrip('/')}/grid/task_status?task_id={task_id}"
    return _http_get(url, api_key, timeout=timeout)
