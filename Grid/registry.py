"""
grid/registry.py — Grid Master OS Phase 7
Authoritative cluster topology for the master node.
Wraps node_registry.py with distributed-context additions:
  • Platform-aware health thresholds
  • Quarantine tracking
  • Active task count (from heartbeat payloads)
  • Token lifecycle management for re-registration (RC-5)
  • Thread-safe in-memory state

All persistent node state uses the existing node_registry / database layer.
Volatile cluster state (quarantine, active tasks, tokens) is in-memory
and reconstructs within one heartbeat cycle after master restart.
"""
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import node_registry as nr
from grid.config import (
    HEARTBEAT_OFFLINE_SECONDS,
    HEARTBEAT_STALE_SECONDS,
    HF_HEARTBEAT_STALE_SECONDS,
    QUARANTINE_MINUTES,
)
from grid.models import HeartbeatPayload, NodeInfo, RegistrationResponse
from grid.config import MASTER_VERSION

log = logging.getLogger("gridmaster.grid.registry")

# ── MODULE-LEVEL THREAD-SAFE STATE ───────────────────────────
_lock = threading.Lock()

# {node_id: list[int]}  — active task ids from last heartbeat
_active_tasks: dict[str, list[int]] = {}

# {node_id: float}  — expiry unix timestamp for quarantined nodes
_quarantine: dict[str, float] = {}

# {node_id: str}  — current registration token per node  (RC-5)
_node_tokens: dict[str, str] = {}

# {node_id: list[str]}  — capabilities declared at registration
# node_registry.py (Phase 1) does not expose a capabilities column;
# capabilities are stored in-memory and injected into cluster snapshots.
_node_capabilities: dict[str, list[str]] = {}


# ── REGISTRATION ─────────────────────────────────────────────

def register_node(info: NodeInfo, api_key_meta: dict | None = None) -> dict:
    """
    Register or re-register a worker node.

    On re-registration (node_id already known):
      • Revokes the previous registration token (RC-5)
      • Issues a fresh token
      • Upserts the node record

    Parameters
    ----------
    info         : NodeInfo dataclass describing the worker
    api_key_meta : optional metadata from security.auth.verify() —
                   used to look up owner for audit; may be None in tests

    Returns
    -------
    dict matching RegistrationResponse schema
    """
    from security.auth import issue_token, revoke_token  # late import avoids cycle

    with _lock:
        # RC-5: revoke any existing token for this node before issuing a new one
        existing_token = _node_tokens.get(info.node_id)
        if existing_token:
            revoked = revoke_token(existing_token)
            log.info(
                "Registry: revoked old token for node %s (revoked=%s)",
                info.node_id, revoked,
            )

        # Upsert via existing node_registry primitive
        nr.register(
            node_id   = info.node_id,
            node_name = f"{info.platform}-{info.node_id[:8]}",
            platform  = info.platform,
            role      = nr.ROLE_WORKER,
        )

        # Store capabilities in-memory — node_registry (Phase 1) has no capabilities column.
        # The cluster snapshot injects these so load_balancer can filter by capability.
        _node_capabilities[info.node_id] = (
            list(info.capabilities) if info.capabilities else ["general"]
        )

        # Issue new registration token
        token_info = issue_token(role="node", owner=info.node_id, ttl=3600)
        _node_tokens[info.node_id] = token_info["token"]

        # Initialise active task list if not present
        if info.node_id not in _active_tasks:
            _active_tasks[info.node_id] = []

    log.info(
        "Registry: registered node %s platform=%s capabilities=%s",
        info.node_id, info.platform, info.capabilities,
    )

    return {
        "status":             "ok",
        "node_id":            info.node_id,
        "registration_token": token_info["token"],
        "token_expires_at":   token_info["expires_at"],
        "master_version":     MASTER_VERSION,
    }


def unregister_node(node_id: str) -> bool:
    """
    Mark a node as inactive and clear its in-memory state.
    Does not delete the node record — history is preserved.
    Returns True if the node was found and deregistered.
    """
    with _lock:
        node = nr.get_node(node_id)
        if node is None:
            log.warning("Registry: unregister called for unknown node %s", node_id)
            return False
        nr.set_offline(node_id)
        _active_tasks.pop(node_id, None)
        token = _node_tokens.pop(node_id, None)
        if token:
            try:
                from security.auth import revoke_token
                revoke_token(token)
            except Exception:
                pass

    log.info("Registry: unregistered node %s", node_id)
    return True


# ── HEARTBEAT ─────────────────────────────────────────────────

def record_heartbeat(payload: HeartbeatPayload) -> bool:
    """
    Process an incoming heartbeat from a worker node.
    Updates last_heartbeat via node_registry and refreshes active task list.

    Returns True if node was found, False if node is not registered
    (caller should instruct worker to re-register).
    """
    node = nr.get_node(payload.node_id)
    if node is None:
        log.warning("Registry: heartbeat from unregistered node %s", payload.node_id)
        return False

    nr.heartbeat(payload.node_id)
    with _lock:
        _active_tasks[payload.node_id] = list(payload.active_task_ids)

    log.debug(
        "Registry: heartbeat node=%s active_tasks=%s",
        payload.node_id, payload.active_task_ids,
    )
    return True


# ── QUERIES ───────────────────────────────────────────────────

def get_node(node_id: str) -> dict[str, Any] | None:
    """Return a single node record by node_id, or None if not found."""
    return nr.get_node(node_id)


def get_all_nodes() -> list[dict[str, Any]]:
    """Return all registered nodes regardless of status."""
    return nr.list_all_nodes()


def get_online_nodes() -> list[dict[str, Any]]:
    """Return all nodes currently classified as online and not quarantined."""
    with _lock:
        quarantined = set(_get_active_quarantine())
    return [
        n for n in nr.get_available_nodes(role=nr.ROLE_WORKER)
        if n["node_id"] not in quarantined
        and classify_health(n["node_id"]) == "online"
    ]


def get_nodes_by_capability(capability: str) -> list[dict[str, Any]]:
    """
    Return online nodes that list the given capability.
    Uses node_registry's json_each() query for exact capability matching.
    """
    try:
        candidates = nr.find_by_capability(capability)
    except AttributeError:
        # Fallback if node_registry doesn't expose find_by_capability
        candidates = get_online_nodes()
    with _lock:
        quarantined = set(_get_active_quarantine())
    return [
        n for n in candidates
        if n["node_id"] not in quarantined
        and classify_health(n["node_id"]) == "online"
    ]


def get_cluster_snapshot() -> list[dict[str, Any]]:
    """
    Return all nodes with computed health classification and active task count.
    Used by load_balancer and heartbeat_monitor.
    """
    nodes = nr.list_all_nodes()
    with _lock:
        tasks_snap    = dict(_active_tasks)
        quarantine_set = set(_get_active_quarantine())

    snapshot = []
    for n in nodes:
        nid = n["node_id"]
        snapshot.append({
            **n,
            "health":       classify_health(nid),
            "active_tasks": tasks_snap.get(nid, []),
            "quarantined":  nid in quarantine_set,
            "capabilities": _node_capabilities.get(nid, ["general"]),
        })
    return snapshot


# ── TASK COUNTING ─────────────────────────────────────────────

def increment_active_tasks(node_id: str, task_id: int) -> None:
    """Add task_id to the in-memory active task list for node_id."""
    with _lock:
        if node_id not in _active_tasks:
            _active_tasks[node_id] = []
        if task_id not in _active_tasks[node_id]:
            _active_tasks[node_id].append(task_id)


def decrement_active_tasks(node_id: str, task_id: int) -> None:
    """Remove task_id from the in-memory active task list for node_id."""
    with _lock:
        tasks = _active_tasks.get(node_id, [])
        _active_tasks[node_id] = [t for t in tasks if t != task_id]


def get_active_task_count(node_id: str) -> int:
    """Return the current active task count for a node."""
    with _lock:
        return len(_active_tasks.get(node_id, []))


# ── HEALTH CLASSIFICATION ─────────────────────────────────────

def classify_health(node_id: str) -> str:
    """
    Classify a node's health based on last_heartbeat age.

    Returns
    -------
    "online"  — heartbeat is fresh
    "stale"   — heartbeat is older than stale threshold
    "offline" — heartbeat is older than offline threshold or node is offline
    "unknown" — node not found
    """
    node = nr.get_node(node_id)
    if node is None:
        return "unknown"

    if node.get("status") == "offline":
        return "offline"

    last_hb = node.get("last_heartbeat")
    if not last_hb:
        return "offline"

    try:
        hb_time   = datetime.fromisoformat(last_hb)
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        age       = (now_naive - hb_time).total_seconds()
    except (ValueError, TypeError):
        return "offline"

    # Platform-aware thresholds
    platform = node.get("platform", "local")
    stale_threshold = (
        HF_HEARTBEAT_STALE_SECONDS
        if platform == "huggingface"
        else HEARTBEAT_STALE_SECONDS
    )

    if age >= HEARTBEAT_OFFLINE_SECONDS:
        return "offline"
    if age >= stale_threshold:
        return "stale"
    return "online"


def mark_offline(node_id: str) -> None:
    """Explicitly mark a node as offline. Called by failure handler."""
    nr.set_offline(node_id)
    with _lock:
        _active_tasks[node_id] = []
    log.info("Registry: marked node %s offline", node_id)


# ── QUARANTINE ────────────────────────────────────────────────

def quarantine_node(node_id: str,
                    minutes: int | None = None) -> None:
    """
    Temporarily exclude a node from dispatch.
    The quarantine expires automatically after `minutes` minutes.
    """
    duration = minutes if minutes is not None else QUARANTINE_MINUTES
    expiry   = time.monotonic() + duration * 60
    with _lock:
        _quarantine[node_id] = expiry
    log.warning(
        "Registry: node %s quarantined for %d minutes", node_id, duration
    )


def lift_quarantine(node_id: str) -> None:
    """Manually remove a node from quarantine."""
    with _lock:
        _quarantine.pop(node_id, None)
    log.info("Registry: quarantine lifted for node %s", node_id)


def is_quarantined(node_id: str) -> bool:
    """
    Return True if node is currently quarantined.
    Auto-lifts expired quarantines on check.
    """
    with _lock:
        expiry = _quarantine.get(node_id)
        if expiry is None:
            return False
        if time.monotonic() > expiry:
            del _quarantine[node_id]
            log.info("Registry: quarantine expired for node %s", node_id)
            return False
        return True


def get_node_token(node_id: str) -> str | None:
    """Return the current active registration token for a node. (RC-5)"""
    with _lock:
        return _node_tokens.get(node_id)


# ── STALE NODE DETECTION ─────────────────────────────────────

def get_stale_nodes() -> list[str]:
    """Return node_ids classified as stale but not yet offline."""
    return [
        n["node_id"] for n in nr.list_all_nodes()
        if classify_health(n["node_id"]) == "stale"
    ]


def get_offline_nodes() -> list[str]:
    """Return node_ids classified as offline."""
    return [
        n["node_id"] for n in nr.list_all_nodes()
        if classify_health(n["node_id"]) == "offline"
    ]


# ── PRIVATE HELPERS ───────────────────────────────────────────

def _get_active_quarantine() -> list[str]:
    """
    Return node_ids with non-expired quarantine entries.
    Must be called with _lock held.
    """
    now = time.monotonic()
    expired = [nid for nid, exp in _quarantine.items() if now > exp]
    for nid in expired:
        del _quarantine[nid]
        log.info("Registry: quarantine expired for node %s (cleanup)", nid)
    return list(_quarantine.keys())


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def reset_state() -> None:
    """
    Clear all in-memory volatile state.
    Used in tests and after master restart to force clean reconstruction.
    """
    with _lock:
        _active_tasks.clear()
        _quarantine.clear()
        _node_tokens.clear()
        _node_capabilities.clear()


def _reset_db_for_tests() -> None:
    """
    TESTING ONLY — wipe both in-memory state and all node_registry DB records.
    Never call in production. Provides full isolation between test cases.
    """
    import database as _db
    with _lock:
        _active_tasks.clear()
        _quarantine.clear()
        _node_tokens.clear()
        _node_capabilities.clear()
    try:
        _db._exec("DELETE FROM node_registry")
    except Exception:
        pass
