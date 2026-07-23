"""
grid/heartbeat_monitor.py — Grid Master OS Phase 7 Step 5
Artifact: P7S5_grid_heartbeat_monitor.py
Package:  grid
Path:     grid/heartbeat_monitor.py
Status:   [K] KEEP — canonical

Master-side heartbeat monitor.  Runs as a daemon thread, periodically
classifying all registered nodes and triggering failure handling when
nodes transition to stale or offline.

Responsibilities
----------------
• Classify every registered node: online / stale / offline / recovering
• Detect state TRANSITIONS (not just current state) — avoid re-triggering
  failure logic on every check cycle for already-offline nodes
• Call grid.failure.handle_offline_node() on offline transition
• Call grid.registry.mark_offline() for newly-offline nodes
• Support manual record_heartbeat() for incoming heartbeat API calls
• Verify optional HMAC signature on incoming heartbeat payloads
• Support configurable check interval
• Expose check_all_nodes() for synchronous testing without the thread
• Support graceful shutdown via stop_monitor()

Does NOT import:
  grid.heartbeat_sender  — worker-side only
  grid.client            — master does not initiate calls to workers in this step
  grid.dispatcher        — Step 7
"""
from __future__ import annotations

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
    NODE_SECRET,
)
from grid.failure import handle_offline_node
from grid.models import HeartbeatPayload
from grid.registry import (
    classify_health,
    get_all_nodes,
    get_cluster_snapshot,
    mark_offline,
    record_heartbeat,
)
from grid.signing import verify_heartbeat
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.heartbeat_monitor")

# ── MODULE STATE ──────────────────────────────────────────────
_lock           = threading.Lock()
_stop_event     = threading.Event()
_monitor_thread: threading.Thread | None = None

# Track the last known health classification per node to detect transitions
# {node_id: "online" | "stale" | "offline" | "recovering" | "unknown"}
_last_classification: dict[str, str] = {}

# Default monitor check interval in seconds
_DEFAULT_INTERVAL: int = 30


# ══════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════

def start_monitor(interval_seconds: int = _DEFAULT_INTERVAL) -> None:
    """
    Start the heartbeat monitor as a master-side daemon thread.
    Safe to call once; subsequent calls while running are no-ops.

    Parameters
    ----------
    interval_seconds : how often to check all node health states
    """
    global _monitor_thread, _stop_event

    with _lock:
        if _monitor_thread is not None and _monitor_thread.is_alive():
            log.debug("Heartbeat monitor already running — ignoring start()")
            return
        _stop_event = threading.Event()

    _monitor_thread = threading.Thread(
        target  = _monitor_loop,
        args    = (interval_seconds,),
        name    = "gridmaster-heartbeat-monitor",
        daemon  = True,
    )
    _monitor_thread.start()
    log.info("Heartbeat monitor started: interval=%ds", interval_seconds)


def stop_monitor(timeout: float = 5.0) -> None:
    """
    Signal the monitor thread to stop and wait for it to exit.

    Parameters
    ----------
    timeout : seconds to wait for clean shutdown (default 5.0)
    """
    global _monitor_thread

    _stop_event.set()
    if _monitor_thread is not None:
        _monitor_thread.join(timeout=timeout)
        if _monitor_thread.is_alive():
            log.warning("Heartbeat monitor did not stop within %.1fs", timeout)
        else:
            log.info("Heartbeat monitor stopped cleanly")
    with _lock:
        _monitor_thread = None


def is_running() -> bool:
    """Return True if the monitor thread is alive."""
    with _lock:
        return _monitor_thread is not None and _monitor_thread.is_alive()


def record_heartbeat_from_request(payload_dict: dict[str, Any],
                                   verify_signature: bool = True) -> dict[str, Any]:
    """
    Process an incoming heartbeat from a worker node.
    Called by grid.master (Step 9) when POST /grid/heartbeat is received.

    Steps
    -----
    1. Validate required fields.
    2. Optionally verify HMAC signature (requires NODE_SECRET).
    3. Update node health via grid.registry.record_heartbeat().
    4. Check for stale→online recovery transition.

    Parameters
    ----------
    payload_dict     : raw dict from the HTTP request body
    verify_signature : if True and NODE_SECRET is set, reject bad signatures

    Returns
    -------
    {
      "accepted":  bool,
      "node_id":   str,
      "server_ts": str,
      "error":     str | None,
    }
    """
    node_id = payload_dict.get("node_id", "")
    if not node_id:
        return {"accepted": False, "node_id": "", "server_ts": _now_iso(),
                "error": "missing node_id"}

    # Signature verification (optional — only when NODE_SECRET is configured)
    if verify_signature and NODE_SECRET:
        sig = payload_dict.get("signature", "")
        check_dict = {k: v for k, v in payload_dict.items() if k != "signature"}
        if not verify_heartbeat(check_dict, sig, NODE_SECRET):
            log.warning("Heartbeat signature invalid for node=%s", node_id)
            log_event(AuditEvent.AUTH_FAILURE,
                      detail=f"heartbeat_invalid_signature node={node_id}")
            return {"accepted": False, "node_id": node_id, "server_ts": _now_iso(),
                    "error": "invalid signature"}

    # Build dataclass for record_heartbeat
    try:
        payload = HeartbeatPayload.from_dict(payload_dict)
    except (ValueError, TypeError) as exc:
        log.warning("Malformed heartbeat from node=%s: %s", node_id, exc)
        return {"accepted": False, "node_id": node_id, "server_ts": _now_iso(),
                "error": f"malformed payload: {exc}"}

    known = record_heartbeat(payload)
    if not known:
        log.warning("Heartbeat from unregistered node=%s", node_id)
        return {"accepted": False, "node_id": node_id, "server_ts": _now_iso(),
                "error": "node not registered"}

    # Check for recovery: was offline/stale, now online
    _check_recovery(node_id)

    return {
        "accepted":  True,
        "node_id":   node_id,
        "server_ts": _now_iso(),
        "error":     None,
    }


def check_all_nodes() -> dict[str, Any]:
    """
    Synchronously classify every registered node and act on transitions.
    Called by the monitor thread on each cycle; also callable directly in tests.

    State machine
    -------------
    • online  → stale    : log warning; stop dispatching (handled by load_balancer)
    • online  → offline  : call handle_offline_node(); mark_offline()
    • stale   → offline  : same as above
    • offline → online   : recovery — log info, clear stale flag
    • *       → *        : no transition; no action

    Returns
    -------
    {
      "online":   [node_ids],
      "stale":    [node_ids],
      "offline":  [node_ids],
      "recovering": [node_ids],
      "online_count": int,
      "stale_count":  int,
      "offline_count": int,
    }
    """
    nodes     = get_all_nodes()
    online    = []
    stale     = []
    offline   = []
    recovering = []

    for node in nodes:
        nid    = node["node_id"]
        health = classify_health(nid)

        with _lock:
            previous = _last_classification.get(nid, "unknown")
            _last_classification[nid] = health

        _handle_transition(nid, previous, health, recovering)

        if health == "online":
            online.append(nid)
        elif health == "stale":
            stale.append(nid)
        else:
            offline.append(nid)

    summary = {
        "online":        online,
        "stale":         stale,
        "offline":       offline,
        "recovering":    recovering,
        "online_count":  len(online),
        "stale_count":   len(stale),
        "offline_count": len(offline),
    }
    if stale:
        log.warning("Stale nodes detected: %s", stale)
    if offline:
        log.warning("Offline nodes detected: %s", offline)

    return summary


def get_node_classification(node_id: str) -> str:
    """
    Return the last known classification for a node from the monitor's cache.
    Returns "unknown" if the node has never been checked.

    Parameters
    ----------
    node_id : worker node to query
    """
    with _lock:
        return _last_classification.get(node_id, "unknown")


def get_all_classifications() -> dict[str, str]:
    """Return a snapshot of all node_id → classification mappings."""
    with _lock:
        return dict(_last_classification)


def get_stale_threshold(platform: str = "render") -> int:
    """
    Return the stale-detection threshold in seconds for the given platform.
    HF Spaces uses a longer threshold due to cold-start latency.

    Parameters
    ----------
    platform : "render" | "huggingface" | "local"
    """
    return (
        HF_HEARTBEAT_STALE_SECONDS
        if platform == "huggingface"
        else HEARTBEAT_STALE_SECONDS
    )


# ══════════════════════════════════════════════════════════════
# MONITOR LOOP (private)
# ══════════════════════════════════════════════════════════════

def _monitor_loop(interval: int) -> None:
    """Main loop: check all nodes, then sleep for interval seconds."""
    log.debug("Monitor loop started: interval=%ds", interval)
    while not _stop_event.is_set():
        try:
            check_all_nodes()
        except Exception as exc:
            log.error("monitor_loop: unexpected error during check: %s", exc)
        _stop_event.wait(timeout=interval)
    log.debug("Monitor loop exited")


def _handle_transition(node_id:    str,
                        previous:   str,
                        current:    str,
                        recovering: list[str]) -> None:
    """
    Detect health transitions and trigger appropriate actions.

    Parameters
    ----------
    node_id    : the node being evaluated
    previous   : classification from the last check cycle
    current    : classification from this check cycle
    recovering : mutable list to append recovering node_ids to
    """
    if previous == current:
        return   # no transition; no action

    log.info(
        "Node health transition: node=%s %s → %s", node_id, previous, current
    )

    # Recovery: any degraded state → online
    if current == "online" and previous in ("stale", "offline", "unknown"):
        _handle_recovery(node_id, previous, recovering)
        return

    # Degradation to offline
    if current == "offline" and previous != "offline":
        _handle_newly_offline(node_id)
        return

    # Degradation to stale (from online) — log only; load_balancer excludes stale
    if current == "stale" and previous == "online":
        log.warning("Node %s transitioned online → stale", node_id)
        log_event(AuditEvent.ADMIN_ACTION,
                  detail=f"node_stale node={node_id}")


def _handle_newly_offline(node_id: str) -> None:
    """
    Respond to a node transitioning to offline.
    Marks it offline in the registry and hands off to failure.handle_offline_node().
    """
    log.warning("Node %s is now offline — triggering failure handler", node_id)
    try:
        result = handle_offline_node(node_id)
        log.info(
            "Offline node %s handled: reassigned=%d abandoned=%d",
            node_id,
            result.get("reassigned", 0),
            result.get("abandoned",  0),
        )
    except Exception as exc:
        log.error("handle_offline_node(%s) raised: %s", node_id, exc)


def _handle_recovery(node_id:    str,
                      previous:   str,
                      recovering: list[str]) -> None:
    """
    Respond to a node recovering from stale/offline back to online.
    Sets node status back to online in node_registry.
    """
    log.info("Node %s recovered: %s → online", node_id, previous)
    recovering.append(node_id)
    try:
        nr.set_online(node_id)
    except Exception as exc:
        log.warning("set_online(%s) failed: %s", node_id, exc)
    log_event(AuditEvent.ADMIN_ACTION,
              detail=f"node_recovered node={node_id} previous={previous}")


def _check_recovery(node_id: str) -> None:
    """
    After a fresh heartbeat is received, check whether the node was
    previously stale or offline and mark it recovering if so.
    Called from record_heartbeat_from_request().
    """
    with _lock:
        previous = _last_classification.get(node_id, "unknown")

    if previous in ("stale", "offline"):
        current = classify_health(node_id)
        if current == "online":
            with _lock:
                _last_classification[node_id] = "online"
            log.info("Node %s heartbeat received — marking recovered", node_id)
            try:
                nr.set_online(node_id)
            except Exception as exc:
                log.warning("set_online(%s) after recovery: %s", node_id, exc)
            log_event(AuditEvent.ADMIN_ACTION,
                      detail=f"node_recovered_via_heartbeat node={node_id}")


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — clear classification cache and stop thread if running."""
    global _monitor_thread

    _stop_event.set()
    if _monitor_thread and _monitor_thread.is_alive():
        _monitor_thread.join(timeout=2.0)
    with _lock:
        _last_classification.clear()
        _monitor_thread = None
