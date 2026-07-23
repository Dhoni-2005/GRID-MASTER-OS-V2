"""
grid/heartbeat_sender.py — Grid Master OS Phase 7 Step 5
Artifact: P7S5_grid_heartbeat_sender.py
Package:  grid
Path:     grid/heartbeat_sender.py
Status:   [K] KEEP — canonical

Worker-side heartbeat system.  Runs as a daemon thread, sending signed
heartbeat payloads to the master at configurable intervals.

Responsibilities
----------------
• Send signed HeartbeatPayload to master on a fixed interval
• Embed CPU %, memory %, active task IDs, node status, grid version
• Detect 401/404 responses and set a re-registration flag for worker_runtime
• Track missed heartbeats; enter partition mode after threshold
• Expose update_active_tasks() so worker_runtime can keep the payload current
• Support graceful shutdown via stop_sender()

Does NOT import:
  grid.heartbeat_monitor  — master-side only
  grid.dispatcher         — Step 7
  grid.worker_runtime     — Step 10
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from grid.config import (
    HEARTBEAT_INTERVAL_SECONDS,
    MASTER_URL,
    MASTER_VERSION,
    NODE_ID,
    NODE_SECRET,
)
from grid.models import HeartbeatPayload
from grid.signing import sign_heartbeat

log = logging.getLogger("gridmaster.grid.heartbeat_sender")

# ── MODULE STATE ──────────────────────────────────────────────
_lock                 = threading.Lock()
_stop_event:          threading.Event       = threading.Event()
_sender_thread:       threading.Thread | None = None
_active_task_ids:     list[int]             = []
_needs_reregistration: bool                 = False
_missed_heartbeats:   int                   = 0
_partition_mode:      bool                  = False

# Consecutive misses before entering partition mode
_PARTITION_THRESHOLD: int = 3


# ══════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════

def start_sender(master_url:       str | None = None,
                 node_id:          str | None = None,
                 api_key:          str        = "",
                 interval_seconds: int | None = None) -> None:
    """
    Start the heartbeat sender as a daemon background thread.
    Safe to call once; calling again while already running is a no-op.

    Parameters
    ----------
    master_url       : base URL of the master node (defaults to grid.config.MASTER_URL)
    node_id          : this worker's node_id (defaults to grid.config.NODE_ID)
    api_key          : node-role API key for X-API-Key authentication
    interval_seconds : override for HEARTBEAT_INTERVAL_SECONDS
    """
    global _sender_thread, _stop_event, _missed_heartbeats, _partition_mode

    with _lock:
        if _sender_thread is not None and _sender_thread.is_alive():
            log.debug("Heartbeat sender already running — ignoring start()")
            return

        _stop_event       = threading.Event()
        _missed_heartbeats = 0
        _partition_mode   = False

    effective_url      = master_url      or MASTER_URL
    effective_node_id  = node_id         or NODE_ID
    effective_interval = interval_seconds or HEARTBEAT_INTERVAL_SECONDS

    _sender_thread = threading.Thread(
        target  = _sender_loop,
        args    = (effective_url, effective_node_id, api_key, effective_interval),
        name    = "gridmaster-heartbeat-sender",
        daemon  = True,
    )
    _sender_thread.start()
    log.info(
        "Heartbeat sender started: node=%s interval=%ds",
        effective_node_id, effective_interval,
    )


def stop_sender(timeout: float = 5.0) -> None:
    """
    Signal the heartbeat sender thread to stop and wait for it to exit.

    Parameters
    ----------
    timeout : seconds to wait for the thread to finish (default 5.0)
    """
    global _sender_thread

    _stop_event.set()
    if _sender_thread is not None:
        _sender_thread.join(timeout=timeout)
        if _sender_thread.is_alive():
            log.warning("Heartbeat sender did not stop within %.1fs", timeout)
        else:
            log.info("Heartbeat sender stopped cleanly")
    with _lock:
        _sender_thread = None


def update_active_tasks(task_ids: list[int]) -> None:
    """
    Update the list of task IDs included in the next heartbeat payload.
    Called by grid.worker_runtime when task execution state changes.

    Parameters
    ----------
    task_ids : current list of in-progress task IDs on this worker
    """
    global _active_task_ids
    with _lock:
        _active_task_ids = list(task_ids)


def needs_reregistration() -> bool:
    """
    Return True (and reset the flag) if the master signalled that this
    node must re-register (401 or 404 on heartbeat).

    Called each iteration by grid.worker_runtime._main_loop().
    """
    global _needs_reregistration
    with _lock:
        flag = _needs_reregistration
        _needs_reregistration = False
    return flag


def is_in_partition_mode() -> bool:
    """Return True if consecutive missed heartbeats exceeded the threshold."""
    with _lock:
        return _partition_mode


def is_running() -> bool:
    """Return True if the sender thread is alive."""
    with _lock:
        return _sender_thread is not None and _sender_thread.is_alive()


def get_missed_heartbeat_count() -> int:
    """Return current count of consecutive missed heartbeats."""
    with _lock:
        return _missed_heartbeats


# ══════════════════════════════════════════════════════════════
# SENDER LOOP (private)
# ══════════════════════════════════════════════════════════════

def _sender_loop(master_url: str,
                  node_id:    str,
                  api_key:    str,
                  interval:   int) -> None:
    """
    Main loop: build → sign → send heartbeat, then sleep.
    Runs until _stop_event is set.
    """
    from grid.client import (
        GridAuthError, GridClientError, GridNotFoundError, heartbeat as _hb,
    )

    log.debug("Sender loop started: master=%s node=%s", master_url, node_id)

    while not _stop_event.is_set():
        payload = _build_payload(node_id)
        _send_heartbeat(
            master_url = master_url,
            node_id    = node_id,
            api_key    = api_key,
            payload    = payload,
            hb_fn      = _hb,
            auth_err   = GridAuthError,
            nf_err     = GridNotFoundError,
            client_err = GridClientError,
        )
        _stop_event.wait(timeout=interval)

    log.debug("Sender loop exited for node=%s", node_id)


def _build_payload(node_id: str) -> HeartbeatPayload:
    """
    Construct a HeartbeatPayload with current resource metrics.
    CPU/memory are read from psutil if available; falls back to None.
    """
    cpu_pct: float | None = None
    mem_pct: float | None = None
    try:
        import psutil  # optional dependency
        cpu_pct = psutil.cpu_percent(interval=None)
        mem_pct = psutil.virtual_memory().percent
    except ImportError:
        pass
    except Exception as exc:
        log.debug("psutil error: %s", exc)

    with _lock:
        active_ids = list(_active_task_ids)

    return HeartbeatPayload(
        node_id         = node_id,
        timestamp_utc   = _now_iso(),
        active_task_ids = active_ids,
        cpu_percent     = cpu_pct,
        memory_percent  = mem_pct,
    )


def _send_heartbeat(master_url: str,
                     node_id:    str,
                     api_key:    str,
                     payload:    HeartbeatPayload,
                     hb_fn,
                     auth_err,
                     nf_err,
                     client_err) -> None:
    """
    Attempt to deliver one heartbeat to the master.
    Updates _missed_heartbeats and _needs_reregistration as appropriate.
    Separated from _sender_loop to make it independently testable.
    """
    global _missed_heartbeats, _needs_reregistration, _partition_mode

    payload_dict = payload.to_dict()
    # Sign the heartbeat if NODE_SECRET is configured
    if NODE_SECRET:
        try:
            payload_dict["signature"] = sign_heartbeat(payload_dict, NODE_SECRET)
        except Exception as exc:
            log.warning("Heartbeat signing failed: %s", exc)

    try:
        resp = hb_fn(master_url, payload, api_key)
        with _lock:
            _missed_heartbeats = 0
            _partition_mode    = False
        log.debug(
            "Heartbeat OK: node=%s server_ts=%s", node_id, resp.get("server_ts", "?")
        )

    except auth_err:
        # 401 — API key rejected; node must re-register with a valid key
        log.warning("Heartbeat 401 for node=%s — re-registration required", node_id)
        with _lock:
            _needs_reregistration = True
            _missed_heartbeats   += 1

    except nf_err:
        # 404 — master doesn't know this node; must re-register
        log.warning("Heartbeat 404 for node=%s — master lost state; re-registering", node_id)
        with _lock:
            _needs_reregistration = True
            _missed_heartbeats   += 1

    except client_err as exc:
        with _lock:
            _missed_heartbeats += 1
            missed = _missed_heartbeats
            if missed >= _PARTITION_THRESHOLD:
                _partition_mode = True
        log.warning(
            "Heartbeat network error node=%s missed=%d: %s",
            node_id, _missed_heartbeats, exc,
        )
        if _partition_mode:
            log.warning(
                "Node %s entering partition mode after %d missed heartbeats",
                node_id, _missed_heartbeats,
            )

    except Exception as exc:
        log.error("Heartbeat unexpected error node=%s: %s", node_id, exc)
        with _lock:
            _missed_heartbeats += 1


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — reset all module state."""
    global _active_task_ids, _needs_reregistration
    global _missed_heartbeats, _partition_mode, _sender_thread

    _stop_event.set()
    if _sender_thread and _sender_thread.is_alive():
        _sender_thread.join(timeout=2.0)

    with _lock:
        _active_task_ids      = []
        _needs_reregistration = False
        _missed_heartbeats    = 0
        _partition_mode       = False
        _sender_thread        = None
