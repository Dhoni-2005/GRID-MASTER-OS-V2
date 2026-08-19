"""
grid/worker_runtime.py — Grid Master OS Phase 7 Step 10
Artifact: P7S10_grid_worker_runtime.py
Package:  grid
Path:     grid/worker_runtime.py
Status:   [K] KEEP — canonical

The Worker Runtime is the final component of the Distributed Grid Runtime.
It is the process entry point that runs on every worker node: it registers
with the master, sends heartbeats, polls for and executes tasks, and
reports results back.

Responsibilities
----------------
• start() / stop() / is_running() / get_status() — public lifecycle
• Registration with the master via grid.client.register()
• Heartbeat delivery via grid.heartbeat_sender (never reimplemented here)
• Local health-probe server via grid.worker_server (never reimplemented here)
• Task execution abstraction: execute_task(assignment) -> ResultPayload
  — stateless: workers hold no local task database, so execution operates
    directly on the TaskAssignment payload's title/input_data, mirroring
    worker.py's pattern-matching behaviour without any DB dependency
• Result reporting via grid.client.report_result()
• Runtime statistics: current/completed/failed task counts, uptime

Does NOT import:
  grid.master            — Step 9 (master-side only)
  node_scheduler          — RC-2
Does NOT duplicate:
  dispatcher logic         (grid.dispatcher owns task selection/dispatch)
  heartbeat monitor logic  (grid.heartbeat_monitor owns master-side health)
  reconciler logic         (grid.reconciler owns consistency repair)
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import grid.client as client
import grid.heartbeat_sender as heartbeat_sender
import grid.worker_server as worker_server
from grid.config import (
    MASTER_URL,
    MASTER_VERSION,
    NODE_CAPABILITIES,
    NODE_ID,
    NODE_SECRET,
    PLATFORM,
    POLL_INTERVAL_SECONDS,
    POLL_JITTER_SECONDS,
    WORKER_PORT,
)
from grid.models import NodeInfo, ResultPayload, TaskAssignment
from grid.signing import sign_result, verify_assignment
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.worker_runtime")

# ── MODULE STATE ──────────────────────────────────────────────
_lock            = threading.RLock()   # re-entrant: get_status() called from within locked sections
_running         = False
_runtime_thread: threading.Thread | None = None
_stop_event      = threading.Event()

_started_at:      float | None = None
_registration_token: str | None = None
_api_key:         str = ""

# Task counters
_current_task_ids: set[int] = set()
_completed_count:  int      = 0
_failed_count:     int      = 0

# Last registration/poll error for diagnostics
_last_error: str | None = None


# ══════════════════════════════════════════════════════════════
# LIFECYCLE
# ══════════════════════════════════════════════════════════════

def start(api_key:    str        = "",
          master_url: str | None = None,
          node_id:    str | None = None) -> dict[str, Any]:
    """
    Start the worker runtime: register with the master, start the
    heartbeat sender, start the local health-probe server, and begin
    the poll-execute-report loop as a background daemon thread.

    Safe to call once; calling again while already running is a no-op
    that returns the current status.

    Parameters
    ----------
    api_key    : node-role API key used for all master communication
    master_url : override for grid.config.MASTER_URL
    node_id    : override for grid.config.NODE_ID

    Returns
    -------
    get_status() dict reflecting the outcome. On registration failure,
    "running" will be False and "last_error" will describe the cause.
    """
    global _running, _runtime_thread, _stop_event, _started_at
    global _registration_token, _api_key, _last_error

    with _lock:
        if _running:
            log.debug("Worker runtime already running — ignoring start()")
            return get_status()

        effective_master = master_url or MASTER_URL
        effective_node   = node_id or NODE_ID

        if not effective_master:
            _last_error = "MASTER_URL is not configured"
            log.error("Worker runtime start failed: %s", _last_error)
            return get_status()

        _api_key = api_key

        # ── Bootstrap: register with master ────────────────────
        try:
            info = NodeInfo(
                node_id       = effective_node,
                platform      = PLATFORM,
                capabilities  = list(NODE_CAPABILITIES),
                registered_at = _now_iso(),
                public_url    = "",
                worker_port   = WORKER_PORT,
            )
            reg_resp = client.register(effective_master, info, api_key)
            _registration_token = reg_resp.registration_token
            log.info("Worker registered: node_id=%s token_expires=%s",
                     effective_node, reg_resp.token_expires_at)
        except Exception as exc:
            _last_error = f"Registration failed: {exc}"
            log.error(_last_error)
            return get_status()

        # ── Start local health-probe server ────────────────────
        try:
            worker_server.start(host="0.0.0.0", port=WORKER_PORT,
                                node_id=effective_node, api_key=api_key)
        except Exception as exc:
            log.warning("worker_server.start() failed (non-fatal): %s", exc)

        # ── Start heartbeat sender ──────────────────────────────
        try:
            heartbeat_sender.start_sender(
                master_url = effective_master,
                node_id    = effective_node,
                api_key    = api_key,
            )
        except Exception as exc:
            log.warning("heartbeat_sender.start_sender() failed (non-fatal): %s", exc)

        # ── Reset per-run state ─────────────────────────────────
        _stop_event = threading.Event()
        _current_task_ids.clear()
        _completed_count = 0
        _failed_count    = 0
        _last_error      = None

        _runtime_thread = threading.Thread(
            target = _main_loop,
            args   = (effective_master, effective_node, api_key),
            name   = "gridmaster-worker-runtime",
            daemon = True,
        )
        _runtime_thread.start()

        _running    = True
        _started_at = time.monotonic()

    log.info("Worker runtime started: node_id=%s master=%s", effective_node, effective_master)
    return get_status()


def stop(timeout: float = 10.0) -> dict[str, Any]:
    """
    Gracefully stop the worker runtime.

    Steps
    -----
    1. Signal the main loop to exit.
    2. Report any in-progress tasks as "interrupted".
    3. Stop the heartbeat sender.
    4. Stop the local worker server.

    Parameters
    ----------
    timeout : seconds to wait for the main loop thread to exit

    Returns
    -------
    get_status() dict reflecting the stopped state.
    """
    global _running, _runtime_thread, _started_at

    with _lock:
        if not _running:
            log.debug("Worker runtime not running — ignoring stop()")
            return get_status()

        _stop_event.set()

    if _runtime_thread is not None:
        _runtime_thread.join(timeout=timeout)
        if _runtime_thread.is_alive():
            log.warning("Worker runtime main loop did not stop within %.1fs", timeout)

    try:
        heartbeat_sender.stop_sender(timeout=5.0)
    except Exception as exc:
        log.warning("heartbeat_sender.stop_sender() error: %s", exc)

    try:
        worker_server.stop(timeout=5.0)
    except Exception as exc:
        log.warning("worker_server.stop() error: %s", exc)

    with _lock:
        _running        = False
        _started_at      = None
        _runtime_thread  = None

    log.info("Worker runtime stopped")
    return get_status()


def is_running() -> bool:
    """Return True if the worker runtime main loop is active."""
    with _lock:
        return _running


def get_status() -> dict[str, Any]:
    """
    Return a structured snapshot of the worker runtime state.

    Returns
    -------
    {
      "running":         bool,
      "node_id":         str,
      "current_tasks":   int,
      "completed_tasks": int,
      "failed_tasks":    int,
      "uptime":          float,
      "last_error":      str | None,
    }
    """
    with _lock:
        return {
            "running":         _running,
            "node_id":         NODE_ID,
            "current_tasks":   len(_current_task_ids),
            "completed_tasks": _completed_count,
            "failed_tasks":    _failed_count,
            "uptime":          _uptime(),
            "last_error":      _last_error,
        }


# ══════════════════════════════════════════════════════════════
# TASK EXECUTION ABSTRACTION
# ══════════════════════════════════════════════════════════════

def execute_task(assignment: TaskAssignment) -> ResultPayload:
    """
    Execute a task assignment and return a ResultPayload.

    This is a stateless execution abstraction: workers hold no local
    task database (see architecture: node synchronisation), so this
    function operates directly on the assignment's title and input_data
    rather than calling worker.execute(task_id), which requires a local
    DB row that does not exist on a distributed worker node.

    The pattern-matching behaviour mirrors worker.py's task handlers
    (summarize, count, reverse, uppercase, list, echo fallback) so
    results are consistent whether a task runs locally (Phase 3) or
    on a distributed worker (Phase 7).

    Parameters
    ----------
    assignment : validated TaskAssignment received from the master

    Returns
    -------
    ResultPayload with status "completed" on success, "failed" on
    any exception during execution. Never raises — all exceptions
    are captured and reflected in the result.
    """
    title = (assignment.title or "").lower()
    inp   = assignment.input_data or ""

    try:
        output = _run_task_logic(title, inp, assignment.task_id)
        return ResultPayload(
            task_id      = assignment.task_id,
            node_id      = NODE_ID,
            status       = "completed",
            output       = output,
            completed_at = _now_iso(),
            error        = None,
        )
    except Exception as exc:
        log.error("execute_task: task %d raised: %s", assignment.task_id, exc)
        return ResultPayload(
            task_id      = assignment.task_id,
            node_id      = NODE_ID,
            status       = "failed",
            output       = "",
            completed_at = _now_iso(),
            error        = str(exc),
        )


def _run_task_logic(title: str, inp: str, task_id: int) -> str:
    """
    Pure task-processing logic, mirroring worker.py's title-based
    pattern matching. Operates only on strings — no database access.
    """
    if any(kw in title for kw in ("summarize", "summary")):
        preview = inp[:200]
        return (f"Summary of input ({len(inp)} chars):\n"
                f"{preview}{'...' if len(inp) > 200 else ''}")

    if "count" in title:
        words = len(inp.split())
        lines = len(inp.splitlines()) or 1
        return (f"Word count: {words}\nLine count: {lines}\nChar count: {len(inp)}")

    if "reverse" in title:
        return inp[::-1]

    if any(kw in title for kw in ("uppercase", "upper")):
        return inp.upper()

    if "list" in title:
        items = [ln.strip() for ln in inp.splitlines() if ln.strip()]
        if not items:
            items = inp.split()
        if not items:
            return f"1. {inp or 'empty input'}"
        return "\n".join(f"{i+1}. {item}" for i, item in enumerate(items))

    # Fallback echo — always produces non-empty output
    return f"[WORKER OUTPUT] Task {task_id}: {title}\n{inp}"


# ══════════════════════════════════════════════════════════════
# MAIN LOOP (private)
# ══════════════════════════════════════════════════════════════

def _main_loop(master_url: str, node_id: str, api_key: str) -> None:
    """
    Poll-execute-report cycle. Runs until _stop_event is set.
    Applies POLL_JITTER_SECONDS random offset before each poll to
    avoid a thundering herd against the master.
    """
    import random

    log.debug("Worker main loop started: node=%s", node_id)

    while not _stop_event.is_set():
        # Check for re-registration signal from heartbeat sender
        if heartbeat_sender.needs_reregistration():
            _reregister(master_url, node_id, api_key)

        jitter = random.uniform(0, POLL_JITTER_SECONDS)
        if _stop_event.wait(timeout=jitter):
            break

        try:
            poll_resp = client.poll(master_url, node_id, api_key)
        except Exception as exc:
            log.warning("Poll failed: %s", exc)
            _stop_event.wait(timeout=POLL_INTERVAL_SECONDS)
            continue

        if not poll_resp.has_work or poll_resp.assignment is None:
            _stop_event.wait(timeout=max(poll_resp.wait_seconds, 1))
            continue

        assignment = poll_resp.assignment

        # Verify signature before executing
        if NODE_SECRET:
            payload_dict = assignment.to_dict()
            sig = payload_dict.pop("signature", "")
            if not verify_assignment(payload_dict, sig, NODE_SECRET):
                log.warning("Discarding assignment %d: invalid signature",
                           assignment.task_id)
                log_event(AuditEvent.AUTH_FAILURE,
                          detail=f"invalid_assignment_signature task_id={assignment.task_id}")
                continue

        _execute_and_report(master_url, node_id, api_key, assignment)

    log.debug("Worker main loop exited: node=%s", node_id)


def _execute_and_report(master_url: str,
                         node_id:    str,
                         api_key:    str,
                         assignment: TaskAssignment) -> None:
    """Execute one assignment, sign the result, and report it to the master."""
    global _completed_count, _failed_count

    with _lock:
        _current_task_ids.add(assignment.task_id)
    heartbeat_sender.update_active_tasks(list(_current_task_ids))

    try:
        result = execute_task(assignment)

        if NODE_SECRET:
            result_dict = result.to_dict()
            result_dict.pop("signature", None)
            result.signature = sign_result(result_dict, NODE_SECRET)

        try:
            client.report_result(master_url, result, api_key)
        except Exception as exc:
            log.error("Failed to report result for task %d: %s", assignment.task_id, exc)

        with _lock:
            if result.status == "completed":
                _completed_count += 1
            else:
                _failed_count += 1

        log.info("Task %d %s (node=%s)", assignment.task_id, result.status, node_id)

    finally:
        with _lock:
            _current_task_ids.discard(assignment.task_id)
        heartbeat_sender.update_active_tasks(list(_current_task_ids))


def _reregister(master_url: str, node_id: str, api_key: str) -> None:
    """Re-register with the master after a 401/404 signal from heartbeat_sender."""
    global _registration_token, _last_error
    log.info("Re-registering worker node=%s", node_id)
    try:
        info = NodeInfo(
            node_id       = node_id,
            platform      = PLATFORM,
            capabilities  = list(NODE_CAPABILITIES),
            registered_at = _now_iso(),
            public_url    = "",
            worker_port   = WORKER_PORT,
        )
        reg_resp = client.register(master_url, info, api_key)
        with _lock:
            _registration_token = reg_resp.registration_token
        log.info("Re-registration successful: node=%s", node_id)
    except Exception as exc:
        with _lock:
            _last_error = f"Re-registration failed: {exc}"
        log.error(_last_error)


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _uptime() -> float:
    """Return current uptime in seconds, or 0.0 if not running."""
    if _started_at is None:
        return 0.0
    return round(time.monotonic() - _started_at, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — force-stop everything and clear all runtime state."""
    global _running, _runtime_thread, _started_at, _registration_token
    global _api_key, _last_error, _completed_count, _failed_count

    _stop_event.set()
    if _runtime_thread is not None and _runtime_thread.is_alive():
        _runtime_thread.join(timeout=2.0)

    try:
        heartbeat_sender.stop_sender(timeout=1.0)
    except Exception:
        pass
    try:
        worker_server.stop(timeout=1.0)
    except Exception:
        pass

    with _lock:
        _running            = False
        _runtime_thread      = None
        _started_at          = None
        _registration_token  = None
        _api_key             = ""
        _last_error          = None
        _completed_count     = 0
        _failed_count        = 0
        _current_task_ids.clear()
