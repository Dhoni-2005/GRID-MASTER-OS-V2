"""
grid/dispatcher.py — Grid Master OS Phase 7 Step 7
Artifact: P7S7_grid_dispatcher.py
Package:  grid
Path:     grid/dispatcher.py
Status:   [K] KEEP — canonical

The Grid Dispatcher routes planned tasks from the master database to the
most suitable available worker node.

Responsibilities
----------------
• Read "planned" tasks from the database in priority order
• Select the best available worker via grid.load_balancer (RC-2: never touches node_scheduler)
• Build, sign, and HTTP-POST TaskAssignment payloads to the worker's /worker/assign endpoint
• Mark tasks "dispatched_to_node" + record assigned_node_id + dispatched_at
• Write a grid_dispatch_log row for every dispatch attempt
• On HTTP failure: call grid.failure.handle_dispatch_failure(), return task to "planned"
• On timeout: delegate to grid.failure.handle_timed_out_tasks()
• Background dispatcher loop with configurable poll interval and graceful shutdown
• Manual dispatch trigger (dispatch_once()) for testing and on-demand use
• Thread-safe: task claiming uses the UPDATE statement's own rowcount as the
  atomic serialisation point — two dispatchers racing for the same task can
  never both succeed, even when targeting the same node

Does NOT import:
  grid.memory_sync       — Step 8
  grid.master            — Step 9
  grid.worker_runtime    — Step 10
  node_scheduler         — RC-2
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import database as db
from grid.config import (
    BATCH_SIZE,
    DISPATCH_TIMEOUT_SECONDS,
    NODE_SECRET,
    POLL_INTERVAL_SECONDS,
)
from grid.failure import (
    handle_dispatch_failure,
    handle_timed_out_tasks,
    reset_failure_count,
)
from grid.load_balancer import choose_best_node
from grid.models import TaskAssignment
from grid.signing import sign_assignment
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.dispatcher")

# ── MODULE STATE ──────────────────────────────────────────────
_lock            = threading.Lock()
_stop_event      = threading.Event()
_wake_event      = threading.Event()
_dispatch_thread: threading.Thread | None = None

# Tracks task_ids currently being dispatched to prevent concurrent duplicates
_in_flight: set[int] = set()

_stats: dict[str, int] = {
    "dispatched": 0,
    "failed":     0,
    "retried":    0,
    "abandoned":  0,
    "timed_out":  0,
}


# ══════════════════════════════════════════════════════════════
# LIFECYCLE
# ══════════════════════════════════════════════════════════════

def start(poll_interval: int | None = None) -> None:
    """
    Start the background dispatcher loop as a daemon thread.
    Safe to call once; subsequent calls while running are no-ops.

    Parameters
    ----------
    poll_interval : seconds between dispatch cycles (default POLL_INTERVAL_SECONDS)
    """
    global _dispatch_thread, _stop_event, _wake_event

    with _lock:
        if _dispatch_thread is not None and _dispatch_thread.is_alive():
            log.debug("Dispatcher already running — ignoring start()")
            return
        _stop_event = threading.Event()
        _wake_event = threading.Event()

    interval = poll_interval if poll_interval is not None else POLL_INTERVAL_SECONDS

    _dispatch_thread = threading.Thread(
        target=_dispatch_loop, args=(interval,),
        name="gridmaster-dispatcher", daemon=True,
    )
    _dispatch_thread.start()
    log.info("Dispatcher started: poll_interval=%ds batch_size=%d", interval, BATCH_SIZE)


def stop(timeout: float = 10.0) -> None:
    """
    Signal the dispatcher loop to stop and wait for clean exit.

    Parameters
    ----------
    timeout : seconds to wait for thread exit (default 10.0)
    """
    global _dispatch_thread

    _stop_event.set()
    _wake_event.set()

    if _dispatch_thread is not None:
        _dispatch_thread.join(timeout=timeout)
        if _dispatch_thread.is_alive():
            log.warning("Dispatcher thread did not stop within %.1fs", timeout)
        else:
            log.info("Dispatcher stopped cleanly")

    with _lock:
        _dispatch_thread = None


def wake() -> None:
    """Wake the dispatcher loop immediately. Called when a new task is submitted."""
    _wake_event.set()


def is_running() -> bool:
    """Return True if the dispatcher background thread is alive."""
    with _lock:
        return _dispatch_thread is not None and _dispatch_thread.is_alive()


# ══════════════════════════════════════════════════════════════
# DISPATCH ENTRY POINTS
# ══════════════════════════════════════════════════════════════

def dispatch_once(project_id: int | None = None,
                  api_key:    str        = "") -> dict[str, Any]:
    """
    Run one dispatch cycle synchronously. Handles timed-out tasks first,
    then dispatches up to BATCH_SIZE planned tasks.

    Parameters
    ----------
    project_id : restrict dispatch to one project (None = all projects)
    api_key    : API key to use when calling worker /worker/assign endpoints

    Returns
    -------
    {"dispatched": int, "skipped": int, "failed": int, "timed_out": int}
    """
    timeout_result = handle_timed_out_tasks()
    timed_out = timeout_result.get("processed", 0)
    if timed_out:
        _stats["timed_out"] += timed_out
        log.info("Timeout sweep: %d tasks processed", timed_out)

    planned    = _fetch_planned_tasks(project_id)
    dispatched = 0
    skipped    = 0
    failed     = 0

    for task in planned[:BATCH_SIZE]:
        task_id = task["id"]

        with _lock:
            if task_id in _in_flight:
                skipped += 1
                continue
            _in_flight.add(task_id)

        try:
            result = _dispatch_task(task, api_key)
            if result == "dispatched":
                dispatched += 1
                _stats["dispatched"] += 1
            elif result == "no_node":
                skipped += 1
            else:
                failed += 1
                _stats["failed"] += 1
        finally:
            with _lock:
                _in_flight.discard(task_id)

    return {
        "dispatched": dispatched,
        "skipped":    skipped,
        "failed":     failed,
        "timed_out":  timed_out,
    }


def dispatch_task_direct(task: dict[str, Any], api_key: str = "") -> str:
    """
    Dispatch a single specific task to the best available worker.

    Parameters
    ----------
    task    : task dict from database
    api_key : API key for worker authentication

    Returns
    -------
    "dispatched" | "no_node" | "failed" | "duplicate"
    """
    task_id = task.get("id")
    if task_id is None:
        log.error("dispatch_task_direct: task has no 'id'")
        return "failed"

    with _lock:
        if task_id in _in_flight:
            return "duplicate"
        _in_flight.add(task_id)

    try:
        return _dispatch_task(task, api_key)
    finally:
        with _lock:
            _in_flight.discard(task_id)


def record_result(task_id: int,
                  node_id: str,
                  status:  str,
                  output:  str = "",
                  error:   str = "") -> dict[str, Any]:
    """
    Process a task result reported by a worker node.

    Parameters
    ----------
    task_id : integer task id
    node_id : worker node that produced the result
    status  : "completed" | "failed" | "rejected" | "interrupted"
    output  : task output string
    error   : error message (on failure)

    Returns
    -------
    {"recorded": bool, "task_id": int, "status": str}
    """
    rows = db._query(
        "SELECT id, status, assigned_node_id, project_id FROM tasks WHERE id=?",
        (task_id,),
    )
    if not rows:
        log.warning("record_result: task %d not found", task_id)
        return {"recorded": False, "task_id": task_id, "status": "not_found"}

    task = rows[0]

    if task["status"] in ("completed", "rejected", "failed", "abandoned"):
        log.info("record_result: task %d already terminal (%s) — idempotent",
                 task_id, task["status"])
        return {"recorded": False, "task_id": task_id, "status": task["status"]}

    if task.get("assigned_node_id") and task["assigned_node_id"] != node_id:
        log.warning(
            "record_result: task %d assigned to %s but result from %s",
            task_id, task["assigned_node_id"], node_id,
        )
        return {"recorded": False, "task_id": task_id, "status": "wrong_node"}

    try:
        if status == "completed":
            db.complete_task_atomic(
                task_id,
                output         = output,
                node_id        = node_id,
                memory_content = output or f"Task {task_id} completed by {node_id}",
                project_id     = task.get("project_id"),
            )
        elif status in ("failed", "interrupted"):
            db.fail_task_atomic(
                task_id,
                status     = status,
                output     = output,
                node_id    = node_id,
                problem    = error or f"Task {task_id} {status}",
                cause      = error or "",
                fix        = "",
                tags       = [],
                project_id = task.get("project_id"),
            )
        elif status == "rejected":
            db.update_task_status(task_id, "rejected", output=output)
        else:
            log.warning("record_result: unknown status '%s' for task %d", status, task_id)
            return {"recorded": False, "task_id": task_id, "status": "unknown_status"}
    except Exception as exc:
        log.error("record_result: DB error for task %d: %s", task_id, exc)
        return {"recorded": False, "task_id": task_id, "status": "db_error"}

    _update_dispatch_log(task_id, outcome=status)

    if status == "completed":
        reset_failure_count(node_id)

    log_event(AuditEvent.TASK_SUBMITTED,
              detail=f"task_result task_id={task_id} node={node_id} status={status}")
    log.info("Result recorded: task_id=%d node=%s status=%s", task_id, node_id, status)
    return {"recorded": True, "task_id": task_id, "status": status}


# ══════════════════════════════════════════════════════════════
# INTERNAL DISPATCH LOGIC
# ══════════════════════════════════════════════════════════════

def _dispatch_loop(interval: int) -> None:
    """Background loop: dispatch, sleep, repeat until stop signal."""
    log.debug("Dispatcher loop started: interval=%ds", interval)
    while not _stop_event.is_set():
        try:
            dispatch_once()
        except Exception as exc:
            log.error("Dispatcher loop unhandled error: %s", exc)
        _wake_event.clear()
        _wake_event.wait(timeout=interval)
    log.debug("Dispatcher loop exited")


def _dispatch_task(task: dict[str, Any], api_key: str) -> str:
    """Core dispatch logic for one task. Returns 'dispatched', 'no_node', or 'failed'."""
    task_id = task["id"]

    node = choose_best_node(task)
    if node is None:
        log.debug("No eligible node for task %d", task_id)
        return "no_node"

    node_id = node["node_id"]

    assignment = _build_assignment(task, node_id)

    if NODE_SECRET:
        payload_dict = assignment.to_dict()
        sig = sign_assignment(
            {k: v for k, v in payload_dict.items() if k != "signature"},
            NODE_SECRET,
        )
        assignment.signature = sig
    else:
        log.debug("NODE_SECRET not set — dispatch without HMAC signature")

    try:
        claimed = _claim_task(task_id, node_id)
    except Exception as exc:
        log.error("DB error claiming task %d: %s", task_id, exc)
        return "failed"

    if not claimed:
        log.debug("Task %d already claimed by a concurrent dispatcher", task_id)
        return "no_node"

    success = _send_to_worker(task_id, node_id, node, assignment, api_key)

    if success:
        reset_failure_count(node_id)
        log_event(AuditEvent.TASK_SUBMITTED,
                  detail=f"task_dispatched task_id={task_id} node={node_id}")
        log.info("Dispatched task %d → node %s", task_id, node_id)
        return "dispatched"
    else:
        _revert_dispatch(task_id)
        handle_dispatch_failure(node_id, task_id)
        _stats["failed"] += 1
        return "failed"


def _send_to_worker(task_id:    int,
                    node_id:    str,
                    node:       dict[str, Any],
                    assignment: TaskAssignment,
                    api_key:    str) -> bool:
    """POST the assignment to the worker's /worker/assign endpoint."""
    public_url  = node.get("public_url") or node.get("url") or ""
    worker_port = node.get("worker_port", 8001)

    if not public_url:
        public_url = f"http://{node_id}:{worker_port}"

    worker_base = public_url.rstrip("/")

    try:
        from grid.client import _http_post
        resp = _http_post(
            url         = f"{worker_base}/worker/assign",
            payload     = assignment.to_dict(),
            api_key     = api_key,
            timeout     = DISPATCH_TIMEOUT_SECONDS,
            max_retries = 1,
        )
        accepted = resp.get("accepted", False)
        if not accepted:
            log.warning("Worker %s rejected task %d: %s", node_id, task_id, resp)
            return False
        return True
    except Exception as exc:
        log.warning("Dispatch HTTP error: task=%d node=%s: %s", task_id, node_id, exc)
        return False


def _build_assignment(task: dict[str, Any], node_id: str) -> TaskAssignment:
    """Construct a TaskAssignment dataclass from a task DB row."""
    return TaskAssignment(
        task_id     = task["id"],
        project_id  = task["project_id"],
        title       = task.get("title", ""),
        input_data  = task.get("input_data", ""),
        priority    = task.get("priority", 5),
        assigned_at = _now_iso(),
        signature   = "",
    )


def _claim_task(task_id: int, node_id: str) -> bool:
    """
    Atomically claim a task by transitioning it from "planned" to
    "dispatched_to_node". Uses the raw sqlite3 connection's rowcount
    (not database.py's _exec, which returns lastrowid) as the true
    atomicity signal — this is the only correct way to detect whether
    THIS call won the race when multiple dispatchers target the same node.

    Returns True if this call performed the transition; False if the
    task was already claimed (by another dispatcher or already dispatched).
    """
    conn = db.get_db()
    with conn:
        cursor = conn.execute(
            "UPDATE tasks SET status='dispatched_to_node', "
            "assigned_node_id=?, dispatched_at=? "
            "WHERE id=? AND status='planned'",
            (node_id, _now_iso(), task_id),
        )
        won = cursor.rowcount == 1

    if won:
        _insert_dispatch_log(task_id, node_id)
    return won


def _revert_dispatch(task_id: int) -> None:
    """Return a task to "planned" status after a failed dispatch attempt."""
    db._exec(
        "UPDATE tasks SET status='planned', "
        "assigned_node_id=NULL, dispatched_at=NULL WHERE id=?",
        (task_id,),
    )
    _update_dispatch_log(task_id, outcome="dispatch_failed")
    log.debug("Reverted dispatch: task %d returned to planned", task_id)


def _fetch_planned_tasks(project_id: int | None = None) -> list[dict[str, Any]]:
    """Fetch up to BATCH_SIZE planned tasks ordered by priority DESC, created_at ASC."""
    try:
        return db.list_tasks(project_id=project_id, status="planned")
    except Exception as exc:
        log.error("_fetch_planned_tasks: DB error: %s", exc)
        return []


# ══════════════════════════════════════════════════════════════
# DISPATCH LOG HELPERS
# ══════════════════════════════════════════════════════════════

def _insert_dispatch_log(task_id: int, node_id: str) -> None:
    """Insert a new row into grid_dispatch_log for this dispatch attempt."""
    try:
        db._exec(
            "INSERT INTO grid_dispatch_log (task_id, node_id, dispatched_at) "
            "VALUES (?, ?, ?)",
            (task_id, node_id, _now_iso()),
        )
    except Exception as exc:
        log.debug("_insert_dispatch_log task=%d: %s", task_id, exc)


def _update_dispatch_log(task_id: int, outcome: str) -> None:
    """Update the most recent dispatch log row for this task with an outcome."""
    try:
        db._exec(
            "UPDATE grid_dispatch_log SET outcome=?, result_received=? "
            "WHERE id=(SELECT id FROM grid_dispatch_log "
            "          WHERE task_id=? ORDER BY id DESC LIMIT 1)",
            (outcome, _now_iso(), task_id),
        )
    except Exception as exc:
        log.debug("_update_dispatch_log task=%d outcome=%s: %s", task_id, outcome, exc)


# ══════════════════════════════════════════════════════════════
# STATISTICS & MONITORING
# ══════════════════════════════════════════════════════════════

def get_stats() -> dict[str, Any]:
    """Return cumulative dispatch statistics since the dispatcher started."""
    with _lock:
        in_flight = len(_in_flight)
    return {**_stats, "in_flight": in_flight}


def get_pending_count(project_id: int | None = None) -> int:
    """Return the number of tasks currently in "planned" status."""
    return len(_fetch_planned_tasks(project_id))


def get_in_flight_task_ids() -> list[int]:
    """Return the list of task_ids currently being dispatched."""
    with _lock:
        return list(_in_flight)


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — stop thread, clear all state."""
    global _dispatch_thread
    stop(timeout=2.0)
    with _lock:
        _in_flight.clear()
        _dispatch_thread = None
    for k in _stats:
        _stats[k] = 0
