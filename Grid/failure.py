"""
grid/failure.py — Grid Master OS Phase 7 Step 4
Artifact: P7S4_grid_failure.py
Package:  grid
Path:     grid/failure.py
Status:   [K] KEEP — canonical

Central failure-management component for the distributed Grid runtime.

Responsibilities
----------------
• Failure detection and classification (temporary vs permanent)
• Node failure representation and tracking
• Task failure representation and retry tracking
• Retry policy enforcement (configurable limits, exponential backoff)
• Dispatch-failure counter with automatic quarantine trigger
• Timeout detection for dispatched tasks
• Task reassignment (reset to "planned") with retry-limit enforcement
• Node quarantine delegation to grid.registry
• Structured audit logging of every failure event
• Recovery recommendation utilities

This module is intentionally dependency-light:
  Imports: database, grid.config, grid.registry, security.audit
  Does NOT import: grid.client, grid.dispatcher, grid.heartbeat_*, worker, reviewer

Used by (Steps 5–10 — not yet implemented):
  grid/heartbeat_monitor.py  — calls handle_offline_node()
  grid/dispatcher.py         — calls handle_dispatch_failure(), reassign_task()
  grid/master.py             — calls get_failure_summary()
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import database as db
from grid.config import (
    DISPATCH_FAILURE_THRESHOLD,
    MAX_REASSIGNMENTS,
    QUARANTINE_MINUTES,
)
from grid.registry import (
    classify_health,
    mark_offline,
    quarantine_node,
)
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.failure")

# ── THREAD SAFETY ─────────────────────────────────────────────
_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════
# ENUMERATIONS
# ══════════════════════════════════════════════════════════════

class FailureKind(str, Enum):
    """
    High-level classification of a failure event.

    TRANSIENT  — likely self-correcting (network blip, restart).
                 Retry is appropriate.
    PERMANENT  — node is consistently failing or task is fundamentally broken.
                 Reassign to a different node or abandon the task.
    TIMEOUT    — no result received within the dispatch window.
                 Treated as transient unless retry limit is exceeded.
    CRASH      — worker process exited without reporting a result.
                 Treated as permanent for the current node.
    """
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    TIMEOUT   = "timeout"
    CRASH     = "crash"


class RecoveryAction(str, Enum):
    """
    Recommended action after a failure has been classified.

    RETRY_SAME_NODE   — requeue on the same node (rare; only for transient flickers).
    REASSIGN          — move task to a different node.
    QUARANTINE_NODE   — stop routing to this node temporarily.
    ABANDON_TASK      — retry limit exceeded; mark task abandoned.
    OFFLINE_NODE      — node is offline; reassign all its tasks.
    NO_ACTION         — informational failure; no intervention required.
    """
    RETRY_SAME_NODE = "retry_same_node"
    REASSIGN        = "reassign"
    QUARANTINE_NODE = "quarantine_node"
    ABANDON_TASK    = "abandon_task"
    OFFLINE_NODE    = "offline_node"
    NO_ACTION       = "no_action"


# ══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════

@dataclass
class NodeFailureEvent:
    """
    Represents a single failure event attributed to a worker node.

    Attributes
    ----------
    node_id       : the worker node that failed
    kind          : failure classification
    detail        : human-readable description
    task_id       : task being dispatched when the failure occurred (optional)
    occurred_at   : ISO-8601 timestamp of the failure
    """
    node_id:     str
    kind:        FailureKind
    detail:      str
    task_id:     int | None = None
    occurred_at: str        = field(
        default_factory=lambda: datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id":     self.node_id,
            "kind":        self.kind.value,
            "detail":      self.detail,
            "task_id":     self.task_id,
            "occurred_at": self.occurred_at,
        }


@dataclass
class TaskFailureEvent:
    """
    Represents a single failure event for a task.

    Attributes
    ----------
    task_id       : the task that failed
    node_id       : worker node that was executing it (optional)
    kind          : failure classification
    detail        : human-readable description
    retry_count   : number of times this task has been retried so far
    occurred_at   : ISO-8601 timestamp
    """
    task_id:     int
    kind:        FailureKind
    detail:      str
    node_id:     str | None = None
    retry_count: int        = 0
    occurred_at: str        = field(
        default_factory=lambda: datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id":     self.task_id,
            "node_id":     self.node_id,
            "kind":        self.kind.value,
            "detail":      self.detail,
            "retry_count": self.retry_count,
            "occurred_at": self.occurred_at,
        }


@dataclass
class FailureDecision:
    """
    Output of classify_and_decide(): the recommended recovery action
    and all context needed to execute it.

    Attributes
    ----------
    action         : what the caller should do
    kind           : classification of the triggering failure
    reason         : human-readable explanation of the decision
    should_quarantine_node : whether the node should be quarantined
    should_abandon_task    : whether the task is beyond retrying
    retry_count            : current retry count for the task
    """
    action:                  RecoveryAction
    kind:                    FailureKind
    reason:                  str
    should_quarantine_node:  bool = False
    should_abandon_task:     bool = False
    retry_count:             int  = 0


# ══════════════════════════════════════════════════════════════
# IN-MEMORY FAILURE COUNTERS
# ══════════════════════════════════════════════════════════════

# {node_id: int}  — consecutive dispatch failures per node
_node_failure_counts: dict[str, int] = {}

# {node_id: float}  — monotonic timestamp of last failure
_node_last_failure_ts: dict[str, float] = {}


# ══════════════════════════════════════════════════════════════
# RETRY POLICY
# ══════════════════════════════════════════════════════════════

def backoff_seconds(retry_count: int,
                    base: float = 2.0,
                    cap:  float = 60.0) -> float:
    """
    Compute the exponential backoff delay for a given retry count.

    Formula: min(base ** retry_count, cap)

    Parameters
    ----------
    retry_count : number of previous failed attempts (0-indexed)
    base        : base multiplier in seconds (default 2.0)
    cap         : maximum delay in seconds (default 60.0)

    Returns
    -------
    Delay in seconds as a float.

    Examples
    --------
    backoff_seconds(0)  →  1.0 s   (2^0 = 1)
    backoff_seconds(1)  →  2.0 s
    backoff_seconds(2)  →  4.0 s
    backoff_seconds(3)  →  8.0 s
    backoff_seconds(6)  → 60.0 s   (capped)
    """
    return min(base ** retry_count, cap)


def has_exceeded_retry_limit(retry_count: int,
                              limit: int | None = None) -> bool:
    """
    Return True if retry_count has reached or exceeded the retry limit.

    Parameters
    ----------
    retry_count : current number of retries already attempted
    limit       : maximum allowed retries; defaults to MAX_REASSIGNMENTS from config

    Returns
    -------
    True if the task should be abandoned; False if further retries are allowed.
    """
    effective_limit = limit if limit is not None else MAX_REASSIGNMENTS
    return retry_count >= effective_limit


def get_retry_count_for_task(task_id: int) -> int:
    """
    Read the current retry count for a task from the agent_notes table.
    Counts notes with role='reassignment' to determine how many times
    the task has been reassigned across nodes.

    Parameters
    ----------
    task_id : integer task id

    Returns
    -------
    Number of reassignment events recorded for this task (0 if none).
    """
    try:
        rows = db._query(
            "SELECT COUNT(*) AS cnt FROM agent_notes "
            "WHERE task_id=? AND agent_role='reassignment'",
            (task_id,),
        )
        return rows[0]["cnt"] if rows else 0
    except Exception as exc:
        log.warning("get_retry_count_for_task(%d): %s", task_id, exc)
        return 0


# ══════════════════════════════════════════════════════════════
# FAILURE CLASSIFICATION
# ══════════════════════════════════════════════════════════════

def classify_failure(event: NodeFailureEvent | TaskFailureEvent,
                     retry_count: int = 0) -> FailureDecision:
    """
    Classify a failure event and recommend a recovery action.

    Classification rules
    --------------------
    1. CRASH or PERMANENT kind → OFFLINE_NODE + REASSIGN
    2. TIMEOUT kind → REASSIGN if retries remain, else ABANDON_TASK
    3. TRANSIENT kind:
         - retry_count < limit → REASSIGN
         - retry_count >= limit → ABANDON_TASK + QUARANTINE_NODE
    4. Node health is offline (detected live) → OFFLINE_NODE

    Parameters
    ----------
    event       : the failure event to classify
    retry_count : number of retries already attempted for the affected task

    Returns
    -------
    FailureDecision with the recommended RecoveryAction and context.
    """
    kind = event.kind
    node_id = event.node_id if isinstance(event, NodeFailureEvent) else event.node_id

    # Live health check supplements the event kind
    if node_id and classify_health(node_id) == "offline":
        return FailureDecision(
            action    = RecoveryAction.OFFLINE_NODE,
            kind      = FailureKind.CRASH,
            reason    = f"Node {node_id} is offline (live health check)",
            should_quarantine_node = False,   # offline_node flow handles this
            should_abandon_task    = False,
            retry_count            = retry_count,
        )

    if kind in (FailureKind.CRASH, FailureKind.PERMANENT):
        return FailureDecision(
            action                 = RecoveryAction.REASSIGN,
            kind                   = kind,
            reason                 = f"{kind.value} failure on node {node_id}: {event.detail}",
            should_quarantine_node = True,
            should_abandon_task    = False,
            retry_count            = retry_count,
        )

    exceeded = has_exceeded_retry_limit(retry_count)

    if kind == FailureKind.TIMEOUT:
        if exceeded:
            return FailureDecision(
                action                 = RecoveryAction.ABANDON_TASK,
                kind                   = kind,
                reason                 = f"Timeout: retry limit {MAX_REASSIGNMENTS} exceeded",
                should_quarantine_node = False,
                should_abandon_task    = True,
                retry_count            = retry_count,
            )
        return FailureDecision(
            action                 = RecoveryAction.REASSIGN,
            kind                   = kind,
            reason                 = f"Timeout on node {node_id} (attempt {retry_count + 1})",
            should_quarantine_node = False,
            should_abandon_task    = False,
            retry_count            = retry_count,
        )

    # TRANSIENT
    if exceeded:
        return FailureDecision(
            action                 = RecoveryAction.ABANDON_TASK,
            kind                   = kind,
            reason                 = (
                f"Transient failure limit reached ({retry_count} retries). "
                f"Abandoning task."
            ),
            should_quarantine_node = True,
            should_abandon_task    = True,
            retry_count            = retry_count,
        )

    return FailureDecision(
        action                 = RecoveryAction.REASSIGN,
        kind                   = kind,
        reason                 = (
            f"Transient failure on node {node_id} "
            f"(attempt {retry_count + 1}/{MAX_REASSIGNMENTS})"
        ),
        should_quarantine_node = False,
        should_abandon_task    = False,
        retry_count            = retry_count,
    )


# ══════════════════════════════════════════════════════════════
# NODE FAILURE HANDLING
# ══════════════════════════════════════════════════════════════

def handle_dispatch_failure(node_id: str,
                             task_id: int | None = None) -> None:
    """
    Record a dispatch failure for a node and trigger quarantine when
    the consecutive-failure threshold is reached.

    Called by grid.dispatcher when a worker HTTP call fails.
    On success, call reset_failure_count() to clear the counter.

    Parameters
    ----------
    node_id : worker node that failed to accept dispatch
    task_id : task being dispatched (for audit context, optional)
    """
    with _lock:
        _node_failure_counts[node_id] = _node_failure_counts.get(node_id, 0) + 1
        _node_last_failure_ts[node_id] = time.monotonic()
        count = _node_failure_counts[node_id]

    log.warning(
        "Dispatch failure: node=%s task=%s consecutive_failures=%d",
        node_id, task_id, count,
    )
    log_event(
        AuditEvent.ADMIN_ACTION,
        detail=f"dispatch_failure node={node_id} task={task_id} count={count}",
    )

    if count >= DISPATCH_FAILURE_THRESHOLD:
        log.warning(
            "Node %s reached dispatch failure threshold (%d). Quarantining.",
            node_id, DISPATCH_FAILURE_THRESHOLD,
        )
        quarantine_node(node_id, minutes=QUARANTINE_MINUTES)
        log_event(
            AuditEvent.ADMIN_ACTION,
            detail=(
                f"node_quarantined node={node_id} "
                f"reason=dispatch_failure_threshold count={count}"
            ),
        )


def reset_failure_count(node_id: str) -> None:
    """
    Reset the consecutive dispatch-failure counter for a node.
    Called by grid.dispatcher after a successful dispatch.

    Parameters
    ----------
    node_id : worker node that successfully accepted a dispatch
    """
    with _lock:
        if node_id in _node_failure_counts:
            previous = _node_failure_counts.pop(node_id, 0)
            _node_last_failure_ts.pop(node_id, None)
            log.debug(
                "Failure count reset for node %s (was %d)", node_id, previous
            )


def handle_offline_node(node_id: str) -> dict[str, Any]:
    """
    Handle a node that has been detected as offline.

    Steps
    -----
    1. Mark the node offline in node_registry.
    2. Query all tasks in "dispatched_to_node" status assigned to this node.
    3. For each task: call reassign_task() to reset to "planned" or abandon.
    4. Log the offline event to the audit trail.

    Parameters
    ----------
    node_id : the offline worker node

    Returns
    -------
    {
      "node_id":    str,
      "reassigned": int,   — tasks returned to "planned"
      "abandoned":  int,   — tasks that hit retry limit → "abandoned"
    }

    Called by: grid.heartbeat_monitor (Step 5)
    """
    mark_offline(node_id)
    log.warning("Handling offline node: %s", node_id)

    # Find all tasks dispatched to this node still awaiting result
    try:
        tasks = _list_dispatched_tasks_for_node(node_id)
    except Exception as exc:
        log.error("handle_offline_node: db error querying tasks for %s: %s", node_id, exc)
        tasks = []

    reassigned = 0
    abandoned  = 0
    for task in tasks:
        result = reassign_task(task["id"])
        if result == "reassigned":
            reassigned += 1
        elif result == "abandoned":
            abandoned += 1

    log_event(
        AuditEvent.ADMIN_ACTION,
        detail=(
            f"node_offline node={node_id} "
            f"reassigned={reassigned} abandoned={abandoned}"
        ),
    )
    log.info(
        "Offline node %s handled: %d reassigned, %d abandoned",
        node_id, reassigned, abandoned,
    )
    return {
        "node_id":    node_id,
        "reassigned": reassigned,
        "abandoned":  abandoned,
    }


# ══════════════════════════════════════════════════════════════
# TASK REASSIGNMENT
# ══════════════════════════════════════════════════════════════

def reassign_task(task_id: int,
                  node_id: str | None = None,
                  reason:  str        = "node_offline") -> str:
    """
    Attempt to reassign a task back to the "planned" queue.

    If the task has already been reassigned MAX_REASSIGNMENTS times,
    it is set to "abandoned" instead.

    Steps
    -----
    1. Read current retry count from agent_notes.
    2. If retries < MAX_REASSIGNMENTS:
         a. Reset status to "planned", clear assigned_node_id.
         b. Insert a reassignment note in agent_notes.
         c. Update grid_dispatch_log outcome.
    3. Else:
         a. Set status to "abandoned".
         b. Insert an abandonment note.

    Parameters
    ----------
    task_id : integer id of the task to reassign
    node_id : node that was holding the task (for logging)
    reason  : short string describing why reassignment is occurring

    Returns
    -------
    "reassigned" if the task was reset to "planned"
    "abandoned"  if the retry limit was exceeded
    "not_found"  if the task_id does not exist
    """
    try:
        task = db._query("SELECT * FROM tasks WHERE id=?", (task_id,))
    except Exception as exc:
        log.error("reassign_task(%d): db error: %s", task_id, exc)
        return "not_found"

    if not task:
        log.warning("reassign_task: task %d not found", task_id)
        return "not_found"

    retry_count = get_retry_count_for_task(task_id)
    now         = _now_iso()

    if retry_count >= MAX_REASSIGNMENTS:
        # Abandon
        try:
            db._exec(
                "UPDATE tasks SET status='abandoned', assigned_node_id=NULL WHERE id=?",
                (task_id,),
            )
            _insert_agent_note(
                task_id   = task_id,
                role      = "reassignment",
                note      = (
                    f"ABANDONED after {retry_count} reassignment(s). "
                    f"node={node_id} reason={reason}"
                ),
                created_at = now,
            )
            _update_dispatch_log_outcome(task_id, "abandoned")
        except Exception as exc:
            log.error("reassign_task: abandon failed for task %d: %s", task_id, exc)

        log.warning(
            "Task %d abandoned after %d retries (max=%d)",
            task_id, retry_count, MAX_REASSIGNMENTS,
        )
        log_event(
            AuditEvent.ADMIN_ACTION,
            detail=(
                f"task_abandoned task_id={task_id} "
                f"retries={retry_count} reason={reason}"
            ),
        )
        return "abandoned"

    # Reassign to "planned"
    try:
        db._exec(
            "UPDATE tasks SET status='planned', "
            "assigned_node_id=NULL, dispatched_at=NULL WHERE id=?",
            (task_id,),
        )
        _insert_agent_note(
            task_id    = task_id,
            role       = "reassignment",
            note       = (
                f"Reassigned (attempt {retry_count + 1}/{MAX_REASSIGNMENTS}). "
                f"node={node_id} reason={reason}"
            ),
            created_at = now,
        )
        _update_dispatch_log_outcome(task_id, "reassigned")
    except Exception as exc:
        log.error("reassign_task: reassign failed for task %d: %s", task_id, exc)
        return "not_found"

    log.info(
        "Task %d reassigned to 'planned' (attempt %d/%d)",
        task_id, retry_count + 1, MAX_REASSIGNMENTS,
    )
    log_event(
        AuditEvent.ADMIN_ACTION,
        detail=(
            f"task_reassigned task_id={task_id} "
            f"attempt={retry_count + 1}/{MAX_REASSIGNMENTS} "
            f"node={node_id} reason={reason}"
        ),
    )
    return "reassigned"


# ══════════════════════════════════════════════════════════════
# TIMEOUT DETECTION
# ══════════════════════════════════════════════════════════════

def detect_timed_out_tasks(timeout_seconds: int | None = None) -> list[dict[str, Any]]:
    """
    Find all tasks that have been in "dispatched_to_node" status
    for longer than timeout_seconds without a result being reported.

    Uses the idx_tasks_dispatched index created in db_adapter.init_schema().

    Parameters
    ----------
    timeout_seconds : override for the dispatch timeout; defaults to
                      DISPATCH_TIMEOUT_SECONDS from grid.config

    Returns
    -------
    List of task dicts with keys: id, title, assigned_node_id, dispatched_at
    """
    from grid.config import DISPATCH_TIMEOUT_SECONDS
    cutoff_seconds = timeout_seconds if timeout_seconds is not None else DISPATCH_TIMEOUT_SECONDS

    try:
        rows = db._query(
            "SELECT id, title, assigned_node_id, dispatched_at "
            "FROM tasks "
            "WHERE status='dispatched_to_node' "
            "  AND dispatched_at IS NOT NULL "
            "  AND dispatched_at < datetime('now', ? || ' seconds')",
            (f"-{cutoff_seconds}",),
        )
        if rows:
            log.info("Timeout detection: %d timed-out tasks found", len(rows))
        return rows
    except Exception as exc:
        log.error("detect_timed_out_tasks: db error: %s", exc)
        return []


def handle_timed_out_tasks(timeout_seconds: int | None = None) -> dict[str, Any]:
    """
    Detect and handle all timed-out dispatched tasks in one pass.

    For each timed-out task: calls reassign_task() which resets to
    "planned" or abandons it based on the retry count.

    Parameters
    ----------
    timeout_seconds : dispatch timeout override (see detect_timed_out_tasks)

    Returns
    -------
    {"processed": int, "reassigned": int, "abandoned": int}

    Called by: grid.dispatcher.collect_pending_results() (Step 7)
    """
    tasks       = detect_timed_out_tasks(timeout_seconds)
    reassigned  = 0
    abandoned   = 0

    for task in tasks:
        result = reassign_task(
            task_id = task["id"],
            node_id = task.get("assigned_node_id"),
            reason  = "dispatch_timeout",
        )
        if result == "reassigned":
            reassigned += 1
        elif result == "abandoned":
            abandoned += 1

    return {
        "processed":  len(tasks),
        "reassigned": reassigned,
        "abandoned":  abandoned,
    }


# ══════════════════════════════════════════════════════════════
# MONITORING & SUMMARIES
# ══════════════════════════════════════════════════════════════

def get_failure_summary() -> dict[str, Any]:
    """
    Return an in-memory snapshot of the current failure state.
    Used by grid.master monitoring endpoints (Step 9).

    Returns
    -------
    {
      "nodes_with_failures": int,
      "failure_counts": {node_id: count},
      "last_failure_ts": {node_id: seconds_ago},
    }
    """
    with _lock:
        counts = dict(_node_failure_counts)
        ts_map = dict(_node_last_failure_ts)

    now = time.monotonic()
    return {
        "nodes_with_failures": len(counts),
        "failure_counts": counts,
        "last_failure_ts": {
            nid: round(now - ts, 1)
            for nid, ts in ts_map.items()
        },
    }


def get_node_failure_count(node_id: str) -> int:
    """
    Return the current consecutive dispatch-failure count for a node.

    Parameters
    ----------
    node_id : worker node to query

    Returns
    -------
    Integer count; 0 if no failures recorded.
    """
    with _lock:
        return _node_failure_counts.get(node_id, 0)


def is_node_failing(node_id: str) -> bool:
    """
    Return True if the node has any recorded consecutive dispatch failures.

    Parameters
    ----------
    node_id : worker node to check
    """
    return get_node_failure_count(node_id) > 0


def get_tasks_for_node(node_id: str,
                        status: str = "dispatched_to_node") -> list[dict[str, Any]]:
    """
    Return all tasks currently assigned to a node with the given status.
    Convenience wrapper used by handle_offline_node() and tests.

    Parameters
    ----------
    node_id : worker node to query
    status  : task status filter (default: "dispatched_to_node")

    Returns
    -------
    List of task dicts.
    """
    return _list_dispatched_tasks_for_node(node_id, status)


# ══════════════════════════════════════════════════════════════
# RECOVERY RECOMMENDATIONS
# ══════════════════════════════════════════════════════════════

def recommend_recovery(node_id: str | None = None,
                        task_id: int | None = None) -> dict[str, Any]:
    """
    Produce a human-readable recovery recommendation for a node or task.
    Used by monitoring dashboards and admin endpoints in grid.master (Step 9).

    Parameters
    ----------
    node_id : optional node to analyse
    task_id : optional task to analyse

    Returns
    -------
    {
      "node_id":         str | None,
      "task_id":         int | None,
      "node_health":     str,          — online/stale/offline/unknown
      "failure_count":   int,
      "task_retry_count":int,
      "recommended_action": str,
      "detail":          str,
    }
    """
    node_health   = classify_health(node_id) if node_id else "unknown"
    failure_count = get_node_failure_count(node_id) if node_id else 0
    retry_count   = get_retry_count_for_task(task_id) if task_id else 0

    if node_health == "offline":
        action = RecoveryAction.OFFLINE_NODE.value
        detail = f"Node {node_id} is offline. Reassign all its tasks."
    elif failure_count >= DISPATCH_FAILURE_THRESHOLD:
        action = RecoveryAction.QUARANTINE_NODE.value
        detail = (
            f"Node {node_id} has {failure_count} consecutive failures "
            f"(threshold={DISPATCH_FAILURE_THRESHOLD}). Quarantine recommended."
        )
    elif task_id and retry_count >= MAX_REASSIGNMENTS:
        action = RecoveryAction.ABANDON_TASK.value
        detail = (
            f"Task {task_id} has been reassigned {retry_count} times "
            f"(max={MAX_REASSIGNMENTS}). Abandon."
        )
    elif task_id and retry_count > 0:
        action = RecoveryAction.REASSIGN.value
        detail = (
            f"Task {task_id} has {retry_count} retries. "
            f"Reassign to a different node."
        )
    elif node_health == "stale":
        action = RecoveryAction.NO_ACTION.value
        detail = f"Node {node_id} is stale. Await next heartbeat before acting."
    else:
        action = RecoveryAction.NO_ACTION.value
        detail = "No intervention required at this time."

    return {
        "node_id":            node_id,
        "task_id":            task_id,
        "node_health":        node_health,
        "failure_count":      failure_count,
        "task_retry_count":   retry_count,
        "recommended_action": action,
        "detail":             detail,
    }


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _list_dispatched_tasks_for_node(node_id: str,
                                     status: str = "dispatched_to_node") -> list[dict[str, Any]]:
    """Return tasks currently assigned to node_id with the given status."""
    try:
        return db._query(
            "SELECT * FROM tasks WHERE assigned_node_id=? AND status=?",
            (node_id, status),
        )
    except Exception as exc:
        log.error("_list_dispatched_tasks_for_node(%s): %s", node_id, exc)
        return []


def _insert_agent_note(task_id:    int,
                        role:       str,
                        note:       str,
                        created_at: str | None = None) -> None:
    """Insert one row into agent_notes. Silently ignores DB errors."""
    try:
        db._exec(
            "INSERT INTO agent_notes (task_id, agent_role, note, created_at) "
            "VALUES (?, ?, ?, ?)",
            (task_id, role, note, created_at or _now_iso()),
        )
    except Exception as exc:
        log.warning("_insert_agent_note(task=%d): %s", task_id, exc)


def _update_dispatch_log_outcome(task_id: int, outcome: str) -> None:
    """
    Update the most recent grid_dispatch_log row for task_id with the given outcome.
    Silently ignores errors (log table is informational; not blocking).
    """
    try:
        db._exec(
            "UPDATE grid_dispatch_log SET outcome=?, result_received=? "
            "WHERE id = ("
            "  SELECT id FROM grid_dispatch_log "
            "  WHERE task_id=? ORDER BY id DESC LIMIT 1"
            ")",
            (outcome, _now_iso(), task_id),
        )
    except Exception as exc:
        log.debug("_update_dispatch_log_outcome(task=%d outcome=%s): %s",
                  task_id, outcome, exc)


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string (no timezone suffix)."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """
    TESTING ONLY — clear all in-memory failure counters.
    Never call in production.
    """
    with _lock:
        _node_failure_counts.clear()
        _node_last_failure_ts.clear()
