"""
grid/memory_sync.py — Grid Master OS Phase 7 Step 8
Artifact: P7S8_grid_memory_sync.py
Package:  grid
Path:     grid/memory_sync.py
Status:   [K] KEEP — canonical

Bridges completed distributed task results into the existing memory system.
Every task that grid.dispatcher.record_result() marks "completed" should
produce exactly one memory entry describing what happened, which worker
node produced it, and when — without duplicating the write if called twice.

Responsibilities
----------------
• sync_memory_entry()    — write one task's result into memory_manager
• sync_completed_tasks() — batch-sync all eligible completed tasks
• get_sync_status()      — report sync coverage and pending backlog
• Duplicate prevention: checks memory_entries for an existing grid-sync
  entry (identified by a stable tag) before writing
• Retry tracking: failed syncs are recorded in-memory and retried on
  the next sync_completed_tasks() call, with exponential backoff via
  grid.failure.backoff_seconds()
• Thread-safe: a lock serialises the check-then-write sequence per task_id

Does NOT import:
  grid.master            — Step 9
  grid.worker_runtime     — Step 10
  node_scheduler          — RC-2
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import database as db
import memory_manager as mm
from grid.failure import backoff_seconds
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.memory_sync")

# ── SYNC TAG ──────────────────────────────────────────────────
# Every memory entry created by this module carries this tag so
# duplicate-prevention can identify prior grid syncs cheaply.
_SYNC_TAG = "grid_sync"

# ── MODULE STATE ──────────────────────────────────────────────
_lock = threading.Lock()

# {task_id: {"attempts": int, "last_attempt_ts": float, "last_error": str}}
_failed_syncs: dict[int, dict[str, Any]] = {}

# {task_id: float} — monotonic timestamp of last successful sync (for status reporting)
_synced_tasks: dict[int, float] = {}


# ══════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════

def sync_memory_entry(task_id: int) -> dict[str, Any]:
    """
    Sync a single completed task's result into the memory system.

    Flow
    ----
    1. Read the task row (must exist and be "completed").
    2. Check for an existing grid_sync memory entry for this task_id
       (duplicate prevention).
    3. Build a content string from task output, node, and timestamps.
    4. Call memory_manager.remember() with the grid_sync tag.
    5. Record success/failure in the in-memory retry tracker.

    Parameters
    ----------
    task_id : integer id of a completed task

    Returns
    -------
    {
      "synced":     bool,
      "task_id":    int,
      "memory_id":  int | None,
      "reason":     str,   — "ok" | "duplicate" | "not_completed" | "not_found" | "write_failed"
    }
    """
    with _lock:
        task = _get_task(task_id)
        if task is None:
            return {"synced": False, "task_id": task_id, "memory_id": None,
                    "reason": "not_found"}

        if task.get("status") != "completed":
            return {"synced": False, "task_id": task_id, "memory_id": None,
                    "reason": "not_completed"}

        if _has_existing_sync(task_id):
            log.debug("memory_sync: task %d already synced — skipping", task_id)
            _synced_tasks[task_id] = time.monotonic()
            _failed_syncs.pop(task_id, None)
            return {"synced": False, "task_id": task_id, "memory_id": None,
                    "reason": "duplicate"}

        content = _build_memory_content(task)
        summary = _build_summary(task)

        try:
            memory_id = mm.remember(
                task_id     = task_id,
                content     = content,
                entry_type  = "task_result",
                tags        = [_SYNC_TAG, task.get("assigned_node_id") or "unknown_node"],
                importance  = mm.SCORE_LOG if hasattr(mm, "SCORE_LOG") else 3,
                project_id  = task.get("project_id"),
                summary     = summary,
            )
        except Exception as exc:
            log.error("memory_sync: remember() raised for task %d: %s", task_id, exc)
            memory_id = -1

        if memory_id is None or memory_id < 0:
            _record_failure(task_id, "memory_manager.remember() returned invalid id")
            log_event(AuditEvent.ADMIN_ACTION,
                      detail=f"memory_sync_failed task_id={task_id}")
            return {"synced": False, "task_id": task_id, "memory_id": None,
                    "reason": "write_failed"}

        _synced_tasks[task_id] = time.monotonic()
        _failed_syncs.pop(task_id, None)

        log.info("memory_sync: task %d synced → memory_id=%d", task_id, memory_id)
        log_event(AuditEvent.ADMIN_ACTION,
                  detail=f"memory_synced task_id={task_id} memory_id={memory_id}")
        return {"synced": True, "task_id": task_id, "memory_id": memory_id, "reason": "ok"}


def sync_completed_tasks(project_id: int | None = None,
                          limit:      int        = 100) -> dict[str, Any]:
    """
    Batch-sync all eligible completed tasks that have not yet been synced.
    Applies exponential backoff to tasks that previously failed to sync.

    Parameters
    ----------
    project_id : restrict to one project (None = all projects)
    limit      : maximum number of tasks to process in one call

    Returns
    -------
    {
      "synced":     int,
      "duplicate":  int,
      "failed":     int,
      "skipped_backoff": int,
      "total_checked":   int,
    }
    """
    tasks = _get_completed_tasks(project_id, limit)
    synced = 0
    duplicate = 0
    failed = 0
    skipped_backoff = 0

    for task in tasks:
        task_id = task["id"]

        if _is_in_backoff(task_id):
            skipped_backoff += 1
            continue

        result = sync_memory_entry(task_id)
        if result["synced"]:
            synced += 1
        elif result["reason"] == "duplicate":
            duplicate += 1
        elif result["reason"] == "write_failed":
            failed += 1

    return {
        "synced":           synced,
        "duplicate":        duplicate,
        "failed":           failed,
        "skipped_backoff":  skipped_backoff,
        "total_checked":    len(tasks),
    }


def get_sync_status() -> dict[str, Any]:
    """
    Return a status snapshot of the memory sync subsystem.

    Returns
    -------
    {
      "synced_count":       int,   — tasks successfully synced (this process lifetime)
      "failed_count":       int,   — tasks currently in retry backlog
      "failed_task_ids":    [int],
      "pending_completed":  int,   — completed tasks with no memory_sync entry yet
    }
    """
    with _lock:
        synced_count = len(_synced_tasks)
        failed_count = len(_failed_syncs)
        failed_ids   = list(_failed_syncs.keys())

    pending = _count_pending_completed()

    return {
        "synced_count":      synced_count,
        "failed_count":      failed_count,
        "failed_task_ids":   failed_ids,
        "pending_completed": pending,
    }


def retry_failed_syncs() -> dict[str, Any]:
    """
    Attempt to re-sync every task currently in the failure backlog,
    respecting exponential backoff timing.

    Returns
    -------
    {"retried": int, "succeeded": int, "still_failing": int}
    """
    with _lock:
        candidates = list(_failed_syncs.keys())

    retried = 0
    succeeded = 0
    still_failing = 0

    for task_id in candidates:
        if _is_in_backoff(task_id):
            continue
        retried += 1
        result = sync_memory_entry(task_id)
        if result["synced"]:
            succeeded += 1
        elif result["reason"] == "write_failed":
            still_failing += 1

    return {"retried": retried, "succeeded": succeeded, "still_failing": still_failing}


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _get_task(task_id: int) -> dict[str, Any] | None:
    """Fetch one task row by id."""
    try:
        return db.get_task(task_id)
    except Exception as exc:
        log.error("_get_task(%d): %s", task_id, exc)
        return None


def _get_completed_tasks(project_id: int | None, limit: int) -> list[dict[str, Any]]:
    """Fetch completed tasks, optionally scoped to a project."""
    try:
        return db.list_tasks(project_id=project_id, status="completed")[:limit]
    except Exception as exc:
        log.error("_get_completed_tasks: %s", exc)
        return []


def _has_existing_sync(task_id: int) -> bool:
    """
    Check whether a memory_entries row already exists for this task_id
    that was written by this module (identified by the _SYNC_TAG).
    """
    try:
        rows = db._query(
            "SELECT id, tags FROM memory_entries WHERE task_id=?",
            (task_id,),
        )
    except Exception as exc:
        log.error("_has_existing_sync(%d): %s", task_id, exc)
        return False

    for row in rows:
        tags = row.get("tags") or ""
        if _SYNC_TAG in tags:
            return True
    return False


def _build_memory_content(task: dict[str, Any]) -> str:
    """
    Build a descriptive memory content string from a completed task row.
    Includes output, node, project, and timing metadata.
    """
    output      = task.get("output", "") or "(no output recorded)"
    node_id     = task.get("assigned_node_id") or "unknown_node"
    dispatched  = task.get("dispatched_at") or "unknown"
    title       = task.get("title", "")

    return (
        f"Task '{title}' (id={task['id']}) completed by node '{node_id}'.\n"
        f"Dispatched at: {dispatched}\n"
        f"Output:\n{output}"
    )


def _build_summary(task: dict[str, Any]) -> str:
    """Build a short one-line summary for the memory entry."""
    title = task.get("title", "")
    node  = task.get("assigned_node_id") or "unknown_node"
    return f"Grid task '{title}' completed on {node}"


def _record_failure(task_id: int, error: str) -> None:
    """Track a failed sync attempt for later retry with backoff."""
    entry = _failed_syncs.setdefault(task_id, {"attempts": 0})
    entry["attempts"]        += 1
    entry["last_attempt_ts"]  = time.monotonic()
    entry["last_error"]       = error


def _is_in_backoff(task_id: int) -> bool:
    """
    Return True if task_id is in the failure backlog and still within
    its exponential backoff window (should not be retried yet).
    """
    entry = _failed_syncs.get(task_id)
    if entry is None:
        return False
    elapsed = time.monotonic() - entry.get("last_attempt_ts", 0)
    wait    = backoff_seconds(entry.get("attempts", 0))
    return elapsed < wait


def _count_pending_completed() -> int:
    """Count completed tasks that have no grid_sync memory entry yet."""
    try:
        completed = db.list_tasks(status="completed")
    except Exception as exc:
        log.error("_count_pending_completed: %s", exc)
        return 0

    pending = 0
    for task in completed:
        if not _has_existing_sync(task["id"]):
            pending += 1
    return pending


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — clear all in-memory sync tracking state."""
    with _lock:
        _failed_syncs.clear()
        _synced_tasks.clear()
