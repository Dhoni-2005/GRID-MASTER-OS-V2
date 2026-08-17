"""
grid/reconciler.py — Grid Master OS Phase 7 Step 8
Artifact: P7S8_grid_reconciler.py
Package:  grid
Path:     grid/reconciler.py
Status:   [K] KEEP — canonical

Maintains consistency between the task database, dispatcher state,
worker results, and the memory system. Runs as a periodic background
sweep that detects and repairs drift caused by crashes, network
partitions, or missed events.

Responsibilities
----------------
• detect_missing_memory_entries() — completed tasks with no memory_sync entry
• detect_stale_tasks()            — tasks stuck in "dispatched_to_node" past timeout
• repair_inconsistent_state()     — apply fixes for both categories above
• reconciliation_loop()           — background thread running repairs periodically
• Delegates timeout handling to grid.failure (never duplicates that logic)
• Delegates memory sync to grid.memory_sync (never duplicates that logic)

Does NOT import:
  grid.master            — Step 9
  grid.worker_runtime     — Step 10
  node_scheduler          — RC-2
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

import database as db
from grid.config import DISPATCH_TIMEOUT_SECONDS
from grid.failure import handle_timed_out_tasks
from grid.memory_sync import sync_completed_tasks, _has_existing_sync
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.reconciler")

# ── MODULE STATE ──────────────────────────────────────────────
_lock              = threading.Lock()
_stop_event        = threading.Event()
_reconciler_thread: threading.Thread | None = None

_DEFAULT_INTERVAL_SECONDS: int = 60

# Cumulative repair statistics
_stats: dict[str, int] = {
    "memory_repairs":   0,
    "stale_repairs":    0,
    "cycles_run":       0,
}


# ══════════════════════════════════════════════════════════════
# DETECTION
# ══════════════════════════════════════════════════════════════

def detect_missing_memory_entries(project_id: int | None = None,
                                   limit:      int        = 200) -> list[dict[str, Any]]:
    """
    Find completed tasks that have no corresponding grid_sync memory entry.

    Parameters
    ----------
    project_id : restrict to one project (None = all)
    limit      : maximum number of tasks to inspect

    Returns
    -------
    List of task dicts (id, title, project_id, assigned_node_id, completed status)
    that are missing a memory entry.
    """
    try:
        completed = db.list_tasks(project_id=project_id, status="completed")[:limit]
    except Exception as exc:
        log.error("detect_missing_memory_entries: DB error: %s", exc)
        return []

    missing = [t for t in completed if not _has_existing_sync(t["id"])]

    if missing:
        log.info("Reconciler: %d completed tasks missing memory entries", len(missing))
    return missing


def detect_stale_tasks(timeout_seconds: int | None = None) -> list[dict[str, Any]]:
    """
    Find tasks stuck in "dispatched_to_node" status for longer than the
    dispatch timeout window without a result being reported.

    This reuses the same index-backed query pattern as
    grid.failure.detect_timed_out_tasks() but is exposed here as a
    read-only detection function for reconciliation reporting purposes
    (the actual repair delegates to grid.failure).

    Parameters
    ----------
    timeout_seconds : override for DISPATCH_TIMEOUT_SECONDS

    Returns
    -------
    List of stale task dicts.
    """
    cutoff = timeout_seconds if timeout_seconds is not None else DISPATCH_TIMEOUT_SECONDS
    try:
        rows = db._query(
            "SELECT id, title, assigned_node_id, dispatched_at, project_id "
            "FROM tasks "
            "WHERE status='dispatched_to_node' "
            "  AND dispatched_at IS NOT NULL "
            "  AND datetime(dispatched_at) < datetime('now', ? || ' seconds')",
            (f"-{cutoff}",),
        )
    except Exception as exc:
        log.error("detect_stale_tasks: DB error: %s", exc)
        return []

    if rows:
        log.info("Reconciler: %d stale dispatched tasks detected", len(rows))
    return rows


def detect_orphaned_dispatch_logs() -> list[dict[str, Any]]:
    """
    Find grid_dispatch_log rows referencing task_ids that no longer exist
    in the tasks table (should never normally happen — defensive check).

    Returns
    -------
    List of orphaned log row dicts.
    """
    try:
        rows = db._query(
            "SELECT gdl.* FROM grid_dispatch_log gdl "
            "LEFT JOIN tasks t ON gdl.task_id = t.id "
            "WHERE t.id IS NULL"
        )
    except Exception as exc:
        log.error("detect_orphaned_dispatch_logs: DB error: %s", exc)
        return []
    return rows


# ══════════════════════════════════════════════════════════════
# REPAIR
# ══════════════════════════════════════════════════════════════

def repair_inconsistent_state(project_id: int | None = None) -> dict[str, Any]:
    """
    Run one full repair pass:
      1. Sync any completed tasks missing memory entries (via grid.memory_sync).
      2. Reassign or abandon any stale dispatched tasks (via grid.failure).

    This function never duplicates the logic in memory_sync or failure —
    it only orchestrates calls to those modules and reports the combined result.

    Parameters
    ----------
    project_id : restrict repair scope to one project (None = all)

    Returns
    -------
    {
      "memory_synced":     int,
      "memory_failed":     int,
      "stale_processed":   int,
      "stale_reassigned":  int,
      "stale_abandoned":   int,
    }
    """
    # 1. Memory repair — delegate entirely to grid.memory_sync
    memory_result = sync_completed_tasks(project_id=project_id)

    with _lock:
        _stats["memory_repairs"] += memory_result.get("synced", 0)

    # 2. Stale task repair — delegate entirely to grid.failure
    stale_result = handle_timed_out_tasks()

    with _lock:
        _stats["stale_repairs"] += (
            stale_result.get("reassigned", 0) + stale_result.get("abandoned", 0)
        )

    summary = {
        "memory_synced":    memory_result.get("synced", 0),
        "memory_failed":    memory_result.get("failed", 0),
        "stale_processed":  stale_result.get("processed", 0),
        "stale_reassigned": stale_result.get("reassigned", 0),
        "stale_abandoned":  stale_result.get("abandoned", 0),
    }

    if any(summary.values()):
        log.info("Reconciler repair pass: %s", summary)
        log_event(AuditEvent.ADMIN_ACTION,
                  detail=f"reconciler_repair {summary}")

    return summary


def repair_missing_memory_only(project_id: int | None = None) -> dict[str, Any]:
    """
    Run only the memory-entry repair step, without touching stale tasks.
    Useful for targeted repair or testing.

    Parameters
    ----------
    project_id : restrict to one project (None = all)

    Returns
    -------
    Result dict from grid.memory_sync.sync_completed_tasks().
    """
    result = sync_completed_tasks(project_id=project_id)
    with _lock:
        _stats["memory_repairs"] += result.get("synced", 0)
    return result


def repair_stale_tasks_only() -> dict[str, Any]:
    """
    Run only the stale-task repair step, without touching memory sync.
    Useful for targeted repair or testing.

    Returns
    -------
    Result dict from grid.failure.handle_timed_out_tasks().
    """
    result = handle_timed_out_tasks()
    with _lock:
        _stats["stale_repairs"] += (
            result.get("reassigned", 0) + result.get("abandoned", 0)
        )
    return result


# ══════════════════════════════════════════════════════════════
# BACKGROUND LOOP
# ══════════════════════════════════════════════════════════════

def start(interval_seconds: int = _DEFAULT_INTERVAL_SECONDS) -> None:
    """
    Start the reconciliation loop as a background daemon thread.
    Safe to call once; subsequent calls while running are no-ops.

    Parameters
    ----------
    interval_seconds : seconds between reconciliation passes
    """
    global _reconciler_thread, _stop_event

    with _lock:
        if _reconciler_thread is not None and _reconciler_thread.is_alive():
            log.debug("Reconciler already running — ignoring start()")
            return
        _stop_event = threading.Event()

    _reconciler_thread = threading.Thread(
        target=reconciliation_loop, args=(interval_seconds,),
        name="gridmaster-reconciler", daemon=True,
    )
    _reconciler_thread.start()
    log.info("Reconciler started: interval=%ds", interval_seconds)


def stop(timeout: float = 10.0) -> None:
    """
    Signal the reconciliation loop to stop and wait for clean exit.

    Parameters
    ----------
    timeout : seconds to wait for thread exit (default 10.0)
    """
    global _reconciler_thread

    _stop_event.set()
    if _reconciler_thread is not None:
        _reconciler_thread.join(timeout=timeout)
        if _reconciler_thread.is_alive():
            log.warning("Reconciler thread did not stop within %.1fs", timeout)
        else:
            log.info("Reconciler stopped cleanly")

    with _lock:
        _reconciler_thread = None


def is_running() -> bool:
    """Return True if the reconciliation background thread is alive."""
    with _lock:
        return _reconciler_thread is not None and _reconciler_thread.is_alive()


def reconciliation_loop(interval_seconds: int) -> None:
    """
    Main reconciliation loop body. Runs repair_inconsistent_state()
    repeatedly until stop() is called.

    Parameters
    ----------
    interval_seconds : seconds to sleep between passes
    """
    log.debug("Reconciliation loop started: interval=%ds", interval_seconds)
    while not _stop_event.is_set():
        try:
            repair_inconsistent_state()
            with _lock:
                _stats["cycles_run"] += 1
        except Exception as exc:
            log.error("Reconciliation loop unhandled error: %s", exc)

        _stop_event.wait(timeout=interval_seconds)

    log.debug("Reconciliation loop exited")


# ══════════════════════════════════════════════════════════════
# MONITORING
# ══════════════════════════════════════════════════════════════

def get_stats() -> dict[str, int]:
    """
    Return cumulative reconciler statistics since process start.

    Returns
    -------
    {"memory_repairs": int, "stale_repairs": int, "cycles_run": int}
    """
    with _lock:
        return dict(_stats)


def get_consistency_report(project_id: int | None = None) -> dict[str, Any]:
    """
    Produce a read-only snapshot of current consistency issues without
    repairing anything. Useful for dashboards and pre-repair inspection.

    Parameters
    ----------
    project_id : restrict scope to one project (None = all)

    Returns
    -------
    {
      "missing_memory_count": int,
      "stale_task_count":     int,
      "orphaned_log_count":   int,
    }
    """
    missing = detect_missing_memory_entries(project_id=project_id)
    stale   = detect_stale_tasks()
    orphans = detect_orphaned_dispatch_logs()

    return {
        "missing_memory_count": len(missing),
        "stale_task_count":     len(stale),
        "orphaned_log_count":   len(orphans),
    }


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """TESTING ONLY — stop thread and clear all statistics."""
    global _reconciler_thread
    stop(timeout=2.0)
    with _lock:
        _reconciler_thread = None
        for k in _stats:
            _stats[k] = 0
