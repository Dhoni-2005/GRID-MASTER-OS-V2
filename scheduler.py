"""
scheduler.py — Grid Master OS Kernel v1.0 (Phase 4 Step 1)

Scheduler: selects which task should execute next.

Extracted from coordinator._select_task() to isolate scheduling
logic from orchestration logic. Coordinator delegates here.

Phase 4 Step 1 behaviour (unchanged from Phase 3):
    - Highest priority first.
    - Earliest created_at as tie-breaker.

Future scheduler upgrades (weighted scoring, node-affinity,
deadline-aware ordering) replace only select_task()'s internals
without changing its signature or Coordinator's usage of it.

Communication:
    Imports database.py only. No Worker, Reviewer, Planner,
    network, threading, multiprocessing, or LLM dependencies.
"""
import database as db

_MODULE = "[SCHEDULER]"

# Statuses the scheduler will consider dispatchable
_DISPATCHABLE = {"planned", "review_pending"}


def select_task(project_id: int) -> dict | None:
    """
    Return the next task to dispatch for a project, or None
    if no dispatchable task exists.

    Selection rule (Phase 4 Step 1 — unchanged from Phase 3):
        1. Status must be in {'planned', 'review_pending'}.
        2. Highest priority first.
        3. Earliest created_at as tie-breaker.

    Parameters
    ----------
    project_id : project to select a task from

    Returns
    -------
    dict  : the selected task record
    None  : no dispatchable task found
    """
    candidates: list[dict] = []
    for status in _DISPATCHABLE:
        candidates.extend(
            db.list_tasks(project_id=project_id, status=status)
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda t: (-t.get("priority", 0),
                       t.get("created_at", ""))
    )
    return candidates[0]


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "scheduler"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    pid = _db.create_project("Scheduler Test", "Phase 4 Step 1 self-test")

    # Test 1: no tasks → None
    assert select_task(pid) is None, "Expected None for empty project"
    print("  Test 1 passed: no tasks → None")

    # Test 2: single planned task → returned
    t1 = _db.create_task(pid, "Task A", priority=5)
    _db.update_task_status(t1, "planned")
    sel = select_task(pid)
    assert sel is not None and sel["id"] == t1
    print("  Test 2 passed: single planned task selected")

    # Test 3: higher priority wins
    t2 = _db.create_task(pid, "Task B", priority=9)
    _db.update_task_status(t2, "planned")
    sel2 = select_task(pid)
    assert sel2["id"] == t2, f"Expected higher priority task, got {sel2['id']}"
    print("  Test 3 passed: higher priority task selected first")

    # Test 4: tie-breaker — earliest created_at wins
    t3 = _db.create_task(pid, "Task C", priority=9)  # same priority as t2
    _db.update_task_status(t3, "planned")
    sel3 = select_task(pid)
    assert sel3["id"] == t2, \
        f"Expected earlier-created task on tie, got {sel3['id']}"
    print("  Test 4 passed: tie-breaker uses earliest created_at")

    # Test 5: review_pending also dispatchable
    t4 = _db.create_task(pid, "Task D", priority=20)
    _db.update_task_status(t4, "review_pending")
    sel4 = select_task(pid)
    assert sel4["id"] == t4
    print("  Test 5 passed: review_pending task is dispatchable")

    # Test 6: completed/failed/rejected excluded
    t5 = _db.create_task(pid, "Task E", priority=99)
    _db.update_task_status(t5, "completed")
    sel5 = select_task(pid)
    assert sel5["id"] != t5, "Completed task must never be selected"
    print("  Test 6 passed: completed task excluded from selection")

    # Test 7: project isolation
    other_pid = _db.create_project("Other", "isolation test")
    t6 = _db.create_task(other_pid, "Other project task", priority=50)
    _db.update_task_status(t6, "planned")
    sel6 = select_task(pid)
    assert sel6["id"] != t6, "Scheduler must not cross project boundaries"
    print("  Test 7 passed: project isolation enforced")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 4 Step 1 — Scheduler).")
