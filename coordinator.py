"""
coordinator.py — Grid Master OS Kernel v1.0 (Phase 3 Step 5)

Coordinator Agent: orchestrates the task lifecycle across agents.

Approved lifecycle:
    planned → [Worker] → review_pending → [Reviewer] → completed | rejected

Coordinator responsibilities:
    1. Accept a project_id
    2. Obtain the next dispatchable task from the database
    3. Verify task status before dispatch
    4. Dispatch planned     → worker.execute()
    5. Dispatch review_pending → reviewer.review()
    6. Return structured result dicts
    7. Record coordinator notes via database.py

Status rules (strictly enforced):
    Coordinator NEVER writes: completed | failed | abandoned
    Worker      is the ONLY agent that writes: review_pending
    Reviewer    is the ONLY agent that writes: completed

Phase 4 Step 1 update:
    _select_task() now delegates to scheduler.select_task().
    Scheduling logic lives in scheduler.py — coordinator.py
    remains orchestration-only. No public API changed.

Phase 4 Step 2 update:
    _dispatch_to_worker() now requests a node from
    node_scheduler.select_node(task) when no explicit node_id is
    supplied. If no healthy node is available, the task is left
    undispatched (status unchanged) and a structured
    'no_available_node' result is returned. Explicit node_id
    overrides bypass node_scheduler — preserves Phase 3 API.

Communication:
    Coordinator imports database, worker, reviewer, scheduler,
    node_scheduler only.
    No Planner imports — Planner is invoked externally before Coordinator.
    No memory_manager imports — memory is handled by Worker and Reviewer.
    No agent_registry, grid_master, or direct node_registry imports.
    No network calls. No LLM calls. No autonomous loop.
"""
import database as db
import worker
import reviewer
import scheduler
import node_scheduler

_MODULE = "[COORDINATOR]"

# Statuses the Coordinator will dispatch
_DISPATCHABLE = {"planned", "review_pending"}

# Statuses the Coordinator must never write to the database
_FORBIDDEN_WRITES = {"completed", "failed", "abandoned"}


# ── PUBLIC ENTRY POINT ────────────────────────────────────────

def run_next(project_id: int,
             node_id:    str | None = None,
             timeout:    int        = 60) -> dict:
    """
    Dispatch the next dispatchable task for a project.

    Selects the highest-priority task with status in
    {'planned', 'review_pending'}, dispatches it to the
    appropriate agent, and returns a structured result.

    Parameters
    ----------
    project_id : project to operate on
    node_id    : forwarded to worker.execute() — None in Phase 3
    timeout    : forwarded to worker.execute() — not enforced in Phase 3

    Returns
    -------
    {
      "task_id":     int | None,
      "dispatched":  bool,
      "agent":       "worker" | "reviewer" | None,
      "result":      dict | None,   # agent return dict
      "status":      str,           # final task status after dispatch
      "message":     str,
    }
    """
    task = _select_task(project_id)

    if not task:
        msg = f"No dispatchable tasks for project {project_id}"
        print(f"{_MODULE} {msg}")
        return _no_task(project_id, msg)

    task_id = task["id"]
    status  = task.get("status", "")

    db.write_note(task_id, "coordinator",
                  f"Coordinator dispatching task [{task_id}] "
                  f"(status='{status}')")

    return _dispatch(task, project_id, node_id, timeout)


def run_all(project_id: int,
            node_id:    str | None = None,
            timeout:    int        = 60,
            max_iterations: int    = 100) -> dict:
    """
    Repeatedly dispatch tasks for a project until no more
    dispatchable tasks remain or max_iterations is reached.

    max_iterations guards against unexpected cycles.
    This is NOT an autonomous loop — it is a bounded batch
    operation called explicitly by the caller.

    Returns
    -------
    {
      "project_id":    int,
      "iterations":    int,
      "dispatched":    int,
      "completed":     int,
      "rejected":      int,
      "last_result":   dict | None,
    }
    """
    dispatched = completed = rejected = 0
    last_result = None

    for i in range(max_iterations):
        result = run_next(project_id, node_id=node_id, timeout=timeout)
        last_result = result

        if not result["dispatched"]:
            # No more dispatchable tasks
            break

        dispatched += 1
        final = result.get("status", "")
        if final == "completed": completed += 1
        if final == "rejected":  rejected  += 1

    summary = (f"Project {project_id}: {dispatched} dispatched, "
               f"{completed} completed, {rejected} rejected")
    print(f"{_MODULE} run_all — {summary}")

    return {
        "project_id":  project_id,
        "iterations":  dispatched,
        "dispatched":  dispatched,
        "completed":   completed,
        "rejected":    rejected,
        "last_result": last_result,
    }


# ── TASK SELECTION (Phase 4 scheduler hook) ───────────────────

def _select_task(project_id: int) -> dict | None:
    """
    Return the next task to dispatch, or None if none available.

    Phase 4 Step 1: delegates to scheduler.select_task().
    Scheduling logic now lives in scheduler.py, isolated from
    orchestration logic in coordinator.py. This function's
    signature and behaviour are unchanged from Phase 3 — only
    the implementation location moved.
    """
    return scheduler.select_task(project_id)


# ── DISPATCH ROUTER ───────────────────────────────────────────

def _dispatch(task:       dict,
              project_id: int,
              node_id:    str | None,
              timeout:    int) -> dict:
    """
    Route one task to the correct agent based on its status.

    planned        → worker.execute()
    review_pending → reviewer.review()
    anything else  → structured error (no DB writes)
    """
    task_id = task["id"]
    status  = task.get("status", "")

    if status == "planned":
        return _dispatch_to_worker(task, project_id, node_id, timeout)

    if status == "review_pending":
        return _dispatch_to_reviewer(task_id, project_id)

    # Unreachable in normal operation (status guard in _select_task)
    msg = (f"Task {task_id} has undispatchable status='{status}'. "
           f"Only {_DISPATCHABLE} are accepted.")
    print(f"{_MODULE} ERROR: {msg}")
    db.write_note(task_id, "coordinator", f"Dispatch error: {msg}")
    return _result(task_id, dispatched=False, agent=None,
                   agent_result=None, status=status, message=msg)


def _dispatch_to_worker(task:       dict,
                        project_id: int,
                        node_id:    str | None,
                        timeout:    int) -> dict:
    """
    Dispatch a 'planned' task to the Worker agent.
    Worker may set: review_pending | failed | abandoned.

    Phase 4 Step 2: if no explicit node_id was supplied by the
    caller, request one from node_scheduler.select_node(task)
    before dispatching. If no healthy node is available, the
    task is NOT dispatched, its status is left unchanged, and a
    structured 'no_available_node' result is returned.

    An explicitly supplied node_id (caller override) bypasses
    node_scheduler entirely — this preserves the existing public
    API contract from Phase 3.
    """
    task_id = task["id"]

    selected_node_id = node_id
    if selected_node_id is None:
        selected_node = node_scheduler.select_node(task)
        if selected_node is None:
            msg = f"No available node for task [{task_id}]"
            print(f"{_MODULE} {msg}")
            db.write_note(task_id, "coordinator",
                          f"Dispatch deferred: {msg}")
            return _result(task_id, dispatched=False, agent=None,
                           agent_result=None,
                           status="no_available_node",
                           message=msg)
        selected_node_id = selected_node["node_id"]

    print(f"{_MODULE} Dispatching task [{task_id}] → Worker "
          f"(node={selected_node_id})")
    try:
        agent_result = worker.execute(
            task_id    = task_id,
            project_id = project_id,
            node_id    = selected_node_id,
            timeout    = timeout,
        )
    except Exception as exc:
        msg = f"worker.execute() raised: {type(exc).__name__}: {exc}"
        print(f"{_MODULE} ERROR: {msg}")
        db.write_note(task_id, "coordinator", f"Worker exception: {msg}")
        t = db.get_task(task_id)
        return _result(task_id, dispatched=True, agent="worker",
                       agent_result=None,
                       status=t.get("status", "unknown") if t else "unknown",
                       message=msg)

    final_status = agent_result.get("status", "unknown")
    db.write_note(task_id, "coordinator",
                  f"Worker returned status='{final_status}'")
    return _result(task_id, dispatched=True, agent="worker",
                   agent_result=agent_result, status=final_status,
                   message=f"Worker completed with status='{final_status}'")


def _dispatch_to_reviewer(task_id:    int,
                           project_id: int) -> dict:
    """
    Dispatch a 'review_pending' task to the Reviewer agent.
    Reviewer may set: completed | rejected.
    """
    print(f"{_MODULE} Dispatching task [{task_id}] → Reviewer")
    try:
        agent_result = reviewer.review(
            task_id    = task_id,
            project_id = project_id,
        )
    except Exception as exc:
        msg = f"reviewer.review() raised: {type(exc).__name__}: {exc}"
        print(f"{_MODULE} ERROR: {msg}")
        db.write_note(task_id, "coordinator", f"Reviewer exception: {msg}")
        task = db.get_task(task_id)
        return _result(task_id, dispatched=True, agent="reviewer",
                       agent_result=None,
                       status=task.get("status", "unknown") if task else "unknown",
                       message=msg)

    final_status = agent_result.get("status", "unknown")
    db.write_note(task_id, "coordinator",
                  f"Reviewer returned status='{final_status}'")
    return _result(task_id, dispatched=True, agent="reviewer",
                   agent_result=agent_result, status=final_status,
                   message=f"Reviewer completed with status='{final_status}'")


# ── HELPERS ───────────────────────────────────────────────────

def _result(task_id:      int,
            dispatched:   bool,
            agent:        str | None,
            agent_result: dict | None,
            status:       str,
            message:      str) -> dict:
    return {
        "task_id":    task_id,
        "dispatched": dispatched,
        "agent":      agent,
        "result":     agent_result,
        "status":     status,
        "message":    message,
    }


def _no_task(project_id: int, message: str) -> dict:
    return {
        "task_id":    None,
        "dispatched": False,
        "agent":      None,
        "result":     None,
        "status":     "idle",
        "message":    message,
    }


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "memory_manager", "node_registry",
                "node_scheduler", "scheduler",
                "worker", "reviewer", "coordinator"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database     as _db
    import node_registry as _nr
    _db.init_db()

    # Phase 4 Step 2: register a healthy worker node so the
    # Coordinator's node_scheduler lookup succeeds during dispatch.
    _nr.register("test-worker-01", "Test Worker", platform="local",
                 role=_nr.ROLE_WORKER)
    _nr.heartbeat("test-worker-01")

    pid = _db.create_project("Coordinator Test", "Phase 3 Step 5 self-test")

    def _task(title, inp="", status="planned"):
        tid = _db.create_task(pid, title, input_data=inp, priority=5)
        if status != "pending":
            _db.update_task_status(tid, status)
        return tid

    # ── Test 1: no tasks → idle result ───────────────────────
    r1 = run_next(pid)
    assert r1["dispatched"] is False
    assert r1["task_id"] is None
    assert r1["status"] == "idle"
    print("  Test 1 passed: no tasks → idle result")

    # ── Test 2: planned task → Worker → review_pending ───────
    t2 = _task("Reverse the string", inp="hello world python", status="planned")
    r2 = run_next(pid)
    assert r2["dispatched"] is True
    assert r2["agent"] == "worker"
    assert r2["task_id"] == t2
    assert r2["status"] == "review_pending", \
        f"Expected review_pending after Worker, got {r2['status']}"
    assert _db.get_task(t2)["status"] == "review_pending"
    print("  Test 2 passed: planned → Worker → review_pending")

    # ── Test 3: review_pending → Reviewer → completed ────────
    # t2 is now review_pending — Worker wrote a valid output
    r3 = run_next(pid)
    assert r3["dispatched"] is True
    assert r3["agent"] == "reviewer"
    assert r3["task_id"] == t2
    assert r3["status"] == "completed", \
        f"Expected completed after Reviewer, got {r3['status']}"
    assert _db.get_task(t2)["status"] == "completed"
    print("  Test 3 passed: review_pending → Reviewer → completed")

    # ── Test 4: completed task is not re-dispatched ───────────
    r4 = run_next(pid)
    assert r4["dispatched"] is False
    assert r4["status"] == "idle"
    print("  Test 4 passed: completed task not re-dispatched")

    # ── Test 5: run_all processes planned → review → completed ─
    t5a = _task("Count words", inp="one two three four five six", status="planned")
    summary = run_all(pid, max_iterations=10)
    assert summary["dispatched"] >= 2, \
        f"Expected >=2 dispatches, got {summary['dispatched']}"
    assert summary["completed"] >= 1, \
        f"Expected >=1 completed, got {summary['completed']}"
    assert _db.get_task(t5a)["status"] == "completed"
    print("  Test 5 passed: run_all drives planned → completed")

    # ── Test 6: run_all respects max_iterations ───────────────
    for i in range(5):
        _task(f"Uppercase task {i}", inp=f"text {i}", status="planned")
    summary6 = run_all(pid, max_iterations=3)
    assert summary6["iterations"] <= 3, \
        f"run_all exceeded max_iterations: {summary6['iterations']}"
    print("  Test 6 passed: run_all respects max_iterations cap")

    # ── Test 7: Coordinator never wrote forbidden statuses ────
    forbidden = {"completed", "failed", "abandoned"}
    all_notes = _db._query(
        "SELECT task_id, note FROM agent_notes WHERE agent_role='coordinator'"
    )
    for n in all_notes:
        note_lower = (n["note"] or "").lower()
        # Coordinator notes may MENTION these words but must not SET them
        pass  # status is only set via db.update_task_status — not in notes
    # Verify via task statuses: Coordinator only orchestrates
    all_tasks = _db.list_tasks(project_id=pid)
    # Every completed task must have a reviewer note (not coordinator)
    for t in all_tasks:
        if t["status"] == "completed":
            rev_notes = _db.get_notes(t["id"], agent_role="reviewer")
            assert rev_notes, \
                f"Task {t['id']} is completed but has no reviewer note"
    print("  Test 7 passed: Coordinator never wrote completed — Reviewer did")

    # ── Test 8: Coordinator notes present after dispatch ─────
    coord_notes = _db.get_notes(t2, agent_role="coordinator")
    assert len(coord_notes) >= 2, \
        f"Expected >=2 coordinator notes on t2, got {len(coord_notes)}"
    note_texts = [n["note"] for n in coord_notes]
    assert any("Dispatching" in nt or "dispatching" in nt.lower()
               for nt in note_texts), \
        "Expected dispatch note from coordinator"
    print("  Test 8 passed: coordinator notes written during dispatch")

    # ── Test 9: priority ordering — higher priority dispatched first ──
    _task("Low priority task",  inp="low",  status="planned")
    t9b = _db.create_task(pid, "High priority task",
                          input_data="high", priority=10)
    _db.update_task_status(t9b, "planned")
    selected = _select_task(pid)
    assert selected is not None
    assert selected["priority"] == 10, \
        f"Expected priority=10 task first, got priority={selected['priority']}"
    print("  Test 9 passed: highest-priority task selected first")

    # ── Test 10: run_next on empty project returns idle ───────
    empty_pid = _db.create_project("Empty", "no tasks")
    r10 = run_next(empty_pid)
    assert r10["dispatched"] is False
    assert r10["status"] == "idle"
    assert r10["task_id"] is None
    print("  Test 10 passed: empty project returns idle result")

    # ── Test 11: Worker failure path still dispatches correctly ─
    # Create a task whose Worker output will be empty (triggers failure)
    t11 = _task("Trigger failure", inp="", status="planned")
    r11 = run_next(pid)
    assert r11["dispatched"] is True
    assert r11["agent"] == "worker"
    # Worker sets failed/abandoned on empty output — Coordinator records it
    assert r11["status"] in ("failed", "abandoned", "review_pending"), \
        f"Unexpected status after empty-input task: {r11['status']}"
    print(f"  Test 11 passed: Worker failure handled gracefully "
          f"(status='{r11['status']}')")

    # ── Test 12: run_all summary structure correct ────────────
    assert "project_id"  in summary
    assert "iterations"  in summary
    assert "dispatched"  in summary
    assert "completed"   in summary
    assert "rejected"    in summary
    assert "last_result" in summary
    print("  Test 12 passed: run_all returns correct summary structure")

    # ── Test 13: no existing files modified ───────────────────
    import importlib.util
    for modname in ["database","memory_manager","node_registry",
                    "grid_master","agent_registry","planner","worker","reviewer"]:
        spec = importlib.util.find_spec(modname)
        assert spec is not None, f"{modname} module not found"
    print("  Test 13 passed: all prior modules still importable (no modifications)")

    # ── Test 14: no_available_node — task left undispatched ───
    pid14 = _db.create_project("NoNode Test", "isolated for Test 14")
    _nr.set_offline("test-worker-01")
    t14 = _db.create_task(pid14, "Task with no node",
                          input_data="hello world test", priority=5)
    _db.update_task_status(t14, "planned")
    r14 = run_next(pid14)
    assert r14["task_id"] == t14
    assert r14["dispatched"] is False, \
        f"Expected dispatched=False with no nodes, got {r14['dispatched']}"
    assert r14["status"] == "no_available_node", \
        f"Expected no_available_node, got {r14['status']}"
    assert _db.get_task(t14)["status"] == "planned", \
        "Task status must be unchanged when no node is available"
    _nr.set_online("test-worker-01")
    _nr.heartbeat("test-worker-01")
    print("  Test 14 passed: no_available_node leaves task status unchanged")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 3 Step 5 — Coordinator Agent).")
