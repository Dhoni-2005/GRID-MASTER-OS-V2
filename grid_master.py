"""
grid_master.py — Grid Master OS Kernel v1.1
Coordinator layer. Phase 1 only.

Uses db.complete_task_atomic() and db.fail_task_atomic()
for transaction-safe state transitions.
Uses node_registry.select_node_weighted() as scheduler extension point.
Self-test uses close_db() before file removal to prevent lock errors.

Phase 2 will plug Planner, Worker, Reviewer into dispatch().
"""
import datetime
import database       as db
import memory_manager as mm
import node_registry  as nr

_MODULE = "[GM]"


# ── BOOT ─────────────────────────────────────────────────────

def boot() -> None:
    db.init_db()
    db.register_agent("coordinator", "coordinator",
                       ["intake_task","dispatch_task","track_status","report"])
    nr.register("coordinator-01", "Grid Master Coordinator",
                platform="local", role=nr.ROLE_COORDINATOR,
                capabilities=["task_routing","memory_query","node_selection"])
    nr.heartbeat("coordinator-01")
    print(f"{_MODULE} Grid Master OS booted.")


# ── TASK INTAKE ──────────────────────────────────────────────

def submit_task(title: str, input_data: str = "",
                project_id: int | None = None,
                priority: int = 5,
                parent_task_id: int | None = None) -> dict:
    if project_id is None:
        existing = db.list_projects(status="active")
        project_id = (existing[0]["id"] if existing
                      else db.create_project("Default Project",
                                             "Auto-created by Coordinator"))
    task_id = db.create_task(project_id=project_id, title=title,
                              input_data=input_data, priority=priority,
                              parent_task_id=parent_task_id)
    db.write_note(task_id, "coordinator",
                  f"Task received: '{title}' (priority={priority})")
    mm.remember(task_id=task_id, content=f"Task submitted: {title}",
                entry_type="intake", importance=mm.SCORE_LOG,
                project_id=project_id)
    print(f"{_MODULE} Task accepted: [{task_id}] {title}")
    return db.get_task(task_id)


# ── MEMORY CONSULTATION ──────────────────────────────────────

def consult_memory(task_id: int, keyword: str,
                   project_id: int | None = None) -> dict:
    context = mm.build_context(task_id=task_id, project_id=project_id,
                                keyword=keyword, limit=10)
    parts = []
    if context["memories"]:
        parts.append(f"{len(context['memories'])} memories")
    if context["failures"]:
        parts.append(f"{len(context['failures'])} past failures on '{keyword}'")
    if context["knowledge"]:
        parts.append(f"{len(context['knowledge'])} knowledge entries")
    note = ("Memory context: " + ", ".join(parts) if parts
            else "No prior memory found.")
    db.write_note(task_id, "coordinator", note)
    print(f"{_MODULE} Memory [{task_id}]: {note}")
    return context


# ── NODE SELECTION ───────────────────────────────────────────

def select_node(role: str = nr.ROLE_WORKER,
                task_id: int | None = None) -> dict | None:
    """
    Phase 1: delegates to nr.select_node_weighted() which returns
    the first available node. Phase 2 scheduler replaces that stub.
    """
    candidates = nr.get_available_nodes(role=role)
    chosen     = nr.select_node_weighted(candidates)
    if not chosen:
        msg = f"No available nodes for role='{role}'"
        print(f"{_MODULE} {msg}")
        if task_id:
            db.write_note(task_id, "coordinator", f"Node selection failed: {msg}")
        return None
    if task_id:
        db.write_note(task_id, "coordinator",
                      f"Node selected: {chosen['node_name']} ({chosen['platform']})")
    print(f"{_MODULE} Node selected: {chosen['node_name']} ({chosen['node_id']})")
    return chosen


# ── DISPATCH ─────────────────────────────────────────────────

def dispatch(task_id: int, role: str = nr.ROLE_WORKER) -> dict:
    """
    Phase 1: consult memory → select node → mark dispatched.
    Phase 2: Planner decomposes, Worker executes, Reviewer validates.
    """
    task = db.get_task(task_id)
    if not task:
        return _error(task_id, f"Task {task_id} not found.")
    if task["status"] not in ("pending", "failed"):
        return _error(task_id,
                      f"Task {task_id} cannot be dispatched "
                      f"(status='{task['status']}').")

    context = consult_memory(task_id=task_id, keyword=task["title"],
                              project_id=task.get("project_id"))
    node = select_node(role=role, task_id=task_id)
    if not node:
        # Atomic: mark blocked + store memory in one transaction
        db.fail_task_atomic(
            task_id=task_id, status="blocked",
            output="No available nodes.",
            node_id=None,
            problem="No available nodes for dispatch.",
            cause="All nodes offline or busy.",
            fix="Register and heartbeat at least one worker node.",
            tags=["dispatch", "no-nodes"],
            project_id=task.get("project_id"),
        )
        return _error(task_id, "No available nodes.")

    # Mark dispatched and busy — not yet atomic (single writes are safe here)
    db.update_task_status(task_id, "dispatched",
                          output=f"Assigned to {node['node_name']}")
    nr.set_busy(node["node_id"])
    db.write_note(task_id, "coordinator",
                  f"Dispatched to {node['node_id']} at {_now()}")
    mm.remember(task_id=task_id,
                content=f"Task dispatched to {node['node_name']} ({node['platform']})",
                entry_type="dispatch", importance=mm.SCORE_LOG,
                project_id=task.get("project_id"))
    print(f"{_MODULE} Task [{task_id}] dispatched → {node['node_name']}")
    return {
        "task_id":   task_id,
        "node_id":   node["node_id"],
        "node_name": node["node_name"],
        "status":    "dispatched",
        "context_summary": {
            "memories":  len(context["memories"]),
            "failures":  len(context["failures"]),
            "knowledge": len(context["knowledge"]),
        },
    }


# ── RESULT HANDLING ──────────────────────────────────────────

def record_success(task_id: int, output: str,
                   node_id: str | None = None,
                   lesson: str = "") -> None:
    """
    Atomic: task completed + node released + memory stored.
    Optionally extracts lesson to knowledge_base if it matches patterns.
    """
    task       = db.get_task(task_id)
    project_id = task.get("project_id") if task else None

    db.complete_task_atomic(
        task_id=task_id, output=output, node_id=node_id,
        memory_content=f"Task completed. Output: {output[:200]}",
        project_id=project_id, lesson=lesson,
    )

    if lesson and any(kw in lesson.lower()
                      for kw in ["pattern","always","never","use","avoid"]):
        title = task["title"] if task else f"task_{task_id}"
        mm.extract_knowledge(
            topic=f"lesson_{title[:40]}", content=lesson,
            summary=lesson[:100], source=f"task:{task_id}",
            tags=["lesson","auto-extracted"],
        )
    db.write_note(task_id, "coordinator",
                  f"Completed. Node {node_id} released.")
    print(f"{_MODULE} Task [{task_id}] completed.")


def record_failure(task_id: int, problem: str, cause: str = "",
                   fix: str = "", node_id: str | None = None,
                   tags: list | None = None) -> None:
    """
    Atomic: task status + node released + failure_memory + memory entry.
    Enforces max 3 retries before abandoning.
    """
    task       = db.get_task(task_id)
    project_id = task.get("project_id") if task else None

    notes   = db.get_notes(task_id, agent_role="coordinator")
    retries = sum(1 for n in notes if "retry attempt" in n["note"])
    status  = "abandoned" if retries >= 3 else "failed"

    db.fail_task_atomic(
        task_id=task_id, status=status,
        output=f"{status.capitalize()}: {problem}",
        node_id=node_id, problem=problem,
        cause=cause, fix=fix, tags=tags or [],
        project_id=project_id,
    )
    db.write_note(task_id, "coordinator",
                  f"retry attempt {retries + 1}: {problem}")
    print(f"{_MODULE} Task [{task_id}] {status}: {problem}")


# ── STATUS ───────────────────────────────────────────────────

def task_status(task_id: int) -> dict:
    task = db.get_task(task_id)
    if not task:
        return {"error": f"Task {task_id} not found"}
    return {"task": task, "notes": db.get_notes(task_id)}


def system_status() -> dict:
    stats   = db.db_stats()
    nodes   = nr.registry_stats()
    health  = nr.check_all_health()
    mem     = mm.memory_stats()
    agents  = db.get_active_agents()
    pending    = db._scalar("SELECT COUNT(*) FROM tasks WHERE status='pending'")
    dispatched = db._scalar("SELECT COUNT(*) FROM tasks WHERE status='dispatched'")
    return {
        "timestamp":   _now(),
        "database":    stats,
        "nodes":       nodes,
        "node_health": health,
        "memory":      mem,
        "agents":      [a["agent_name"] for a in agents],
        "tasks": {
            "pending":    pending,
            "dispatched": dispatched,
            "total":      stats.get("tasks", 0),
        },
    }


def list_tasks(project_id: int | None = None,
               status: str | None = None) -> list[dict]:
    return db.list_tasks(project_id=project_id, status=status)


# ── HELPERS ──────────────────────────────────────────────────

def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()

def _error(task_id: int, msg: str) -> dict:
    print(f"{_MODULE} ERROR [{task_id}]: {msg}")
    return {"task_id": task_id, "status": "error", "message": msg}


# ── SELF-TEST ────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp

    # Reload all modules so they pick up the new DB path
    for mod in ["database", "memory_manager", "node_registry", "grid_master"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db

    boot()

    # Register test worker nodes
    nr.register("work-t01", "Test Worker A", platform="render",
                role=nr.ROLE_WORKER, url="https://grid-nodes.onrender.com",
                capabilities=["python"])
    nr.register("work-t02", "Test Worker B", platform="huggingface",
                role=nr.ROLE_WORKER, url="https://node.hf.space",
                capabilities=["python","data"])
    nr.heartbeat("work-t01")
    nr.heartbeat("work-t02")

    pid = _db.create_project("GM Self-Test", "grid_master boot test")

    # 1. Submit
    task = submit_task("Add /health endpoint", "Flask route.",
                       project_id=pid, priority=8)
    tid  = task["id"]
    assert task["status"] == "pending", f"Expected pending, got {task['status']}"

    # 2. Dispatch
    result = dispatch(tid, role=nr.ROLE_WORKER)
    assert result.get("status") == "dispatched", f"Dispatch failed: {result}"
    assert "node_id" in result

    # 3. Record success (atomic)
    record_success(tid, output="@app.route('/health') → jsonify(ok)",
                   node_id=result["node_id"],
                   lesson="Always use jsonify() for Flask endpoints.")
    s = _db.get_task(tid)
    assert s["status"] == "completed", f"Expected completed, got {s['status']}"

    # 4. Submit and fail a task (atomic failure)
    t2   = submit_task("Failing task", "Will fail.", project_id=pid, priority=3)
    t2id = t2["id"]
    dispatch(t2id)
    record_failure(t2id, problem="ModuleNotFoundError: requests",
                   cause="Missing dep", fix="pip install requests",
                   node_id="work-t02", tags=["python","dep"])
    s2 = _db.get_task(t2id)
    assert s2["status"] == "failed", f"Expected failed, got {s2['status']}"

    # 5. System status
    sys_s = system_status()
    assert sys_s["tasks"]["total"] >= 2
    assert sys_s["nodes"]["total"] >= 3   # coord + 2 workers

    # 6. Safe teardown — close connection before removing file
    _db.close_db()
    try:
        os.remove(tmp)
    except OSError as e:
        print(f"{_MODULE} Warning: could not remove test DB: {e}")

    print(f"{_MODULE} Self-test passed.")
