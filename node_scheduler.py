"""
node_scheduler.py — Grid Master OS Kernel v1.0 (Phase 4 Step 2)

Node Scheduler: chooses the best execution node for a task.

Responsibilities:
    - Select a healthy, available worker node for a given task.
    - Does NOT execute tasks.
    - Does NOT review tasks.
    - Does NOT modify task status.

Phase 4 Step 2 policy (simple, deterministic):
    - Ignore offline nodes.
    - Ignore busy nodes.
    - Select the first available healthy worker node.
    - Return None if no node is available.

Uses node_registry.py exclusively — no direct database access,
no networking, no threading, no multiprocessing, no LLM calls.

Future scheduler upgrades (load-based, latency-weighted,
capability-matched selection) replace only select_node()'s
internals without changing its signature.
"""
import node_registry as nr

_MODULE = "[NODE_SCHEDULER]"


def select_node(task: dict) -> dict | None:
    """
    Select the best available node to execute the given task.

    Parameters
    ----------
    task : task dict (as returned by database.get_task()).
           Reserved for future capability-matching — Phase 4 Step 2
           does not yet inspect task contents for node selection.

    Returns
    -------
    dict : the selected node record, or
    None : no healthy, available worker node exists
    """
    candidates = nr.get_available_nodes(role=nr.ROLE_WORKER)

    if not candidates:
        print(f"{_MODULE} No available worker nodes "
              f"for task [{task.get('id', '?')}]")
        return None

    # get_available_nodes() already filters status='online', which
    # satisfies "ignore offline nodes" and "ignore busy nodes".
    # The explicit health check below additionally guards against
    # stale heartbeats on nodes still marked 'online'.
    healthy_candidates = []
    for node in candidates:
        health = nr.get_node_health(node["node_id"])
        if health.get("healthy"):
            healthy_candidates.append(node)

    if not healthy_candidates:
        print(f"{_MODULE} No healthy worker nodes "
              f"for task [{task.get('id', '?')}] "
              f"({len(candidates)} online but unhealthy)")
        return None

    selected = healthy_candidates[0]
    print(f"{_MODULE} Selected node '{selected['node_id']}' "
          f"for task [{task.get('id', '?')}]")
    return selected


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "node_registry", "node_scheduler"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    dummy_task = {"id": 1, "title": "Test task"}

    # ── Test 1: no nodes registered → None ───────────────────
    result = select_node(dummy_task)
    assert result is None, "Expected None with no nodes registered"
    print("  Test 1 passed: no nodes registered → None")

    # ── Test 2: single healthy worker → selected ──────────────
    nr.register("worker-01", "Worker A", platform="local",
                role=nr.ROLE_WORKER)
    nr.heartbeat("worker-01")
    result2 = select_node(dummy_task)
    assert result2 is not None
    assert result2["node_id"] == "worker-01"
    print("  Test 2 passed: single healthy worker selected")

    # ── Test 3: offline node ignored ──────────────────────────
    nr.register("worker-02", "Worker B", platform="local",
                role=nr.ROLE_WORKER)
    nr.heartbeat("worker-02")
    nr.set_offline("worker-02")
    result3 = select_node(dummy_task)
    assert result3["node_id"] != "worker-02", \
        "Offline node must never be selected"
    print("  Test 3 passed: offline node ignored")

    # ── Test 4: busy node ignored ─────────────────────────────
    nr.register("worker-03", "Worker C", platform="local",
                role=nr.ROLE_WORKER)
    nr.heartbeat("worker-03")
    nr.set_busy("worker-03")
    result4 = select_node(dummy_task)
    assert result4["node_id"] != "worker-03", \
        "Busy node must never be selected"
    print("  Test 4 passed: busy node ignored")

    # ── Test 5: stale heartbeat (unhealthy) ignored ───────────
    nr.register("worker-04", "Worker D", platform="local",
                role=nr.ROLE_WORKER)
    # No heartbeat called — node has no last_heartbeat, unhealthy
    result5 = select_node(dummy_task)
    assert result5["node_id"] != "worker-04", \
        "Node with no heartbeat must never be selected"
    print("  Test 5 passed: node with stale/missing heartbeat ignored")

    # ── Test 6: only non-worker roles registered → None ──────
    nr.set_offline("worker-01")  # remove the only healthy worker
    nr.register("planner-01", "Planner", platform="local",
                role=nr.ROLE_PLANNER)
    nr.heartbeat("planner-01")
    result6 = select_node(dummy_task)
    assert result6 is None, \
        "Planner-role node must never be selected as a worker"
    print("  Test 6 passed: non-worker roles excluded")

    # ── Test 7: node_scheduler does not modify task status ───
    tid = _db.create_task(
        _db.create_project("NS Test", "node_scheduler self-test"),
        "Some task", priority=5,
    )
    _db.update_task_status(tid, "planned")
    task = _db.get_task(tid)
    nr.set_online("worker-01")
    select_node(task)
    after = _db.get_task(tid)
    assert after["status"] == "planned", \
        "node_scheduler must never modify task status"
    print("  Test 7 passed: task status unchanged by node_scheduler")

    # ── Test 8: returns full node dict, not just ID ───────────
    result8 = select_node(dummy_task)
    assert isinstance(result8, dict)
    assert "node_id" in result8 and "node_name" in result8 \
       and "role" in result8
    print("  Test 8 passed: returns full node record")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 4 Step 2 — Node Scheduler).")
