"""
agent_registry.py — Grid Master OS Kernel v1.0 (Phase 3 Step 1)

Agent registry layer over the existing agent_registry table.
No schema changes — uses database.py exclusively.

Valid roles (same as node_registry.py for consistency):
    coordinator, planner, worker, reviewer, memory_manager

Valid statuses:
    active, busy, inactive

Capability queries use SQLite json_each() — requires SQLite >= 3.38.
register() is safe to call on every agent startup (upsert behaviour).
"""
import database as db

_MODULE = "[AGENTS]"

VALID_ROLES = {
    "coordinator",
    "planner",
    "worker",
    "reviewer",
    "memory_manager",
}

VALID_STATUSES = {"active", "busy", "inactive"}


# ── REGISTRATION ─────────────────────────────────────────────

def register(name: str,
             role: str,
             capabilities: list | None = None) -> int:
    """
    Register an agent or refresh an existing registration.
    Safe to call on every agent startup — upsert behaviour.
    Sets status='active' on re-registration.

    Parameters
    ----------
    name         : unique agent identifier  e.g. "planner-01"
    role         : must be one of VALID_ROLES
    capabilities : list of string capability tags
                   e.g. ["read_memory", "create_plan"]

    Returns rowid of inserted or updated record, -1 on error.
    """
    if role not in VALID_ROLES:
        raise ValueError(
            f"{_MODULE} Invalid role '{role}'. Must be one of {VALID_ROLES}"
        )
    if capabilities is not None:
        bad = [c for c in capabilities if not isinstance(c, str)]
        if bad:
            raise ValueError(
                f"{_MODULE} capabilities must be strings. Got: {bad}"
            )
    try:
        aid = db.register_agent(
            agent_name   = name,
            agent_role   = role,
            capabilities = capabilities or [],
        )
        print(f"{_MODULE} Registered: {name} ({role})")
        return aid
    except Exception as e:
        print(f"{_MODULE} register error: {e}")
        return -1


# ── STATUS MANAGEMENT ────────────────────────────────────────

def set_status(name: str, status: str) -> bool:
    """
    Update an agent's status.

    Valid values: 'active', 'busy', 'inactive'
    Returns True on success, False on error.
    """
    if status not in VALID_STATUSES:
        raise ValueError(
            f"{_MODULE} Invalid status '{status}'. "
            f"Must be one of {VALID_STATUSES}"
        )
    try:
        db._exec(
            "UPDATE agent_registry SET status=?, updated_at=? "
            "WHERE agent_name=?",
            (status, db._now(), name),
        )
        return True
    except Exception as e:
        print(f"{_MODULE} set_status error for '{name}': {e}")
        return False


def deregister(name: str) -> bool:
    """
    Mark an agent as inactive. Called on clean shutdown.
    Does not delete the record — history is preserved.
    Returns True on success, False on error.
    """
    try:
        result = set_status(name, "inactive")
        if result:
            print(f"{_MODULE} Deregistered: {name}")
        return result
    except Exception as e:
        print(f"{_MODULE} deregister error for '{name}': {e}")
        return False


# ── QUERY ────────────────────────────────────────────────────

def get_agent(name: str) -> dict | None:
    """
    Return a single agent record by name, or None if not found.
    Returns agents of any status.
    """
    try:
        return db._query_one(
            "SELECT * FROM agent_registry WHERE agent_name=?",
            (name,),
        )
    except Exception as e:
        print(f"{_MODULE} get_agent error for '{name}': {e}")
        return None


def find_by_role(role: str,
                 status: str = "active") -> list[dict]:
    """
    Return all agents with the given role and status.

    Parameters
    ----------
    role   : role to filter on — any string is accepted here
             (caller may query for future roles not yet in VALID_ROLES)
    status : default 'active'; pass None to return all statuses

    Returns list of agent dicts, empty list if none found.
    """
    try:
        if status is not None:
            return db._query(
                "SELECT * FROM agent_registry "
                "WHERE agent_role=? AND status=? "
                "ORDER BY agent_name",
                (role, status),
            )
        return db._query(
            "SELECT * FROM agent_registry "
            "WHERE agent_role=? "
            "ORDER BY agent_name",
            (role,),
        )
    except Exception as e:
        print(f"{_MODULE} find_by_role error for '{role}': {e}")
        return []


def find_by_capability(capability: str,
                       status: str = "active") -> list[dict]:
    """
    Return all agents whose capabilities JSON array contains
    `capability` as an exact string match.

    Uses SQLite json_each() — requires SQLite >= 3.38 (2022-02-22).

    Parameters
    ----------
    capability : exact capability string to match e.g. "read_memory"
    status     : default 'active'; pass None to return all statuses

    Returns list of agent dicts, empty list if none found or error.
    """
    capability = capability.strip()
    if not capability:
        print(f"{_MODULE} find_by_capability: empty capability string")
        return []
    try:
        if status is not None:
            return db._query(
                "SELECT DISTINCT a.* FROM agent_registry a, "
                "json_each(a.capabilities) je "
                "WHERE je.value=? AND a.status=? "
                "ORDER BY a.agent_name",
                (capability, status),
            )
        return db._query(
            "SELECT DISTINCT a.* FROM agent_registry a, "
            "json_each(a.capabilities) je "
            "WHERE je.value=? "
            "ORDER BY a.agent_name",
            (capability,),
        )
    except Exception as e:
        print(f"{_MODULE} find_by_capability error for '{capability}': {e}")
        return []


# ── REGISTRY STATS ───────────────────────────────────────────

def registry_stats() -> dict:
    """
    Return counts by status and role.
    Used by system_status() in grid_master.
    """
    try:
        rows = db._query(
            "SELECT agent_role, status, COUNT(*) as count "
            "FROM agent_registry "
            "GROUP BY agent_role, status "
            "ORDER BY agent_role, status"
        )
        by_status: dict = {}
        by_role:   dict = {}
        for r in rows:
            s = r["status"]
            role = r["agent_role"]
            c = r["count"]
            by_status[s]    = by_status.get(s, 0) + c
            by_role[role]   = by_role.get(role, 0) + c
        total = sum(by_status.values())
        return {
            "total":     total,
            "by_status": by_status,
            "by_role":   by_role,
        }
    except Exception as e:
        print(f"{_MODULE} registry_stats error: {e}")
        return {}


# ── SELF-TEST ────────────────────────────────────────────────

if __name__ == "__main__":
    import os, tempfile, importlib, sys, json

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp

    for mod in ["database", "agent_registry"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    # ── 1. register() upsert behaviour ───────────────────────
    aid1 = register("coordinator-01", "coordinator",
                    ["route_tasks", "manage_projects"])
    assert aid1 != -1, "register failed"
    aid1b = register("coordinator-01", "coordinator",  # re-register
                     ["route_tasks", "manage_projects", "report"])
    agent = get_agent("coordinator-01")
    assert agent is not None, "Agent not found after register"
    assert agent["status"] == "active", \
        f"Re-register must set status=active, got {agent['status']}"

    # ── 2. register all roles ────────────────────────────────
    register("planner-01",    "planner",       ["read_memory", "create_plan", "subtask"])
    register("worker-01",     "worker",        ["execute_task", "write_code", "read_memory"])
    register("worker-02",     "worker",        ["execute_task", "data_analysis"])
    register("reviewer-01",   "reviewer",      ["review_output", "approve", "extract_knowledge"])
    register("mem-mgr-01",    "memory_manager",["store_memory", "compress", "summarize"])

    # ── 3. invalid role raises ValueError ────────────────────
    raised = False
    try:
        register("bad-agent", "hacker", ["exploit"])
    except ValueError:
        raised = True
    assert raised, "Expected ValueError for invalid role"

    # ── 4. invalid capability type raises ValueError ─────────
    raised2 = False
    try:
        register("bad-caps", "worker", ["ok", 123, None])
    except ValueError:
        raised2 = True
    assert raised2, "Expected ValueError for non-string capability"

    # ── 5. set_status() transitions ──────────────────────────
    ok = set_status("worker-01", "busy")
    assert ok, "set_status failed"
    assert get_agent("worker-01")["status"] == "busy"

    set_status("worker-01", "active")
    assert get_agent("worker-01")["status"] == "active"

    raised3 = False
    try:
        set_status("worker-01", "flying")
    except ValueError:
        raised3 = True
    assert raised3, "Expected ValueError for invalid status"

    # ── 6. deregister() → status=inactive ────────────────────
    ok2 = deregister("worker-02")
    assert ok2
    assert get_agent("worker-02")["status"] == "inactive", \
        "deregister must set status=inactive"

    # ── 7. find_by_role() — active only by default ───────────
    workers = find_by_role("worker")
    names   = [a["agent_name"] for a in workers]
    assert "worker-01" in names,  "worker-01 should be active"
    assert "worker-02" not in names, "worker-02 is inactive — must not appear"

    # find_by_role with status=None returns all statuses
    all_workers = find_by_role("worker", status=None)
    all_names   = [a["agent_name"] for a in all_workers]
    assert "worker-02" in all_names, "status=None must include inactive agents"

    # ── 8. find_by_capability() — exact match via json_each() ─
    planners_mem = find_by_capability("read_memory")
    cap_names    = [a["agent_name"] for a in planners_mem]
    assert "planner-01" in cap_names, \
        "planner-01 has read_memory capability"
    assert "worker-01"  in cap_names, \
        "worker-01 has read_memory capability"
    assert "coordinator-01" not in cap_names, \
        "coordinator-01 does not have read_memory"

    # partial match must NOT return results
    partial = find_by_capability("read_mem")
    assert not partial, \
        f"Partial capability match must return empty, got {partial}"

    # empty string returns empty
    empty_cap = find_by_capability("")
    assert empty_cap == [], "Empty capability must return []"

    # inactive agent excluded from find_by_capability default
    active_execute = find_by_capability("execute_task")
    active_names   = [a["agent_name"] for a in active_execute]
    assert "worker-02" not in active_names, \
        "Inactive worker-02 must not appear in default capability search"

    # status=None includes inactive
    all_execute = find_by_capability("execute_task", status=None)
    all_e_names = [a["agent_name"] for a in all_execute]
    assert "worker-02" in all_e_names, \
        "status=None must include inactive agents in capability search"

    # ── 9. registry_stats() ──────────────────────────────────
    stats = registry_stats()
    assert stats["total"] >= 6,           f"Expected >=6 agents, got {stats['total']}"
    assert "active"   in stats["by_status"]
    assert "inactive" in stats["by_status"]
    assert "worker"   in stats["by_role"]
    assert "planner"  in stats["by_role"]

    # ── 10. get_agent() for unknown name ─────────────────────
    missing = get_agent("does-not-exist")
    assert missing is None, "get_agent for unknown name must return None"

    _db.close_db()
    os.remove(tmp)
    print(f"{_MODULE} Self-test passed (Phase 3 Step 1 — Agent Registry).")
