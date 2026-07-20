"""
node_registry.py — Grid Master OS Kernel v1.1
Node lifecycle management. Uses database._query/_exec abstractions.
No direct get_db() calls. All DB access through database module.

Extension point for Phase 2 scheduler:
  select_node_weighted() stub is included but not called yet.
  grid_master.select_node() will call it once scheduler is built.
"""
import datetime
import database as db

_MODULE = "[NODES]"

ROLE_COORDINATOR    = "coordinator"
ROLE_PLANNER        = "planner"
ROLE_WORKER         = "worker"
ROLE_REVIEWER       = "reviewer"
ROLE_MEMORY_MANAGER = "memory_manager"

VALID_ROLES = {
    ROLE_COORDINATOR, ROLE_PLANNER, ROLE_WORKER,
    ROLE_REVIEWER, ROLE_MEMORY_MANAGER,
}

HEARTBEAT_TIMEOUT_SECONDS = 120


# ── REGISTRATION ─────────────────────────────────────────────

def register(node_id: str, node_name: str, platform: str = "unknown",
             role: str = ROLE_WORKER, url: str = "",
             capabilities: list | None = None) -> int:
    if role not in VALID_ROLES:
        raise ValueError(f"{_MODULE} Invalid role '{role}'. Valid: {VALID_ROLES}")
    try:
        nid = db.register_node(node_id=node_id, node_name=node_name,
                               platform=platform, role=role, url=url,
                               capabilities=capabilities or [])
        print(f"{_MODULE} Registered: {node_name} ({role}) on {platform}")
        return nid
    except Exception as e:
        print(f"{_MODULE} Registration error: {e}")
        return -1


# ── HEARTBEAT ────────────────────────────────────────────────

def heartbeat(node_id: str) -> bool:
    try:
        db.update_heartbeat(node_id)
        return True
    except Exception as e:
        print(f"{_MODULE} Heartbeat error for {node_id}: {e}")
        return False


# ── STATUS ───────────────────────────────────────────────────

def set_online(node_id: str) -> None:
    try:
        db.set_node_status(node_id, "online")
    except Exception as e:
        print(f"{_MODULE} set_online error: {e}")


def set_offline(node_id: str) -> None:
    try:
        db.set_node_status(node_id, "offline")
        print(f"{_MODULE} Node offline: {node_id}")
    except Exception as e:
        print(f"{_MODULE} set_offline error: {e}")


def set_busy(node_id: str) -> None:
    try:
        db.set_node_status(node_id, "busy")
    except Exception as e:
        print(f"{_MODULE} set_busy error: {e}")


def set_available(node_id: str) -> None:
    try:
        db.set_node_status(node_id, "online")
    except Exception as e:
        print(f"{_MODULE} set_available error: {e}")


# ── HEALTH ───────────────────────────────────────────────────

def get_node_health(node_id: str) -> dict:
    try:
        node = db.get_node(node_id)
        if not node:
            return {"node_id": node_id, "healthy": False, "reason": "not found"}
        hb = node.get("last_heartbeat")
        if not hb:
            return {"node_id": node_id, "healthy": False,
                    "status": node.get("status"), "reason": "no heartbeat recorded"}
        age     = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) -
                   datetime.datetime.fromisoformat(hb)).total_seconds()
        healthy = age < HEARTBEAT_TIMEOUT_SECONDS and node.get("status") == "online"
        return {"node_id": node_id, "node_name": node.get("node_name"),
                "role": node.get("role"), "platform": node.get("platform"),
                "status": node.get("status"), "healthy": healthy,
                "heartbeat_age_s": round(age, 1),
                "timeout_s": HEARTBEAT_TIMEOUT_SECONDS,
                "reason": "ok" if healthy else f"stale heartbeat ({round(age)}s)"}
    except Exception as e:
        print(f"{_MODULE} Health check error: {e}")
        return {"node_id": node_id, "healthy": False, "reason": str(e)}


def check_all_health() -> list[dict]:
    """Scan all nodes. Auto-offline stale ones."""
    reports = []
    try:
        nodes = db.list_all_nodes()
    except Exception as e:
        print(f"{_MODULE} check_all_health error: {e}")
        return []
    for node in nodes:
        nid    = node["node_id"]
        report = get_node_health(nid)
        if not report["healthy"] and node.get("status") == "online":
            set_offline(nid)
            report["auto_offlined"] = True
        reports.append(report)
    return reports


# ── QUERY ────────────────────────────────────────────────────

def get_available_nodes(role: str | None = None) -> list[dict]:
    """Return online nodes filtered by role. Used by Coordinator."""
    try:
        return db.get_online_nodes(role=role)
    except Exception as e:
        print(f"{_MODULE} get_available_nodes error: {e}")
        return []


def get_node(node_id: str) -> dict | None:
    try:
        return db.get_node(node_id)
    except Exception as e:
        print(f"{_MODULE} get_node error: {e}")
        return None


def list_all_nodes() -> list[dict]:
    try:
        return db.list_all_nodes()
    except Exception as e:
        print(f"{_MODULE} list_all_nodes error: {e}")
        return []


# ── SCHEDULER EXTENSION POINT (Phase 2) ──────────────────────

def select_node_weighted(candidates: list[dict]) -> dict | None:
    """
    Phase 2 extension point for weighted node selection.
    Currently: returns first candidate (Phase 1 behaviour).

    Phase 2 implementation will:
      - Score each candidate by avg_latency, success_rate, current_load
      - Return the highest-scoring available node
      - Pull performance data from memory_entries or a future metrics table

    grid_master.select_node() will call this instead of candidates[0]
    once the scheduler is implemented.
    """
    if not candidates:
        return None
    # Phase 1: first available
    # Phase 2: replace with scoring logic
    return candidates[0]


# ── STATS ────────────────────────────────────────────────────

def registry_stats() -> dict:
    try:
        nodes     = list_all_nodes()
        by_status: dict = {}
        by_role:   dict = {}
        for n in nodes:
            s = n.get("status", "unknown")
            r = n.get("role",   "unknown")
            by_status[s] = by_status.get(s, 0) + 1
            by_role[r]   = by_role.get(r,   0) + 1
        return {"total": len(nodes), "by_status": by_status, "by_role": by_role}
    except Exception as e:
        print(f"{_MODULE} registry_stats error: {e}")
        return {}


if __name__ == "__main__":
    import os, tempfile, importlib, sys
    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    if "database" in sys.modules:
        importlib.reload(sys.modules["database"])
    import database as _db
    _db.init_db()
    for nid, name, role in [
        ("coord-01",  "Coordinator",   ROLE_COORDINATOR),
        ("plan-01",   "Planner",       ROLE_PLANNER),
        ("work-01",   "Worker-Render", ROLE_WORKER),
        ("work-02",   "Worker-HF",     ROLE_WORKER),
        ("review-01", "Reviewer",      ROLE_REVIEWER),
        ("mem-01",    "MemoryManager", ROLE_MEMORY_MANAGER),
    ]:
        register(nid, name, platform="local", role=role)
        heartbeat(nid)
    health = get_node_health("work-01")
    assert health["healthy"], f"Expected work-01 healthy, got: {health}"
    set_busy("work-01")
    assert get_node("work-01")["status"] == "busy"
    set_available("work-01")
    workers = get_available_nodes(role=ROLE_WORKER)
    assert len(workers) >= 2, f"Expected >=2 workers, got {len(workers)}"
    chosen = select_node_weighted(workers)
    assert chosen is not None
    reports = check_all_health()
    healthy = sum(1 for r in reports if r["healthy"])
    assert healthy >= 6, f"Expected >=6 healthy, got {healthy}"
    stats = registry_stats()
    assert stats["total"] >= 6
    _db.close_db()
    os.remove(tmp)
    print(f"{_MODULE} Self-test passed.")
