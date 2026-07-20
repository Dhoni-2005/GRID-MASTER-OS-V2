"""
grid/load_balancer.py — Grid Master OS Phase 7  [RC-2]
Capability-weighted, least-busy node selection for distributed task dispatch.
Called directly by grid.dispatcher — does NOT import node_scheduler.py (RC-2).

Selection priority (approved architecture):
  1. Node must be online (classify_health == "online")
  2. Node must not be quarantined
  3. Node must satisfy ALL required task capabilities
  4. Prefer node with fewest active tasks
  5. Tiebreak: lowest node_id (alphabetical) for determinism

Responsibilities:
  • Choose the best available worker node for a given task
  • No task execution
  • No networking
  • No database schema modifications
  • No imports from node_scheduler
"""
import logging
from typing import Any

from grid.registry import (
    get_cluster_snapshot,
    get_active_task_count,
    is_quarantined,
    classify_health,
)

log = logging.getLogger("gridmaster.grid.load_balancer")


# ── PUBLIC API ────────────────────────────────────────────────

def choose_best_node(task: dict[str, Any]) -> dict[str, Any] | None:
    """
    Select the best available worker node for the given task.

    Parameters
    ----------
    task : task dict as returned by database.get_task().
           The key "capabilities" (list[str]) specifies requirements.
           Defaults to ["general"] if absent or empty.

    Returns
    -------
    Node dict (from cluster snapshot) on success, or None if no
    suitable node is available.

    Selection algorithm
    -------------------
    1. Retrieve full cluster snapshot from grid.registry.
    2. Filter to nodes that are online, not quarantined, and healthy.
    3. Filter to nodes that satisfy ALL required capabilities.
    4. Among remaining candidates, select by:
         a. Fewest active tasks (primary)
         b. Lowest node_id string (tiebreak — deterministic)
    """
    required_caps: list[str] = _get_required_capabilities(task)
    snapshot: list[dict[str, Any]] = get_cluster_snapshot()

    if not snapshot:
        log.debug("load_balancer: cluster snapshot is empty")
        return None

    # Step 1: filter to healthy, non-quarantined, online nodes
    candidates = [
        n for n in snapshot
        if _is_eligible(n)
    ]

    if not candidates:
        log.debug(
            "load_balancer: no eligible nodes (total=%d)", len(snapshot)
        )
        return None

    # Step 2: filter by capability requirements
    if required_caps:
        candidates = [
            n for n in candidates
            if _satisfies_capabilities(n, required_caps)
        ]
        if not candidates:
            log.debug(
                "load_balancer: no nodes satisfy capabilities %s", required_caps
            )
            return None

    # Step 3: select by least active tasks, then alphabetical node_id tiebreak
    selected = min(
        candidates,
        key=lambda n: (
            len(n.get("active_tasks", [])),   # primary: fewest active tasks
            n.get("node_id", ""),              # tiebreak: alphabetical
        ),
    )

    log.info(
        "load_balancer: selected node=%s active_tasks=%d capabilities=%s",
        selected["node_id"],
        len(selected.get("active_tasks", [])),
        selected.get("capabilities", []),
    )
    return selected


def get_node_load() -> dict[str, int]:
    """
    Return a snapshot of active task counts per node.

    Returns
    -------
    {node_id: active_task_count} for all nodes in the cluster snapshot.
    Used by monitoring endpoints.
    """
    snapshot = get_cluster_snapshot()
    return {
        n["node_id"]: len(n.get("active_tasks", []))
        for n in snapshot
    }


def get_eligible_nodes(task: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Return all nodes eligible for dispatch, optionally filtered by task capabilities.

    Parameters
    ----------
    task : optional task dict; if None, returns all eligible nodes
           without capability filtering.

    Returns
    -------
    List of eligible node dicts sorted by (active_tasks ASC, node_id ASC).
    """
    snapshot   = get_cluster_snapshot()
    candidates = [n for n in snapshot if _is_eligible(n)]

    if task is not None:
        required_caps = _get_required_capabilities(task)
        if required_caps:
            candidates = [
                n for n in candidates
                if _satisfies_capabilities(n, required_caps)
            ]

    return sorted(
        candidates,
        key=lambda n: (len(n.get("active_tasks", [])), n.get("node_id", "")),
    )


# ── PRIVATE HELPERS ───────────────────────────────────────────

def _is_eligible(node: dict[str, Any]) -> bool:
    """
    Return True if a node is eligible for task dispatch.
    A node must be:
      • health == "online"  (from registry classify_health)
      • not quarantined
      • status != "offline" / "busy" (belt-and-suspenders)
    """
    node_id = node.get("node_id", "")
    health  = node.get("health") or classify_health(node_id)

    if health != "online":
        return False
    if node.get("quarantined", False) or is_quarantined(node_id):
        return False
    if node.get("status") in ("offline", "busy"):
        return False
    return True


def _satisfies_capabilities(node: dict[str, Any],
                             required: list[str]) -> bool:
    """
    Return True if the node's capabilities satisfy ALL required capabilities.
    Capability matching is case-sensitive exact match.
    If a node has no listed capabilities, it is treated as having ["general"].
    """
    node_caps_raw = node.get("capabilities", [])

    # capabilities may be stored as a JSON string (from node_registry)
    if isinstance(node_caps_raw, str):
        import json
        try:
            node_caps = json.loads(node_caps_raw)
        except (json.JSONDecodeError, TypeError):
            node_caps = ["general"]
    elif isinstance(node_caps_raw, list):
        node_caps = node_caps_raw
    else:
        node_caps = ["general"]

    # Empty capability list treated as ["general"]
    if not node_caps:
        node_caps = ["general"]

    node_cap_set = set(node_caps)
    return all(cap in node_cap_set for cap in required)


def _get_required_capabilities(task: dict[str, Any]) -> list[str]:
    """
    Extract required capabilities from a task dict.
    Defaults to ["general"] if not specified or empty.
    """
    caps = task.get("capabilities", []) if task else []
    if isinstance(caps, str):
        import json
        try:
            caps = json.loads(caps)
        except (json.JSONDecodeError, TypeError):
            caps = []
    if not caps:
        caps = ["general"]
    return caps
