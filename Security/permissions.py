"""
security/permissions.py — Grid Master OS Phase 6
Permission definitions for RBAC.
Maps roles to the set of actions they are permitted to perform.
"""

# ── ACTION CATALOGUE ──────────────────────────────────────────
# Every API endpoint and CLI command maps to one action string.
# Adding a new endpoint: define its action here and add to role maps.

class Action:
    # Task execution
    RUN_TASK        = "run_task"

    # Read-only views
    VIEW_STATUS     = "view_status"
    VIEW_PROJECTS   = "view_projects"
    VIEW_NODES      = "view_nodes"
    VIEW_AGENTS     = "view_agents"
    VIEW_MEMORY     = "view_memory"
    VIEW_DB_STATS   = "view_db_stats"
    VIEW_COMMANDS   = "view_commands"

    # Command execution (registry commands)
    EXEC_COMMAND    = "exec_command"

    # Administrative
    MANAGE_KEYS     = "manage_keys"
    MANAGE_NODES    = "manage_nodes"
    MANAGE_AGENTS   = "manage_agents"
    VIEW_AUDIT_LOG  = "view_audit_log"
    ADMIN_ALL       = "admin_all"

    # Node-specific
    NODE_HEARTBEAT  = "node_heartbeat"
    NODE_REGISTER   = "node_register"


# ── ROLE → ACTIONS MAP ────────────────────────────────────────
# Admin  : full access
# Operator: submit tasks, view everything, execute commands
# Viewer : read-only
# Node   : heartbeat and node registration only

ROLE_PERMISSIONS: dict[str, set[str]] = {
    "admin": {
        Action.RUN_TASK,
        Action.VIEW_STATUS,
        Action.VIEW_PROJECTS,
        Action.VIEW_NODES,
        Action.VIEW_AGENTS,
        Action.VIEW_MEMORY,
        Action.VIEW_DB_STATS,
        Action.VIEW_COMMANDS,
        Action.EXEC_COMMAND,
        Action.MANAGE_KEYS,
        Action.MANAGE_NODES,
        Action.MANAGE_AGENTS,
        Action.VIEW_AUDIT_LOG,
        Action.ADMIN_ALL,
        Action.NODE_HEARTBEAT,
        Action.NODE_REGISTER,
    },
    "operator": {
        Action.RUN_TASK,
        Action.VIEW_STATUS,
        Action.VIEW_PROJECTS,
        Action.VIEW_NODES,
        Action.VIEW_AGENTS,
        Action.VIEW_MEMORY,
        Action.VIEW_DB_STATS,
        Action.VIEW_COMMANDS,
        Action.EXEC_COMMAND,
    },
    "viewer": {
        Action.VIEW_STATUS,
        Action.VIEW_PROJECTS,
        Action.VIEW_NODES,
        Action.VIEW_AGENTS,
        Action.VIEW_MEMORY,
        Action.VIEW_DB_STATS,
        Action.VIEW_COMMANDS,
    },
    "node": {
        Action.NODE_HEARTBEAT,
        Action.NODE_REGISTER,
        Action.VIEW_STATUS,
    },
}


def has_permission(role: str, action: str) -> bool:
    """Return True if the given role is permitted to perform the action."""
    perms = ROLE_PERMISSIONS.get(role, set())
    return action in perms or Action.ADMIN_ALL in perms


def get_role_permissions(role: str) -> list[str]:
    """Return sorted list of actions permitted for a role."""
    return sorted(ROLE_PERMISSIONS.get(role, set()))
