"""
interface/command_registry.py — Grid Master OS Phase 5
Central registry mapping command names to callable handlers.
CLI and future REPL use this registry to dispatch commands.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import kernel as _kernel
import database as _db

_MODULE = "[CMD_REGISTRY]"


# ── COMMAND HANDLERS ──────────────────────────────────────────

def _cmd_run_task(title: str = "",
                  input_data: str = "",
                  project_id: int | None = None,
                  priority: int = 5,
                  max_iterations: int = 100) -> dict:
    """Submit and execute a task through the full kernel lifecycle."""
    if not title:
        return {"status": "error", "error": "title is required"}
    return _kernel.run_task(
        title          = title,
        input_data     = input_data,
        project_id     = project_id,
        priority       = priority,
        max_iterations = max_iterations,
    )


def _cmd_show_memory(project_id: int | None = None) -> dict:
    """Return memory statistics for a project."""
    try:
        stats = _db.memory_stats_counts(project_id=project_id)
        return {"status": "ok", "memory": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _cmd_show_nodes() -> dict:
    """Return all registered nodes and their status."""
    try:
        nodes = _db.list_all_nodes()
        return {"status": "ok", "nodes": nodes}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _cmd_show_agents() -> dict:
    """Return all active agents."""
    try:
        agents = _db.get_active_agents()
        return {"status": "ok", "agents": agents}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _cmd_list_projects() -> dict:
    """Return all active projects."""
    try:
        projects = _db.list_projects(status="active")
        return {"status": "ok", "projects": projects}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _cmd_db_stats() -> dict:
    """Return database health statistics."""
    try:
        stats = _db.db_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── REGISTRY ─────────────────────────────────────────────────

COMMANDS: dict[str, dict] = {
    "run_task": {
        "handler":     _cmd_run_task,
        "description": "Submit a task through the full kernel lifecycle.",
        "params": {
            "title":          {"type": str,       "required": True},
            "input_data":     {"type": str,       "required": False, "default": ""},
            "project_id":     {"type": int,       "required": False, "default": None},
            "priority":       {"type": int,       "required": False, "default": 5},
            "max_iterations": {"type": int,       "required": False, "default": 100},
        },
    },
    "show_memory": {
        "handler":     _cmd_show_memory,
        "description": "Show memory statistics.",
        "params": {
            "project_id": {"type": int, "required": False, "default": None},
        },
    },
    "show_nodes": {
        "handler":     _cmd_show_nodes,
        "description": "List all registered compute nodes.",
        "params":      {},
    },
    "show_agents": {
        "handler":     _cmd_show_agents,
        "description": "List all active agents.",
        "params":      {},
    },
    "list_projects": {
        "handler":     _cmd_list_projects,
        "description": "List all active projects.",
        "params":      {},
    },
    "db_stats": {
        "handler":     _cmd_db_stats,
        "description": "Show database health statistics.",
        "params":      {},
    },
}


def dispatch(command: str, **kwargs) -> dict:
    """
    Look up a command by name and execute it with the given kwargs.
    Returns a structured dict. Never raises.
    """
    entry = COMMANDS.get(command)
    if entry is None:
        known = ", ".join(COMMANDS.keys())
        return {"status": "error",
                "error": f"Unknown command '{command}'. Known: {known}"}
    try:
        return entry["handler"](**kwargs)
    except TypeError as e:
        return {"status": "error",
                "error": f"Bad arguments for '{command}': {e}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def list_commands() -> list[dict]:
    """Return a list of all registered commands with descriptions."""
    return [
        {"command": name, "description": meta["description"]}
        for name, meta in COMMANDS.items()
    ]
