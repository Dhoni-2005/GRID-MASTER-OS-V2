"""
interface/common.py — Grid Master OS Phase 5
Shared validation, execution wrapper, and response formatting.
Both cli.py and api.py call these functions exclusively.
No business logic lives here — only adaptation and formatting.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kernel as _kernel

_MODULE = "[COMMON]"

REQUIRED_RESULT_KEYS = {
    "status", "root_task_id", "project_id",
    "subtasks_total", "subtasks_dispatched",
    "subtasks_completed", "subtasks_rejected",
    "plan_text", "error",
}


# ── VALIDATION ────────────────────────────────────────────────

def validate(title: str,
             input_data: str = "",
             project_id: int | None = None,
             priority: int = 5,
             max_iterations: int = 100) -> tuple[dict | None, str | None]:
    """
    Validate all run_task arguments before calling the kernel.
    Returns (args_dict, None) on success.
    Returns (None, error_message) on failure.
    """
    if not title or not title.strip():
        return None, "title is required and cannot be empty"

    if project_id is not None:
        if not isinstance(project_id, int):
            try:
                project_id = int(project_id)
            except (ValueError, TypeError):
                return None, "project_id must be an integer"

    try:
        priority = int(priority)
    except (ValueError, TypeError):
        return None, "priority must be an integer"
    if not 1 <= priority <= 10:
        return None, "priority must be between 1 and 10"

    try:
        max_iterations = int(max_iterations)
    except (ValueError, TypeError):
        return None, "max_iterations must be an integer"
    if max_iterations < 1:
        return None, "max_iterations must be >= 1"

    return {
        "title":          title.strip(),
        "input_data":     (input_data or "").strip(),
        "project_id":     project_id,
        "priority":       priority,
        "max_iterations": max_iterations,
    }, None


# ── KERNEL WRAPPER ────────────────────────────────────────────

def run(title: str,
        input_data: str = "",
        project_id: int | None = None,
        priority: int = 5,
        max_iterations: int = 100) -> dict:
    """
    Call kernel.run_task() with pre-validated arguments.
    Catches all unexpected exceptions and returns a structured error dict.
    This is the ONLY place in the Interface Layer that calls the kernel.
    """
    try:
        return _kernel.run_task(
            title          = title,
            input_data     = input_data,
            project_id     = project_id,
            priority       = priority,
            max_iterations = max_iterations,
        )
    except Exception as exc:
        return {
            "status":              "error",
            "root_task_id":        None,
            "project_id":          project_id,
            "subtasks_total":      0,
            "subtasks_dispatched": 0,
            "subtasks_completed":  0,
            "subtasks_rejected":   0,
            "plan_text":           "",
            "error": f"Kernel exception: {type(exc).__name__}: {exc}",
        }


# ── FORMATTING ────────────────────────────────────────────────

def format_result(result: dict) -> str:
    """Convert a kernel result dict into a human-readable CLI string."""
    lines = []
    status = result.get("status", "unknown")
    if status == "error":
        lines.append(f"[ERROR] {result.get('error', 'Unknown error')}")
        return "\n".join(lines)
    lines.append("=" * 54)
    lines.append("  GRID MASTER OS — Task Complete")
    lines.append("=" * 54)
    lines.append(f"  Status        : {status.upper()}")
    lines.append(f"  Project       : {result.get('project_id', '—')}")
    lines.append(f"  Root Task     : {result.get('root_task_id', '—')}")
    lines.append(f"  Subtasks      : {result.get('subtasks_total', 0)} created")
    lines.append(f"  Dispatched    : {result.get('subtasks_dispatched', 0)}")
    lines.append(f"  Completed     : {result.get('subtasks_completed', 0)}")
    lines.append(f"  Rejected      : {result.get('subtasks_rejected', 0)}")
    plan = result.get("plan_text", "")
    if plan:
        lines.append("")
        lines.append("  Plan:")
        for ln in plan.splitlines():
            lines.append(f"    {ln}")
    lines.append("=" * 54)
    return "\n".join(lines)


def format_error(message: str) -> str:
    """Produce a consistently structured error string for CLI output."""
    return f"[ERROR] {message}"
