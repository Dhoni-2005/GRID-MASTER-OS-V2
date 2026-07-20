"""
planner.py — Grid Master OS Kernel v1.0 (Phase 3 Step 2)

Planner Agent: converts a goal task into an ordered subtask plan.

Responsibilities:
  1. Read the assigned task from the database
  2. Retrieve memory context before planning
  3. Decompose the task into ordered steps (rule-based Phase 3)
  4. Create subtask records using parent_task_id
  5. Write the plan to agent_notes
  6. Set parent task status = "planned"

Constraints:
  - Maximum 10 subtasks per parent task
  - Zero-step decomposition falls back to one step = parent task title
  - Planner reads memory; it never writes memory directly
  - No Worker, Reviewer, Coordinator, or Scheduler logic
  - No autonomous loop
  - _decompose() is the Phase 4 LLM hook — only its body changes later

Phase 4 upgrade path:
  Replace _decompose() body with an LLM API call.
  All other functions remain unchanged.
"""
import database       as db
import memory_manager as mm

_MODULE     = "[PLANNER]"
MAX_SUBTASKS = 10


# ── PUBLIC ENTRY POINT ────────────────────────────────────────

def plan(task_id: int, project_id: int | None = None) -> dict:
    """
    Main entry point. Called by the Coordinator.

    1. Load task from database.
    2. Build memory context.
    3. Decompose into steps.
    4. Create subtask records.
    5. Write plan note.
    6. Update parent task status → "planned".

    Returns:
        {
          "task_id":      int,
          "subtasks":     [list of subtask dicts],
          "plan_text":    str,
          "status":       "planned" | "error",
          "error":        str | None,
        }
    """
    # ── Load task ─────────────────────────────────────────────
    task = db.get_task(task_id)
    if not task:
        msg = f"Task {task_id} not found"
        print(f"{_MODULE} ERROR: {msg}")
        return _error(task_id, msg)

    project_id = project_id or task.get("project_id")

    # ── Build memory context ──────────────────────────────────
    keyword = task.get("title", "")
    context = _get_context(task_id, keyword, project_id)

    _log_context_summary(task_id, context)

    # ── Decompose into steps ──────────────────────────────────
    steps = _decompose(task, context)

    # ── Create subtask records ────────────────────────────────
    subtasks = _create_subtasks(task_id, steps, project_id)

    # ── Write plan to agent_notes ─────────────────────────────
    plan_text = _build_plan_text(task, steps, context)
    _write_plan(task_id, plan_text)

    # ── Update parent task status ─────────────────────────────
    db.update_task_status(task_id, "planned",
                          output=f"{len(subtasks)} subtask(s) created")

    print(f"{_MODULE} Task [{task_id}] planned — {len(subtasks)} subtask(s)")

    return {
        "task_id":   task_id,
        "subtasks":  subtasks,
        "plan_text": plan_text,
        "status":    "planned",
        "error":     None,
    }


# ── CONTEXT RETRIEVAL ─────────────────────────────────────────

def _get_context(task_id: int,
                 keyword: str,
                 project_id: int | None) -> dict:
    """
    Retrieve memory context before planning.
    Calls mm.build_context() which returns memories, failures,
    and knowledge base entries relevant to the keyword.

    Planner reads only — never writes memory.
    """
    try:
        return mm.build_context(
            task_id    = task_id,
            project_id = project_id,
            keyword    = keyword,
            limit      = 10,
        )
    except Exception as e:
        print(f"{_MODULE} _get_context error: {e}")
        return {"memories": [], "failures": [], "knowledge": []}


# ── DECOMPOSITION ─────────────────────────────────────────────

def _decompose(task: dict, context: dict) -> list[str]:
    """
    Convert a task into an ordered list of step strings.

    Phase 3 (rule-based):
      Parses the task `input` field for structure:
        - Numbered lines:  "1. Do X\n2. Do Y"
        - Hyphen lines:    "- Do X\n- Do Y"
        - Blank-separated: paragraphs treated as separate steps
      Falls back to the task `title` as a single step if no
      structure is found in `input`.

    Phase 4 upgrade:
      Replace this function body with an LLM API call.
      Signature must not change — Coordinator calls _decompose()
      without knowing whether it is rule-based or LLM-backed.

    Rules enforced here:
      - Maximum MAX_SUBTASKS (10) steps returned.
      - Empty or whitespace-only steps are stripped.
      - If zero steps result, return [task title] as fallback.
    """
    raw_input = (task.get("input") or "").strip()
    title     = (task.get("title") or "task").strip()

    steps: list[str] = []

    if raw_input:
        # Keep raw lines (with blanks) for paragraph detection.
        # Strip for numbered/bullet detection only.
        raw_lines     = [ln.strip() for ln in raw_input.splitlines()]
        non_empty     = [ln for ln in raw_lines if ln]

        import re as _re
        _numbered_re = _re.compile(r'^\d+[.)]\s+(.+)')

        # Numbered list: "1. step", "10. step", "1) step", "10) step"
        numbered_matches = [
            _numbered_re.match(ln) for ln in non_empty
        ]
        if any(numbered_matches):
            steps = [
                m.group(1).strip()
                for m in numbered_matches
                if m and m.group(1).strip()
            ]

        # Hyphen / bullet list: "- step", "* step", "• step"
        elif any(ln.startswith(("-", "*", "•")) for ln in non_empty):
            steps = [
                ln.lstrip("-*• ").strip()
                for ln in non_empty
                if ln.startswith(("-", "*", "•"))
                and ln.lstrip("-*• ").strip()
            ]

        # Blank-separated paragraphs — use raw_lines to keep blank separators
        else:
            current: list[str] = []
            for ln in raw_lines:
                if ln:
                    current.append(ln)
                elif current:
                    steps.append(" ".join(current))
                    current = []
            if current:
                steps.append(" ".join(current))

    # Enforce cap
    steps = steps[:MAX_SUBTASKS]

    # Fallback: zero steps → use title as single step
    if not steps:
        steps = [title]

    return steps


# ── SUBTASK CREATION ──────────────────────────────────────────

def _create_subtasks(parent_id: int,
                     steps: list[str],
                     project_id: int | None) -> list[dict]:
    """
    Create one task record per step using parent_task_id.
    Priority decreases with step index so the first step
    has the highest priority and is dispatched first.

    Each subtask is immediately transitioned from the database
    default status "pending" to "planned" using the existing
    public db.update_task_status(). This mirrors the same
    transition already applied to the parent task in plan(),
    making Planner the single owner of the initial lifecycle
    state of every task record it creates — parent and children
    alike. Without this, subtasks would be created in a
    non-dispatchable state and silently skipped by
    scheduler.select_task(), which only considers tasks with
    status in {"planned", "review_pending"}.

    Returns list of created task dicts (post-activation).
    """
    subtasks = []
    base_priority = 10  # highest; decrements per step
    for i, step in enumerate(steps):
        priority = max(1, base_priority - i)
        try:
            tid = db.create_task(
                project_id     = project_id,
                title          = step,
                input_data     = step,
                priority       = priority,
                parent_task_id = parent_id,
            )
            # Activate immediately — see docstring above.
            db.update_task_status(tid, "planned")
            db.write_note(tid, "planner",
                          f"Subtask {i+1}/{len(steps)}: {step}")
            task = db.get_task(tid)
            if task:
                subtasks.append(task)
        except Exception as e:
            print(f"{_MODULE} _create_subtasks error on step {i+1}: {e}")
    return subtasks


# ── PLAN WRITING ──────────────────────────────────────────────

def _write_plan(task_id: int, plan_text: str) -> None:
    """
    Write the full plan text to agent_notes with role='planner'.
    Coordinator reads this to understand what was planned.
    """
    try:
        db.write_note(task_id, "planner", plan_text)
    except Exception as e:
        print(f"{_MODULE} _write_plan error: {e}")


def _build_plan_text(task: dict,
                     steps: list[str],
                     context: dict) -> str:
    """
    Build a human-readable plan string for agent_notes.
    Includes context summary so reviewers can see what
    prior knowledge was available at planning time.
    """
    lines = [
        f"PLAN for task [{task.get('id')}]: {task.get('title')}",
        f"Steps ({len(steps)}):",
    ]
    for i, step in enumerate(steps, 1):
        lines.append(f"  {i}. {step}")

    mem_count  = len(context.get("memories",  []))
    fail_count = len(context.get("failures",  []))
    kb_count   = len(context.get("knowledge", []))
    lines.append(
        f"Context: {mem_count} memories, "
        f"{fail_count} prior failures, "
        f"{kb_count} knowledge entries"
    )
    return "\n".join(lines)


# ── HELPERS ───────────────────────────────────────────────────

def _log_context_summary(task_id: int, context: dict) -> None:
    mem  = len(context.get("memories",  []))
    fail = len(context.get("failures",  []))
    kb   = len(context.get("knowledge", []))
    print(f"{_MODULE} Context [{task_id}]: "
          f"{mem} memories, {fail} failures, {kb} knowledge entries")


def _error(task_id: int, msg: str) -> dict:
    return {
        "task_id":   task_id,
        "subtasks":  [],
        "plan_text": "",
        "status":    "error",
        "error":     msg,
    }


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "memory_manager", "planner"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    pid = _db.create_project("Planner Test", "Phase 3 Step 2 self-test")

    # ── Test 1: numbered list decomposition ──────────────────
    t1 = _db.create_task(pid, "Build API endpoint",
                         input_data="1. Create route\n2. Add handler\n3. Write tests",
                         priority=8)
    r1 = plan(t1, project_id=pid)
    assert r1["status"] == "planned",   f"Expected planned, got {r1['status']}"
    assert len(r1["subtasks"]) == 3,    f"Expected 3 subtasks, got {len(r1['subtasks'])}"
    assert r1["subtasks"][0]["title"] == "Create route"
    assert r1["subtasks"][1]["title"] == "Add handler"
    assert r1["subtasks"][2]["title"] == "Write tests"
    assert r1["subtasks"][0]["parent_task_id"] == t1
    parent = _db.get_task(t1)
    assert parent["status"] == "planned", \
        f"Parent must be 'planned', got '{parent['status']}'"
    # Subtask priority decreases with step index
    assert r1["subtasks"][0]["priority"] > r1["subtasks"][1]["priority"]
    # Plan text written to agent_notes
    notes = _db.get_notes(t1, agent_role="planner")
    assert len(notes) >= 1, "Expected at least one planner note"
    assert "PLAN for task" in notes[-1]["note"]
    print(f"  Test 1 passed: numbered list → {len(r1['subtasks'])} subtasks")

    # ── Test 2: hyphen/bullet list decomposition ─────────────
    t2 = _db.create_task(pid, "Review codebase",
                         input_data="- Check imports\n- Run linter\n* Fix warnings",
                         priority=6)
    r2 = plan(t2, project_id=pid)
    assert len(r2["subtasks"]) == 3,   f"Expected 3 bullet subtasks, got {len(r2['subtasks'])}"
    assert r2["subtasks"][0]["title"] == "Check imports"
    assert r2["subtasks"][2]["title"] == "Fix warnings"
    print(f"  Test 2 passed: bullet list → {len(r2['subtasks'])} subtasks")

    # ── Test 3: blank-separated paragraph decomposition ──────
    t3 = _db.create_task(pid, "Deploy service",
                         input_data="Build the Docker image\n\nPush to registry\n\nRun smoke test",
                         priority=5)
    r3 = plan(t3, project_id=pid)
    assert len(r3["subtasks"]) == 3,   f"Expected 3 paragraph subtasks, got {len(r3['subtasks'])}"
    print(f"  Test 3 passed: paragraphs → {len(r3['subtasks'])} subtasks")

    # ── Test 4: no-structure input → 1 subtask from title ────
    t4 = _db.create_task(pid, "Investigate issue",
                         input_data="Just look into it",
                         priority=4)
    r4 = plan(t4, project_id=pid)
    assert len(r4["subtasks"]) == 1, \
        f"Expected 1 fallback subtask, got {len(r4['subtasks'])}"
    # The fallback step should be the input line (not title, since input has content)
    print(f"  Test 4 passed: unstructured input → 1 subtask")

    # ── Test 5: empty input → fallback to task title ─────────
    t5 = _db.create_task(pid, "Fix login bug",
                         input_data="",
                         priority=7)
    r5 = plan(t5, project_id=pid)
    assert len(r5["subtasks"]) == 1, \
        f"Expected 1 fallback subtask, got {len(r5['subtasks'])}"
    assert r5["subtasks"][0]["title"] == "Fix login bug", \
        f"Fallback title mismatch: {r5['subtasks'][0]['title']}"
    print(f"  Test 5 passed: empty input → fallback title subtask")

    # ── Test 6: MAX_SUBTASKS cap enforced ────────────────────
    long_input = "\n".join(f"{i+1}. Step {i+1}" for i in range(15))
    t6 = _db.create_task(pid, "Long task", input_data=long_input, priority=3)
    r6 = plan(t6, project_id=pid)
    assert len(r6["subtasks"]) == MAX_SUBTASKS, \
        f"Expected {MAX_SUBTASKS} subtasks max, got {len(r6['subtasks'])}"
    print(f"  Test 6 passed: 15-step input capped at {MAX_SUBTASKS} subtasks")

    # ── Test 7: unknown task_id returns error ─────────────────
    r7 = plan(99999, project_id=pid)
    assert r7["status"] == "error", \
        f"Expected error for unknown task, got {r7['status']}"
    assert r7["subtasks"] == []
    print(f"  Test 7 passed: unknown task_id returns error dict")

    # ── Test 8: subtasks use correct parent_task_id ───────────
    for st in r1["subtasks"]:
        assert st["parent_task_id"] == t1, \
            f"Subtask {st['id']} has wrong parent_task_id"
    subtasks_from_db = _db.get_subtasks(t1)
    assert len(subtasks_from_db) == 3, \
        f"db.get_subtasks returned {len(subtasks_from_db)}, expected 3"
    print(f"  Test 8 passed: all subtasks linked to correct parent")

    # ── Test 9: memory context is consulted before planning ───
    # Store a failure memory entry and verify context includes it
    _db.store_memory(t1, "Prior attempt failed with ImportError",
                     entry_type="failure", importance_score=7,
                     project_id=pid)
    t9 = _db.create_task(pid, "Build API endpoint",
                         input_data="1. Create route\n2. Test it",
                         priority=5)
    r9 = plan(t9, project_id=pid)
    assert "Context:" in r9["plan_text"], \
        "Plan text must include context summary"
    print(f"  Test 9 passed: plan text includes context summary")

    # ── Test 10: regression — every subtask is immediately "planned" ──
    t10 = _db.create_task(pid, "Deploy pipeline",
                          input_data="1. Build image\n2. Push image\n3. Deploy to staging",
                          priority=6)
    r10 = plan(t10, project_id=pid)
    assert r10["status"] == "planned", \
        f"Parent task status unchanged behaviour expected, got {r10['status']}"
    assert len(r10["subtasks"]) == 3, \
        f"Expected 3 subtasks, got {len(r10['subtasks'])}"
    for st in r10["subtasks"]:
        assert st["status"] == "planned", \
            (f"Subtask {st['id']} ('{st['title']}') has status "
             f"'{st['status']}' — expected 'planned' immediately "
             f"after creation")
    # Verify directly against the database too, not just the
    # returned dicts, to guard against stale in-memory copies.
    for st in r10["subtasks"]:
        fresh = _db.get_task(st["id"])
        assert fresh["status"] == "planned", \
            f"DB row for subtask {st['id']} is '{fresh['status']}', not 'planned'"
    # Parent task behaviour unchanged — still set to "planned" as before.
    parent10 = _db.get_task(t10)
    assert parent10["status"] == "planned", \
        f"Parent task status regression: expected 'planned', got '{parent10['status']}'"
    print(f"  Test 10 passed: all subtasks immediately 'planned' "
          f"(parent behaviour unchanged)")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 3 Step 2 — Planner Agent).")
