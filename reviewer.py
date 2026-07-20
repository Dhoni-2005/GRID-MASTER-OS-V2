"""
reviewer.py — Grid Master OS Kernel v1.0 (Phase 3 Step 4)

Reviewer Agent: evaluates Worker output and decides approved/rejected.

Approved lifecycle position:
    review_pending → [Reviewer] → completed | rejected

Status rules (strictly enforced):
    Reviewer MAY write:   completed | rejected
    Reviewer NEVER writes: planned | pending | review_pending | failed | abandoned

Responsibilities:
    1. Accept tasks with status="review_pending" only
    2. Evaluate Worker output (rule-based Phase 3)
    3. On approval: extract lesson + knowledge, set status="completed"
    4. On rejection: record reason, set status="rejected"
    5. Reviewer is the ONLY agent permitted to write "completed"

Phase 4 upgrade path:
    Replace _evaluate() body with an LLM API call.
    Signature (task, output) -> (bool, str) must not change.
    All other functions remain identical.

Communication:
    Reviewer communicates exclusively through database.py and memory_manager.py.
    No imports of planner.py, worker.py, grid_master.py, or agent_registry.py.
"""
import database       as db
import memory_manager as mm

_MODULE       = "[REVIEWER]"
ACCEPT_STATUS = "review_pending"

# Error markers that cause automatic rejection in Phase 3
_ERROR_MARKERS = (
    "Error:",
    "Exception:",
    "Traceback",
    "FAILED",
)

MIN_OUTPUT_LENGTH = 10


# ── PUBLIC ENTRY POINT ────────────────────────────────────────

def review(task_id: int, project_id: int | None = None) -> dict:
    """
    Main entry point. Called by the Coordinator after Worker completes.

    Parameters
    ----------
    task_id    : ID of the task to review
    project_id : project context (resolved from task if None)

    Returns
    -------
    {
      "task_id":   int,
      "status":    "completed" | "rejected" | "error",
      "approved":  bool,
      "reason":    str,
      "lesson":    str,
      "error":     str | None,
    }
    """
    # ── Load task ─────────────────────────────────────────────
    task = db.get_task(task_id)
    if not task:
        msg = f"Task {task_id} not found"
        print(f"{_MODULE} ERROR: {msg}")
        return _error(task_id, msg)

    # ── Status guard — accept "review_pending" only ───────────
    current = task.get("status", "")
    if current != ACCEPT_STATUS:
        msg = (f"Task {task_id} has status='{current}'. "
               f"Reviewer only accepts '{ACCEPT_STATUS}' tasks.")
        print(f"{_MODULE} ERROR: {msg}")
        return _error(task_id, msg)

    project_id = project_id or task.get("project_id")
    output     = (task.get("output") or "").strip()

    db.write_note(task_id, "reviewer", "Review started.")

    # ── Evaluate ──────────────────────────────────────────────
    approved, reason = _evaluate(task, output)

    if approved:
        lesson = _approve(task_id, output, project_id)
        _extract_lesson(task_id, task, output, project_id)
        print(f"{_MODULE} Task [{task_id}] approved → completed")
        return {
            "task_id":  task_id,
            "status":   "completed",
            "approved": True,
            "reason":   reason,
            "lesson":   lesson,
            "error":    None,
        }
    else:
        _reject(task_id, reason, project_id)
        print(f"{_MODULE} Task [{task_id}] rejected: {reason}")
        return {
            "task_id":  task_id,
            "status":   "rejected",
            "approved": False,
            "reason":   reason,
            "lesson":   "",
            "error":    None,
        }


# ── EVALUATION (Phase 4 LLM hook) ────────────────────────────

def _evaluate(task: dict, output: str) -> tuple[bool, str]:
    """
    Evaluate Worker output and return (approved, reason).

    Phase 3: rule-based checks only.
    Phase 4: replace this body with an LLM API call.
             Signature (task, output) -> (bool, str) must not change.

    Rejection triggers:
      - Output is empty or whitespace only
      - Output length < MIN_OUTPUT_LENGTH characters
      - Output contains known error markers

    Returns
    -------
    (True,  "Approved: ...")  on pass
    (False, "Rejected: ...") on fail
    """
    # Guard: empty output
    if not output:
        return False, "Rejected: output is empty"

    # Guard: minimum length
    if len(output) < MIN_OUTPUT_LENGTH:
        return False, (f"Rejected: output too short "
                       f"({len(output)} chars, minimum {MIN_OUTPUT_LENGTH})")

    # Guard: error markers
    for marker in _ERROR_MARKERS:
        if marker in output:
            return False, f"Rejected: output contains error marker '{marker}'"

    return True, f"Approved: output passed all Phase 3 checks ({len(output)} chars)"


# ── APPROVAL PATH ─────────────────────────────────────────────

def _approve(task_id: int, output: str, project_id: int | None) -> str:
    """
    Perform all approval-path writes.
    Reviewer is the ONLY agent permitted to write status="completed".

    Steps:
      1. Store approval memory entry (SCORE_LESSON importance)
      2. Update task status → "completed"
      3. Write reviewer approval note
      4. Return lesson string for caller
    """
    lesson = (f"Task [{task_id}] approved. "
              f"Output length: {len(output)} chars. "
              f"Output preview: {output[:80]}")

    # 1. Memory entry — lesson importance so it surfaces in Planner context
    mm.remember(
        task_id    = task_id,
        content    = f"Reviewer approved task [{task_id}]. {lesson}",
        entry_type = "lesson",
        tags       = ["reviewer", "approved"],
        importance = mm.SCORE_LESSON,
        project_id = project_id,
        summary    = f"Approved: {output[:60]}",
    )

    # 2. Status → completed (Reviewer's exclusive right)
    db.update_task_status(task_id, "completed",
                          output=output)

    # 3. Note for Coordinator visibility
    db.write_note(task_id, "reviewer",
                  f"APPROVED. {lesson[:120]}")

    return lesson


# ── REJECTION PATH ────────────────────────────────────────────

def _reject(task_id: int, reason: str, project_id: int | None) -> None:
    """
    Perform all rejection-path writes.
    Reviewer does NOT retry — Coordinator handles retry logic.

    Steps:
      1. Store failure memory entry
      2. Update task status → "rejected"
      3. Write reviewer rejection note
    """
    # 1. Failure memory — so future Planner avoids the same issue
    mm.remember_failure(
        task_id    = task_id,
        problem    = reason,
        cause      = "Worker output failed Reviewer evaluation",
        fix        = "Worker must produce output passing all evaluation checks",
        tags       = ["reviewer", "rejected"],
        project_id = project_id,
    )

    # 2. Status → rejected
    db.update_task_status(task_id, "rejected",
                          output=f"Rejected by Reviewer: {reason}")

    # 3. Note for Coordinator visibility
    db.write_note(task_id, "reviewer",
                  f"REJECTED. Reason: {reason[:200]}")


# ── LESSON EXTRACTION ─────────────────────────────────────────

def _extract_lesson(task_id:    int,
                    task:       dict,
                    output:     str,
                    project_id: int | None) -> int:
    """
    Extract a reusable pattern into the knowledge base.
    Called only on the approval path.

    Uses mm.extract_knowledge() — the Knowledge Extractor hook
    defined in Phase 2 memory_manager.py.

    Returns the knowledge base entry id, or -1 on error.
    """
    title  = (task.get("title") or f"task_{task_id}")[:60]
    topic  = f"approved_output_{title.lower().replace(' ', '_')}"
    content = (f"Task: {title}\n"
               f"Output ({len(output)} chars):\n"
               f"{output[:300]}")
    summary = f"Approved output for: {title}"

    return mm.extract_knowledge(
        topic   = topic,
        content = content,
        summary = summary,
        source  = f"task:{task_id}",
        tags    = ["reviewer", "approved", "lesson"],
    )


# ── HELPERS ───────────────────────────────────────────────────

def _error(task_id: int, msg: str) -> dict:
    """Return a structured error dict with no DB side effects."""
    return {
        "task_id":  task_id,
        "status":   "error",
        "approved": False,
        "reason":   msg,
        "lesson":   "",
        "error":    msg,
    }


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys, json

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "memory_manager", "reviewer"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    pid = _db.create_project("Reviewer Test", "Phase 3 Step 4 self-test")

    def _make_task(title, output="", status="review_pending"):
        tid = _db.create_task(pid, title, input_data=title, priority=5)
        _db.update_task_status(tid, status, output=output)
        return tid

    # ── Test 1: unknown task_id → error dict ─────────────────
    r1 = review(99999, project_id=pid)
    assert r1["status"] == "error"
    assert r1["approved"] is False
    assert r1["error"] is not None
    print("  Test 1 passed: unknown task_id → error dict")

    # ── Test 2: wrong status rejected ────────────────────────
    t_wrong = _make_task("wrong status", status="planned")
    r2 = review(t_wrong, project_id=pid)
    assert r2["status"] == "error"
    assert _db.get_task(t_wrong)["status"] == "planned", \
        "Status must be unchanged after guard rejection"
    print("  Test 2 passed: wrong status (planned) rejected by guard")

    t_completed = _make_task("already done", output="done", status="completed")
    r2b = review(t_completed, project_id=pid)
    assert r2b["status"] == "error"
    assert _db.get_task(t_completed)["status"] == "completed"
    print("  Test 2b passed: wrong status (completed) rejected by guard")

    # ── Test 3: approval path ─────────────────────────────────
    good_output = "This is a valid result with sufficient length and no errors."
    t3 = _make_task("Valid task", output=good_output)
    r3 = review(t3, project_id=pid)
    assert r3["status"]   == "completed", f"Expected completed, got {r3['status']}"
    assert r3["approved"] is True
    assert _db.get_task(t3)["status"] == "completed"
    print("  Test 3 passed: approval path → completed")

    # ── Test 4: rejection — empty output ─────────────────────
    t4 = _make_task("Empty output task", output="")
    r4 = review(t4, project_id=pid)
    assert r4["status"]   == "rejected"
    assert r4["approved"] is False
    assert "empty" in r4["reason"].lower()
    assert _db.get_task(t4)["status"] == "rejected"
    print("  Test 4 passed: empty output → rejected")

    # ── Test 5: rejection — output too short ─────────────────
    t5 = _make_task("Short output task", output="tiny")
    r5 = review(t5, project_id=pid)
    assert r5["status"]   == "rejected"
    assert "short" in r5["reason"].lower() or "minimum" in r5["reason"].lower()
    print("  Test 5 passed: short output (<10 chars) → rejected")

    # ── Test 6: rejection — error markers ────────────────────
    for marker in ("Error: something went wrong",
                   "Exception: null pointer",
                   "Traceback (most recent call last)",
                   "FAILED to execute"):
        tm = _make_task(f"Marker test: {marker[:20]}", output=marker)
        rm = review(tm, project_id=pid)
        assert rm["status"]   == "rejected", \
            f"Expected rejected for marker '{marker[:20]}', got {rm['status']}"
        assert rm["approved"] is False
    print("  Test 6 passed: all 4 error markers trigger rejection")

    # ── Test 7: lesson extracted on approval ──────────────────
    good2 = "Another valid result with more than ten characters for sure."
    t7 = _make_task("Lesson extraction task", output=good2)
    r7 = review(t7, project_id=pid)
    assert r7["status"] == "completed"
    assert r7["lesson"] != "", "Lesson must be non-empty on approval"
    # Check memory entry was written
    mem = _db.get_memory(task_id=t7, project_id=pid)
    lesson_entries = [e for e in mem if e["entry_type"] == "lesson"]
    assert lesson_entries, "Approval must write a lesson memory entry"
    assert lesson_entries[0]["importance_score"] == mm.SCORE_LESSON
    print("  Test 7 passed: lesson memory entry written on approval")

    # ── Test 8: knowledge extracted on approval ───────────────
    kb = _db.search_knowledge("Lesson extraction task")
    assert kb, "Knowledge base must contain an entry after approval"
    assert "approved_output_" in kb[0]["topic"]
    print("  Test 8 passed: knowledge extracted to knowledge_base on approval")

    # ── Test 9: failure memory written on rejection ───────────
    t9 = _make_task("Rejection memory task", output="")
    review(t9, project_id=pid)
    failures = _db.search_failures("Reviewer evaluation")
    assert failures, "Rejection must write to failure_memory"
    print("  Test 9 passed: failure_memory written on rejection")

    # ── Test 10: Reviewer never writes forbidden statuses ─────
    forbidden = {"planned", "pending", "review_pending", "failed", "abandoned"}
    all_tasks = _db.list_tasks(project_id=pid)
    for t in all_tasks:
        reviewer_notes = _db.get_notes(t["id"], agent_role="reviewer")
        if reviewer_notes:
            s = _db.get_task(t["id"])["status"]
            assert s not in forbidden or s == "rejected" or s == "error", \
                f"Reviewer must not set status='{s}' on task {t['id']}"
    print("  Test 10 passed: Reviewer never wrote forbidden statuses")

    # ── Test 11: Reviewer is ONLY agent that wrote "completed" ─
    # Only check tasks that have at least one reviewer note —
    # manually seeded "completed" tasks (like t_completed above)
    # have no reviewer note by design and must not break this test.
    all_completed = [t for t in _db.list_tasks(project_id=pid)
                     if t["status"] == "completed"]
    for t in all_completed:
        notes = _db.get_notes(t["id"], agent_role="reviewer")
        if not notes:
            # Task was seeded directly with status=completed in the test
            # setup — not reviewed by Reviewer. Skip it.
            continue
        assert any("APPROVED" in n["note"] for n in notes), \
            f"Task {t['id']} completed without APPROVED reviewer note"
    print("  Test 11 passed: every completed task has an APPROVED reviewer note")

    # ── Test 12: re-review of completed task rejected ─────────
    r12 = review(t3, project_id=pid)   # t3 is already "completed"
    assert r12["status"] == "error", \
        "Re-review of completed task must return error"
    assert _db.get_task(t3)["status"] == "completed", \
        "Status must not change on re-review attempt"
    print("  Test 12 passed: re-review of completed task rejected")

    # ── Test 13: reviewer notes written on both paths ─────────
    notes_approved = _db.get_notes(t3, agent_role="reviewer")
    assert any("APPROVED" in n["note"] for n in notes_approved)

    notes_rejected = _db.get_notes(t4, agent_role="reviewer")
    assert any("REJECTED" in n["note"] for n in notes_rejected)
    print("  Test 13 passed: reviewer notes present on both approval and rejection paths")

    # ── Test 14: _evaluate Phase 4 hook contract ──────────────
    # Verify _evaluate returns (bool, str) tuple in all cases
    dummy_task = {"id": 0, "title": "test", "output": ""}
    result = _evaluate(dummy_task, "")
    assert isinstance(result, tuple) and len(result) == 2
    assert isinstance(result[0], bool)
    assert isinstance(result[1], str)
    result2 = _evaluate(dummy_task, "A" * 50)
    assert result2[0] is True
    print("  Test 14 passed: _evaluate() returns (bool, str) — Phase 4 hook contract valid")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 3 Step 4 — Reviewer Agent).")
