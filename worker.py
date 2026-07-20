"""
worker.py — Grid Master OS Kernel v1.0 (Phase 3 Step 3)

Worker Agent: executes one assigned task and records the outcome.

Approved lifecycle position:
    planned → [Worker] → review_pending → [Reviewer] → completed

Status rules (strictly enforced):
    Worker MAY write:   review_pending | failed | abandoned
    Worker NEVER writes: completed | planned | pending | dispatched | blocked

Responsibilities:
    1. Accept tasks with status="planned" only
    2. Retrieve knowledge context before executing
    3. Execute task using rule-based handlers (Phase 3)
    4. Write output and set status="review_pending" on success
    5. Write failure record and set status="failed"/"abandoned" on error

Phase 4 upgrade path:
    Replace _run() body with an LLM API call.
    Signature (task, context) -> (str, str) must not change.
    All other functions remain identical.

Communication:
    Worker communicates exclusively through database.py and memory_manager.py.
    No direct calls to Planner, Reviewer, Coordinator, or any network endpoint.
"""
import database       as db
import memory_manager as mm

_MODULE      = "[WORKER]"
MAX_RETRIES  = 3
ACCEPT_STATUS = "planned"   # only status Worker will execute


# ── PUBLIC ENTRY POINT ────────────────────────────────────────

def execute(task_id: int,
            project_id: int | None = None,
            node_id:    str | None = None,
            timeout:    int        = 60) -> dict:
    """
    Main entry point. Called by the Coordinator.

    Parameters
    ----------
    task_id    : ID of the task to execute
    project_id : project context (resolved from task if None)
    node_id    : node executing this task (None in Phase 3 local mode)
    timeout    : reserved for Phase 4 enforcement; accepted but not enforced

    Returns
    -------
    {
      "task_id":  int,
      "status":   "review_pending" | "failed" | "abandoned" | "error",
      "output":   str,
      "lesson":   str,
      "error":    str | None,
    }
    """
    # ── Load task ─────────────────────────────────────────────
    task = db.get_task(task_id)
    if not task:
        msg = f"Task {task_id} not found"
        print(f"{_MODULE} ERROR: {msg}")
        return _error(task_id, msg)

    # ── Status guard — accept "planned" only ──────────────────
    current_status = task.get("status", "")
    if current_status != ACCEPT_STATUS:
        msg = (f"Task {task_id} has status='{current_status}'. "
               f"Worker only accepts '{ACCEPT_STATUS}' tasks.")
        print(f"{_MODULE} ERROR: {msg}")
        return _error(task_id, msg)

    # Resolve project_id from task if not supplied
    project_id = project_id or task.get("project_id")

    # ── Signal execution start ────────────────────────────────
    db.write_note(task_id, "worker",
                  f"Execution started (node={node_id or 'local'})")

    # ── Load context ──────────────────────────────────────────
    keyword = task.get("title", "")
    context = _load_context(task_id, keyword, project_id)

    # ── Execute ───────────────────────────────────────────────
    output, lesson = "", ""
    try:
        output, lesson = _run(task, context)
    except Exception as exc:
        problem = f"_run() raised exception: {type(exc).__name__}: {exc}"
        print(f"{_MODULE} FAILURE [{task_id}]: {problem}")
        final_status = _store_failure(
            task_id    = task_id,
            problem    = problem,
            cause      = "Unhandled exception in _run()",
            fix        = "Review _run() handler for this task type.",
            project_id = project_id,
            node_id    = node_id,
            tags       = ["exception", "worker"],
        )
        return {
            "task_id": task_id,
            "status":  final_status,
            "output":  "",
            "lesson":  "",
            "error":   problem,
        }

    # ── Guard against empty output ────────────────────────────
    if not output or not output.strip():
        problem = "Worker produced empty output"
        print(f"{_MODULE} FAILURE [{task_id}]: {problem}")
        final_status = _store_failure(
            task_id    = task_id,
            problem    = problem,
            cause      = "Fallback handler did not produce output",
            fix        = "Ensure fallback handler returns non-empty string.",
            project_id = project_id,
            node_id    = node_id,
            tags       = ["empty-output", "worker"],
        )
        return {
            "task_id": task_id,
            "status":  final_status,
            "output":  "",
            "lesson":  lesson,
            "error":   problem,
        }

    # ── Store success ─────────────────────────────────────────
    _store_result(
        task_id    = task_id,
        output     = output,
        lesson     = lesson,
        project_id = project_id,
        node_id    = node_id,
    )

    print(f"{_MODULE} Task [{task_id}] → review_pending "
          f"({len(output)} chars output)")

    return {
        "task_id": task_id,
        "status":  "review_pending",
        "output":  output,
        "lesson":  lesson,
        "error":   None,
    }


# ── CONTEXT LOADING ───────────────────────────────────────────

def _load_context(task_id:    int,
                  keyword:    str,
                  project_id: int | None) -> dict:
    """
    Retrieve knowledge base entries relevant to this task.
    Worker reads knowledge only — not full planning context.
    mm.build_context() belongs to the Planner, not the Worker.
    """
    knowledge: list[dict] = []
    if keyword:
        try:
            knowledge = mm.recall_knowledge(keyword, limit=5)
        except Exception as e:
            print(f"{_MODULE} _load_context error: {e}")
    count = len(knowledge)
    print(f"{_MODULE} Context [{task_id}]: {count} knowledge entries")
    return {"knowledge": knowledge}


# ── EXECUTION (Phase 4 LLM hook) ─────────────────────────────

def _run(task: dict, context: dict) -> tuple[str, str]:
    """
    Execute the task and return (output, lesson).

    Phase 3: rule-based handlers matched against task title and input.
    Phase 4: replace this body with an LLM API call.
             Signature must not change.

    Returns
    -------
    (output, lesson)
        output : non-empty string result of execution
        lesson : reusable insight for the knowledge base (may be "")
    """
    title = (task.get("title") or "").lower()
    inp   = (task.get("input")  or task.get("title") or "").strip()

    # ── summarize / summary ───────────────────────────────────
    if any(kw in title for kw in ("summarize", "summary")):
        preview = inp[:200]
        output  = (f"Summary of input ({len(inp)} chars):\n"
                   f"{preview}"
                   f"{'...' if len(inp) > 200 else ''}")
        lesson  = "Summarization pattern: preview first 200 chars of input."
        return output, lesson

    # ── count ─────────────────────────────────────────────────
    if "count" in title:
        words = len(inp.split())
        lines = len(inp.splitlines()) or 1
        output = (f"Word count: {words}\n"
                  f"Line count: {lines}\n"
                  f"Char count: {len(inp)}")
        lesson = "Count pattern: words, lines, chars from task input."
        return output, lesson

    # ── reverse ───────────────────────────────────────────────
    if "reverse" in title:
        output = inp[::-1]
        lesson = "Reverse pattern: Python slice [::-1] on input string."
        return output, lesson

    # ── uppercase / upper ─────────────────────────────────────
    if any(kw in title for kw in ("uppercase", "upper")):
        output = inp.upper()
        lesson = "Uppercase pattern: str.upper() on task input."
        return output, lesson

    # ── list ──────────────────────────────────────────────────
    if "list" in title:
        items  = [ln.strip() for ln in inp.splitlines() if ln.strip()]
        if not items:
            items = inp.split()
        output = "\n".join(f"{i+1}. {item}" for i, item in enumerate(items))
        if not output.strip():
            output = f"1. {inp or 'empty input'}"
        lesson = "List pattern: numbered list from input lines."
        return output, lesson

    # ── fallback echo handler ─────────────────────────────────
    # Always produces non-empty output — last line of defence.
    task_id_val = task.get("id", "?")
    output = (f"[WORKER OUTPUT] Task {task_id_val}: {task.get('title', '')}\n"
              f"Input received: {inp[:300]}\n"
              f"Knowledge entries available: {len(context.get('knowledge', []))}\n"
              f"Status: Processed by fallback handler. "
              f"Awaiting Reviewer evaluation.")
    lesson = ""
    return output, lesson


# ── SUCCESS PATH ──────────────────────────────────────────────

def _store_result(task_id:    int,
                  output:     str,
                  lesson:     str,
                  project_id: int | None,
                  node_id:    str | None) -> None:
    """
    Write task result without ever setting status="completed".
    Three sequential writes — Reviewer is the only agent that
    may set "completed".

    1. Update task status to "review_pending" with output.
    2. Store memory entry for the result.
    3. Write worker note.

    complete_task_atomic() is deliberately not called here.
    """
    # 1. Status → review_pending (output stored in tasks.output)
    db.update_task_status(task_id, "review_pending", output=output)

    # 2. Memory entry — result available for future context
    mm.remember(
        task_id    = task_id,
        content    = f"Worker result: {output[:200]}",
        entry_type = "result",
        tags       = ["worker", "result"],
        importance = mm.SCORE_RESULT,
        project_id = project_id,
        summary    = output[:80],
    )

    # 3. Note for Reviewer and Coordinator visibility
    db.write_note(
        task_id,
        "worker",
        f"Output ready for review ({len(output)} chars). "
        f"Node: {node_id or 'local'}. "
        f"Lesson: {lesson[:80] if lesson else 'none'}",
    )


# ── FAILURE PATH ──────────────────────────────────────────────

def _store_failure(task_id:    int,
                   problem:    str,
                   cause:      str,
                   fix:        str,
                   project_id: int | None,
                   node_id:    str | None,
                   tags:       list | None = None) -> str:
    """
    Record a failure atomically and write a worker note.
    Determines "failed" vs "abandoned" from prior failure note count.

    Returns the final status string written ("failed" or "abandoned").
    fail_task_atomic() is used here because failure needs to be atomic:
      task status + failure_memory + memory entry in one transaction.
    """
    prior_failures = db.get_notes(task_id, agent_role="worker")
    failure_notes  = [n for n in prior_failures
                      if "failed" in (n.get("note") or "").lower()
                      or "failure" in (n.get("note") or "").lower()]
    retries = len(failure_notes)

    final_status = "abandoned" if retries >= MAX_RETRIES else "failed"

    db.fail_task_atomic(
        task_id    = task_id,
        status     = final_status,
        output     = f"{final_status.capitalize()}: {problem}",
        node_id    = node_id,
        problem    = problem,
        cause      = cause,
        fix        = fix,
        tags       = tags or ["worker"],
        project_id = project_id,
    )

    db.write_note(
        task_id,
        "worker",
        f"Task {final_status} (attempt {retries + 1}): {problem[:120]}",
    )

    print(f"{_MODULE} Task [{task_id}] {final_status} "
          f"(attempt {retries + 1}/{MAX_RETRIES}): {problem[:60]}")
    return final_status


# ── HELPERS ───────────────────────────────────────────────────

def _error(task_id: int, msg: str) -> dict:
    """Return a structured error dict with no DB side effects."""
    return {
        "task_id": task_id,
        "status":  "error",
        "output":  "",
        "lesson":  "",
        "error":   msg,
    }


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "memory_manager", "worker"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    pid = _db.create_project("Worker Test", "Phase 3 Step 3 self-test")

    def _make_task(title, inp="", status="planned"):
        tid = _db.create_task(pid, title, input_data=inp, priority=5)
        if status != "pending":
            _db.update_task_status(tid, status)
        return tid

    # ── Test 1: status guard — wrong status rejected ──────────
    t_wrong = _make_task("wrong status task", status="pending")
    r = execute(t_wrong, project_id=pid)
    assert r["status"] == "error", f"Expected error for pending task, got {r['status']}"
    assert _db.get_task(t_wrong)["status"] == "pending", \
        "DB status must be unchanged after status guard rejection"
    print("  Test 1 passed: status guard rejects non-planned task")

    # ── Test 2: unknown task_id ───────────────────────────────
    r2 = execute(99999, project_id=pid)
    assert r2["status"] == "error"
    assert r2["task_id"] == 99999
    print("  Test 2 passed: unknown task_id returns error dict")

    # ── Test 3: summarize handler ─────────────────────────────
    t3 = _make_task("Summarize this text",
                    inp="The quick brown fox jumps over the lazy dog. " * 5)
    r3 = execute(t3, project_id=pid)
    assert r3["status"] == "review_pending", \
        f"Expected review_pending, got {r3['status']}"
    assert "Summary" in r3["output"]
    assert _db.get_task(t3)["status"] == "review_pending"
    assert _db.get_task(t3)["output"] != ""
    mem = _db.get_memory(task_id=t3, project_id=pid)
    assert any(e["entry_type"] == "result" for e in mem), \
        "Expected result memory entry"
    notes = _db.get_notes(t3, agent_role="worker")
    assert len(notes) >= 2, "Expected start note + result note"
    print("  Test 3 passed: summarize handler → review_pending")

    # ── Test 4: count handler ─────────────────────────────────
    t4 = _make_task("Count words", inp="hello world foo bar baz")
    r4 = execute(t4, project_id=pid)
    assert r4["status"] == "review_pending"
    assert "Word count: 5" in r4["output"]
    assert "Char count:" in r4["output"]
    print("  Test 4 passed: count handler")

    # ── Test 5: reverse handler ───────────────────────────────
    t5 = _make_task("Reverse this string", inp="hello")
    r5 = execute(t5, project_id=pid)
    assert r5["status"] == "review_pending"
    assert r5["output"] == "olleh"
    print("  Test 5 passed: reverse handler")

    # ── Test 6: uppercase handler ─────────────────────────────
    t6 = _make_task("Uppercase the input", inp="hello world")
    r6 = execute(t6, project_id=pid)
    assert r6["status"] == "review_pending"
    assert r6["output"] == "HELLO WORLD"
    print("  Test 6 passed: uppercase handler")

    # ── Test 7: list handler ──────────────────────────────────
    t7 = _make_task("List the items", inp="apples\nbananas\ncherries")
    r7 = execute(t7, project_id=pid)
    assert r7["status"] == "review_pending"
    assert "1. apples" in r7["output"]
    assert "3. cherries" in r7["output"]
    print("  Test 7 passed: list handler")

    # ── Test 8: fallback echo handler ────────────────────────
    t8 = _make_task("Unknown task type xyz", inp="some data here")
    r8 = execute(t8, project_id=pid)
    assert r8["status"] == "review_pending"
    assert r8["output"].strip() != "", "Fallback must produce non-empty output"
    assert "[WORKER OUTPUT]" in r8["output"]
    print("  Test 8 passed: fallback handler produces non-empty output")

    # ── Test 9: Worker never writes "completed" ───────────────
    for tid in [t3, t4, t5, t6, t7, t8]:
        s = _db.get_task(tid)["status"]
        assert s != "completed", \
            f"Worker must never write 'completed', but task {tid} has status='{s}'"
    print("  Test 9 passed: Worker never wrote 'completed' to any task")

    # ── Test 10: lesson stored in worker note ─────────────────
    notes_t3 = _db.get_notes(t3, agent_role="worker")
    lesson_notes = [n for n in notes_t3 if "Lesson:" in n["note"]]
    assert lesson_notes, "Worker note must include lesson summary"
    print("  Test 10 passed: lesson recorded in worker note")

    # ── Test 11: failure path — fail_task_atomic called ───────
    # Create a task and manually make _run fail by patching input
    # to trigger our own test: we'll call _store_failure directly
    t11 = _make_task("Failure path test", inp="trigger failure")
    final = _store_failure(
        task_id    = t11,
        problem    = "Test failure",
        cause      = "Deliberate test",
        fix        = "No fix needed",
        project_id = pid,
        node_id    = None,
        tags       = ["test"],
    )
    assert final == "failed", f"Expected 'failed' on first attempt, got '{final}'"
    assert _db.get_task(t11)["status"] == "failed"
    fail_mem = _db.search_failures("Test failure")
    assert fail_mem, "failure_memory must contain the failure record"
    print("  Test 11 passed: failure path writes correct status and failure_memory")

    # ── Test 12: abandoned after MAX_RETRIES ──────────────────
    t12 = _make_task("Repeated failure task", inp="keep failing")
    # Pre-populate failure notes to simulate MAX_RETRIES reached
    for i in range(MAX_RETRIES):
        _db.write_note(t12, "worker", f"Task failed (attempt {i+1}): simulated")
    final12 = _store_failure(
        task_id    = t12,
        problem    = "Max retries exceeded",
        cause      = "Persistent failure",
        fix        = "Investigate root cause",
        project_id = pid,
        node_id    = None,
        tags       = ["test", "retry"],
    )
    assert final12 == "abandoned", \
        f"Expected 'abandoned' after {MAX_RETRIES} retries, got '{final12}'"
    assert _db.get_task(t12)["status"] == "abandoned"
    print(f"  Test 12 passed: abandoned after {MAX_RETRIES} failures")

    # ── Test 13: already-executed task rejected on re-execute ─
    # t3 is now "review_pending" — Worker must reject it
    r_re = execute(t3, project_id=pid)
    assert r_re["status"] == "error", \
        "Worker must reject task already in review_pending status"
    assert _db.get_task(t3)["status"] == "review_pending", \
        "Status must be unchanged after guard rejection"
    print("  Test 13 passed: re-execution of review_pending task rejected")

    _db.close_db()
    os.remove(tmp)
    print(f"\n{_MODULE} Self-test passed (Phase 3 Step 3 — Worker Agent).")
