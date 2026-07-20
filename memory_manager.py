"""
memory_manager.py — Grid Master OS Kernel v1.2
Unified memory layer. Uses database._exec/_query abstractions.
No direct get_db() calls — all writes go through database helpers.

Phase 2 Step 1 additions:
  - _validate_tags()         — sanitise tag lists before storage
  - recall_by_tag()          — retrieve entries by exact tag match
  - list_known_tags()        — tag inventory for a project
  - recall_failures_by_tag() — failure_memory filtered by tag
  - remember() now validates tags before storage

Phase 2 Step 2 additions:
  - search() upgraded        — entry_type, after, before, tag filters
  - recall_recent()          — convenience: entries within N hours
  - recall_high_value()      — convenience: entries score >= SCORE_PATTERN
  - memory_stats() fixed     — uses COUNT(*) via db.memory_stats_counts()
"""
import database as db

_MODULE = "[MEMORY]"

SCORE_LOG      = 1
SCORE_RESULT   = 3
SCORE_PATTERN  = 5
SCORE_LESSON   = 7
SCORE_CRITICAL = 10


# ── TAG VALIDATION ────────────────────────────────────────────

def _validate_tags(tags: list | None) -> list[str]:
    """
    Sanitise a tag list before storage.
    - Accepts only string elements; non-strings are silently dropped.
    - Strips whitespace; empty strings dropped.
    - Deduplicates while preserving insertion order.
    - Returns empty list for None input.
    """
    if not tags:
        return []
    seen: set    = set()
    result: list = []
    for t in tags:
        if not isinstance(t, str):
            print(f"{_MODULE} Warning: non-string tag dropped: {t!r}")
            continue
        clean = t.strip()
        if not clean:
            continue
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


# ── CORE WRITE ────────────────────────────────────────────────

def remember(task_id: int, content: str, entry_type: str = "log",
             tags: list | None = None, importance: int = SCORE_LOG,
             project_id: int | None = None, summary: str = "") -> int:
    """
    Store a memory entry. Tags are validated before storage.
    Every agent must call this before exiting a task.
    """
    try:
        return db.store_memory(
            task_id          = task_id,
            content          = content,
            entry_type       = entry_type,
            tags             = _validate_tags(tags),
            importance_score = importance,
            project_id       = project_id,
            summary          = summary,
        )
    except Exception as e:
        print(f"{_MODULE} Write error: {e}")
        return -1


def remember_failure(task_id: int, problem: str, cause: str = "",
                     fix: str = "", tags: list | None = None,
                     project_id: int | None = None) -> int:
    """
    Record a failure. Tags are validated before storage.
    The atomic composite write (failure_memory + memory_entry) is
    handled by grid_master via db.fail_task_atomic() at dispatch level.
    Direct callers still get the failure_memory row here.
    """
    try:
        return db.store_failure(
            task_id    = task_id,
            problem    = problem,
            cause      = cause,
            fix        = fix,
            tags       = _validate_tags(tags),
            project_id = project_id,
        )
    except Exception as e:
        print(f"{_MODULE} Failure write error: {e}")
        return -1


# ── CORE READ ─────────────────────────────────────────────────

def recall(task_id: int | None = None, project_id: int | None = None,
           min_importance: int = SCORE_LOG, limit: int = 20) -> list[dict]:
    """
    Retrieve memory entries, highest importance first.
    Planner calls this before creating a plan.
    """
    try:
        return db.get_memory(
            task_id    = task_id,
            project_id = project_id,
            min_score  = min_importance,
            limit      = limit,
        )
    except Exception as e:
        print(f"{_MODULE} Recall error: {e}")
        return []


def recall_failures(keyword: str, limit: int = 5) -> list[dict]:
    """Search failure memory by keyword."""
    try:
        return db.search_failures(keyword, limit=limit)
    except Exception as e:
        print(f"{_MODULE} Failure recall error: {e}")
        return []


def search(keyword: str = "",
           project_id: int | None = None,
           min_importance: int = SCORE_LOG,
           limit: int = 10,
           entry_type: str | None = None,
           tags: list | None = None,
           after: str | None = None,
           before: str | None = None) -> list[dict]:
    """
    Full-featured memory search. All filters are optional and AND-combined.

    Parameters
    ----------
    keyword    : substring across content, summary, tags. "" = no keyword filter.
    project_id : restrict to one project.
    min_importance : minimum importance_score.
    limit      : max rows returned.
    entry_type : exact match e.g. "log", "result", "lesson", "failure", "summary".
    tags       : if provided, results must contain ALL listed tags.
    after      : ISO-8601 string — entries created after this datetime.
    before     : ISO-8601 string — entries created before this datetime.

    Example:
        search("flask", project_id=1, entry_type="lesson",
               tags=["python"], after="2026-01-01T00:00:00")
    """
    try:
        # Start with db.search_memory for keyword + project + type + date filters
        results = db.search_memory(
            keyword    = keyword,
            project_id = project_id,
            min_score  = min_importance,
            limit      = limit * 4 if tags else limit,  # over-fetch when tag filtering
            entry_type = entry_type,
            after      = after,
            before     = before,
        )
        # Apply tag filtering in Python — requires ALL tags to be present
        # (json_each covers single-tag; multi-tag AND is cleaner here)
        if tags:
            import json
            valid = _validate_tags(tags)
            filtered = []
            for entry in results:
                try:
                    stored = json.loads(entry.get("tags") or "[]")
                except (json.JSONDecodeError, TypeError):
                    stored = []
                if all(t in stored for t in valid):
                    filtered.append(entry)
            results = filtered[:limit]
        return results
    except Exception as e:
        print(f"{_MODULE} Search error: {e}")
        return []


def recall_recent(project_id: int | None = None,
                  hours: int = 24,
                  min_importance: int = SCORE_LOG,
                  limit: int = 20) -> list[dict]:
    """
    Convenience wrapper: return entries created within the last N hours.
    Useful for the Planner to check what has happened recently.

    Example:
        recent = recall_recent(project_id=1, hours=6)
        # entries from the last 6 hours
    """
    from datetime import datetime, timezone, timedelta
    try:
        after = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).replace(tzinfo=None).isoformat()
        return db.search_memory(
            keyword    = "",
            project_id = project_id,
            min_score  = min_importance,
            limit      = limit,
            after      = after,
        )
    except Exception as e:
        print(f"{_MODULE} recall_recent error: {e}")
        return []


def recall_high_value(project_id: int | None = None,
                      limit: int = 20) -> list[dict]:
    """
    Convenience wrapper: return entries with importance_score >= SCORE_PATTERN (5).
    These are the entries most worth reading before starting a new task.
    Ordered by score descending, most recent first within each score tier.

    Example:
        best = recall_high_value(project_id=1, limit=10)
    """
    try:
        return db.get_memory(
            project_id = project_id,
            min_score  = SCORE_PATTERN,
            limit      = limit,
        )
    except Exception as e:
        print(f"{_MODULE} recall_high_value error: {e}")
        return []


# ── TAG QUERIES (Phase 2 — Step 1) ───────────────────────────

def recall_by_tag(tag: str,
                  project_id: int | None = None,
                  min_importance: int = SCORE_LOG,
                  limit: int = 20) -> list[dict]:
    """
    Return memory entries whose tags array contains `tag` exactly.
    Uses db.search_memory_by_tag() which leverages SQLite json_each().

    Example:
        entries = recall_by_tag("flask", project_id=1)
        # returns only entries tagged exactly "flask"
    """
    try:
        tag = tag.strip()
        if not tag:
            print(f"{_MODULE} recall_by_tag: empty tag, returning []")
            return []
        return db.search_memory_by_tag(
            tag        = tag,
            project_id = project_id,
            min_score  = min_importance,
            limit      = limit,
        )
    except Exception as e:
        print(f"{_MODULE} recall_by_tag error: {e}")
        return []


def list_known_tags(project_id: int | None = None) -> list[str]:
    """
    Return all distinct tag values stored across memory_entries.
    Optionally scoped to a project.
    Useful for: discovery, auto-suggest, compression grouping.

    Example:
        tags = list_known_tags(project_id=1)
        # ["flask", "python", "lesson", "failure"]
    """
    try:
        return db.list_tags(project_id=project_id)
    except Exception as e:
        print(f"{_MODULE} list_known_tags error: {e}")
        return []


def recall_failures_by_tag(tag: str, limit: int = 10) -> list[dict]:
    """
    Return failure_memory entries whose tags array contains `tag` exactly.
    Planner can use this to find all known failures in a topic area.

    Example:
        failures = recall_failures_by_tag("flask")
        # returns failures tagged with "flask"
    """
    try:
        tag = tag.strip()
        if not tag:
            print(f"{_MODULE} recall_failures_by_tag: empty tag, returning []")
            return []
        return db.search_failures_by_tag(tag=tag, limit=limit)
    except Exception as e:
        print(f"{_MODULE} recall_failures_by_tag error: {e}")
        return []


# ── KNOWLEDGE BASE ────────────────────────────────────────────

def extract_knowledge(topic: str, content: str, summary: str = "",
                      source: str = "", tags: list | None = None) -> int:
    """
    Knowledge Extractor hook. Stores reusable patterns in knowledge_base.
    Phase 2: called by Memory Manager after Reviewer approval.
    Tags are validated before storage.
    """
    try:
        return db.store_knowledge(
            topic   = topic,
            content = content,
            summary = summary,
            source  = source,
            tags    = _validate_tags(tags),
        )
    except Exception as e:
        print(f"{_MODULE} Knowledge extract error: {e}")
        return -1


def recall_knowledge(query: str, limit: int = 5) -> list[dict]:
    """Search the knowledge base. Worker calls this before executing a task."""
    try:
        return db.search_knowledge(query, limit=limit)
    except Exception as e:
        print(f"{_MODULE} Knowledge recall error: {e}")
        return []


# ── CONTEXT BUILDER ───────────────────────────────────────────

def build_context(task_id: int, project_id: int | None = None,
                  keyword: str = "", limit: int = 10) -> dict:
    """
    Assemble bounded context for the Planner before execution.
    Returns recent memories, relevant failures, and knowledge.
    Never returns more than `limit` entries per section.
    """
    context: dict = {"memories": [], "failures": [], "knowledge": []}
    try:
        context["memories"] = recall(
            project_id     = project_id,
            min_importance = SCORE_RESULT,
            limit          = limit,
        )
        if keyword:
            context["failures"]  = recall_failures(keyword, limit=5)
            context["knowledge"] = recall_knowledge(keyword, limit=5)
    except Exception as e:
        print(f"{_MODULE} Context build error: {e}")
    return context


# ── MEMORY COMPRESSION (Phase 2 — Step 3) ────────────────────

def compress_memory(project_id: int | None = None,
                    older_than_days: int = 7,
                    max_score: int = 2) -> dict:
    """
    Compress old low-importance memory entries into digest records.

    Hard guard: max_score must be < SCORE_PATTERN (5).
    Raising ValueError here prevents callers from accidentally
    compressing patterns, lessons, or criticals.

    Rules enforced inside db.compress_old_entries():
    - Only entry_type 'log' or 'intake' are eligible.
    - Groups smaller than 3 entries are skipped.
    - Failure-linked entries (task_id in failure_memory) are preserved.
    - Each qualifying group is atomically replaced by one digest entry:
        entry_type       = 'compressed'
        importance_score = 3  (above compression ceiling — never re-compressed)
    - No additional memory entry is written here; the digest is the record.

    Returns:
        {"groups_compressed": N, "entries_compressed": M}
    """
    if max_score >= SCORE_PATTERN:
        raise ValueError(
            f"{_MODULE} compress_memory: max_score must be < {SCORE_PATTERN} "
            f"(SCORE_PATTERN). Got {max_score}. "
            f"Entries with importance_score >= {SCORE_PATTERN} must never be compressed."
        )
    try:
        result = db.compress_old_entries(
            project_id      = project_id,
            older_than_days = older_than_days,
            max_score       = max_score,
        )
        print(
            f"{_MODULE} Compression complete — "
            f"groups: {result['groups_compressed']}, "
            f"entries: {result['entries_compressed']}"
        )
        return result
    except Exception as e:
        print(f"{_MODULE} compress_memory error: {e}")
        return {"groups_compressed": 0, "entries_compressed": 0}


# ── MEMORY SUMMARIZATION (Phase 2 — Step 4) ──────────────────

def _build_digest(entries: list[dict]) -> str:
    """
    Build a human-readable digest string from a list of memory entry dicts.

    Phase 3 LLM hook: replace this function body with an API call.
    The signature must not change — only this body is replaced in Phase 3.

    Current (rule-based):
    - Extracts summary field where available, falls back to content[:80].
    - Joins snippets with ' | ' separator.
    - Truncates total to 600 characters to keep digest bounded.
    """
    snippets = []
    for e in entries:
        text = (e.get("summary") or e.get("content") or "")[:80].strip()
        if text:
            snippets.append(text)
    joined = " | ".join(snippets)
    if len(joined) > 600:
        joined = joined[:600] + "..."
    return joined


def summarize_memory(project_id: int | None = None,
                     older_than_days: int = 14,
                     min_score: int = 3,
                     max_score: int = 4,
                     min_group: int = 5) -> dict:
    """
    Summarize accumulated mid-tier memory entries (score 3–4) into
    digest records (entry_type='summary', importance_score=7).

    Source entries are archived (entry_type='archived'), not deleted.
    Summarization is reversible at the record level.

    Hard guard: max_score must be < SCORE_PATTERN (5).
    Raises ValueError if violated — same pattern as compress_memory().

    Returns:
        {
          "groups_summarized":  N,
          "entries_summarized": M,
          "digest_ids":         [list of inserted summary entry ids]
        }
    """
    if max_score >= SCORE_PATTERN:
        raise ValueError(
            f"{_MODULE} summarize_memory: max_score must be < {SCORE_PATTERN} "
            f"(SCORE_PATTERN). Got {max_score}. "
            f"Entries with importance_score >= {SCORE_PATTERN} must never "
            f"be summarized."
        )
    try:
        result = db.summarize_old_entries(
            project_id      = project_id,
            older_than_days = older_than_days,
            min_score       = min_score,
            max_score       = max_score,
            min_group       = min_group,
        )
        print(
            f"{_MODULE} Summarization complete — "
            f"groups: {result['groups_summarized']}, "
            f"entries: {result['entries_summarized']}, "
            f"digests: {result['digest_ids']}"
        )
        return result
    except Exception as e:
        print(f"{_MODULE} summarize_memory error: {e}")
        return {"groups_summarized": 0, "entries_summarized": 0, "digest_ids": []}



# ── MEMORY STATS ──────────────────────────────────────────────

def memory_stats(project_id: int | None = None) -> dict:
    """
    Return memory health counts.
    Uses db.memory_stats_counts() which issues COUNT(*) per score bucket —
    no full table scan. Fixes technical debt item from Phase 1.
    """
    try:
        return db.memory_stats_counts(project_id=project_id)
    except Exception as e:
        print(f"{_MODULE} Stats error: {e}")
        return {}


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import os, tempfile, importlib, sys

    tmp = tempfile.mktemp(suffix=".db")
    os.environ["GRIDMASTER_DB"] = tmp
    for mod in ["database", "memory_manager"]:
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])
    import database as _db
    _db.init_db()

    pid = _db.create_project("MM v1.2 Test", "memory_manager Phase 2 Step 1")
    tid = _db.create_task(pid, "Tag memory test", priority=5)

    # ── Phase 1 existing tests (must still pass) ──────────────
    remember(tid, "Started", importance=SCORE_LOG, project_id=pid)
    remember(tid, "Worker ran Flask route", importance=SCORE_RESULT,
             project_id=pid, tags=["flask", "python"])
    remember_failure(tid, "ImportError: flask", cause="missing dep",
                     fix="pip install flask", tags=["python", "flask"],
                     project_id=pid)
    extract_knowledge("flask_health",
                      "@app.route('/health')\ndef h(): return jsonify({})",
                      summary="Flask health pattern", tags=["flask"])
    ctx = build_context(tid, project_id=pid, keyword="flask")
    assert ctx["memories"], "Expected memories in context"
    stats = memory_stats(project_id=pid)
    assert stats["total_entries"] >= 2, "Expected >=2 entries"

    # ── Phase 2 Step 1: tag validation ────────────────────────
    clean = _validate_tags(["python", "  flask  ", 123, "", "python"])
    assert clean == ["python", "flask"], f"Tag validation failed: {clean}"

    # ── Phase 2 Step 1: recall_by_tag ─────────────────────────
    flask_entries = recall_by_tag("flask", project_id=pid)
    assert len(flask_entries) >= 1, \
        f"Expected >=1 flask-tagged entry, got {len(flask_entries)}"
    for e in flask_entries:
        import json
        assert "flask" in json.loads(e["tags"]), \
            f"Entry {e['id']} missing flask tag: {e['tags']}"

    python_entries = recall_by_tag("python", project_id=pid)
    assert len(python_entries) >= 1, \
        f"Expected >=1 python-tagged entry, got {len(python_entries)}"

    # ── Phase 2 Step 1: list_known_tags ───────────────────────
    known = list_known_tags(project_id=pid)
    assert "flask"  in known, f"'flask' not in known tags: {known}"
    assert "python" in known, f"'python' not in known tags: {known}"

    # ── Phase 2 Step 1: recall_failures_by_tag ────────────────
    flask_fails = recall_failures_by_tag("flask")
    assert len(flask_fails) >= 1, \
        f"Expected >=1 flask failure, got {len(flask_fails)}"

    # ── Edge cases ────────────────────────────────────────────
    assert recall_by_tag("") == [], "Empty tag should return []"
    assert recall_failures_by_tag("") == [], "Empty tag should return []"
    no_match = recall_by_tag("nonexistent_tag_xyz")
    assert no_match == [], f"Non-existent tag should return [], got {no_match}"

    # ── Phase 2 Step 2: upgraded search() ────────────────────
    # entry_type filter
    lessons = search("", project_id=pid, entry_type="log")
    assert all(e["entry_type"] == "log" for e in lessons), \
        "entry_type filter returned wrong types"

    # multi-tag AND filter
    both = search("", project_id=pid, tags=["flask", "python"])
    import json as _json
    for e in both:
        stored = _json.loads(e.get("tags") or "[]")
        assert "flask"  in stored, f"Missing 'flask' in {stored}"
        assert "python" in stored, f"Missing 'python' in {stored}"

    # blank keyword with entry_type returns all matching entries
    all_results = search("", project_id=pid, limit=50)
    assert len(all_results) >= 2, "Expected all entries with blank keyword"

    # after/before date filters
    from datetime import datetime, timezone, timedelta
    past   = (datetime.now(timezone.utc) - timedelta(days=1)).replace(tzinfo=None).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(days=1)).replace(tzinfo=None).isoformat()
    recent = search("", project_id=pid, after=past,   limit=50)
    none   = search("", project_id=pid, after=future,  limit=50)
    assert recent, "Expected entries after yesterday"
    assert not none, "Expected no entries after tomorrow"

    # before filter
    before_results = search("", project_id=pid, before=future, limit=50)
    assert before_results, "Expected entries before tomorrow"

    # ── Phase 2 Step 2: recall_recent ─────────────────────────
    recent_24h = recall_recent(project_id=pid, hours=24)
    assert recent_24h, "Expected entries within last 24h"
    far_future = recall_recent(project_id=pid, hours=0)
    # hours=0 means "after now" — should return nothing
    assert not far_future, f"Expected no entries for hours=0, got {len(far_future)}"

    # ── Phase 2 Step 2: recall_high_value ─────────────────────
    # Store a high-value entry
    remember(tid, "Critical lesson", entry_type="lesson",
             importance=SCORE_PATTERN, project_id=pid, tags=["key"])
    high = recall_high_value(project_id=pid)
    assert high, "Expected high-value entries"
    assert all(e["importance_score"] >= SCORE_PATTERN for e in high), \
        "recall_high_value returned low-score entries"

    # ── Phase 2 Step 2: memory_stats uses COUNT(*) ────────────
    stats2 = memory_stats(project_id=pid)
    assert "total_entries"      in stats2, "Missing total_entries"
    assert "score_distribution" in stats2, "Missing score_distribution"
    assert "total_failures"     in stats2, "Missing total_failures"
    assert stats2["total_entries"] >= 3, \
        f"Expected >= 3 entries, got {stats2['total_entries']}"

    # ── Phase 2 Step 3: compress_memory ──────────────────────
    import json as _json2
    from datetime import datetime, timezone, timedelta as _td2

    # Dedicated task with NO failure_memory record — entries will be eligible
    compress_tid2 = _db.create_task(pid, "mm-compress-task", priority=1)

    def _backdated(content, etype="log", score=1, days_ago=8, task_id=None):
        old_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - _td2(days=days_ago)).isoformat()
        return _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, task_id or compress_tid2, content, "", etype,
             '["python","flask"]', score, old_ts),
        )

    # 3 old log entries — eligible group
    _backdated("compression log A")
    _backdated("compression log B")
    _backdated("compression log C")

    # High-value entry — must survive
    hv_id = _backdated("important pattern", etype="lesson", score=7)

    # Failure-linked entry — must survive
    fail_tid2 = _db.create_task(pid, "fail-task-mm", priority=1)
    _db.store_failure(fail_tid2, "mm test failure", project_id=pid)
    fl_id = _backdated("failure-linked log", task_id=fail_tid2)

    # Two-entry group on different date — must NOT compress (< MIN_GROUP=3)
    _backdated("small group one", days_ago=15)
    _backdated("small group two", days_ago=15)

    # Guard: max_score >= 5 must raise ValueError
    raised = False
    try:
        compress_memory(project_id=pid, max_score=5)
    except ValueError:
        raised = True
    assert raised, "Expected ValueError for max_score >= SCORE_PATTERN"

    # Run compression
    result = compress_memory(project_id=pid, older_than_days=7, max_score=2)
    assert result["entries_compressed"] == 3, \
        f"Expected 3 entries compressed, got {result['entries_compressed']}"
    assert result["groups_compressed"] == 1, \
        f"Expected 1 group, got {result['groups_compressed']}"

    # High-value entry survived
    hv = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (hv_id,))
    assert hv is not None, "High-value entry must survive compression"
    assert hv["importance_score"] >= 5

    # Failure-linked entry survived
    fl = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (fl_id,))
    assert fl is not None, "Failure-linked entry must survive compression"

    # Two-entry group survived unchanged
    tiny = _db.search_memory(keyword="small group", project_id=pid, limit=10)
    assert len(tiny) == 2, f"Small group should be untouched, got {len(tiny)}"

    # Digest entry has correct fields
    digests = _db.search_memory(entry_type="compressed", project_id=pid)
    assert len(digests) >= 1, "Expected at least one digest entry"
    d = digests[0]
    assert d["importance_score"] == 3, \
        f"Digest score must be 3, got {d['importance_score']}"
    assert d["entry_type"] == "compressed"
    assert "[COMPRESSED]" in d["content"]
    tags_stored = _json2.loads(d.get("tags") or "[]")
    assert "python" in tags_stored, "Digest must carry source tags"

    # Re-run: nothing left to compress (digests are score=3, above max_score=2)
    result2 = compress_memory(project_id=pid, older_than_days=7, max_score=2)
    assert result2["entries_compressed"] == 0, \
        f"Second run must compress 0, got {result2['entries_compressed']}"

    # ── Phase 2 Step 4: summarize_memory — full lifecycle ───────
    #  score-1 log entries → compress → age → summarize
    sum_tid = _db.create_task(pid, "mm-sum-task", priority=1)
    log_tid2 = _db.create_task(pid, "mm-lifecycle-logs", priority=1)

    def _mid(text, score=3, days_ago=15, etype="result", tid=None):
        old_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - _td2(days=days_ago)).isoformat()
        return _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, tid or sum_tid, text, f"sum: {text[:25]}",
             etype, '["python","test"]', score, old_ts),
        )

    # Insert 3 old log entries and compress them (lifecycle Step 3 → Step 4)
    lc_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
             - _td2(days=20)).isoformat()
    for i in range(3):
        _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, log_tid2, f"lc log {i}", "", "log", '["lc"]', 1, lc_ts),
        )
    compress_memory(project_id=pid, older_than_days=7, max_score=2)
    # Back-date compressed digest past the 14-day summarization window
    _db._exec(
        "UPDATE memory_entries SET created_at=? "
        "WHERE entry_type='compressed' AND project_id=?",
        (lc_ts, pid),
    )

    # 4 more mid-tier result entries — pinned to the SAME days_ago (20)
    # as the compressed digest (lc_ts) so they always land in the same
    # YYYY-MM month bucket regardless of the current calendar date.
    for i in range(4):
        _mid(f"mid entry {i}", days_ago=20)

    # High-value entry (score=7) — must survive
    hv3_id = _mid("high value lesson", score=7, etype="lesson")

    # Failure-linked entry — must survive
    fail_tid3 = _db.create_task(pid, "fail-sum-mm", priority=1)
    _db.store_failure(fail_tid3, "mm sum failure", project_id=pid)
    fl3_id = _mid("failure-linked mid", tid=fail_tid3)

    # Group of 4 on different month — must NOT summarize (< min_group=5)
    for i in range(4):
        _mid(f"tiny sum {i}", days_ago=60)

    # Guard: max_score >= 5 raises ValueError
    guard_raised = False
    try:
        summarize_memory(project_id=pid, max_score=5)
    except ValueError:
        guard_raised = True
    assert guard_raised, "Expected ValueError for max_score >= SCORE_PATTERN"

    # _build_digest produces bounded output
    sample = [{"summary": f"entry {i}", "content": f"content {i}"} for i in range(10)]
    digest_text = _build_digest(sample)
    assert len(digest_text) <= 620
    assert "entry 0" in digest_text

    # Run summarization — compressed digest (1) + 4 result entries = group of 5
    sum_res = summarize_memory(project_id=pid, older_than_days=14,
                               min_score=3, max_score=4, min_group=5)
    assert sum_res["groups_summarized"]  >= 1, \
        f"Expected >=1 group, got {sum_res['groups_summarized']}"
    assert sum_res["entries_summarized"] >= 5, \
        f"Expected >=5 entries, got {sum_res['entries_summarized']}"

    # Critical: compressed digest must now be archived — full lifecycle confirmed
    archived = _db._query(
        "SELECT * FROM memory_entries WHERE entry_type='archived' AND project_id=?",
        (pid,)
    )
    assert any("[COMPRESSED]" in (r.get("content") or "") for r in archived), \
        "Compressed digest must be archived after summarization — lifecycle broken"

    # High-value entry untouched
    hv3 = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (hv3_id,))
    assert hv3["importance_score"] == 7 and hv3["entry_type"] == "lesson"

    # Failure-linked entry untouched
    fl3 = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (fl3_id,))
    assert fl3["entry_type"] != "archived"

    # Small group untouched
    tiny = _db.search_memory(keyword="tiny sum", project_id=pid, limit=10)
    assert len(tiny) == 4

    # Digest has correct fields
    digest_id = sum_res["digest_ids"][0]
    drow = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (digest_id,))
    assert drow["entry_type"] == "summary" and drow["importance_score"] == 7
    assert "[SUMMARY]" in drow["content"]

    # Re-run returns zero
    sum_res2 = summarize_memory(project_id=pid, older_than_days=14,
                                min_score=3, max_score=4, min_group=5)
    assert sum_res2["entries_summarized"] == 0

    _db.close_db()
    os.remove(tmp)
    print(f"{_MODULE} Self-test passed (Phase 2 Steps 1–4: "
          f"Tag Memory + Search Memory + Compression + Summarization).")
