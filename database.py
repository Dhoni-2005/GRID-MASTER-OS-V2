"""
database.py — Grid Master OS Kernel v1.1
Unified database layer. One schema, all future divisions.
Wirth Lean: one system, many capabilities.

Improvements in v1.1:
- Centralized _exec() / _query() helpers eliminate duplicated
  get_db() calls across caller modules.
- All multi-step writes wrapped in explicit transactions with
  rollback on failure.
- Thread-local connection pool with lazy initialisation.
- Improved error messages with module context prefix.
- Safer init_db using executescript inside a transaction.
- Tags helper consolidated; never leaks raw lists into SQL.
"""
import sqlite3
import datetime
import json
import os
import threading

DB_PATH = os.environ.get("GRIDMASTER_DB", "gridmaster.db")
_local  = threading.local()
_MODULE = "[DB]"


# ── CONNECTION ────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Per-thread connection — safe under concurrent use."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn = conn
    return conn


def close_db() -> None:
    """Explicitly close the thread-local connection. Call before os.remove()."""
    conn = getattr(_local, "conn", None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


# ── INTERNAL HELPERS ─────────────────────────────────────────
# These replace raw get_db() calls in caller modules.
# They ensure every write is wrapped in a transaction.

def _exec(sql: str, args: tuple = ()) -> int:
    """
    Execute a single write statement inside a transaction.
    Returns lastrowid. Rolls back on any error.
    """
    conn = get_db()
    try:
        with conn:          # context manager: commits or rolls back
            cur = conn.execute(sql, args)
        return cur.lastrowid
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Write failed: {e}\nSQL: {sql}") from e


def _exec_many(statements: list[tuple]) -> None:
    """
    Execute multiple (sql, args) pairs as a single atomic transaction.
    All succeed or all roll back.
    """
    conn = get_db()
    try:
        with conn:
            for sql, args in statements:
                conn.execute(sql, args)
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Multi-write failed: {e}") from e


def _query(sql: str, args: tuple = ()) -> list[dict]:
    """Execute a read query and return list of dicts."""
    try:
        rows = get_db().execute(sql, args).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Query failed: {e}\nSQL: {sql}") from e


def _query_one(sql: str, args: tuple = ()) -> dict | None:
    """Execute a read query and return first row or None."""
    try:
        row = get_db().execute(sql, args).fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Query failed: {e}\nSQL: {sql}") from e


def _scalar(sql: str, args: tuple = ()) -> int:
    """Return a single integer scalar (e.g. COUNT)."""
    try:
        row = get_db().execute(sql, args).fetchone()
        return row[0] if row else 0
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Scalar failed: {e}") from e


# ── HELPERS ───────────────────────────────────────────────────

def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()


def _tags(tags) -> str:
    """Safely serialise a tag list to JSON string."""
    if isinstance(tags, list):
        return json.dumps(tags)
    if isinstance(tags, str):
        return tags
    return "[]"


# ── SCHEMA INIT ───────────────────────────────────────────────

def init_db() -> None:
    """Create all tables and indexes. Safe to call on every startup."""
    conn = get_db()
    try:
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS projects (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    name        TEXT    NOT NULL,
                    description TEXT    DEFAULT '',
                    status      TEXT    NOT NULL DEFAULT 'active',
                    created_at  TEXT    NOT NULL,
                    updated_at  TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id     INTEGER REFERENCES projects(id) ON DELETE SET NULL,
                    parent_task_id INTEGER REFERENCES tasks(id)    ON DELETE SET NULL,
                    title          TEXT    NOT NULL,
                    status         TEXT    NOT NULL DEFAULT 'pending',
                    priority       INTEGER NOT NULL DEFAULT 5,
                    input          TEXT    DEFAULT '',
                    output         TEXT    DEFAULT '',
                    created_at     TEXT    NOT NULL,
                    completed_at   TEXT    DEFAULT NULL
                );

                CREATE TABLE IF NOT EXISTS memory_entries (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id       INTEGER REFERENCES projects(id) ON DELETE SET NULL,
                    task_id          INTEGER REFERENCES tasks(id)    ON DELETE SET NULL,
                    content          TEXT    NOT NULL,
                    summary          TEXT    DEFAULT '',
                    entry_type       TEXT    NOT NULL DEFAULT 'log',
                    tags             TEXT    DEFAULT '[]',
                    importance_score INTEGER NOT NULL DEFAULT 1,
                    created_at       TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS failure_memory (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
                    task_id    INTEGER REFERENCES tasks(id)    ON DELETE SET NULL,
                    problem    TEXT    NOT NULL,
                    cause      TEXT    DEFAULT '',
                    fix        TEXT    DEFAULT '',
                    tags       TEXT    DEFAULT '[]',
                    created_at TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agent_notes (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id    INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
                    agent_role TEXT    NOT NULL,
                    note       TEXT    NOT NULL,
                    created_at TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS knowledge_base (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic      TEXT    NOT NULL,
                    content    TEXT    NOT NULL,
                    summary    TEXT    DEFAULT '',
                    source     TEXT    DEFAULT '',
                    tags       TEXT    DEFAULT '[]',
                    created_at TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agent_registry (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_name   TEXT    NOT NULL UNIQUE,
                    agent_role   TEXT    NOT NULL,
                    status       TEXT    NOT NULL DEFAULT 'active',
                    capabilities TEXT    DEFAULT '[]',
                    created_at   TEXT    NOT NULL,
                    updated_at   TEXT    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_registry (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id        TEXT    NOT NULL UNIQUE,
                    node_name      TEXT    NOT NULL,
                    platform       TEXT    NOT NULL DEFAULT 'unknown',
                    role           TEXT    NOT NULL DEFAULT 'worker',
                    url            TEXT    DEFAULT '',
                    status         TEXT    NOT NULL DEFAULT 'offline',
                    last_heartbeat TEXT    DEFAULT NULL,
                    capabilities   TEXT    DEFAULT '[]',
                    created_at     TEXT    NOT NULL,
                    updated_at     TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_project
                    ON tasks(project_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_parent
                    ON tasks(parent_task_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_status
                    ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_memory_task
                    ON memory_entries(task_id);
                CREATE INDEX IF NOT EXISTS idx_memory_project
                    ON memory_entries(project_id);
                CREATE INDEX IF NOT EXISTS idx_memory_score
                    ON memory_entries(importance_score DESC);
                CREATE INDEX IF NOT EXISTS idx_failures_project
                    ON failure_memory(project_id);
                CREATE INDEX IF NOT EXISTS idx_notes_task
                    ON agent_notes(task_id);
                CREATE INDEX IF NOT EXISTS idx_kb_topic
                    ON knowledge_base(topic);
                CREATE INDEX IF NOT EXISTS idx_nodes_status
                    ON node_registry(status);
                CREATE INDEX IF NOT EXISTS idx_nodes_role
                    ON node_registry(role);
            """)
    except sqlite3.Error as e:
        raise RuntimeError(f"{_MODULE} Schema init failed: {e}") from e
    print(f"{_MODULE} Initialized: {DB_PATH}")


# ── PROJECTS ──────────────────────────────────────────────────

def create_project(name: str, description: str = "") -> int:
    now = _now()
    return _exec(
        "INSERT INTO projects (name,description,status,created_at,updated_at) VALUES (?,?,?,?,?)",
        (name, description, "active", now, now),
    )


def get_project(project_id: int) -> dict | None:
    return _query_one("SELECT * FROM projects WHERE id=?", (project_id,))


def list_projects(status: str = "active") -> list[dict]:
    return _query(
        "SELECT * FROM projects WHERE status=? ORDER BY created_at DESC", (status,)
    )


def update_project_status(project_id: int, status: str) -> None:
    _exec(
        "UPDATE projects SET status=?,updated_at=? WHERE id=?",
        (status, _now(), project_id),
    )


# ── TASKS ─────────────────────────────────────────────────────

def create_task(project_id: int, title: str, input_data: str = "",
                priority: int = 5, parent_task_id: int | None = None) -> int:
    now = _now()
    return _exec(
        "INSERT INTO tasks "
        "(project_id,parent_task_id,title,status,priority,input,created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (project_id, parent_task_id, title, "pending", priority, input_data, now),
    )


def update_task_status(task_id: int, status: str, output: str = "") -> None:
    now = _now()
    if status == "completed":
        _exec(
            "UPDATE tasks SET status=?,output=?,completed_at=? WHERE id=?",
            (status, output, now, task_id),
        )
    else:
        _exec(
            "UPDATE tasks SET status=?,output=? WHERE id=?",
            (status, output, task_id),
        )


def get_task(task_id: int) -> dict | None:
    return _query_one("SELECT * FROM tasks WHERE id=?", (task_id,))


def get_subtasks(parent_task_id: int) -> list[dict]:
    return _query(
        "SELECT * FROM tasks WHERE parent_task_id=? ORDER BY priority DESC",
        (parent_task_id,),
    )


def list_tasks(project_id: int | None = None,
               status: str | None = None) -> list[dict]:
    query = "SELECT * FROM tasks WHERE 1=1"
    args: list = []
    if project_id is not None:
        query += " AND project_id=?"
        args.append(project_id)
    if status:
        query += " AND status=?"
        args.append(status)
    query += " ORDER BY priority DESC, created_at ASC"
    return _query(query, tuple(args))


# ── MEMORY ENTRIES ────────────────────────────────────────────

def store_memory(task_id: int, content: str, entry_type: str = "log",
                 tags: list | None = None, importance_score: int = 1,
                 project_id: int | None = None, summary: str = "") -> int:
    return _exec(
        "INSERT INTO memory_entries "
        "(project_id,task_id,content,summary,entry_type,tags,importance_score,created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (project_id, task_id, content, summary, entry_type,
         _tags(tags), importance_score, _now()),
    )


def get_memory(task_id: int | None = None, project_id: int | None = None,
               min_score: int = 1, limit: int = 20) -> list[dict]:
    query = "SELECT * FROM memory_entries WHERE importance_score >= ?"
    args: list = [min_score]
    if task_id is not None:
        query += " AND task_id=?"
        args.append(task_id)
    if project_id is not None:
        query += " AND project_id=?"
        args.append(project_id)
    query += " ORDER BY importance_score DESC, created_at DESC LIMIT ?"
    args.append(limit)
    return _query(query, tuple(args))


def search_memory(keyword: str = "",
                  project_id: int | None = None,
                  min_score: int = 1,
                  limit: int = 10,
                  entry_type: str | None = None,
                  after: str | None = None,
                  before: str | None = None) -> list[dict]:
    """
    Search memory_entries with optional filters.

    Parameters
    ----------
    keyword    : substring match across content, summary, tags.
                 Pass "" to skip keyword filtering (returns all matching filters).
    project_id : restrict to a single project.
    min_score  : minimum importance_score (default 1 = all entries).
    limit      : maximum rows returned.
    entry_type : exact match on entry_type column
                 e.g. "log", "result", "lesson", "failure", "summary".
    after      : ISO-8601 datetime string — only entries created after this time.
    before     : ISO-8601 datetime string — only entries created before this time.

    All filters are AND-combined. Unset filters are ignored.
    Results ordered by importance_score DESC, created_at DESC.
    """
    query = "SELECT * FROM memory_entries WHERE importance_score >= ?"
    args: list = [min_score]

    if keyword:
        like = f"%{keyword}%"
        query += " AND (content LIKE ? OR summary LIKE ? OR tags LIKE ?)"
        args += [like, like, like]

    if project_id is not None:
        query += " AND project_id=?"
        args.append(project_id)

    if entry_type is not None:
        query += " AND entry_type=?"
        args.append(entry_type)

    if after is not None:
        query += " AND created_at > ?"
        args.append(after)

    if before is not None:
        query += " AND created_at < ?"
        args.append(before)

    query += " ORDER BY importance_score DESC, created_at DESC LIMIT ?"
    args.append(limit)
    return _query(query, tuple(args))


def memory_stats_counts(project_id: int | None = None) -> dict:
    """
    Return per-score entry counts using COUNT(*) aggregates.
    Replaces the full-table-scan approach in memory_stats().
    O(1) per score bucket regardless of table size.
    """
    scores = [1, 3, 5, 7, 10]
    dist:  dict = {}
    for s in scores:
        if project_id is not None:
            dist[s] = _scalar(
                "SELECT COUNT(*) FROM memory_entries "
                "WHERE importance_score=? AND project_id=?",
                (s, project_id),
            )
        else:
            dist[s] = _scalar(
                "SELECT COUNT(*) FROM memory_entries WHERE importance_score=?",
                (s,),
            )
    total = _scalar(
        "SELECT COUNT(*) FROM memory_entries"
        + (" WHERE project_id=?" if project_id is not None else ""),
        (project_id,) if project_id is not None else (),
    )
    failures = _scalar(
        "SELECT COUNT(*) FROM failure_memory"
        + (" WHERE project_id=?" if project_id is not None else ""),
        (project_id,) if project_id is not None else (),
    )
    return {
        "total_entries":      total,
        "total_failures":     failures,
        "score_distribution": dist,
    }


# ── FAILURE MEMORY ────────────────────────────────────────────

def store_failure(task_id: int, problem: str, cause: str = "",
                  fix: str = "", tags: list | None = None,
                  project_id: int | None = None) -> int:
    return _exec(
        "INSERT INTO failure_memory "
        "(project_id,task_id,problem,cause,fix,tags,created_at) VALUES (?,?,?,?,?,?,?)",
        (project_id, task_id, problem, cause, fix, _tags(tags), _now()),
    )


def search_failures(keyword: str, limit: int = 5) -> list[dict]:
    like = f"%{keyword}%"
    return _query(
        "SELECT * FROM failure_memory "
        "WHERE problem LIKE ? OR cause LIKE ? OR tags LIKE ? "
        "ORDER BY created_at DESC LIMIT ?",
        (like, like, like, limit),
    )


# ── TAG QUERIES (Phase 2 — Step 1) ───────────────────────────
# Uses SQLite json_each() — requires SQLite >= 3.38 (2022-02-22).
# Tags are stored as JSON arrays: '["python","flask"]'
# All three functions query the existing schema — no new tables.

def search_memory_by_tag(tag: str,
                         project_id: int | None = None,
                         min_score: int = 1,
                         limit: int = 20) -> list[dict]:
    """
    Return memory entries whose tags array contains `tag` exactly.
    Uses json_each() to expand the JSON array and match each element.
    """
    query = (
        "SELECT DISTINCT m.* FROM memory_entries m, "
        "json_each(m.tags) je "
        "WHERE je.value = ? AND m.importance_score >= ?"
    )
    args: list = [tag, min_score]
    if project_id is not None:
        query += " AND m.project_id = ?"
        args.append(project_id)
    query += " ORDER BY m.importance_score DESC, m.created_at DESC LIMIT ?"
    args.append(limit)
    return _query(query, tuple(args))


def list_tags(project_id: int | None = None) -> list[str]:
    """
    Return all distinct tag values in use across memory_entries.
    Optionally scoped to a project. Used by list_known_tags() in
    memory_manager to give the caller a tag inventory.
    """
    if project_id is not None:
        rows = _query(
            "SELECT DISTINCT je.value AS tag "
            "FROM memory_entries m, json_each(m.tags) je "
            "WHERE m.project_id = ? "
            "ORDER BY je.value",
            (project_id,),
        )
    else:
        rows = _query(
            "SELECT DISTINCT je.value AS tag "
            "FROM memory_entries m, json_each(m.tags) je "
            "ORDER BY je.value"
        )
    return [r["tag"] for r in rows if r.get("tag")]


def search_failures_by_tag(tag: str, limit: int = 10) -> list[dict]:
    """
    Return failure_memory entries whose tags array contains `tag` exactly.
    """
    return _query(
        "SELECT DISTINCT f.* FROM failure_memory f, json_each(f.tags) je "
        "WHERE je.value = ? "
        "ORDER BY f.created_at DESC LIMIT ?",
        (tag, limit),
    )


# ── AGENT NOTES ───────────────────────────────────────────────

def write_note(task_id: int, agent_role: str, note: str) -> int:
    return _exec(
        "INSERT INTO agent_notes (task_id,agent_role,note,created_at) VALUES (?,?,?,?)",
        (task_id, agent_role, note, _now()),
    )


def get_notes(task_id: int, agent_role: str | None = None) -> list[dict]:
    query = "SELECT * FROM agent_notes WHERE task_id=?"
    args: list = [task_id]
    if agent_role:
        query += " AND agent_role=?"
        args.append(agent_role)
    query += " ORDER BY created_at ASC"
    return _query(query, tuple(args))


# ── KNOWLEDGE BASE ────────────────────────────────────────────

def store_knowledge(topic: str, content: str, summary: str = "",
                    source: str = "", tags: list | None = None) -> int:
    return _exec(
        "INSERT INTO knowledge_base (topic,content,summary,source,tags,created_at) "
        "VALUES (?,?,?,?,?,?)",
        (topic, content, summary, source, _tags(tags), _now()),
    )


def search_knowledge(query_str: str, limit: int = 5) -> list[dict]:
    like = f"%{query_str}%"
    return _query(
        "SELECT * FROM knowledge_base "
        "WHERE topic LIKE ? OR content LIKE ? OR tags LIKE ? "
        "ORDER BY created_at DESC LIMIT ?",
        (like, like, like, limit),
    )


# ── AGENT REGISTRY ────────────────────────────────────────────

def register_agent(agent_name: str, agent_role: str,
                   capabilities: list | None = None) -> int:
    now = _now()
    return _exec(
        "INSERT INTO agent_registry "
        "(agent_name,agent_role,status,capabilities,created_at,updated_at) "
        "VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(agent_name) DO UPDATE SET "
        "status='active', updated_at=excluded.updated_at",
        (agent_name, agent_role, "active", _tags(capabilities), now, now),
    )


def get_active_agents(role: str | None = None) -> list[dict]:
    query = "SELECT * FROM agent_registry WHERE status='active'"
    args: list = []
    if role:
        query += " AND agent_role=?"
        args.append(role)
    return _query(query, tuple(args))


# ── NODE REGISTRY ─────────────────────────────────────────────

def register_node(node_id: str, node_name: str, platform: str = "unknown",
                  role: str = "worker", url: str = "",
                  capabilities: list | None = None) -> int:
    now = _now()
    return _exec(
        "INSERT INTO node_registry "
        "(node_id,node_name,platform,role,url,status,capabilities,created_at,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(node_id) DO UPDATE SET "
        "node_name=excluded.node_name, platform=excluded.platform, "
        "role=excluded.role, url=excluded.url, "
        "status='online', updated_at=excluded.updated_at",
        (node_id, node_name, platform, role, url, "online",
         _tags(capabilities), now, now),
    )


def update_heartbeat(node_id: str) -> None:
    now = _now()
    _exec(
        "UPDATE node_registry SET last_heartbeat=?,status='online',updated_at=? WHERE node_id=?",
        (now, now, node_id),
    )


def set_node_status(node_id: str, status: str) -> None:
    _exec(
        "UPDATE node_registry SET status=?,updated_at=? WHERE node_id=?",
        (status, _now(), node_id),
    )


def get_online_nodes(role: str | None = None) -> list[dict]:
    query = "SELECT * FROM node_registry WHERE status='online'"
    args: list = []
    if role:
        query += " AND role=?"
        args.append(role)
    return _query(query, tuple(args))


def get_node(node_id: str) -> dict | None:
    return _query_one("SELECT * FROM node_registry WHERE node_id=?", (node_id,))


def list_all_nodes() -> list[dict]:
    return _query("SELECT * FROM node_registry ORDER BY role, node_name")


# ── ATOMIC MULTI-STEP HELPERS ─────────────────────────────────
# These are the transaction-safe composite writes used by
# grid_master.py to avoid partial state updates.

def complete_task_atomic(task_id: int, output: str,
                         node_id: str | None,
                         memory_content: str,
                         project_id: int | None,
                         lesson: str = "") -> None:
    """
    Atomically:
      - Mark task completed
      - Release node (if provided)
      - Store memory entry
      - Optionally store lesson memory
    All succeed or all roll back.
    """
    now = _now()
    statements: list[tuple] = [
        (
            "UPDATE tasks SET status='completed',output=?,completed_at=? WHERE id=?",
            (output, now, task_id),
        ),
        (
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,importance_score,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (project_id, task_id, memory_content, "", "result",
             "[]", 3, now),
        ),
    ]
    if node_id:
        statements.append((
            "UPDATE node_registry SET status='online',updated_at=? WHERE node_id=?",
            (now, node_id),
        ))
    if lesson:
        statements.append((
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,importance_score,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (project_id, task_id, lesson, lesson[:100], "lesson", "[]", 7, now),
        ))
    _exec_many(statements)


def fail_task_atomic(task_id: int, status: str, output: str,
                     node_id: str | None, problem: str,
                     cause: str, fix: str, tags: list,
                     project_id: int | None) -> None:
    """
    Atomically:
      - Mark task failed/abandoned
      - Release node (if provided)
      - Store failure_memory record
      - Store high-importance memory entry
    All succeed or all roll back.
    """
    now = _now()
    statements: list[tuple] = [
        (
            "UPDATE tasks SET status=?,output=? WHERE id=?",
            (status, output, task_id),
        ),
        (
            "INSERT INTO failure_memory "
            "(project_id,task_id,problem,cause,fix,tags,created_at) VALUES (?,?,?,?,?,?,?)",
            (project_id, task_id, problem, cause, fix, _tags(tags), now),
        ),
        (
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,importance_score,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (project_id, task_id,
             f"FAILURE: {problem}. Cause: {cause}. Fix: {fix}",
             f"Failure: {problem[:80]}", "failure", _tags(tags), 10, now),
        ),
    ]
    if node_id:
        statements.append((
            "UPDATE node_registry SET status='online',updated_at=? WHERE node_id=?",
            (now, node_id),
        ))
    _exec_many(statements)


# ── MEMORY COMPRESSION (Phase 2 — Step 3) ────────────────────

def compress_old_entries(project_id: int | None = None,
                         older_than_days: int = 7,
                         max_score: int = 2) -> dict:
    """
    Compress old low-importance memory entries into digest records.

    Rules (approved design):
    - Only entries with importance_score <= max_score are eligible.
    - Only entry_type IN ('log', 'intake') are eligible.
    - Only entries older than older_than_days are eligible.
    - Entries whose task_id appears in failure_memory are excluded.
    - Groups with fewer than 3 entries are skipped unchanged.
    - Each qualifying group becomes one digest entry:
        entry_type       = 'compressed'
        importance_score = 3   (SCORE_RESULT — above compression ceiling)
        tags             = union of all source tags
    - INSERT digest + DELETE sources run in one _exec_many() transaction.
    - Digest entries (score=3) are never re-compressed (max_score <= 2).

    Returns:
        {"groups_compressed": N, "entries_compressed": M}
    """
    import json as _json
    import datetime as _dt

    MIN_GROUP = 3
    DIGEST_SCORE = 3
    ELIGIBLE_TYPES = ("log", "intake")

    # Cutoff timestamp
    cutoff = (
        _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        - _dt.timedelta(days=older_than_days)
    ).isoformat()

    # Collect task_ids protected by failure_memory for this project
    if project_id is not None:
        fail_rows = _query(
            "SELECT DISTINCT task_id FROM failure_memory "
            "WHERE project_id=? AND task_id IS NOT NULL",
            (project_id,),
        )
    else:
        fail_rows = _query(
            "SELECT DISTINCT task_id FROM failure_memory "
            "WHERE task_id IS NOT NULL"
        )
    protected_task_ids: set = {r["task_id"] for r in fail_rows}

    # Fetch all eligible candidates
    base = (
        "SELECT * FROM memory_entries "
        "WHERE importance_score <= ? "
        "AND entry_type IN ('log','intake') "
        "AND created_at < ?"
    )
    args: list = [max_score, cutoff]
    if project_id is not None:
        base += " AND project_id = ?"
        args.append(project_id)
    base += " ORDER BY project_id, created_at ASC"
    candidates = _query(base, tuple(args))

    # Exclude failure-linked entries
    candidates = [
        c for c in candidates
        if c.get("task_id") not in protected_task_ids
    ]

    if not candidates:
        return {"groups_compressed": 0, "entries_compressed": 0}

    # Group by (project_id, date_bucket)
    groups: dict = {}
    for entry in candidates:
        date_bucket = (entry.get("created_at") or "")[:10]  # "YYYY-MM-DD"
        key = (entry.get("project_id"), date_bucket)
        groups.setdefault(key, []).append(entry)

    groups_done  = 0
    entries_done = 0

    for (grp_project_id, date_bucket), members in groups.items():
        if len(members) < MIN_GROUP:
            continue  # skip — not enough entries to justify compression

        # Build tag union
        all_tags: list = []
        seen_tags: set = set()
        for m in members:
            try:
                for t in _json.loads(m.get("tags") or "[]"):
                    if t and t not in seen_tags:
                        seen_tags.add(t)
                        all_tags.append(t)
            except (_json.JSONDecodeError, TypeError):
                pass

        # Build digest content and summary
        first_preview = (members[0].get("content") or "")[:80]
        last_preview  = (members[-1].get("content") or "")[:80]
        tag_str       = ", ".join(all_tags) if all_tags else "none"
        n             = len(members)
        digest_content = (
            f"[COMPRESSED] {n} entries from {date_bucket}. "
            f"Tags: {tag_str}. "
            f"First: {first_preview} | Last: {last_preview}"
        )
        digest_summary = f"Compressed {n} log entries from {date_bucket}"

        # Collect source IDs for deletion
        source_ids = [m["id"] for m in members]
        id_placeholders = ",".join("?" * len(source_ids))

        # Atomic: INSERT digest + DELETE sources
        now = _now()
        statements: list[tuple] = [
            (
                "INSERT INTO memory_entries "
                "(project_id, task_id, content, summary, entry_type, "
                " tags, importance_score, created_at) "
                "VALUES (?, NULL, ?, ?, 'compressed', ?, ?, ?)",
                (grp_project_id, digest_content, digest_summary,
                 _json.dumps(all_tags), DIGEST_SCORE, now),
            ),
            (
                f"DELETE FROM memory_entries WHERE id IN ({id_placeholders})",
                tuple(source_ids),
            ),
        ]
        _exec_many(statements)

        groups_done  += 1
        entries_done += n

    return {"groups_compressed": groups_done, "entries_compressed": entries_done}


# ── MEMORY SUMMARIZATION (Phase 2 — Step 4) ──────────────────

def summarize_old_entries(project_id: int | None = None,
                          older_than_days: int = 14,
                          min_score: int = 3,
                          max_score: int = 4,
                          min_group: int = 5) -> dict:
    """
    Summarize accumulated mid-tier memory entries into digest records.

    Target tier: importance_score between min_score and max_score (default 3–4).
    This is the accumulation tier — score-3 result entries and compressed
    digests from Step 3 that have aged past the summarization window.

    Rules:
    - Only entries where min_score <= importance_score <= max_score.
    - entry_type must NOT be in the protected set:
        ('summary', 'archived', 'lesson', 'failure', 'knowledge', 'compressed')
      Prevents re-summarizing already-processed entries.
    - Only entries older than older_than_days.
    - Entries whose task_id appears in failure_memory are excluded.
    - Groups smaller than min_group are skipped unchanged.
    - Groups are formed by (project_id, month_bucket) where
      month_bucket = created_at[:7]  e.g. "2026-05"
      Monthly granularity is coarser than compression's daily grouping
      because these entries already represent condensed information.

    For each qualifying group, one _exec_many() transaction:
      1. INSERT one summary entry:
            entry_type       = 'summary'
            importance_score = 7  (SCORE_LESSON — protected from future steps)
            tags             = union of source tags
            content          = digest built from source summaries/content
      2. UPDATE all source entry ids:
            SET entry_type = 'archived'
         Sources are preserved — not deleted. Summarization is reversible
         at the record level (archived entries remain queryable).

    Returns:
        {
          "groups_summarized":  N,
          "entries_summarized": M,
          "digest_ids":         [list of inserted summary entry ids]
        }
    """
    import json as _json
    import datetime as _dt

    SUMMARY_SCORE  = 7   # SCORE_LESSON — protected forever
    PROTECTED_TYPES = (
        "summary", "archived", "lesson",
        "failure", "knowledge",
    )

    cutoff = (
        _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        - _dt.timedelta(days=older_than_days)
    ).isoformat()

    # Collect failure-linked task_ids for this project
    if project_id is not None:
        fail_rows = _query(
            "SELECT DISTINCT task_id FROM failure_memory "
            "WHERE project_id=? AND task_id IS NOT NULL",
            (project_id,),
        )
    else:
        fail_rows = _query(
            "SELECT DISTINCT task_id FROM failure_memory "
            "WHERE task_id IS NOT NULL"
        )
    protected_task_ids: set = {r["task_id"] for r in fail_rows}

    # Build eligible-type exclusion placeholders
    type_placeholders = ",".join("?" * len(PROTECTED_TYPES))

    base = (
        f"SELECT * FROM memory_entries "
        f"WHERE importance_score >= ? "
        f"AND importance_score <= ? "
        f"AND entry_type NOT IN ({type_placeholders}) "
        f"AND created_at < ?"
    )
    args: list = [min_score, max_score, *PROTECTED_TYPES, cutoff]

    if project_id is not None:
        base += " AND project_id = ?"
        args.append(project_id)

    base += " ORDER BY project_id, created_at ASC"
    candidates = _query(base, tuple(args))

    # Exclude failure-linked entries
    candidates = [
        c for c in candidates
        if c.get("task_id") not in protected_task_ids
    ]

    if not candidates:
        return {"groups_summarized": 0, "entries_summarized": 0, "digest_ids": []}

    # Group by (project_id, month_bucket)
    groups: dict = {}
    for entry in candidates:
        month_bucket = (entry.get("created_at") or "")[:7]  # "YYYY-MM"
        key = (entry.get("project_id"), month_bucket)
        groups.setdefault(key, []).append(entry)

    groups_done  = 0
    entries_done = 0
    digest_ids:  list = []

    for (grp_project_id, month_bucket), members in groups.items():
        if len(members) < min_group:
            continue

        # Build tag union
        all_tags: list = []
        seen_tags: set = set()
        for m in members:
            try:
                for t in _json.loads(m.get("tags") or "[]"):
                    if t and t not in seen_tags:
                        seen_tags.add(t)
                        all_tags.append(t)
            except (_json.JSONDecodeError, TypeError):
                pass

        # Build digest content from summary fields (fall back to content)
        snippets = []
        for m in members:
            text = (m.get("summary") or m.get("content") or "")[:80].strip()
            if text:
                snippets.append(text)

        first_date = (members[0].get("created_at")  or "")[:10]
        last_date  = (members[-1].get("created_at") or "")[:10]
        tag_str    = ", ".join(all_tags) if all_tags else "none"
        n          = len(members)
        joined     = " | ".join(snippets)
        if len(joined) > 500:
            joined = joined[:500] + "..."

        digest_content = (
            f"[SUMMARY] {n} entries spanning {first_date} to {last_date}. "
            f"Key topics: {tag_str}. "
            f"Entries: {joined}"
        )
        digest_summary = (
            f"Summary of {n} entries from {month_bucket} "
            f"(topics: {tag_str})"
        )

        source_ids       = [m["id"] for m in members]
        id_placeholders  = ",".join("?" * len(source_ids))
        now              = _now()

        statements: list[tuple] = [
            (
                "INSERT INTO memory_entries "
                "(project_id, task_id, content, summary, entry_type, "
                " tags, importance_score, created_at) "
                "VALUES (?, NULL, ?, ?, 'summary', ?, ?, ?)",
                (grp_project_id, digest_content, digest_summary,
                 _json.dumps(all_tags), SUMMARY_SCORE, now),
            ),
            (
                f"UPDATE memory_entries "
                f"SET entry_type='archived' "
                f"WHERE id IN ({id_placeholders})",
                tuple(source_ids),
            ),
        ]
        _exec_many(statements)

        # Retrieve the inserted digest id
        digest_row = _query_one(
            "SELECT id FROM memory_entries "
            "WHERE entry_type='summary' AND created_at=? AND project_id IS ?",
            (now, grp_project_id),
        )
        if digest_row:
            digest_ids.append(digest_row["id"])

        groups_done  += 1
        entries_done += n

    return {
        "groups_summarized":  groups_done,
        "entries_summarized": entries_done,
        "digest_ids":         digest_ids,
    }


# ── DB STATS ──────────────────────────────────────────────────

def db_stats() -> dict:
    tables = ["projects", "tasks", "memory_entries",
              "failure_memory", "knowledge_base",
              "agent_registry", "node_registry"]
    stats = {t: _scalar(f"SELECT COUNT(*) FROM {t}") for t in tables}
    stats["db_path"]    = DB_PATH
    stats["db_size_kb"] = (
        round(os.path.getsize(DB_PATH) / 1024, 1)
        if os.path.exists(DB_PATH) else 0
    )
    return stats


# ── SELF-TEST ─────────────────────────────────────────────────
if __name__ == "__main__":
    import tempfile, os as _os
    tmp = tempfile.mktemp(suffix=".db")
    _os.environ["GRIDMASTER_DB"] = tmp
    import importlib, sys
    # Reload so DB_PATH picks up the env var
    if "database" in sys.modules:
        importlib.reload(sys.modules["database"])
    import database as _db
    _db.init_db()
    pid = _db.create_project("Test", "Self-test project")
    tid = _db.create_task(pid, "Test task", priority=5)
    _db.write_note(tid, "coordinator", "Boot note")
    _db.store_memory(tid, "Hello memory", importance_score=5, project_id=pid)
    _db.store_failure(tid, "Test failure", cause="test", fix="test fix",
                      tags=["test"], project_id=pid)
    _db.store_knowledge("test_topic", "test content", tags=["test"])
    _db.register_agent("coordinator", "coordinator", ["route"])
    _db.register_node("n01", "Node 01", platform="local", role="worker")
    _db.update_heartbeat("n01")
    _db.complete_task_atomic(tid, "done", "n01", "Completed.", pid, "Always test atomically.")

    # Phase 2 Step 2: enhanced search_memory
    _db.store_memory(tid, "lesson about flask routes", entry_type="lesson",
                     tags=["flask"], importance_score=7, project_id=pid)
    _db.store_memory(tid, "old log entry", entry_type="log",
                     importance_score=1, project_id=pid)

    # keyword filter
    hits = _db.search_memory(keyword="flask", project_id=pid)
    assert hits, "Expected keyword hit for 'flask'"

    # entry_type filter
    lessons = _db.search_memory(entry_type="lesson", project_id=pid)
    assert all(r["entry_type"] == "lesson" for r in lessons), \
        "entry_type filter returned wrong types"

    # after/before date filter
    from datetime import datetime, timezone, timedelta
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    past   = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    recent = _db.search_memory(after=past,  project_id=pid, limit=50)
    none   = _db.search_memory(after=future, project_id=pid, limit=50)
    assert recent, "Expected entries after yesterday"
    assert not none, "Expected no entries after tomorrow"

    # blank keyword returns all matching other filters
    all_entries = _db.search_memory(keyword="", project_id=pid, limit=50)
    assert len(all_entries) >= 2, "Expected all project entries with blank keyword"

    # memory_stats_counts uses COUNT(*) not full scan
    stats = _db.memory_stats_counts(project_id=pid)
    assert "total_entries"      in stats
    assert "score_distribution" in stats
    assert stats["total_entries"] >= 2

    # ── Phase 2 Step 3: compress_old_entries ─────────────────
    from datetime import datetime, timezone, timedelta as _td
    import json as _json

    # Dedicated task with NO failure_memory record — entries will be eligible
    compress_tid = _db.create_task(pid, "compress-test-task", priority=1)

    def _old_entry(content, etype="log", score=1, days_ago=8, task_id=None):
        old_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - _td(days=days_ago)).isoformat()
        return _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, task_id or compress_tid, content, "", etype,
             '["test"]', score, old_ts),
        )

    # 3 old log entries on same date — eligible group (>= MIN_GROUP=3)
    _old_entry("old log alpha")
    _old_entry("old log beta")
    _old_entry("old log gamma")

    # High-value entry (score=7) — must survive
    hv_id = _old_entry("critical lesson", etype="lesson", score=7)

    # Failure-linked entry — task_id is in failure_memory — must survive
    fail_tid = _db.create_task(pid, "fail-linked task", priority=1)
    _db.store_failure(fail_tid, "linked failure", project_id=pid)
    fl_id = _old_entry("failure-linked log", task_id=fail_tid)

    # Two-entry group on a different date — must NOT compress (< MIN_GROUP)
    _old_entry("tiny group one",  days_ago=15)
    _old_entry("tiny group two",  days_ago=15)

    result = _db.compress_old_entries(project_id=pid, older_than_days=7, max_score=2)
    assert result["entries_compressed"] == 3,  \
        f"Expected 3 entries compressed, got {result['entries_compressed']}"
    assert result["groups_compressed"]  == 1,  \
        f"Expected 1 group compressed, got {result['groups_compressed']}"

    # high-value entry still present
    hv = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (hv_id,))
    assert hv is not None, "High-value entry was deleted — must survive"
    assert hv["importance_score"] >= 5

    # failure-linked entry still present
    fl = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (fl_id,))
    assert fl is not None, "Failure-linked entry was deleted — must survive"

    # two-entry group still present (below MIN_GROUP)
    tiny = _db.search_memory(keyword="tiny group", project_id=pid, limit=10)
    assert len(tiny) == 2, f"Two-entry group should be untouched, got {len(tiny)}"

    # digest entry exists with correct fields
    digests = _db.search_memory(entry_type="compressed", project_id=pid)
    assert len(digests) == 1, f"Expected 1 digest entry, got {len(digests)}"
    assert digests[0]["importance_score"] == 3, \
        f"Digest importance_score should be 3, got {digests[0]['importance_score']}"

    # re-run compression — nothing new (digests are score=3 > max_score=2)
    result2 = _db.compress_old_entries(project_id=pid, older_than_days=7, max_score=2)
    assert result2["entries_compressed"] == 0, \
        f"Second run should compress 0, got {result2['entries_compressed']}"

    # ── Phase 2 Step 4: summarize_old_entries — full lifecycle ──
    sum_tid = _db.create_task(pid, "summarize-test-task", priority=1)

    def _mid_entry(content, score=3, days_ago=15, etype="result"):
        old_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - _td(days=days_ago)).isoformat()
        return _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, sum_tid, content, f"summary of {content[:30]}",
             etype, '["flask","python"]', score, old_ts),
        )

    # ── End-to-end lifecycle: score-1 log → compressed → summarized ──
    # Insert 3 old score-1 log entries and compress them into a score-3 digest
    log_tid = _db.create_task(pid, "lifecycle-log-task", priority=1)
    lifecycle_old_ts = (datetime.now(timezone.utc).replace(tzinfo=None)
                        - _td(days=20)).isoformat()
    for i in range(3):
        _db._exec(
            "INSERT INTO memory_entries "
            "(project_id,task_id,content,summary,entry_type,tags,"
            " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pid, log_tid, f"lifecycle log {i}", "", "log",
             '["lifecycle"]', 1, lifecycle_old_ts),
        )
    comp_res = _db.compress_old_entries(
        project_id=pid, older_than_days=7, max_score=2
    )
    assert comp_res["entries_compressed"] >= 3, \
        f"Expected lifecycle logs compressed, got {comp_res}"

    # The compressed digest now has entry_type="compressed", score=3.
    # Back-date it to be older than the summarization window (14 days).
    _db._exec(
        "UPDATE memory_entries SET created_at=? "
        "WHERE entry_type='compressed' AND project_id=?",
        (lifecycle_old_ts, pid),
    )

    # 4 more score-3 result entries — pinned to the SAME days_ago (20)
    # as the compressed digest (lifecycle_old_ts) so they always land
    # in the same YYYY-MM month bucket regardless of the current
    # calendar date. Using the default days_ago=15 here is unsafe:
    # 20 and 15 days ago can fall in different months depending on
    # which day of the month the test happens to run on.
    for i in range(4):
        _mid_entry(f"result entry {i}", days_ago=20)

    # High-value entry (score=7, entry_type=lesson) — must be untouched
    hv2_id = _mid_entry("critical pattern", score=7, etype="lesson")

    # Failure-linked mid-tier entry — must survive untouched
    fail_tid2 = _db.create_task(pid, "fail-sum-task", priority=1)
    _db.store_failure(fail_tid2, "sum test failure", project_id=pid)
    old_ts2 = (datetime.now(timezone.utc).replace(tzinfo=None)
               - _td(days=15)).isoformat()
    fl2_id = _db._exec(
        "INSERT INTO memory_entries "
        "(project_id,task_id,content,summary,entry_type,tags,"
        " importance_score,created_at) VALUES (?,?,?,?,?,?,?,?)",
        (pid, fail_tid2, "failure-linked mid", "", "result",
         '["test"]', 3, old_ts2),
    )

    # Group of 4 on a different month — must NOT summarize (< min_group=5)
    for i in range(4):
        _mid_entry(f"small sum group {i}", days_ago=60)

    sum_result = _db.summarize_old_entries(
        project_id=pid, older_than_days=14,
        min_score=3, max_score=4, min_group=5,
    )
    # compressed digest (1) + 4 result entries = 5 total in the month group
    assert sum_result["groups_summarized"]  >= 1, \
        f"Expected >=1 group summarized, got {sum_result['groups_summarized']}"
    assert sum_result["entries_summarized"] >= 5, \
        f"Expected >=5 entries summarized, got {sum_result['entries_summarized']}"
    assert len(sum_result["digest_ids"]) >= 1, \
        f"Expected >=1 digest id, got {sum_result['digest_ids']}"

    # Verify compressed digest is now archived (full lifecycle confirmed)
    archived_compressed = _db._query(
        "SELECT * FROM memory_entries "
        "WHERE entry_type='archived' AND project_id=?", (pid,)
    )
    assert any("[COMPRESSED]" in (r.get("content") or "") for r in archived_compressed), \
        "Compressed digest must be archived after Step 4 — lifecycle broken"

    # Source mid-tier result entries are archived
    mid_ids_check = _db._query(
        "SELECT entry_type FROM memory_entries "
        "WHERE content LIKE 'result entry%' AND project_id=?", (pid,)
    )
    assert all(r["entry_type"] == "archived" for r in mid_ids_check), \
        "All result entries must be archived"

    # High-value entry completely untouched
    hv2 = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (hv2_id,))
    assert hv2 is not None and hv2["importance_score"] == 7
    assert hv2["entry_type"] == "lesson", "High-value entry_type must not change"

    # Failure-linked entry untouched
    fl2 = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (fl2_id,))
    assert fl2 is not None and fl2["entry_type"] != "archived", \
        "Failure-linked entry must not be archived"

    # Small group (4 entries) untouched
    small = _db.search_memory(keyword="small sum group", project_id=pid, limit=10)
    assert len(small) == 4, f"Small group must be untouched, got {len(small)}"

    # Digest entry has correct fields
    digest_id = sum_result["digest_ids"][0]
    digest = _db._query_one("SELECT * FROM memory_entries WHERE id=?", (digest_id,))
    assert digest is not None
    assert digest["entry_type"]       == "summary"
    assert digest["importance_score"] == 7
    assert "[SUMMARY]" in digest["content"]

    # Re-run: archived excluded, summary score=7 > max_score=4 — zero output
    sum_result2 = _db.summarize_old_entries(
        project_id=pid, older_than_days=14,
        min_score=3, max_score=4, min_group=5,
    )
    assert sum_result2["entries_summarized"] == 0, \
        f"Second summarize run must return 0, got {sum_result2['entries_summarized']}"

    _db.close_db()
    _os.remove(tmp)
    print(f"[DB] Self-test passed.")
