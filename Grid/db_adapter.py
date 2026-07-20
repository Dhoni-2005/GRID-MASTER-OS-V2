"""
grid/db_adapter.py — Grid Master OS Phase 7
Database backend abstraction and Phase 7 schema migrations.
Supports SQLite (development) and PostgreSQL (production).
All Phase 7 schema changes are applied here, including RC-7 index
and RC-8 grid_dispatch_log table — both created in Step 1.
"""
import logging
import os
import sqlite3
import threading
from typing import Any

from grid.config import DATABASE_BACKEND, POSTGRES_URL

log = logging.getLogger("gridmaster.grid.db_adapter")

_local = threading.local()
_postgres_available: bool = False

try:
    import psycopg2
    import psycopg2.extras
    _postgres_available = True
except ImportError:
    pass


# ── CONNECTION ────────────────────────────────────────────────

def get_connection():
    """
    Return a thread-local database connection.
    Uses SQLite by default; PostgreSQL when DATABASE_BACKEND='postgresql'.
    The connection lifecycle is managed by the caller or by database.py's
    existing close_db() function for SQLite.
    """
    if DATABASE_BACKEND == "postgresql":
        return _get_postgres_connection()
    return _get_sqlite_connection()


def _get_sqlite_connection():
    """Return or create a thread-local SQLite connection (reuses database.py's DB file)."""
    # Reuse the same DB file as database.py by reading its env var
    db_path = os.environ.get("GRIDMASTER_DB", "gridmaster.db")
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(db_path, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


def _get_postgres_connection():
    """Return or create a thread-local PostgreSQL connection."""
    if not _postgres_available:
        raise RuntimeError(
            "psycopg2 is not installed. "
            "Install it with: pip install psycopg2-binary"
        )
    if not POSTGRES_URL:
        raise RuntimeError(
            "DATABASE_BACKEND=postgresql but GRIDMASTER_POSTGRES_URL is not set."
        )
    if not hasattr(_local, "pg_conn") or _local.pg_conn is None or _local.pg_conn.closed:
        _local.pg_conn = psycopg2.connect(POSTGRES_URL)
        _local.pg_conn.autocommit = False
    return _local.pg_conn


def is_postgres() -> bool:
    """Return True when the PostgreSQL backend is configured and available."""
    return DATABASE_BACKEND == "postgresql" and _postgres_available


def close_connection() -> None:
    """Close the thread-local connection if open. Safe to call multiple times."""
    if DATABASE_BACKEND == "postgresql":
        conn = getattr(_local, "pg_conn", None)
        if conn and not conn.closed:
            conn.close()
        _local.pg_conn = None
    else:
        conn = getattr(_local, "conn", None)
        if conn:
            conn.close()
        _local.conn = None


# ── SCHEMA MIGRATIONS ─────────────────────────────────────────

def init_schema() -> None:
    """
    Apply all Phase 7 schema migrations in order.
    Safe to call on every master startup — all operations are idempotent.
    Migrations run before any Flask route is registered.

    Applies:
      1. ALTER TABLE tasks ADD COLUMN assigned_node_id
      2. ALTER TABLE tasks ADD COLUMN dispatched_at
      3. CREATE TABLE grid_dispatch_log          [RC-8]
      4. CREATE INDEX idx_tasks_dispatched       [RC-7]
    """
    conn = get_connection()

    # Migration 1 — assigned_node_id
    _add_column_if_absent(conn, "tasks", "assigned_node_id", "TEXT DEFAULT NULL")

    # Migration 2 — dispatched_at
    _add_column_if_absent(conn, "tasks", "dispatched_at", "TEXT DEFAULT NULL")

    # Migration 3 — grid_dispatch_log [RC-8]
    _create_dispatch_log_table(conn)

    # Migration 4 — idx_tasks_dispatched [RC-7]
    _create_dispatch_index(conn)

    try:
        conn.commit()
    except Exception:
        pass   # SQLite in WAL mode; autocommit may apply

    log.info("Phase 7 schema migrations applied successfully")


def _add_column_if_absent(conn, table: str, column: str, definition: str) -> None:
    """Add a column to a table if it does not already exist. Idempotent."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        log.info("Schema: added column %s.%s", table, column)
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate column" in msg or "already exists" in msg:
            log.debug("Schema: column %s.%s already exists — skipping", table, column)
        else:
            log.error("Schema: unexpected error adding column %s.%s: %s", table, column, exc)
            raise


def _create_dispatch_log_table(conn) -> None:
    """
    Create the grid_dispatch_log table if it does not exist. [RC-8]
    Records every task dispatch event for reassignment tracking and metrics.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS grid_dispatch_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id         INTEGER NOT NULL,
        node_id         TEXT    NOT NULL,
        dispatched_at   TEXT    NOT NULL,
        result_received TEXT,
        outcome         TEXT,
        FOREIGN KEY(task_id) REFERENCES tasks(id)
    )
    """
    conn.execute(sql)
    log.debug("Schema: grid_dispatch_log table ensured")


def _create_dispatch_index(conn) -> None:
    """
    Create partial index on tasks(status, dispatched_at) for timeout detection. [RC-7]
    SQLite supports WHERE clause on CREATE INDEX from v3.8.9 (2015).
    Partial index covers only dispatched_to_node rows — zero overhead on other statuses.
    """
    sql = """
    CREATE INDEX IF NOT EXISTS idx_tasks_dispatched
        ON tasks(status, dispatched_at)
        WHERE status = 'dispatched_to_node'
    """
    try:
        conn.execute(sql)
        log.debug("Schema: idx_tasks_dispatched index ensured")
    except Exception as exc:
        # Older SQLite may not support WHERE on CREATE INDEX
        if "near" in str(exc).lower() or "syntax" in str(exc).lower():
            log.warning(
                "Schema: partial index not supported on this SQLite version. "
                "Falling back to full index on (status, dispatched_at)."
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_dispatched "
                "ON tasks(status, dispatched_at)"
            )
        else:
            raise


# ── QUERY HELPERS ─────────────────────────────────────────────

def execute(sql: str, params: tuple = ()) -> None:
    """Execute a write statement on the grid adapter connection."""
    conn = get_connection()
    with conn:
        conn.execute(sql, params)


def query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """Execute a read statement and return list of row dicts."""
    conn = get_connection()
    cursor = conn.execute(sql, params)
    columns = [d[0] for d in cursor.description] if cursor.description else []
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    """Execute a read statement and return the first row dict or None."""
    rows = query(sql, params)
    return rows[0] if rows else None
