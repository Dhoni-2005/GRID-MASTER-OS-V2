"""
grid/outbox.py — Grid Master OS Phase 7
Persistent SQLite-backed outbound message queue for worker nodes.
Stores memory entries and result payloads locally during network partitions.
Provides crash-safe persistence, idempotent resend, and automatic cleanup.

All writes are thread-safe via a module-level lock.
The outbox database is separate from the main Grid Master database.
"""
import json
import logging
import sqlite3
import threading
import time
from typing import Any

from grid.config import OUTBOX_MAX_ENTRIES, OUTBOX_PATH

log = logging.getLogger("gridmaster.grid.outbox")

# Module-level lock for thread-safe queue access
_lock = threading.Lock()

# Thread-local storage for per-thread connections
_local = threading.local()

# Valid entry types stored in the outbox
ENTRY_TYPE_MEMORY = "memory"
ENTRY_TYPE_RESULT = "result"
VALID_ENTRY_TYPES = {ENTRY_TYPE_MEMORY, ENTRY_TYPE_RESULT}


# ── CONNECTION ────────────────────────────────────────────────

def _get_connection() -> sqlite3.Connection:
    """
    Return or create a thread-local SQLite connection to the outbox database.
    Uses WAL mode for concurrent read safety.
    """
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(OUTBOX_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


# ── SCHEMA INITIALISATION ─────────────────────────────────────

def init() -> None:
    """
    Create the outbox table if it does not exist.
    Safe to call on every worker startup — idempotent.
    """
    with _lock:
        conn = _get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_type   TEXT    NOT NULL,
                payload      TEXT    NOT NULL,
                idempotency_key TEXT,
                retry_count  INTEGER NOT NULL DEFAULT 0,
                created_at   TEXT    NOT NULL,
                last_attempt TEXT
            )
        """)
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_idempotency "
            "ON outbox(idempotency_key) WHERE idempotency_key IS NOT NULL"
        )
        conn.commit()
    log.debug("Outbox initialised at %s", OUTBOX_PATH)


# ── WRITE ─────────────────────────────────────────────────────

def enqueue(entry_type: str,
            payload: dict[str, Any],
            idempotency_key: str | None = None) -> int:
    """
    Add one entry to the outbox queue.

    Parameters
    ----------
    entry_type      : "memory" or "result"
    payload         : dict that will be JSON-serialised and stored
    idempotency_key : optional unique key; duplicate keys are silently ignored

    Returns
    -------
    Row id of the inserted entry, or -1 if duplicate was ignored.

    Raises
    ------
    ValueError  if entry_type is not in VALID_ENTRY_TYPES
    RuntimeError if queue is at capacity after eviction attempt
    """
    if entry_type not in VALID_ENTRY_TYPES:
        raise ValueError(
            f"Invalid entry_type '{entry_type}'. Must be one of {VALID_ENTRY_TYPES}"
        )

    now = _now()
    payload_str = json.dumps(payload, default=str)

    with _lock:
        conn = _get_connection()

        # Enforce capacity — evict oldest if needed
        count = _scalar(conn, "SELECT COUNT(*) FROM outbox")
        if count >= OUTBOX_MAX_ENTRIES:
            log.warning(
                "Outbox at capacity (%d entries). Evicting oldest entry.", count
            )
            conn.execute(
                "DELETE FROM outbox WHERE id = "
                "(SELECT id FROM outbox ORDER BY id ASC LIMIT 1)"
            )

        # Insert — ignore duplicate idempotency keys
        try:
            cursor = conn.execute(
                "INSERT INTO outbox "
                "(entry_type, payload, idempotency_key, retry_count, created_at) "
                "VALUES (?, ?, ?, 0, ?)",
                (entry_type, payload_str, idempotency_key, now),
            )
            conn.commit()
            row_id = cursor.lastrowid
            log.debug("Outbox enqueue: type=%s id=%d", entry_type, row_id)
            return row_id
        except sqlite3.IntegrityError:
            # Duplicate idempotency key — silent ignore
            log.debug("Outbox: duplicate idempotency_key=%s ignored", idempotency_key)
            return -1


# ── READ ──────────────────────────────────────────────────────

def dequeue_all() -> list[dict[str, Any]]:
    """
    Return all pending outbox entries in insertion order.
    Entries are NOT removed — call mark_flushed() after successful delivery.

    Returns
    -------
    List of dicts with keys: id, entry_type, payload (decoded dict),
    idempotency_key, retry_count, created_at, last_attempt.
    """
    with _lock:
        conn = _get_connection()
        cursor = conn.execute(
            "SELECT id, entry_type, payload, idempotency_key, "
            "retry_count, created_at, last_attempt "
            "FROM outbox ORDER BY id ASC"
        )
        rows = cursor.fetchall()

    result = []
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except (json.JSONDecodeError, TypeError):
            log.warning("Outbox: corrupt payload for entry id=%s — skipping", row["id"])
            continue
        result.append({
            "id":              row["id"],
            "entry_type":      row["entry_type"],
            "payload":         payload,
            "idempotency_key": row["idempotency_key"],
            "retry_count":     row["retry_count"],
            "created_at":      row["created_at"],
            "last_attempt":    row["last_attempt"],
        })
    return result


def dequeue_by_type(entry_type: str) -> list[dict[str, Any]]:
    """
    Return pending outbox entries filtered by entry_type.
    Entries are NOT removed.
    """
    all_entries = dequeue_all()
    return [e for e in all_entries if e["entry_type"] == entry_type]


# ── ACKNOWLEDGEMENT ───────────────────────────────────────────

def mark_flushed(entry_ids: list[int]) -> int:
    """
    Delete successfully delivered entries by their row ids.

    Parameters
    ----------
    entry_ids : list of integer row ids returned by enqueue() or dequeue_all()

    Returns
    -------
    Number of entries actually deleted.
    """
    if not entry_ids:
        return 0
    placeholders = ",".join("?" * len(entry_ids))
    with _lock:
        conn = _get_connection()
        cursor = conn.execute(
            f"DELETE FROM outbox WHERE id IN ({placeholders})",
            tuple(entry_ids),
        )
        conn.commit()
        deleted = cursor.rowcount
    log.debug("Outbox: flushed %d entries", deleted)
    return deleted


def record_attempt(entry_id: int) -> None:
    """
    Increment retry_count and update last_attempt timestamp for one entry.
    Called when a delivery attempt fails so the next flush can apply backoff.
    """
    with _lock:
        conn = _get_connection()
        conn.execute(
            "UPDATE outbox SET retry_count = retry_count + 1, last_attempt = ? "
            "WHERE id = ?",
            (_now(), entry_id),
        )
        conn.commit()


# ── STATS ─────────────────────────────────────────────────────

def get_count() -> int:
    """Return the total number of pending entries in the outbox."""
    with _lock:
        conn = _get_connection()
        return _scalar(conn, "SELECT COUNT(*) FROM outbox")


def get_count_by_type(entry_type: str) -> int:
    """Return the count of pending entries for a specific entry_type."""
    with _lock:
        conn = _get_connection()
        return _scalar(
            conn, "SELECT COUNT(*) FROM outbox WHERE entry_type=?", (entry_type,)
        )


def clear_all() -> int:
    """
    Delete all outbox entries. Used during clean shutdown or testing.
    Returns the number of entries deleted.
    """
    with _lock:
        conn = _get_connection()
        cursor = conn.execute("DELETE FROM outbox")
        conn.commit()
        return cursor.rowcount


# ── BACKOFF HELPER ────────────────────────────────────────────

def backoff_seconds(retry_count: int, base: float = 1.0, cap: float = 60.0) -> float:
    """
    Calculate exponential backoff delay for a given retry count.
    Formula: min(base * 2^retry_count, cap)

    Parameters
    ----------
    retry_count : number of previous failed attempts
    base        : base delay in seconds (default 1.0)
    cap         : maximum delay in seconds (default 60.0)

    Returns
    -------
    Float seconds to wait before next attempt.
    """
    delay = min(base * (2 ** retry_count), cap)
    return delay


# ── PRIVATE HELPERS ───────────────────────────────────────────

def _now() -> str:
    """Return current UTC time as ISO-8601 string."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    """Execute a scalar query and return the first column of the first row."""
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    return row[0] if row else 0


def close() -> None:
    """Close the thread-local outbox connection. Safe to call multiple times."""
    conn = getattr(_local, "conn", None)
    if conn:
        conn.close()
        _local.conn = None
