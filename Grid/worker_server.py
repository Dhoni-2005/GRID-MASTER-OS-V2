"""
grid/worker_server.py — Grid Master OS Phase 7 Step 6
Artifact: P7S6_grid_worker_server.py
Package:  grid
Path:     grid/worker_server.py
Status:   [K] KEEP — canonical

Worker-side HTTP server running on every worker node.
Accepts task assignments from the Grid Master, validates them,
queues them for execution, and returns signed acknowledgements.

Responsibilities
----------------
• Start / stop a lightweight Flask HTTP server on WORKER_PORT
• POST /worker/assign   — receive and validate task assignments
• GET  /worker/health   — liveness + queue stats
• GET  /worker/status   — detailed worker state
• GET  /worker/queue    — queue inspection for dispatcher
• POST /worker/dequeue  — pop next task (called by worker_runtime, Step 10)
• Verify HMAC signature on every assignment via grid.signing
• Authenticate every request via security.auth API keys
• Log all security events to security.audit
• Thread-safe in-memory task queue
• Graceful startup and shutdown
• Request-ID tracing on every response

Does NOT import:
  grid.dispatcher        — Step 7
  grid.memory_sync       — Step 8
  grid.master            — Step 9
  grid.worker_runtime    — Step 10
"""
from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request, g

from grid.config import (
    MASTER_VERSION,
    NODE_ID,
    NODE_SECRET,
    WORKER_PORT,
)
from grid.models import TaskAssignment
from grid.signing import verify_assignment
from security.audit import AuditEvent, log_event

log = logging.getLogger("gridmaster.grid.worker_server")

# ── MODULE STATE ──────────────────────────────────────────────
_lock           = threading.Lock()
_task_queue: queue.Queue[TaskAssignment] = queue.Queue()
_server_thread: threading.Thread | None  = None
_flask_server                            = None   # werkzeug Server instance
_start_time: float                       = 0.0
_last_heartbeat_ts: str                  = ""

# Tracks task IDs already accepted to prevent duplicate processing
_accepted_task_ids: set[int] = set()


# ══════════════════════════════════════════════════════════════
# SERVER LIFECYCLE
# ══════════════════════════════════════════════════════════════

def start(host: str = "0.0.0.0",
          port: int | None = None,
          node_id: str | None = None,
          api_key: str = "") -> Flask:
    """
    Start the worker HTTP server as a daemon background thread.
    Returns the Flask app (useful for testing with test_client()).

    Parameters
    ----------
    host    : bind address (default "0.0.0.0")
    port    : TCP port (defaults to WORKER_PORT from grid.config)
    node_id : this worker's node_id (defaults to grid.config.NODE_ID)
    api_key : API key to attach to server context (for health display)

    Returns
    -------
    Flask app instance.
    """
    global _server_thread, _flask_server, _start_time

    effective_port    = port    if port    is not None else WORKER_PORT
    effective_node_id = node_id if node_id else NODE_ID

    app = _create_app(effective_node_id)

    with _lock:
        if _server_thread is not None and _server_thread.is_alive():
            log.debug("Worker server already running — ignoring start()")
            return app

        _start_time = time.monotonic()

    def _run():
        import logging as _lg
        _lg.getLogger("werkzeug").setLevel(logging.ERROR)
        app.run(host=host, port=effective_port, threaded=True, use_reloader=False)

    _server_thread = threading.Thread(
        target = _run,
        name   = "gridmaster-worker-server",
        daemon = True,
    )
    _server_thread.start()
    log.info("Worker server started on %s:%d node=%s", host, effective_port, effective_node_id)
    return app


def stop(timeout: float = 5.0) -> None:
    """
    Request graceful shutdown of the worker server thread.
    Clears the task queue on shutdown.

    Parameters
    ----------
    timeout : seconds to wait for thread exit (default 5.0)
    """
    global _server_thread, _flask_server

    if _flask_server is not None:
        try:
            _flask_server.shutdown()
        except Exception:
            pass

    if _server_thread is not None:
        _server_thread.join(timeout=timeout)
        if _server_thread.is_alive():
            log.warning("Worker server thread did not stop within %.1fs", timeout)
        else:
            log.info("Worker server stopped cleanly")

    with _lock:
        _server_thread = None

    log.info("Worker server shutdown complete")


def is_running() -> bool:
    """Return True if the server thread is alive."""
    with _lock:
        return _server_thread is not None and _server_thread.is_alive()


def update_last_heartbeat(ts: str) -> None:
    """
    Record the timestamp of the most recent heartbeat sent to master.
    Called by grid.heartbeat_sender after each successful send.

    Parameters
    ----------
    ts : ISO-8601 timestamp string
    """
    global _last_heartbeat_ts
    with _lock:
        _last_heartbeat_ts = ts


# ══════════════════════════════════════════════════════════════
# FLASK APPLICATION
# ══════════════════════════════════════════════════════════════

def _create_app(node_id: str) -> Flask:
    """
    Build and configure the Flask application.
    Separated from start() so tests can use test_client() directly.

    Parameters
    ----------
    node_id : this worker's node_id embedded in all responses
    """
    app = Flask(f"gridmaster-worker-{node_id}", static_folder=None)
    app.config["JSON_SORT_KEYS"] = False

    # ── Request-ID injection ──────────────────────────────────
    @app.before_request
    def _attach_request_id():
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        g.node_id    = node_id

    @app.after_request
    def _add_request_id_header(response):
        response.headers["X-Request-ID"] = getattr(g, "request_id", "")
        response.headers["X-Worker-ID"]  = node_id
        return response

    # ── Authentication helper ─────────────────────────────────
    def _authenticate() -> dict | None:
        """
        Verify X-API-Key using security.auth.verify().
        Returns identity dict on success, None on failure.
        Falls back to open access if security package unavailable.
        """
        try:
            from security.auth import verify as _verify
            identity = _verify(request)
            return identity
        except ImportError:
            return {"role": "node", "owner": "system", "auth_method": "fallback"}

    # ── POST /worker/assign ───────────────────────────────────
    @app.route("/worker/assign", methods=["POST"])
    def assign_task():
        """
        Receive a task assignment from the Grid Master.

        Flow:
        1. Authenticate API key.
        2. Parse and validate TaskAssignment payload.
        3. Verify HMAC signature (requires NODE_SECRET).
        4. Check for duplicate task_id.
        5. Enqueue assignment.
        6. Return signed acknowledgement.
        """
        identity = _authenticate()
        if identity is None:
            log_event(AuditEvent.AUTH_FAILURE,
                      detail=f"worker_assign_unauthorized rid={_rid()}")
            return _err(401, "Authentication required")

        body = request.get_json(force=True, silent=True) or {}
        if not body:
            return _err(400, "Empty or non-JSON request body")

        # Parse payload
        try:
            assignment = TaskAssignment.from_dict(body)
        except (ValueError, TypeError, KeyError) as exc:
            log.warning("Malformed assignment payload rid=%s: %s", _rid(), exc)
            return _err(400, f"Malformed payload: {exc}")

        # HMAC signature verification
        if NODE_SECRET:
            payload_for_verify = {k: v for k, v in body.items() if k != "signature"}
            sig = body.get("signature", "")
            if not verify_assignment(payload_for_verify, sig, NODE_SECRET):
                log.warning(
                    "Invalid assignment signature task_id=%d rid=%s",
                    assignment.task_id, _rid(),
                )
                log_event(AuditEvent.AUTH_FAILURE,
                          detail=(f"invalid_assignment_signature "
                                  f"task_id={assignment.task_id} "
                                  f"node={node_id} rid={_rid()}"))
                return _err(400, "Invalid assignment signature")
        else:
            log.debug("NODE_SECRET not set — skipping signature verification")

        # Duplicate detection
        with _lock:
            if assignment.task_id in _accepted_task_ids:
                log.info("Duplicate task_id=%d — returning idempotent ack", assignment.task_id)
                return jsonify({
                    "status":    "ok",
                    "accepted":  True,
                    "duplicate": True,
                    "task_id":   assignment.task_id,
                    "node_id":   node_id,
                    "queue_len": _task_queue.qsize(),
                    "request_id": _rid(),
                }), 200

        # Enqueue
        _task_queue.put(assignment)
        with _lock:
            _accepted_task_ids.add(assignment.task_id)

        log.info(
            "Task accepted: task_id=%d project_id=%d priority=%d rid=%s",
            assignment.task_id, assignment.project_id,
            assignment.priority, _rid(),
        )
        log_event(AuditEvent.TASK_SUBMITTED,
                  detail=(f"worker_accepted task_id={assignment.task_id} "
                          f"node={node_id}"))

        return jsonify({
            "status":    "ok",
            "accepted":  True,
            "duplicate": False,
            "task_id":   assignment.task_id,
            "node_id":   node_id,
            "queue_len": _task_queue.qsize(),
            "request_id": _rid(),
        }), 200

    # ── GET /worker/health ────────────────────────────────────
    @app.route("/worker/health", methods=["GET"])
    def health():
        """
        Liveness endpoint. Returns 200 if the server is running.
        No authentication required — used by monitoring probes.
        """
        return jsonify({
            "status":       "ok",
            "node_id":      node_id,
            "version":      MASTER_VERSION,
            "uptime_s":     _uptime(),
            "queue_len":    _task_queue.qsize(),
            "request_id":   _rid(),
        }), 200

    # ── GET /worker/status ────────────────────────────────────
    @app.route("/worker/status", methods=["GET"])
    def status():
        """
        Detailed worker state. Requires authentication.
        Returns queue length, accepted task count, heartbeat timestamp.
        """
        identity = _authenticate()
        if identity is None:
            return _err(401, "Authentication required")

        with _lock:
            accepted_count = len(_accepted_task_ids)
            last_hb        = _last_heartbeat_ts

        return jsonify({
            "status":           "ok",
            "node_id":          node_id,
            "version":          MASTER_VERSION,
            "uptime_s":         _uptime(),
            "queue_len":        _task_queue.qsize(),
            "accepted_total":   accepted_count,
            "last_heartbeat":   last_hb or None,
            "request_id":       _rid(),
        }), 200

    # ── GET /worker/queue ─────────────────────────────────────
    @app.route("/worker/queue", methods=["GET"])
    def queue_stats():
        """
        Queue inspection endpoint for the dispatcher (Step 7).
        Returns queue length and the next task_id without dequeuing.
        Requires authentication.
        """
        identity = _authenticate()
        if identity is None:
            return _err(401, "Authentication required")

        peek = _peek_next_task()
        return jsonify({
            "status":       "ok",
            "node_id":      node_id,
            "queue_len":    _task_queue.qsize(),
            "next_task_id": peek.task_id if peek else None,
            "request_id":   _rid(),
        }), 200

    # ── POST /worker/dequeue ──────────────────────────────────
    @app.route("/worker/dequeue", methods=["POST"])
    def dequeue():
        """
        Pop the next task from the queue for execution.
        Called by grid.worker_runtime (Step 10).
        Requires authentication.
        Returns the TaskAssignment dict or null if queue is empty.
        """
        identity = _authenticate()
        if identity is None:
            return _err(401, "Authentication required")

        task = pop_next_task()
        if task is None:
            return jsonify({
                "status":     "ok",
                "node_id":    node_id,
                "assignment": None,
                "queue_len":  0,
                "request_id": _rid(),
            }), 200

        return jsonify({
            "status":     "ok",
            "node_id":    node_id,
            "assignment": task.to_dict(),
            "queue_len":  _task_queue.qsize(),
            "request_id": _rid(),
        }), 200

    # ── Error handlers ────────────────────────────────────────
    @app.errorhandler(404)
    def not_found(e):
        return _err(404, f"Route not found: {request.path}")

    @app.errorhandler(405)
    def method_not_allowed(e):
        return _err(405, f"Method {request.method} not allowed on {request.path}")

    @app.errorhandler(500)
    def internal_error(e):
        log.error("Internal server error: %s", e)
        return _err(500, "Internal server error")

    return app


# ══════════════════════════════════════════════════════════════
# QUEUE MANAGEMENT
# ══════════════════════════════════════════════════════════════

def enqueue_task(assignment: TaskAssignment) -> bool:
    """
    Add a TaskAssignment to the local execution queue.
    Called internally by the /worker/assign route.
    Also available for direct injection in tests.

    Parameters
    ----------
    assignment : validated and signature-verified TaskAssignment

    Returns
    -------
    True if enqueued; False if duplicate task_id.
    """
    with _lock:
        if assignment.task_id in _accepted_task_ids:
            return False
        _accepted_task_ids.add(assignment.task_id)

    _task_queue.put(assignment)
    log.debug("Task %d enqueued (queue_len=%d)", assignment.task_id, _task_queue.qsize())
    return True


def pop_next_task(block: bool = False,
                  timeout: float | None = None) -> TaskAssignment | None:
    """
    Remove and return the next task from the queue.
    Called by grid.worker_runtime (Step 10) on each poll iteration.

    Parameters
    ----------
    block   : if True, block until a task is available
    timeout : seconds to wait when block=True (None = wait forever)

    Returns
    -------
    TaskAssignment or None if queue is empty (when block=False).
    """
    try:
        return _task_queue.get(block=block, timeout=timeout)
    except queue.Empty:
        return None


def _peek_next_task() -> TaskAssignment | None:
    """
    Return the next queued task WITHOUT removing it.
    Thread-safe read of the queue head.
    """
    with _task_queue.mutex:
        if _task_queue.queue:
            return _task_queue.queue[0]
    return None


def get_queue_length() -> int:
    """Return the current number of tasks waiting in the queue."""
    return _task_queue.qsize()


def get_queue_stats() -> dict[str, Any]:
    """
    Return a statistics snapshot of the task queue.

    Returns
    -------
    {
      "queue_len":      int,
      "accepted_total": int,
      "next_task_id":   int | None,
    }
    """
    with _lock:
        accepted = len(_accepted_task_ids)
    peek = _peek_next_task()
    return {
        "queue_len":      _task_queue.qsize(),
        "accepted_total": accepted,
        "next_task_id":   peek.task_id if peek else None,
    }


def clear_queue() -> int:
    """
    TESTING ONLY — drain and discard all queued tasks.
    Clears accepted_task_ids as well.

    Returns
    -------
    Number of tasks discarded.
    """
    count = 0
    while not _task_queue.empty():
        try:
            _task_queue.get_nowait()
            count += 1
        except queue.Empty:
            break
    with _lock:
        _accepted_task_ids.clear()
    log.debug("Queue cleared: discarded %d tasks", count)
    return count


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _rid() -> str:
    """Return the current request's X-Request-ID, or empty string outside request context."""
    try:
        return getattr(g, "request_id", "") or ""
    except RuntimeError:
        return ""


def _err(status: int, message: str):
    """Return a structured JSON error response."""
    return jsonify({
        "status":     "error",
        "error":      message,
        "code":       status,
        "node_id":    NODE_ID,
        "request_id": _rid(),
    }), status


def _uptime() -> float:
    """Return server uptime in seconds."""
    if _start_time == 0.0:
        return 0.0
    return round(time.monotonic() - _start_time, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """
    TESTING ONLY — clear all module state.
    Stops any running server thread and empties the queue.
    """
    global _server_thread, _flask_server, _start_time, _last_heartbeat_ts

    stop(timeout=1.0)
    clear_queue()
    with _lock:
        _start_time       = 0.0
        _last_heartbeat_ts = ""
        _server_thread    = None
