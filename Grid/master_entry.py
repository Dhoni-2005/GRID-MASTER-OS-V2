"""
Grid/master_entry.py — GRID-MASTER-OS-V2 Render Deployment Launcher

This file is a THIN adapter between Render's Web Service runtime (and,
now, distributed worker nodes speaking the Phase 7 HTTP wire protocol)
and the existing, unmodified Phase 1 Grid Master coordinator (root-level
grid_master.py). It contains no dispatcher, scheduling, memory, or
node-registry logic of its own — every operational decision is delegated
to grid_master.py, node_registry.py, database.py, and Security/auth.py,
all used exactly as they already exist.

Why this file exists
---------------------
Render's Web Service product requires a process that:
  (a) binds to 0.0.0.0:$PORT and responds to HTTP requests, and
  (b) stays alive in the foreground.

grid_master.boot() performs one-time initialization and returns
immediately — it has no start()/stop()/is_running() lifecycle, and no
HTTP surface of its own. This launcher supplies exactly what
grid_master.py intentionally does not: a minimal HTTP server exposing
both operator-facing health/status views and the worker-facing wire
protocol (register/poll/heartbeat/result) that Grid/worker_runtime.py
and Grid/client.py already speak.

Wire protocol endpoints — confirmed against actual Grid/client.py and
Grid/models.py in this repository (grep-verified, not guessed):

  POST /grid/register   body: NodeInfo.to_dict()
                         reply: RegistrationResponse.to_dict()
  POST /grid/heartbeat   body: HeartbeatPayload.to_dict() (+ optional
                         "signature" key added by heartbeat_sender when
                         GRIDMASTER_NODE_SECRET is set)
                         reply: {"status": "ok", "server_ts": <iso>}
  GET  /grid/poll?node_id=...   (confirmed GET, not POST, per
                         Grid/client.py's poll() using _http_get)
                         reply: PollResponse.to_dict()
  POST /grid/result      body: ResultPayload.to_dict()
                         reply: {"status": "ok", "task_id": N, "recorded": bool}

Each handler calls only existing functions:
  - node_registry.register() / .heartbeat() / .get_node()   (registration)
  - grid_master.list_tasks() / .dispatch() / .record_success() /
    .record_failure()                                        (task flow)
  - Security.auth.issue_token()                              (real, non-
    simulated tokens — reuses the existing Phase 6 token store rather
    than inventing a new one)
  - Grid.signing.verify_heartbeat() / .verify_result() /
    .sign_assignment()                                       (HMAC, only
    when GRIDMASTER_NODE_SECRET is configured on the master — mirrors
    the worker's own conditional signing behavior)

Known architectural limitation — surfaced, not hidden
--------------------------------------------------------
grid_master.dispatch() selects a node internally via select_node(),
which considers ALL registered, available worker-role nodes — it is not
scoped to "the specific node that is currently polling". With a single
registered worker this is a non-issue (select_node() will deterministically
choose that worker). With multiple concurrent workers, a task could be
selected for a different node than the one that called /grid/poll,
because Phase 1's dispatch() was designed for a coordinator that picks
who runs what, not for workers pulling their own assignments. This is not
something this launcher works around silently: doing so would require
either modifying grid_master.py's selection logic (not permitted here) or
duplicating that logic in the adapter (also against the stated
constraint). It is called out here as a known gap for an explicit future
decision, not fixed by assumption.

Package casing note
--------------------
Security/auth.py (needed for real, non-fake token issuance) internally
does `from security.config import ...` — an absolute, lowercase
self-import. The master's current Render Start Command
(`python -m Grid.master_entry`) does not create the `Grid -> grid` /
`Security -> security` symlinks that the worker's start command does.
To keep this launcher self-contained regardless of how it's invoked, a
small in-process sys.modules alias is installed for `security` only
(the one package this file's own new functionality actually needs),
using the same technique validated earlier for the (now unused) Phase 7
target. Grid.signing and grid_master.py itself need no such alias — both
were confirmed, by direct inspection, to have no lowercase self-imports.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from typing import Any

# ══════════════════════════════════════════════════════════════
# Package casing shim — 'security' only (see note above)
# ══════════════════════════════════════════════════════════════

def _install_security_alias() -> None:
    """
    Pre-register sys.modules['security'] pointing at the real, capitalized
    Security/ package before importing anything from it. Required because
    Security/auth.py does an absolute `from security.config import ...`
    internally, and the master's start command does not create a
    filesystem symlink the way the worker's does.
    """
    if "security" in sys.modules:
        return
    import importlib.util

    root      = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(root)  # Grid/ -> repo root
    real_dir  = os.path.join(repo_root, "Security")
    real_init = os.path.join(real_dir, "__init__.py")
    if not os.path.isfile(real_init):
        return

    spec = importlib.util.spec_from_file_location(
        "security", real_init, submodule_search_locations=[real_dir]
    )
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    sys.modules["security"] = module
    spec.loader.exec_module(module)


_install_security_alias()

# ══════════════════════════════════════════════════════════════
# Imports — existing, unmodified modules only
# ══════════════════════════════════════════════════════════════
import grid_master
import database as _database
import node_registry as _nr
from Grid import signing as _signing
from security.auth import issue_token as _issue_token

log = logging.getLogger("gridmaster.master_entry")
logging.basicConfig(
    level  = os.environ.get("LOG_LEVEL", "INFO"),
    format = "%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ══════════════════════════════════════════════════════════════
# CONFIGURATION — from environment variables only
# ══════════════════════════════════════════════════════════════

PORT        = int(os.environ.get("PORT", "8000"))
HOST        = os.environ.get("HOST", "0.0.0.0")
NODE_SECRET = os.environ.get("GRIDMASTER_NODE_SECRET", "")
MASTER_VERSION = "1.0.0-phase1-adapter"

# ══════════════════════════════════════════════════════════════
# RUNTIME STATE
# ══════════════════════════════════════════════════════════════

_lock       = threading.Lock()
_booted     = False
_boot_error: str | None = None


# ══════════════════════════════════════════════════════════════
# WIRE PROTOCOL HANDLER LOGIC (testable independently of HTTP)
# ══════════════════════════════════════════════════════════════

class _MasterHandler:
    """
    Core adapter logic used by the HTTP request handler below. Kept as
    plain static methods so it can be exercised directly in tests without
    a real socket.
    """

    @staticmethod
    def handle_register(body: dict) -> tuple[int, dict]:
        """
        POST /grid/register — body: NodeInfo.to_dict()
        Calls node_registry.register() + .heartbeat(), then issues a
        real token via Security.auth.issue_token() (not fabricated).
        """
        node_id      = body.get("node_id", "")
        platform     = body.get("platform", "unknown")
        capabilities = body.get("capabilities", [])
        public_url   = body.get("public_url", "")

        if not node_id:
            return 400, {"status": "error", "error": "node_id is required"}

        nid = _nr.register(
            node_id      = node_id,
            node_name    = f"Worker {node_id}",
            platform     = platform,
            role         = _nr.ROLE_WORKER,
            url          = public_url,
            capabilities = capabilities,
        )
        if nid == -1:
            return 500, {"status": "error", "error": "node_registry.register() failed"}

        _nr.heartbeat(node_id)

        token_info = _issue_token(role="node", owner=node_id, ttl=3600)

        response = {
            "node_id":             node_id,
            "registration_token":  token_info["token"],
            "token_expires_at":    token_info["expires_at"],
            "master_version":      MASTER_VERSION,
        }
        log.info("Node registered: node_id=%s platform=%s capabilities=%s",
                 node_id, platform, capabilities)
        return 200, response

    @staticmethod
    def handle_heartbeat(body: dict) -> tuple[int, dict]:
        """
        POST /grid/heartbeat — body: HeartbeatPayload.to_dict()
        Optionally HMAC-verified when GRIDMASTER_NODE_SECRET is set on
        the master (mirrors the worker's own conditional signing).
        """
        node_id = body.get("node_id", "")
        if not node_id:
            return 400, {"status": "error", "error": "node_id is required"}

        if NODE_SECRET:
            sig = body.get("signature", "")
            check_body = {k: v for k, v in body.items() if k != "signature"}
            if sig and not _signing.verify_heartbeat(check_body, sig, NODE_SECRET):
                log.warning("Heartbeat signature invalid for node=%s", node_id)
                return 400, {"status": "error", "error": "invalid signature"}

        node = _nr.get_node(node_id)
        if node is None:
            log.warning("Heartbeat from unregistered node=%s", node_id)
            return 404, {"status": "error", "error": "node not registered"}

        _nr.heartbeat(node_id)
        return 200, {"status": "ok", "server_ts": _now_iso()}

    @staticmethod
    def handle_poll(node_id: str) -> tuple[int, dict]:
        """
        GET /grid/poll?node_id=... (confirmed GET, not POST, by direct
        inspection of Grid/client.py's poll() implementation).

        Finds the oldest pending task and dispatches it via the existing
        grid_master.dispatch(), then builds a signed TaskAssignment-shaped
        response. See the module docstring for the known single-vs-multi-
        worker node-selection limitation.
        """
        if not node_id:
            return 400, {"status": "error", "error": "node_id query param is required"}

        node = _nr.get_node(node_id)
        if node is None:
            return 404, {"status": "error", "error": "node not registered"}

        pending = grid_master.list_tasks(status="pending")
        if not pending:
            return 200, {"has_work": False, "wait_seconds": 5, "assignment": None}

        task_id = pending[0]["id"]
        result  = grid_master.dispatch(task_id, role=_nr.ROLE_WORKER)

        if result.get("status") != "dispatched":
            return 200, {"has_work": False, "wait_seconds": 5, "assignment": None}

        task = _database.get_task(task_id)
        assignment = {
            "task_id":     task_id,
            "project_id":  task.get("project_id"),
            "title":       task.get("title", ""),
            "input_data":  task.get("input", ""),
            "priority":    task.get("priority", 5),
            "assigned_at": _now_iso(),
            "signature":   "",
        }
        if NODE_SECRET:
            sig_input = {k: v for k, v in assignment.items() if k != "signature"}
            assignment["signature"] = _signing.sign_assignment(sig_input, NODE_SECRET)

        log.info("Task %d dispatched to node=%s (via poll)", task_id, node_id)
        return 200, {"has_work": True, "wait_seconds": 0, "assignment": assignment}

    @staticmethod
    def handle_result(body: dict) -> tuple[int, dict]:
        """
        POST /grid/result — body: ResultPayload.to_dict()
        Routes to grid_master.record_success() / .record_failure(),
        the existing, unmodified coordinator functions.
        """
        task_id = body.get("task_id")
        node_id = body.get("node_id", "")
        status  = body.get("status", "")
        output  = body.get("output", "")
        error   = body.get("error") or ""

        if task_id is None or not node_id or not status:
            return 400, {"status": "error", "error": "task_id, node_id, status are required"}

        if NODE_SECRET:
            sig = body.get("signature", "")
            check_body = {k: v for k, v in body.items() if k != "signature"}
            if not _signing.verify_result(check_body, sig, NODE_SECRET):
                log.warning("Result signature invalid for task=%s node=%s", task_id, node_id)
                return 400, {"status": "error", "error": "invalid signature"}

        existing = _database.get_task(task_id)
        if existing is None:
            return 404, {"status": "error", "task_id": task_id, "recorded": False}

        if existing.get("status") in ("completed", "abandoned"):
            return 200, {"status": "ok", "task_id": task_id, "recorded": False}

        if status == "completed":
            grid_master.record_success(task_id, output=output, node_id=node_id)
        else:
            grid_master.record_failure(
                task_id, problem=error or f"Worker reported status={status}",
                node_id=node_id,
            )

        log.info("Result recorded: task_id=%s node=%s status=%s", task_id, node_id, status)
        return 200, {"status": "ok", "task_id": task_id, "recorded": True}


# ══════════════════════════════════════════════════════════════
# HTTP SERVER — stdlib only, no new dependency
# ══════════════════════════════════════════════════════════════

from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs


class _RequestHandler(BaseHTTPRequestHandler):
    """Routes GET/POST requests to health/status views or the wire protocol."""

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log.debug("HTTP %s", fmt % args)

    def _write_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path   = parsed.path
        query  = parse_qs(parsed.query)

        if path == "/health":
            self._handle_health()
        elif path in ("/", "/status"):
            self._handle_status()
        elif path == "/grid/poll":
            node_id = (query.get("node_id") or [""])[0]
            code, resp = _MasterHandler.handle_poll(node_id)
            self._write_json(code, resp)
        else:
            self._write_json(404, {"status": "error", "error": f"Not found: {path}"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        body = self._read_json_body()

        if path == "/grid/register":
            code, resp = _MasterHandler.handle_register(body)
        elif path == "/grid/heartbeat":
            code, resp = _MasterHandler.handle_heartbeat(body)
        elif path == "/grid/result":
            code, resp = _MasterHandler.handle_result(body)
        else:
            code, resp = 404, {"status": "error", "error": f"Not found: {path}"}

        self._write_json(code, resp)

    def _handle_health(self) -> None:
        with _lock:
            booted = _booted
            error  = _boot_error
        if booted:
            self._write_json(200, {"status": "ok", "booted": True})
        else:
            self._write_json(503, {"status": "error", "booted": False, "error": error})

    def _handle_status(self) -> None:
        try:
            status = grid_master.system_status()
        except Exception as exc:
            log.error("system_status() query failed: %s", exc)
            self._write_json(503, {"status": "error", "error": str(exc)})
            return
        self._write_json(200, status)


def _run_health_server(stop_event: threading.Event) -> None:
    server = HTTPServer((HOST, PORT), _RequestHandler)
    server.timeout = 1.0
    log.info("HTTP server listening on %s:%d", HOST, PORT)
    while not stop_event.is_set():
        server.handle_request()
    server.server_close()
    log.info("HTTP server stopped")


# ══════════════════════════════════════════════════════════════
# SHUTDOWN HANDLING
# ══════════════════════════════════════════════════════════════

_shutdown_event = threading.Event()


def _handle_signal(signum: int, frame: Any) -> None:
    log.info("Received signal %s — initiating graceful shutdown", signum)
    _shutdown_event.set()


# ══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

def main() -> int:
    global _booted, _boot_error

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info("Booting Grid Master coordinator via grid_master.boot()")
    try:
        grid_master.boot()
    except Exception as exc:
        log.error("grid_master.boot() failed: %s", exc)
        with _lock:
            _boot_error = str(exc)
        return 1

    with _lock:
        _booted = True

    log.info("Grid Master coordinator booted successfully")

    health_thread = threading.Thread(
        target = _run_health_server,
        args   = (_shutdown_event,),
        name   = "master-entry-http-server",
        daemon = True,
    )
    health_thread.start()

    _shutdown_event.wait()

    log.info("Shutting down: stopping HTTP server")
    health_thread.join(timeout=5.0)

    log.info("Shutting down: closing database connection")
    try:
        _database.close_db()
    except Exception as exc:
        log.warning("close_db() raised during shutdown: %s", exc)

    log.info("Shutdown complete")
    return 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


if __name__ == "__main__":
    sys.exit(main())
