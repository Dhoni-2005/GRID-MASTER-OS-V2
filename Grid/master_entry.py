# Render deployment path verification
"""
master_entry.py — GRID-MASTER-OS-V2 Render Deployment Launcher

This file is a THIN adapter between Render's Web Service runtime and the
existing, unmodified Grid Master runtime (Grid/master.py). It contains no
dispatcher, heartbeat, reconciliation, registry, or failure-management
logic of its own — every operational decision is delegated to Grid.master.

Why this file exists
---------------------
Render's Web Service product requires a process that:
  (a) binds to 0.0.0.0:$PORT and responds to HTTP requests, and
  (b) stays alive in the foreground.

Grid.master.start() launches background daemon threads (dispatcher,
heartbeat monitor, reconciler) and returns immediately — it does not,
and should not, bind to a network port or block. This launcher supplies
exactly the two things Grid.master intentionally does not: a minimal
health-check HTTP surface, and a foreground process that keeps those
background threads alive until Render sends SIGTERM.

Package casing note
--------------------
The repository's packages are named with capitalized folders on disk:
    Grid/, Interface/, Security/
but the source files inside those folders import each other using
lowercase names:
    import grid.dispatcher, from grid.config import ...
    from security.audit import ...
This is consistent across every file in Grid/ (confirmed directly by
inspecting Grid/master.py) and is not something this launcher is
permitted to fix by renaming folders or editing Phase 0-7 files.

Instead, this launcher registers lowercase aliases in sys.modules that
point at the real, capitalized packages BEFORE importing Grid.master.
Python's import system resolves `import grid.X` by using the aliased
module's __path__ (which is the real Grid/ directory), so every internal
`import grid.*` / `import security.*` statement inside the existing
codebase resolves correctly without any of those files being touched.

This shim is deployment-environment plumbing, not application logic.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from typing import Any

# ══════════════════════════════════════════════════════════════
# STEP 0 — Package casing shim (must run before any Grid.* import)
# ══════════════════════════════════════════════════════════════
#
# The real packages on disk are capitalized: Grid/, Interface/, Security/.
# Every file inside them imports its siblings using lowercase names
# (import grid.X, from security.Y import Z). We alias lowercase names
# in sys.modules to the real capitalized package objects so those
# internal imports resolve without modifying any existing file.

def _install_package_casing_shim() -> None:
    """
    Register sys.modules aliases so that lowercase `import grid.*` /
    `import interface.*` / `import security.*` statements inside the
    existing codebase resolve to the real, capitalized packages on disk.

    This cannot be done with a simple `__import__(real_name)` followed by
    an alias assignment: each package's submodules use ABSOLUTE lowercase
    self-imports internally (e.g. Security/auth.py does
    `from security.config import ...`), so the alias must already exist
    in sys.modules BEFORE the real package's __init__.py begins executing
    — otherwise that internal import fails before we ever get a chance
    to register it. We build each module from its real __init__.py file
    via importlib, register it under the lowercase name FIRST, then run
    its __init__.py — so any nested self-import resolves immediately.

    Order matters: `security` has no dependency on `grid` or `interface`,
    but `grid` (grid.failure, grid.registry, grid.master, ...) imports
    from `security.*`, so `security` must be aliased before `grid`.
    """
    import importlib.util

    root = os.path.dirname(os.path.abspath(__file__))
    # Order: security first (no deps on the others), then grid, then interface.
    alias_order = [
        ("security",  "Security"),
        ("grid",      "Grid"),
        ("interface", "Interface"),
    ]

    for lowercase_name, real_name in alias_order:
        if lowercase_name in sys.modules:
            continue

        real_dir  = os.path.join(root, real_name)
        real_init = os.path.join(real_dir, "__init__.py")
        if not os.path.isfile(real_init):
            # Package not present under this name — nothing to alias.
            # The later `import Grid.master` will surface a clear error
            # if Grid/ itself is genuinely missing.
            continue

        spec = importlib.util.spec_from_file_location(
            lowercase_name, real_init, submodule_search_locations=[real_dir]
        )
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        # Register BEFORE exec_module: this is what breaks the circular
        # bootstrap — any absolute `from <lowercase_name>.X import Y`
        # triggered while executing __init__.py now finds the alias.
        sys.modules[lowercase_name] = module
        spec.loader.exec_module(module)


_install_package_casing_shim()

# ══════════════════════════════════════════════════════════════
# STEP 1 — Import the existing, unmodified Grid Master runtime
# ══════════════════════════════════════════════════════════════
import Grid.master as grid_master  # noqa: E402  (must follow the shim above)
import database as _database       # noqa: E402  (base schema bootstrap only — see main())

log = logging.getLogger("gridmaster.master_entry")
logging.basicConfig(
    level  = os.environ.get("LOG_LEVEL", "INFO"),
    format = "%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ══════════════════════════════════════════════════════════════
# CONFIGURATION — from environment variables only
# ══════════════════════════════════════════════════════════════

PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "0.0.0.0")

HEARTBEAT_INTERVAL  = int(os.environ.get("GRIDMASTER_HEARTBEAT_INTERVAL", "30"))
DISPATCHER_INTERVAL = os.environ.get("GRIDMASTER_DISPATCHER_INTERVAL")
DISPATCHER_INTERVAL = int(DISPATCHER_INTERVAL) if DISPATCHER_INTERVAL else None
RECONCILER_INTERVAL = int(os.environ.get("GRIDMASTER_RECONCILER_INTERVAL", "60"))

# ══════════════════════════════════════════════════════════════
# HEALTH SERVER — minimal, delegates all status to Grid.master
# ══════════════════════════════════════════════════════════════
#
# Uses only the Python standard library (http.server). This avoids
# adding a new framework dependency; the launcher's HTTP surface is
# intentionally tiny (two read-only routes) so stdlib is sufficient
# and there is no risk of it growing into a second application server.

from http.server import BaseHTTPRequestHandler, HTTPServer
import json


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler exposing /health and / as read-only status views."""

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        # Route access logs through the standard logger instead of stderr.
        log.debug("HTTP %s", fmt % args)

    def _write_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]

        if path == "/health":
            self._handle_health()
        elif path in ("/", "/status"):
            self._handle_status()
        else:
            self._write_json(404, {"status": "error", "error": f"Not found: {path}"})

    def _handle_health(self) -> None:
        """
        Liveness check for Render. Returns 200 as long as this process
        is alive and Grid.master reports its runtime as running.
        Never computes health itself — reads it from Grid.master.
        """
        try:
            running = grid_master.is_running()
        except Exception as exc:
            log.error("Health check failed to query Grid.master: %s", exc)
            self._write_json(503, {"status": "error", "error": str(exc)})
            return

        if running:
            self._write_json(200, {"status": "ok", "running": True})
        else:
            # Process is alive but the Grid runtime isn't up — still a
            # meaningful distinction for Render's health probe to see.
            self._write_json(503, {"status": "error", "running": False})

    def _handle_status(self) -> None:
        """
        Full status view — proxies grid_master.get_runtime_status()
        verbatim. No status computation happens in this launcher.
        """
        try:
            status = grid_master.get_runtime_status()
        except Exception as exc:
            log.error("Status query failed: %s", exc)
            self._write_json(503, {"status": "error", "error": str(exc)})
            return
        self._write_json(200, status)


def _run_health_server(stop_event: threading.Event) -> None:
    """
    Run the HTTP health server until stop_event is set.
    Uses a short socket timeout so the serve loop can check stop_event
    periodically instead of blocking forever on accept().
    """
    server = HTTPServer((HOST, PORT), _HealthHandler)
    server.timeout = 1.0  # seconds; makes handle_request() non-blocking-ish

    log.info("Health server listening on %s:%d", HOST, PORT)
    while not stop_event.is_set():
        server.handle_request()
    server.server_close()
    log.info("Health server stopped")


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
    """
    Start the Grid Master runtime, start the health server, and block
    until a shutdown signal is received. Returns a process exit code.
    """
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Bootstrap the base database schema (projects, tasks, memory_entries,
    # node_registry, agent_notes tables). This is the existing, unmodified
    # database.init_db() from the Phase 1 root module — required because
    # Grid.master.start() only runs Phase 7 ALTER TABLE migrations via
    # grid.db_adapter.init_schema(), which assumes the base tables already
    # exist. On a fresh deployment (empty DB file) they do not yet, so this
    # call must happen first. This is environment bootstrap, not dispatcher/
    # heartbeat/reconciler logic — database.py already owns this function.
    log.info("Bootstrapping base database schema via database.init_db()")
    try:
        _database.init_db()
    except Exception as exc:
        log.error("Base database schema initialization failed: %s", exc)
        return 1

    log.info("Starting Grid Master runtime via Grid.master.start()")
    status = grid_master.start(
        dispatcher_interval = DISPATCHER_INTERVAL,
        heartbeat_interval  = HEARTBEAT_INTERVAL,
        reconciler_interval = RECONCILER_INTERVAL,
    )

    if not status.get("running"):
        log.error(
            "Grid Master failed to start: %s",
            status.get("last_start_error", "unknown error"),
        )
        return 1

    log.info("Grid Master runtime is running (version=%s)", status.get("version"))

    health_thread = threading.Thread(
        target = _run_health_server,
        args   = (_shutdown_event,),
        name   = "master-entry-health-server",
        daemon = True,
    )
    health_thread.start()

    # Block the foreground process until a shutdown signal arrives.
    _shutdown_event.wait()

    log.info("Shutting down: stopping health server")
    health_thread.join(timeout=5.0)

    log.info("Shutting down: stopping Grid Master runtime")
    grid_master.stop(timeout=10.0)

    log.info("Shutdown complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
