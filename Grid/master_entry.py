"""
Grid/master_entry.py — GRID-MASTER-OS-V2 Render Deployment Launcher

This file is a THIN adapter between Render's Web Service runtime and the
existing, unmodified Phase 1 Grid Master coordinator (root-level
grid_master.py). It contains no dispatcher, scheduling, memory, or
node-registry logic of its own — every operational decision is delegated
to grid_master.py and the modules it already uses (database.py,
memory_manager.py, node_registry.py).

Why this file exists
---------------------
Render's Web Service product requires a process that:
  (a) binds to 0.0.0.0:$PORT and responds to HTTP requests, and
  (b) stays alive in the foreground.

grid_master.boot() performs one-time initialization (database schema,
coordinator agent registration, coordinator node registration/heartbeat)
and returns immediately — it does not bind to a network port or block,
and it has no start()/stop()/is_running() lifecycle of its own. This
launcher supplies exactly what grid_master.py intentionally does not:
a minimal health-check HTTP surface, and a foreground process that keeps
the deployment alive on Render after boot() completes.

Architecture note — why this imports grid_master, not Grid.master
-------------------------------------------------------------------
An earlier version of this launcher targeted a Phase 7 orchestrator
(Grid.master, exposing start()/stop()/is_running()/get_runtime_status(),
coordinating a dispatcher/heartbeat-monitor/reconciler trio). That module
does not exist in this repository. The actual, present coordinator is the
root-level grid_master.py — a Phase 1 module exposing boot(),
submit_task(), dispatch(), system_status(), and related functions, with
no autonomous background loop.

This launcher targets grid_master.py as-is. It does NOT invoke Grid/'s
dispatcher.py, heartbeat_monitor.py, or reconciler.py — those files are
part of a separate, not-yet-wired-in runtime and starting them here would
silently misrepresent what this deployment is actually running. If
automatic task dispatch is desired, that requires an explicit design
decision (e.g. a periodic call to grid_master.dispatch()), not something
this launcher should add unannounced while fixing a crash.

No package-casing shim is required here: grid_master.py's own imports
(database, memory_manager, node_registry) are all bare, root-level
imports with no Grid.*/Interface.*/Security.* references, and Python's
`-m` flag already prepends the current working directory (the repo root,
when Render runs `python -m Grid.master_entry` from there) to sys.path.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from typing import Any

import grid_master
import database as _database

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

# ══════════════════════════════════════════════════════════════
# RUNTIME STATE
# ══════════════════════════════════════════════════════════════
#
# grid_master.py (Phase 1) has no is_running()/get_status() of its own —
# boot() is a one-shot call with no lifecycle to query. This launcher
# tracks "has boot() completed successfully" locally, since that is the
# only runtime fact grid_master.py doesn't already expose through
# system_status().

_lock    = threading.Lock()
_booted  = False
_boot_error: str | None = None


# ══════════════════════════════════════════════════════════════
# HEALTH SERVER — minimal, delegates status to grid_master.system_status()
# ══════════════════════════════════════════════════════════════
#
# Uses only the Python standard library (http.server). No new dependency
# is introduced for this launcher's own HTTP surface.

from http.server import BaseHTTPRequestHandler, HTTPServer
import json


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler exposing /health and /status as read-only views."""

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log.debug("HTTP %s", fmt % args)

    def _write_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
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
        Liveness check for Render. Returns 200 once grid_master.boot()
        has completed successfully; 503 otherwise. Reads local boot
        state only — grid_master.py has no is_running() to query.
        """
        with _lock:
            booted = _booted
            error  = _boot_error

        if booted:
            self._write_json(200, {"status": "ok", "booted": True})
        else:
            self._write_json(503, {"status": "error", "booted": False, "error": error})

    def _handle_status(self) -> None:
        """
        Full status view — proxies grid_master.system_status() verbatim.
        No status computation happens in this launcher.
        """
        try:
            status = grid_master.system_status()
        except Exception as exc:
            log.error("system_status() query failed: %s", exc)
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
    server.timeout = 1.0

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
    Boot the Phase 1 Grid Master coordinator once, start the health
    server, and block until a shutdown signal is received.
    """
    global _booted, _boot_error

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info("Booting Grid Master coordinator via grid_master.boot()")
    try:
        # grid_master.boot() already calls database.init_db() internally —
        # no separate database bootstrap call is needed here.
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
        name   = "master-entry-health-server",
        daemon = True,
    )
    health_thread.start()

    # Block the foreground process until a shutdown signal arrives.
    _shutdown_event.wait()

    log.info("Shutting down: stopping health server")
    health_thread.join(timeout=5.0)

    log.info("Shutting down: closing database connection")
    try:
        _database.close_db()
    except Exception as exc:
        log.warning("close_db() raised during shutdown: %s", exc)

    log.info("Shutdown complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
