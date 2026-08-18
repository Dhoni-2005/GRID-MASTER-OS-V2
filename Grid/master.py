"""
grid/master.py — Grid Master OS Phase 7 Step 9
Artifact: P7S9_grid_master.py
Package:  grid
Path:     grid/master.py
Status:   [K] KEEP — canonical

Central runtime coordinator for the Distributed Grid Runtime.

Responsibilities
----------------
• start() / stop() / is_running() / get_status() — public lifecycle
• Orchestrate (never reimplement) the following services in order:
    1. Database connection + Phase 7 schema migrations (grid.db_adapter)
    2. Registry (grid.registry — no explicit start/stop; stateless module)
    3. Heartbeat monitor (grid.heartbeat_monitor.start_monitor / stop_monitor)
    4. Dispatcher (grid.dispatcher.start / stop)
    5. Reconciler (grid.reconciler.start / stop)
• Track runtime state: started timestamp, running flag, per-service health
• get_runtime_status() — structured status dict matching the spec example
• Graceful shutdown in reverse startup order
• Startup failure recovery: if any service fails to start, already-started
  services are torn down and the runtime returns to a clean stopped state

This module is intentionally thin — it contains no dispatch logic, no
heartbeat logic, no reconciliation logic, and no database queries beyond
what grid.db_adapter.init_schema() already provides. Every operation is a
call into an already-completed Phase 7 module.

Does NOT import:
  grid.worker_runtime   — Step 10
  node_scheduler        — RC-2
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import grid.db_adapter as db_adapter
import grid.dispatcher as dispatcher
import grid.heartbeat_monitor as heartbeat_monitor
import grid.reconciler as reconciler
import grid.registry as registry
from grid.config import MASTER_VERSION, POLL_INTERVAL_SECONDS

log = logging.getLogger("gridmaster.grid.master")

# ── SERVICE NAMES (used consistently across status reporting) ──
SERVICE_DISPATCHER = "dispatcher"
SERVICE_HEARTBEAT  = "heartbeat"
SERVICE_RECONCILER = "reconciler"

_ALL_SERVICES = (SERVICE_DISPATCHER, SERVICE_HEARTBEAT, SERVICE_RECONCILER)

# ── MODULE STATE ──────────────────────────────────────────────
_lock          = threading.RLock()   # re-entrant: start()/stop() call get_runtime_status()
                                      # from within their own lock scope
_running       = False
_started_at:   float | None = None   # monotonic timestamp
_started_at_iso: str | None = None   # wall-clock ISO for display
_last_start_error: str | None = None


# ══════════════════════════════════════════════════════════════
# LIFECYCLE
# ══════════════════════════════════════════════════════════════

def start(dispatcher_interval: int | None = None,
          heartbeat_interval:  int        = 30,
          reconciler_interval: int        = 60) -> dict[str, Any]:
    """
    Start the Grid Master runtime and all its background services.

    Startup sequence (per spec):
      1. Initialize database connection + apply Phase 7 schema migrations
      2. Registry is stateless — no explicit start required, just verified reachable
      3. Start heartbeat monitor
      4. Start dispatcher
      5. Start reconciler

    Safe to call once; calling again while already running is a no-op that
    returns the current status without restarting anything.

    On failure partway through startup, all services started so far are
    stopped and the runtime returns to a clean "not running" state.

    Parameters
    ----------
    dispatcher_interval : override for dispatcher poll interval (default from grid.config)
    heartbeat_interval  : seconds between heartbeat monitor check cycles
    reconciler_interval : seconds between reconciliation passes

    Returns
    -------
    get_runtime_status() dict reflecting the outcome.
    """
    global _running, _started_at, _started_at_iso, _last_start_error

    with _lock:
        if _running:
            log.debug("Grid Master already running — ignoring start()")
            return get_runtime_status()

        _last_start_error = None
        started_services: list[str] = []

        try:
            # Step 1 — Database + schema migrations
            log.info("Grid Master starting: initializing database schema")
            db_adapter.init_schema()

            # Step 2 — Registry is stateless; verify it is reachable
            _verify_registry_reachable()

            # Step 3 — Heartbeat monitor
            log.info("Grid Master starting: heartbeat monitor")
            heartbeat_monitor.start_monitor(interval_seconds=heartbeat_interval)
            started_services.append(SERVICE_HEARTBEAT)

            # Step 4 — Dispatcher
            log.info("Grid Master starting: dispatcher")
            interval = dispatcher_interval if dispatcher_interval is not None else POLL_INTERVAL_SECONDS
            dispatcher.start(poll_interval=interval)
            started_services.append(SERVICE_DISPATCHER)

            # Step 5 — Reconciler
            log.info("Grid Master starting: reconciler")
            reconciler.start(interval_seconds=reconciler_interval)
            started_services.append(SERVICE_RECONCILER)

        except Exception as exc:
            log.error("Grid Master startup failed: %s — rolling back", exc)
            _last_start_error = str(exc)
            _rollback_partial_start(started_services)
            _running = False
            _started_at = None
            _started_at_iso = None
            return get_runtime_status()

        _running         = True
        _started_at      = time.monotonic()
        _started_at_iso  = _now_iso()

    log.info("Grid Master runtime started successfully (version=%s)", MASTER_VERSION)
    return get_runtime_status()


def stop(timeout: float = 10.0) -> dict[str, Any]:
    """
    Gracefully stop the Grid Master runtime and all its background services.

    Shutdown sequence — reverse of startup order:
      1. Stop reconciler
      2. Stop dispatcher
      3. Stop heartbeat monitor

    Safe to call when not running (no-op).

    Parameters
    ----------
    timeout : seconds to wait for each service to stop cleanly

    Returns
    -------
    get_runtime_status() dict reflecting the stopped state.
    """
    global _running, _started_at, _started_at_iso

    with _lock:
        if not _running:
            log.debug("Grid Master not running — ignoring stop()")
            return get_runtime_status()

        log.info("Grid Master stopping: reconciler")
        _safe_stop(reconciler.stop, timeout, SERVICE_RECONCILER)

        log.info("Grid Master stopping: dispatcher")
        _safe_stop(dispatcher.stop, timeout, SERVICE_DISPATCHER)

        log.info("Grid Master stopping: heartbeat monitor")
        _safe_stop(heartbeat_monitor.stop_monitor, timeout, SERVICE_HEARTBEAT)

        _running        = False
        _started_at     = None
        _started_at_iso = None

    log.info("Grid Master runtime stopped")
    return get_runtime_status()


def is_running() -> bool:
    """Return True if the Grid Master runtime is currently started."""
    with _lock:
        return _running


def get_status() -> dict[str, Any]:
    """
    Alias for get_runtime_status(), provided for API naming parity with
    other Grid modules' is_running()/get_status() convention.
    """
    return get_runtime_status()


# ══════════════════════════════════════════════════════════════
# STATUS REPORTING
# ══════════════════════════════════════════════════════════════

def get_runtime_status() -> dict[str, Any]:
    """
    Return a structured snapshot of the Grid Master runtime state.

    Returns
    -------
    {
      "running":     bool,
      "version":     str,
      "started_at":  str | None,   — ISO-8601
      "uptime":      float,        — seconds, 0 if not running
      "services": {
        "dispatcher": "running" | "stopped",
        "heartbeat":  "running" | "stopped",
        "reconciler": "running" | "stopped",
      },
      "last_start_error": str | None,
    }
    """
    with _lock:
        running        = _running
        started_at_iso = _started_at_iso
        uptime         = _uptime()
        last_error     = _last_start_error

    services = {
        SERVICE_DISPATCHER: "running" if dispatcher.is_running() else "stopped",
        SERVICE_HEARTBEAT:  "running" if heartbeat_monitor.is_running() else "stopped",
        SERVICE_RECONCILER: "running" if reconciler.is_running() else "stopped",
    }

    return {
        "running":          running,
        "version":          MASTER_VERSION,
        "started_at":       started_at_iso,
        "uptime":           uptime,
        "services":         services,
        "last_start_error": last_error,
    }


def get_service_health() -> dict[str, Any]:
    """
    Return detailed health information from each orchestrated service,
    by delegating to each service's own status/stats accessors.
    Never recomputes health itself — pure aggregation.

    Returns
    -------
    {
      "dispatcher": {...dispatcher.get_stats()...},
      "reconciler": {...reconciler.get_stats()...},
      "cluster":    {...registry cluster snapshot summary...},
    }
    """
    try:
        dispatcher_stats = dispatcher.get_stats()
    except Exception as exc:
        log.warning("get_service_health: dispatcher.get_stats() failed: %s", exc)
        dispatcher_stats = {"error": str(exc)}

    try:
        reconciler_stats = reconciler.get_stats()
    except Exception as exc:
        log.warning("get_service_health: reconciler.get_stats() failed: %s", exc)
        reconciler_stats = {"error": str(exc)}

    try:
        nodes = registry.get_cluster_snapshot()
        online   = sum(1 for n in nodes if n.get("health") == "online")
        stale    = sum(1 for n in nodes if n.get("health") == "stale")
        offline  = sum(1 for n in nodes if n.get("health") == "offline")
        cluster_summary = {
            "total":   len(nodes),
            "online":  online,
            "stale":   stale,
            "offline": offline,
        }
    except Exception as exc:
        log.warning("get_service_health: registry snapshot failed: %s", exc)
        cluster_summary = {"error": str(exc)}

    return {
        "dispatcher": dispatcher_stats,
        "reconciler": reconciler_stats,
        "cluster":    cluster_summary,
    }


# ══════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ══════════════════════════════════════════════════════════════

def _verify_registry_reachable() -> None:
    """
    Registry (grid.registry) has no explicit start/stop lifecycle — it is
    a stateless module backed directly by node_registry/database. This
    check simply confirms it can be queried without raising, so startup
    fails fast if the database layer beneath it is broken.

    Raises
    ------
    Whatever exception grid.registry.get_all_nodes() raises, unmodified.
    """
    registry.get_all_nodes()   # will raise if DB layer is unreachable


def _safe_stop(stop_fn, timeout: float, service_name: str) -> None:
    """
    Call a service's stop function, catching and logging any exception
    so that one failing service does not prevent the others from
    stopping during shutdown.
    """
    try:
        stop_fn(timeout=timeout)
    except Exception as exc:
        log.error("Error stopping service '%s': %s", service_name, exc)


def _rollback_partial_start(started_services: list[str]) -> None:
    """
    Tear down services that were successfully started before a later
    step in start() failed. Called only from the startup failure path.
    Stops in reverse order of the list passed in.
    """
    for service in reversed(started_services):
        try:
            if service == SERVICE_RECONCILER:
                reconciler.stop(timeout=5.0)
            elif service == SERVICE_DISPATCHER:
                dispatcher.stop(timeout=5.0)
            elif service == SERVICE_HEARTBEAT:
                heartbeat_monitor.stop_monitor(timeout=5.0)
        except Exception as exc:
            log.error("Rollback: error stopping '%s': %s", service, exc)


def _uptime() -> float:
    """Return current uptime in seconds, or 0.0 if not running."""
    if _started_at is None:
        return 0.0
    return round(time.monotonic() - _started_at, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _reset_for_tests() -> None:
    """
    TESTING ONLY — force-stop everything and clear runtime state
    regardless of current status. Does not raise if services were
    never started.
    """
    global _running, _started_at, _started_at_iso, _last_start_error

    for stop_fn in (reconciler.stop, dispatcher.stop, heartbeat_monitor.stop_monitor):
        try:
            stop_fn(timeout=2.0)
        except Exception:
            pass

    with _lock:
        _running          = False
        _started_at       = None
        _started_at_iso   = None
        _last_start_error = None
