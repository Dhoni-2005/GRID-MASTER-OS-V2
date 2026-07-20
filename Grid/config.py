"""
grid/config.py — Grid Master OS Phase 7
All distributed runtime configuration from environment variables.
No secrets stored in code. Zero side effects on import.
"""
import os
import logging

log = logging.getLogger("gridmaster.grid.config")

# ── MASTER CONNECTIVITY ───────────────────────────────────────
MASTER_URL: str = os.environ.get("GRIDMASTER_MASTER_URL", "")

# ── NODE IDENTITY ─────────────────────────────────────────────
NODE_ID: str = os.environ.get("GRIDMASTER_NODE_ID", "")
PLATFORM: str = os.environ.get("GRIDMASTER_PLATFORM", "local")  # render|huggingface|local
_raw_caps = os.environ.get("GRIDMASTER_CAPABILITIES", "general")
NODE_CAPABILITIES: list[str] = [c.strip() for c in _raw_caps.split(",") if c.strip()]

# ── SECRETS ───────────────────────────────────────────────────
NODE_SECRET: str = os.environ.get("GRIDMASTER_NODE_SECRET", "")

# ── HEARTBEAT THRESHOLDS ──────────────────────────────────────
HEARTBEAT_INTERVAL_SECONDS: int = int(os.environ.get("GRIDMASTER_HB_INTERVAL", 20))
HEARTBEAT_STALE_SECONDS: int = int(os.environ.get("GRIDMASTER_HB_STALE", 60))
HEARTBEAT_OFFLINE_SECONDS: int = int(os.environ.get("GRIDMASTER_HB_OFFLINE", 120))
HF_HEARTBEAT_STALE_SECONDS: int = int(os.environ.get("GRIDMASTER_HF_HB_STALE", 180))

# ── WORKER POLLING ────────────────────────────────────────────
POLL_INTERVAL_SECONDS: int = int(os.environ.get("GRIDMASTER_POLL_INTERVAL", 5))
POLL_JITTER_SECONDS: int = int(os.environ.get("GRIDMASTER_POLL_JITTER", 10))

# ── DISPATCH ──────────────────────────────────────────────────
DISPATCH_TIMEOUT_SECONDS: int = int(os.environ.get("GRIDMASTER_DISPATCH_TIMEOUT", 30))
MAX_REASSIGNMENTS: int = int(os.environ.get("GRIDMASTER_MAX_REASSIGN", 3))
BATCH_SIZE: int = int(os.environ.get("GRIDMASTER_BATCH_SIZE", 10))

# ── NODE MANAGEMENT ───────────────────────────────────────────
QUARANTINE_MINUTES: int = int(os.environ.get("GRIDMASTER_QUARANTINE_MINS", 10))
DISPATCH_FAILURE_THRESHOLD: int = int(os.environ.get("GRIDMASTER_FAILURE_THRESHOLD", 3))

# ── OUTBOX ────────────────────────────────────────────────────
OUTBOX_MAX_ENTRIES: int = int(os.environ.get("GRIDMASTER_OUTBOX_MAX", 1000))
OUTBOX_PATH: str = os.environ.get("GRIDMASTER_OUTBOX_PATH", "gridmaster_outbox.db")
OUTBOX_FLUSH_BATCH: int = 20

# ── WORKER HTTP SERVER ────────────────────────────────────────
WORKER_PORT: int = int(os.environ.get("GRIDMASTER_WORKER_PORT", 8001))

# ── DATABASE ──────────────────────────────────────────────────
DATABASE_BACKEND: str = os.environ.get("GRIDMASTER_DB_BACKEND", "sqlite")  # sqlite|postgresql
POSTGRES_URL: str = os.environ.get("GRIDMASTER_POSTGRES_URL", "")

# ── RATE LIMITING ─────────────────────────────────────────────
NODE_RPM_LIMIT: int = int(os.environ.get("GRIDMASTER_NODE_RPM", 10))
GLOBAL_RPM_LIMIT: int = int(os.environ.get("GRIDMASTER_GLOBAL_RPM", 60))

# ── MASTER VERSION ────────────────────────────────────────────
MASTER_VERSION: str = "1.0.0-phase7"


def validate_worker_config() -> None:
    """
    Validate that all required configuration is present for worker startup.
    Raises RuntimeError listing every missing value.
    Called once at worker process start.
    """
    missing: list[str] = []
    if not MASTER_URL:
        missing.append("GRIDMASTER_MASTER_URL")
    if not NODE_SECRET:
        missing.append("GRIDMASTER_NODE_SECRET")
    if PLATFORM not in ("render", "huggingface", "local"):
        missing.append(f"GRIDMASTER_PLATFORM (got '{PLATFORM}'; must be render|huggingface|local)")
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def validate_master_config() -> None:
    """
    Validate that all required configuration is present for master startup.
    Raises RuntimeError listing every missing value.
    """
    missing: list[str] = []
    if not NODE_SECRET:
        missing.append("GRIDMASTER_NODE_SECRET")
    if DATABASE_BACKEND == "postgresql" and not POSTGRES_URL:
        missing.append("GRIDMASTER_POSTGRES_URL")
    if missing:
        raise RuntimeError(
            f"Missing required environment variables for master: {', '.join(missing)}"
        )
