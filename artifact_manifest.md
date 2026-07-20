# Grid Master OS — Artifact Manifest
**Repository state:** Phase 7 Step 3 complete  
**Total canonical files:** 37 Python files  
**Naming convention:** `P<Phase>S<Step>_<package>_<filename>`

---

## Root Package — Core Kernel

| Artifact ID | Repo path | Phase/Step | Status |
|---|---|---|---|
| P1S1_root_database | database.py | P1 S1 | [K] KEEP |
| P2S1_root_memory_manager | memory_manager.py | P2 S1 | [K] KEEP |
| P1S1_root_node_registry | node_registry.py | P1 S1 | [K] KEEP |
| P1S1_root_grid_master | grid_master.py | P1 S1 | [K] KEEP |
| P3S1_root_agent_registry | agent_registry.py | P3 S1 | [K] KEEP |
| P3S2_root_planner | planner.py | P3 S2 | [K] KEEP |
| P3S3_root_worker | worker.py | P3 S3 | [K] KEEP |
| P3S4_root_reviewer | reviewer.py | P3 S4 | [K] KEEP |
| P3S5_root_coordinator | coordinator.py | P3 S5 + P4 S1 | [K] KEEP |
| P4S1_root_scheduler | scheduler.py | P4 S1 | [K] KEEP |
| P4S2_root_node_scheduler | node_scheduler.py | P4 S2 | [K] KEEP — RC-2: never modified |
| P4S3_root_kernel | kernel.py | P4 S3 | [K] KEEP |

---

## interface/ Package

| Artifact ID | Repo path | Phase/Step | Notes |
|---|---|---|---|
| P5S1_interface_init | interface/__init__.py | P5 S1 | [K] KEEP |
| P5S1_interface_common | interface/common.py | P5 S1 | [K] KEEP |
| P5S1_interface_cli | interface/cli.py | P5 S1 | [K] KEEP |
| P6S1_interface_api | interface/api.py | P6 S1 | [K] KEEP — Phase 6 version with RBAC |
| P6S1_interface_auth | interface/auth.py | P6 S1 | [K] KEEP — delegates to security.auth |
| P5S1_interface_command_registry | interface/command_registry.py | P5 S1 | [K] KEEP |
| P5S1_interface_websocket | interface/websocket.py | P5 S1 | [K] KEEP — placeholder |

---

## security/ Package

| Artifact ID | Repo path | Phase/Step | Notes |
|---|---|---|---|
| P6S1_security_init | security/__init__.py | P6 S1 | [K] KEEP |
| P6S1_security_config | security/config.py | P6 S1 | [K] KEEP |
| P6S1_security_auth | security/auth.py | P6 S1 | [K] KEEP |
| P6S1_security_api_keys | security/api_keys.py | P6 S1 | [K] KEEP |
| P6S1_security_audit | security/audit.py | P6 S1 | [K] KEEP |
| P6S1_security_authorization | security/authorization.py | P6 S1 | [K] KEEP |
| P6S1_security_encryption | security/encryption.py | P6 S1 | [K] KEEP |
| P6S1_security_middleware | security/middleware.py | P6 S1 | [K] KEEP |
| P6S1_security_permissions | security/permissions.py | P6 S1 | [K] KEEP |

---

## grid/ Package — Phase 7 Steps 1–3

| Artifact ID | Repo path | Step | Notes |
|---|---|---|---|
| P7S0_grid_init | grid/__init__.py | stub | [K] KEEP — expanded in Step 10 |
| P7S1_grid_config | grid/config.py | 1 | [K] KEEP |
| P7S1_grid_models | grid/models.py | 1 | [K] KEEP |
| P7S1_grid_signing | grid/signing.py | 1 | [K] KEEP — RC-1 |
| P7S1_grid_db_adapter | grid/db_adapter.py | 1 | [K] KEEP — RC-7, RC-8 |
| P7S2_grid_outbox | grid/outbox.py | 2 | [K] KEEP |
| P7S2_grid_client | grid/client.py | 2 | [K] KEEP |
| P7S3_grid_registry | grid/registry.py | 3 | [K] KEEP — RC-5 |
| P7S3_grid_load_balancer | grid/load_balancer.py | 3 | [K] KEEP — RC-2 |

---

## Missing — Steps 4–10 (not yet implemented)

| Repo path | Step |
|---|---|
| grid/failure.py | 4 |
| grid/heartbeat_sender.py | 5 |
| grid/heartbeat_monitor.py | 5 |
| grid/worker_server.py | 6 |
| grid/dispatcher.py | 7 |
| grid/memory_sync.py | 8 |
| grid/reconciler.py | 8 |
| grid/master.py | 9 |
| grid/worker_runtime.py | 10 |

---

## Deleted from Outputs (do not upload to GitHub)

| Filename | Reason |
|---|---|
| api.py (flat) | Superseded by P6S1_interface_api |
| auth.py (flat) | Superseded by P6S1_interface_auth |
| interface_api.py | Workaround name — use interface/api.py |
| interface_auth.py | Workaround name — use interface/auth.py |
| security__init__.py | Workaround name — use security/__init__.py |
| security_*.py (×9) | Workaround names — use security/X.py |
| analytics.py | Foreign — not part of this project |
| benchmark.py | Foreign |
| dashboard*.py (×4) | Foreign |
| job_queue.py | Foreign |
| master.py | Foreign |
| master_upgraded.py | Foreign |
| master_v3.py | Foreign |
| node_manager.py | Foreign |
