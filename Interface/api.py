"""
interface/api.py — Grid Master OS Phase 5/6
Flask REST API. Uses require_permission() for RBAC on all routes.
Thin adapter — all execution delegates to common.py → kernel.run_task().
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, request, jsonify, g
from interface        import common
from interface.command_registry import dispatch, list_commands
import database as _db

# Security layer (Phase 6)
try:
    from security.authorization import require_permission
    from security.permissions   import Action
    from security.middleware    import register_middleware, sanitise_request_body
    _SEC = True
except ImportError:
    _SEC = False
    # Fallback no-op decorator if security package absent
    def require_permission(action):
        def decorator(fn): return fn
        return decorator
    class Action:
        RUN_TASK=VIEW_STATUS=VIEW_PROJECTS=VIEW_NODES=VIEW_AGENTS=""
        VIEW_MEMORY=VIEW_DB_STATS=VIEW_COMMANDS=EXEC_COMMAND=""


def create_app() -> Flask:
    app = Flask(__name__, static_folder=None)
    app.config["JSON_SORT_KEYS"] = False

    if _SEC:
        register_middleware(app)

    # ── POST /run ─────────────────────────────────────────────
    @app.route("/run", methods=["POST"])
    @require_permission(Action.RUN_TASK)
    def run_task_route():
        body = request.get_json(force=True, silent=True) or {}
        if _SEC:
            try:
                body = sanitise_request_body(body)
            except ValueError as e:
                return jsonify({"status": "error", "error": str(e)}), 400

        args_dict, error = common.validate(
            title          = body.get("title", ""),
            input_data     = body.get("input_data", ""),
            project_id     = body.get("project_id"),
            priority       = body.get("priority", 5),
            max_iterations = body.get("max_iterations", 100),
        )
        if error:
            return jsonify({"status": "error", "error": error}), 400

        result      = common.run(**args_dict)
        status_code = 200
        if (result.get("status") == "error"
                and "does not exist" in (result.get("error") or "")):
            status_code = 400
        return jsonify(result), status_code

    # ── GET /status ───────────────────────────────────────────
    @app.route("/status", methods=["GET"])
    @require_permission(Action.VIEW_STATUS)
    def status_route():
        try:
            nodes  = _db.list_all_nodes()
            online = sum(1 for n in nodes if n.get("status") == "online")
            return jsonify({
                "status":       "ok",
                "version":      "1.0.0",
                "phase":        "Phase 6 — Security Layer",
                "database":     _db.db_stats(),
                "nodes_total":  len(nodes),
                "nodes_online": online,
            }), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── GET /commands ─────────────────────────────────────────
    @app.route("/commands", methods=["GET"])
    @require_permission(Action.VIEW_COMMANDS)
    def commands_route():
        return jsonify({"status": "ok", "commands": list_commands()}), 200

    # ── POST /command ─────────────────────────────────────────
    @app.route("/command", methods=["POST"])
    @require_permission(Action.EXEC_COMMAND)
    def command_route():
        body    = request.get_json(force=True, silent=True) or {}
        command = body.get("command", "")
        if not command:
            return jsonify({"status": "error",
                            "error": "command field is required"}), 400
        kwargs = {k: v for k, v in body.items() if k != "command"}
        result = dispatch(command, **kwargs)
        return jsonify(result), 200 if result.get("status") == "ok" else 400

    # ── GET /projects ─────────────────────────────────────────
    @app.route("/projects", methods=["GET"])
    @require_permission(Action.VIEW_PROJECTS)
    def projects_route():
        try:
            return jsonify({"status": "ok",
                            "projects": _db.list_projects(status="active")}), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── GET /nodes ────────────────────────────────────────────
    @app.route("/nodes", methods=["GET"])
    @require_permission(Action.VIEW_NODES)
    def nodes_route():
        try:
            return jsonify({"status": "ok",
                            "nodes": _db.list_all_nodes()}), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── GET /agents ───────────────────────────────────────────
    @app.route("/agents", methods=["GET"])
    @require_permission(Action.VIEW_AGENTS)
    def agents_route():
        try:
            return jsonify({"status": "ok",
                            "agents": _db.get_active_agents()}), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── GET /memory/stats ─────────────────────────────────────
    @app.route("/memory/stats", methods=["GET"])
    @require_permission(Action.VIEW_MEMORY)
    def memory_stats_route():
        try:
            pid = request.args.get("project_id", type=int)
            return jsonify({"status": "ok",
                            "memory": _db.memory_stats_counts(project_id=pid)}), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── GET /db/stats ─────────────────────────────────────────
    @app.route("/db/stats", methods=["GET"])
    @require_permission(Action.VIEW_DB_STATS)
    def db_stats_route():
        try:
            return jsonify({"status": "ok", "stats": _db.db_stats()}), 200
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    # ── Error handlers ────────────────────────────────────────
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"status": "error", "error": str(e)}), 400

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"status": "error",
                        "error": f"Route not found: {request.path}"}), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        return jsonify({"status": "error",
                        "error": f"Method {request.method} not allowed"}), 405

    @app.errorhandler(500)
    def internal_error(e):
        return jsonify({"status": "error",
                        "error": "Internal server error"}), 500

    return app


if __name__ == "__main__":
    port = int(os.environ.get("GRIDMASTER_PORT", 8000))
    app  = create_app()
    print(f"[API] Grid Master OS API starting on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
