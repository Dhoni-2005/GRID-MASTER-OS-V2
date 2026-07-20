"""
interface/cli.py — Grid Master OS Phase 5
Command-line interface. Thin argparse adapter over common.py.
No business logic — delegates everything to common.validate() + common.run().

Usage examples:
    python -m interface.cli --title "Reverse text" --input "hello world"
    python -m interface.cli --title "Build API" --input "1. Route\n2. Test" --priority 8
    python -m interface.cli --command show_nodes
    python -m interface.cli --command list_projects
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interface import common
from interface.command_registry import dispatch, list_commands


# ── PARSER ────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gridmaster",
        description="GRID MASTER OS — Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  gridmaster --title 'Reverse text' --input 'hello world'\n"
            "  gridmaster --title 'Build API' --input '1. Route\\n2. Test' --priority 8\n"
            "  gridmaster --command show_nodes\n"
            "  gridmaster --command list_projects\n"
            "  gridmaster --commands  (list all available commands)\n"
        ),
    )

    # Task submission
    parser.add_argument("--title",      type=str,  default="",   help="Task title (required for run_task)")
    parser.add_argument("--input",      type=str,  default="",   help="Task input data (supports numbered lists)")
    parser.add_argument("--project-id", type=int,  default=None, help="Project ID (auto-created if omitted)")
    parser.add_argument("--priority",   type=int,  default=5,    help="Task priority 1–10 (default: 5)")
    parser.add_argument("--max-iter",   type=int,  default=100,  help="Max coordinator iterations (default: 100)")

    # Registry commands
    parser.add_argument("--command",  type=str, default=None, help="Run a registry command (e.g. show_nodes)")
    parser.add_argument("--commands", action="store_true",    help="List all available registry commands")

    return parser


# ── MAIN ──────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    """
    CLI entry point. Returns 0 on success, 1 on error.
    argv: list of argument strings (defaults to sys.argv[1:]).
    """
    parser = build_parser()
    args   = parser.parse_args(argv)

    # ── List commands ─────────────────────────────────────────
    if args.commands:
        cmds = list_commands()
        print("\nAvailable commands:\n")
        for c in cmds:
            print(f"  --command {c['command']:<20} {c['description']}")
        print()
        return 0

    # ── Registry command dispatch ─────────────────────────────
    if args.command:
        result = dispatch(args.command)
        if result.get("status") == "error":
            print(common.format_error(result.get("error", "Command failed")))
            return 1
        import json
        print(json.dumps(result, indent=2, default=str))
        return 0

    # ── run_task (default action) ─────────────────────────────
    args_dict, error = common.validate(
        title          = args.title,
        input_data     = args.input,
        project_id     = args.project_id,
        priority       = args.priority,
        max_iterations = args.max_iter,
    )
    if error:
        print(common.format_error(error))
        return 1

    result = common.run(**args_dict)
    print(common.format_result(result))
    return 1 if result.get("status") == "error" else 0


if __name__ == "__main__":
    sys.exit(main())
