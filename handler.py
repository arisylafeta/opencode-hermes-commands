"""Hermes plugin: /oc slash command for OpenCode session control.

Parses /oc subcommands from WhatsApp and routes them to opencode_bridge.py,
which reads/writes the shared SQLite DB that the OpenCode plugin polls.

Usage:
    /oc list                     List active sessions
    /oc show <id>                Show last AI message for a session
    /oc <id> <prompt>            Send a prompt to a session
    /oc status <id>              Check command status for a session
"""

import logging
import os
import shlex
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

PLUGIN_DIR = Path(__file__).resolve().parent
BRIDGE_CANDIDATES = [
    PLUGIN_DIR / "opencode_bridge.py",
    Path("/usr/local/bin/opencode_bridge.py"),
]
COMMAND_TIMEOUT = 15  # seconds


def resolve_bridge_script() -> Path | None:
    for candidate in BRIDGE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def handle_oc(raw_args: str) -> str | None:
    """Handle the /oc slash command."""
    bridge_script = resolve_bridge_script()
    if bridge_script is None:
        return "Bridge error: opencode_bridge.py not found"

    args = raw_args.strip().split() if raw_args.strip() else []

    if not args:
        return (
            "OC help\n"
            "==========\n"
            "/oc help\n"
            "/oc list\n"
            "/oc show <id>\n"
            "/oc reply <id> <message>\n"
            "/oc questions\n"
            "/oc answer <token> <answer>\n"
            "/oc ok <token>\n"
            "/oc no <token> [reason]\n"
            "/oc kill <id> [id...]\n"
            "/oc status <id>\n"
            "/oc health\n"
            "/oc new [--agent <name>] [--model <provider/model>] [--preset <name>] [--dir <path>] <prompt>"
        )

    try:
        args = shlex.split(raw_args)
    except ValueError as exc:
        return f"Parse error: {exc}"

    cmd = ["python3", str(bridge_script), "/oc", *args]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        return "Command timed out"
    except Exception as exc:
        logger.error("opencode_bridge.py execution failed: %s", exc)
        return f"Bridge error: {exc}"

    output = result.stdout.strip()
    if not output and result.stderr.strip():
        output = result.stderr.strip()

    if output.startswith("{"):
        import json

        try:
            data = json.loads(output)
            if "error" in data:
                return f"Error: {data['error']}"
            if "message" in data:
                return data["message"]
            return output
        except json.JSONDecodeError:
            pass

    return output or "Done"
