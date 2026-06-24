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
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

HERMES_HOME = Path(os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes"))
BRIDGE_SCRIPT = Path("/usr/local/bin/opencode_bridge.py")
COMMAND_TIMEOUT = 15  # seconds


def handle_oc(raw_args: str) -> str | None:
    """Handle the /oc slash command.

    Routes to opencode_bridge.py with /oc as the first argument, passing
    the raw_args verbatim as additional arguments.
    """
    if not BRIDGE_SCRIPT.exists():
        return "\u274c opencode_bridge.py not found at /usr/local/bin/"

    args = raw_args.strip().split() if raw_args.strip() else []

    if not args:
        return (
            "\u2139\ufe0f Usage:\n"
            "/oc list - List active sessions\n"
            "/oc show <id> - Show last AI message\n"
            "/oc <id> <prompt> - Send a prompt\n"
            "/oc status <id> - Check command status"
        )

    cmd = ["python3", str(BRIDGE_SCRIPT), "/oc"] + args

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT,
            env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        return "\u23f3 Command timed out"
    except Exception as exc:
        logger.error("opencode_bridge.py execution failed: %s", exc)
        return f"\u274c Bridge error: {exc}"

    output = result.stdout.strip()
    if not output and result.stderr.strip():
        output = result.stderr.strip()

    # If the bridge returned JSON, try to make it human-readable
    if output.startswith("{"):
        import json
        try:
            data = json.loads(output)
            if "error" in data:
                return f"\u274c {data['error']}"
            if "message" in data:
                return f"\u2705 {data['message']}"
            return output
        except json.JSONDecodeError:
            pass

    return output or "\u2705 Done"
