#!/usr/bin/env python3
"""
opencode_bridge.py — Bridge between Hermes and opencode sessions.

Hermes calls this script via the terminal tool to:
  - List pending opencode sessions waiting for input
  - Approve/reject permissions
  - Answer questions
  - Continue or dismiss sessions
  - List and interact with opencode sessions via short IDs

This script also understands WhatsApp-style slash replies:
  /ok <token>
  /no <token> [reason]
  /say <token> <message>
  /skip <token>

It also supports /oc session commands:
  /oc list [--json]
  /oc show <id>
  /oc status <command_id>
  /oc <id> <prompt>

The script reads/writes a shared SQLite database that the opencode plugin
also reads/writes. The plugin polls the commands table and executes
actions via input.client (permission.reply, session.prompt, etc.).

Usage:
  opencode_bridge.py pending
      List pending correlations (sessions waiting for input).

  opencode_bridge.py approve <token>
      Alias for /ok. Approve a permission request.

  opencode_bridge.py reject <token> [message]
      Alias for /no. Reject a permission request with an optional message.

  opencode_bridge.py answer <token> <answer>
      Alias for /say. Answer a question from an opencode session.

  opencode_bridge.py continue <token> <message>
      Alias for /say. Send a prompt to continue an opencode session.

  opencode_bridge.py /ok <token>
      Approve a permission request.

  opencode_bridge.py /no <token> [reason]
      Reject a permission request with an optional reason.

  opencode_bridge.py /say <token> <message>
      Send a verbatim prompt/answer to the session.

  opencode_bridge.py /skip <token>
      Dismiss the notification without sending any input.

  opencode_bridge.py status <token>
      Check the status of the latest command for a token.

  opencode_bridge.py resolve <token>
      Show the correlation details for a token (session ID, event type, etc.).

  opencode_bridge.py /oc list [--json]
      List active opencode sessions by short ID.

  opencode_bridge.py /oc show <id>
      Show the latest assistant message for a session.

  opencode_bridge.py   /oc status <short_id>
      Check the status of the latest command queued for session #<short_id>.

  opencode_bridge.py /oc <id> <prompt>
      Send a prompt to an opencode session by short ID.

Database: ~/.hermes/plugins/opencode-hermes-commands/state.db (shared with the plugin)
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = os.environ.get(
    "HERMES_RELAY_DB_PATH",
    str(Path.home() / ".hermes" / "plugins" / "opencode-hermes-commands" / "state.db"),
)

COMMAND_TTL_SECONDS = 300  # commands expire after 5 minutes


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 3000")
    return conn


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS correlations (
            token TEXT PRIMARY KEY,
            opencode_session_id TEXT NOT NULL,
            directory TEXT NOT NULL,
            event_type TEXT NOT NULL,
            request_id TEXT,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS commands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_kind TEXT NOT NULL DEFAULT 'correlation',
            token TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            claimed_at INTEGER,
            result TEXT,
            expires_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands(token, status)"
    )

    # sessions table (for /oc commands)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            short_id INTEGER UNIQUE NOT NULL,
            directory TEXT NOT NULL,
            title TEXT,
            status TEXT,
            parent_id TEXT,
            is_child INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            last_assistant_text TEXT,
            last_activity_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_short_id ON sessions(short_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(deleted, is_child, last_activity_at)")

    # Migration: add target_kind to commands if missing
    try:
        conn.execute("SELECT target_kind FROM commands LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE commands ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'correlation'")

    conn.commit()


def cmd_pending(conn: sqlite3.Connection) -> int:
    """List pending correlations (sessions waiting for input)."""
    now = int(time.time() * 1000)
    rows = conn.execute(
        """
        SELECT token, opencode_session_id, directory, event_type, request_id, created_at
        FROM correlations
        WHERE expires_at > ?
        ORDER BY created_at DESC
        """,
        (now,),
    ).fetchall()

    if not rows:
        print(json.dumps({"pending": [], "message": "no pending opencode sessions"}))
        return 0

    pending = []
    for r in rows:
        # Check if there's already a pending command for this token
        existing = conn.execute(
            "SELECT 1 FROM commands WHERE token = ? AND status = 'pending'",
            (r["token"],),
        ).fetchone()
        if existing:
            continue  # already has a pending command, skip

        pending.append(
            {
                "token": r["token"],
                "session_id": r["opencode_session_id"],
                "directory": r["directory"],
                "event_type": r["event_type"],
                "request_id": r["request_id"],
                "age_seconds": (now - r["created_at"]) // 1000,
            }
        )

    print(json.dumps({"pending": pending}, indent=2))
    return 0


def cmd_oc_list(conn: sqlite3.Connection) -> int:
    """List active opencode sessions by short ID."""
    rows = conn.execute(
        """
        SELECT short_id, title, status, directory, last_activity_at
        FROM sessions
        WHERE deleted = 0 AND is_child = 0
        ORDER BY last_activity_at DESC
        """
    ).fetchall()

    as_json = "--json" in sys.argv

    if as_json:
        sessions = [
            {
                "short_id": r["short_id"],
                "title": r["title"],
                "status": r["status"],
                "directory": r["directory"],
                "last_activity_at": r["last_activity_at"],
            }
            for r in rows
        ]
        print(json.dumps({"sessions": sessions}, indent=2))
        return 0

    if not rows:
        print("No active opencode sessions.")
        return 0

    print("opencode sessions:")
    for r in rows:
        title = r["title"] or "untitled"
        directory_name = Path(r["directory"]).name if r["directory"] else "unknown"
        print(f"  #{r['short_id']}  {r['status'] or 'unknown'}   {directory_name} — {title}")
    return 0


def cmd_oc_show(conn: sqlite3.Connection, session_id: str) -> int:
    """Show the latest assistant message for a session."""
    try:
        short_id = int(session_id)
    except ValueError:
        print(json.dumps({"error": f"invalid session id '{session_id}'", "ok": False}))
        return 1

    row = conn.execute(
        """
        SELECT title, status, last_assistant_text, session_id
        FROM sessions
        WHERE short_id = ? AND deleted = 0
        """,
        (short_id,),
    ).fetchone()

    if not row:
        print(json.dumps({"error": f"No session #{short_id} found.", "ok": False}))
        return 1

    title = row["title"] or "untitled"
    text = row["last_assistant_text"]
    if not text:
        print(f"Session #{short_id} ({title}) has no assistant message yet.")
        return 0

    if len(text) > 1200:
        text = text[:1200] + "..."

    print(f"opencode #{short_id} — {title}")
    print(f"Status: {row['status'] or 'unknown'}")
    print()
    print(text)
    return 0


def create_session_command(conn: sqlite3.Connection, short_id: int, message: str) -> int:
    """Write a session command to the commands table."""
    now = int(time.time() * 1000)
    token = str(short_id)
    target_kind = "session"

    # Check if there's already a pending command for this session.
    existing = conn.execute(
        "SELECT id FROM commands WHERE token = ? AND target_kind = ? AND status = 'pending'",
        (token, target_kind),
    ).fetchone()
    if existing:
        print(
            json.dumps(
                {
                    "error": f"command already pending for session #{short_id}",
                    "command_id": existing["id"],
                    "ok": False,
                }
            )
        )
        return 1

    conn.execute(
        """
        INSERT INTO commands (target_kind, token, action, payload, created_at, status, expires_at)
        VALUES (?, ?, 'continue', ?, ?, 'pending', ?)
        """,
        (
            target_kind,
            token,
            json.dumps({"message": message}),
            now,
            now + COMMAND_TTL_SECONDS * 1000,
        ),
    )
    conn.commit()
    return 0


def cmd_oc_prompt(conn: sqlite3.Connection, session_id: str, message: str) -> int:
    """Send a prompt to an opencode session by short ID."""
    try:
        short_id = int(session_id)
    except ValueError:
        print(json.dumps({"error": f"invalid session id '{session_id}'", "ok": False}))
        return 1

    row = conn.execute(
        "SELECT session_id, directory FROM sessions WHERE short_id = ? AND deleted = 0",
        (short_id,),
    ).fetchone()

    if not row:
        print(json.dumps({"error": f"No session #{short_id} found.", "ok": False}))
        return 1

    create_session_command(conn, short_id, message)

    print(
        f"Queued prompt for session #{short_id}. The opencode plugin will execute it within a few seconds."
    )
    return 0


def create_command(
    conn: sqlite3.Connection, token: str, action: str, payload: dict
) -> int:
    """Write a command to the commands table."""
    now = int(time.time() * 1000)

    # Verify the correlation exists and is not expired.
    corr = conn.execute(
        "SELECT * FROM correlations WHERE token = ? AND expires_at > ?",
        (token, now),
    ).fetchone()
    if not corr:
        print(
            json.dumps(
                {"error": f"no active correlation for token '{token}'", "ok": False}
            )
        )
        return 1

    if action == "skip":
        params = ("correlation", token, action, json.dumps(payload), now, now + COMMAND_TTL_SECONDS * 1000)
        conn.execute(
            """
            INSERT INTO commands (target_kind, token, action, payload, created_at, status, expires_at)
            VALUES (?, ?, ?, ?, ?, 'done', ?)
            """,
            params,
        )
        conn.commit()
        print(
            json.dumps(
                {
                    "ok": True,
                    "token": token,
                    "action": action,
                    "event_type": corr["event_type"],
                    "message": f"token '{token}' skipped.",
                }
            )
        )
        return 0

    # Validate action compatibility with the event type.
    event_type = corr["event_type"]
    valid_actions = {
        "permission.asked": {"approve", "reject"},
        "question.asked": {"answer"},
        "session.idle": {"continue"},
    }
    allowed = valid_actions.get(event_type, set())
    # "continue" and "say" are always allowed for any active session (user may
    # want to send a message even if the event was a permission or question).
    allowed = allowed | {"continue", "say"}
    if action not in allowed:
        print(
            json.dumps(
                {
                    "error": f"action '{action}' not valid for event type '{event_type}'",
                    "valid_actions": sorted(allowed),
                    "ok": False,
                }
            )
        )
        return 1

    # Check if there's already a pending command for this token.
    target_kind = "correlation"
    existing = conn.execute(
        "SELECT id FROM commands WHERE token = ? AND target_kind = ? AND status = 'pending'",
        (token, target_kind),
    ).fetchone()
    if existing:
        print(
            json.dumps(
                {
                    "error": f"command already pending for token '{token}'",
                    "command_id": existing["id"],
                    "ok": False,
                }
            )
        )
        return 1

    conn.execute(
        """
        INSERT INTO commands (target_kind, token, action, payload, created_at, status, expires_at)
        VALUES (?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            target_kind,
            token,
            action,
            json.dumps(payload),
            now,
            now + COMMAND_TTL_SECONDS * 1000,
        ),
    )
    conn.commit()

    print(
        json.dumps(
            {
                "ok": True,
                "token": token,
                "action": action,
                "event_type": event_type,
                "message": f"command '{action}' queued for token '{token}'. "
                "The opencode plugin will execute it within a few seconds.",
            }
        )
    )
    return 0


def cmd_skip(conn: sqlite3.Connection, token: str) -> int:
    return create_command(conn, token, "skip", {})


def cmd_ok(conn: sqlite3.Connection, token: str) -> int:
    return create_command(conn, token, "approve", {"reply": "once"})


def cmd_no(conn: sqlite3.Connection, token: str, message: str = "") -> int:
    return create_command(conn, token, "reject", {"reply": "reject", "message": message})


def cmd_say(conn: sqlite3.Connection, token: str, text: str) -> int:
    return create_command(conn, token, "continue", {"message": text})


def cmd_approve(conn: sqlite3.Connection, token: str) -> int:
    return create_command(conn, token, "approve", {"reply": "once"})


def cmd_reject(conn: sqlite3.Connection, token: str, message: str = "") -> int:
    return create_command(conn, token, "reject", {"reply": "reject", "message": message})


def cmd_answer(conn: sqlite3.Connection, token: str, answer: str) -> int:
    return create_command(conn, token, "answer", {"answer": answer})


def cmd_continue(conn: sqlite3.Connection, token: str, message: str) -> int:
    return create_command(conn, token, "continue", {"message": message})


def cmd_status(conn: sqlite3.Connection, token: str, target_kind: str | None = None) -> int:
    """Check the status of the latest command for a token."""
    if target_kind is None:
        row = conn.execute(
            """
            SELECT id, action, status, result, created_at, claimed_at
            FROM commands
            WHERE token = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (token,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT id, action, status, result, created_at, claimed_at
            FROM commands
            WHERE token = ? AND target_kind = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (token, target_kind),
        ).fetchone()

    if not row:
        print(json.dumps({"token": token, "status": "no_command", "ok": False}))
        return 0

    result = None
    if row["result"]:
        try:
            result = json.loads(row["result"])
        except Exception:
            result = row["result"]

    print(
        json.dumps(
            {
                "token": token,
                "command_id": row["id"],
                "action": row["action"],
                "status": row["status"],
                "result": result,
                "created_at": row["created_at"],
                "claimed_at": row["claimed_at"],
                "ok": row["status"] == "done",
            },
            indent=2,
        )
    )
    return 0


def cmd_resolve(conn: sqlite3.Connection, token: str) -> int:
    """Show correlation details for a token."""
    row = conn.execute(
        "SELECT * FROM correlations WHERE token = ?", (token,)
    ).fetchone()

    if not row:
        print(json.dumps({"error": f"no correlation for token '{token}'", "ok": False}))
        return 1

    print(
        json.dumps(
            {
                "token": row["token"],
                "session_id": row["opencode_session_id"],
                "directory": row["directory"],
                "event_type": row["event_type"],
                "request_id": row["request_id"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "ok": True,
            },
            indent=2,
        )
    )
    return 0


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 1

    action = sys.argv[1]

    if not os.path.exists(DB_PATH):
        print(
            json.dumps(
                {
                    "error": f"database not found at {DB_PATH}",
                    "hint": "the opencode plugin creates this on first run. "
                    "Start an opencode session with the hermes-relay plugin loaded.",
                    "ok": False,
                }
            )
        )
        return 1

    conn = get_db()
    ensure_tables(conn)

    try:
        if action == "/oc":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: /oc list | /oc show <id> | /oc <id> <prompt>", "ok": False}))
                return 1

            subcommand = sys.argv[2]

            if subcommand == "list":
                return cmd_oc_list(conn)

            elif subcommand == "show":
                if len(sys.argv) < 4:
                    print(json.dumps({"error": "usage: /oc show <id>", "ok": False}))
                    return 1
                return cmd_oc_show(conn, sys.argv[3])

            elif subcommand == "status":
                # Show command status for a session command
                if len(sys.argv) < 4:
                    print(json.dumps({"error": "usage: /oc status <short_id>", "ok": False}))
                    return 1
                return cmd_status(conn, sys.argv[3], target_kind="session")

            else:
                # Treat as: /oc <id> <prompt>
                try:
                    session_id = int(subcommand)
                except ValueError:
                    print(json.dumps({"error": f"unknown /oc subcommand '{subcommand}'. Usage: /oc list | /oc show <id> | /oc <id> <prompt>", "ok": False}))
                    return 1
                if len(sys.argv) < 4:
                    print(json.dumps({"error": f"usage: /oc {session_id} <prompt>", "ok": False}))
                    return 1
                message = " ".join(sys.argv[3:])
                return cmd_oc_prompt(conn, session_id, message)

        elif action == "pending":
            return cmd_pending(conn)

        elif action == "approve":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: approve <token>", "ok": False}))
                return 1
            return cmd_approve(conn, sys.argv[2])

        elif action == "reject":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: reject <token> [message]", "ok": False}))
                return 1
            message = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
            return cmd_reject(conn, sys.argv[2], message)

        elif action == "answer":
            if len(sys.argv) < 4:
                print(json.dumps({"error": "usage: answer <token> <answer>", "ok": False}))
                return 1
            answer = " ".join(sys.argv[3:])
            return cmd_answer(conn, sys.argv[2], answer)

        elif action == "continue":
            if len(sys.argv) < 4:
                print(json.dumps({"error": "usage: continue <token> <message>", "ok": False}))
                return 1
            message = " ".join(sys.argv[3:])
            return cmd_continue(conn, sys.argv[2], message)

        elif action == "/ok":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: /ok <token>", "ok": False}))
                return 1
            return cmd_ok(conn, sys.argv[2])

        elif action == "/no":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: /no <token> [reason]", "ok": False}))
                return 1
            message = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
            return cmd_no(conn, sys.argv[2], message)

        elif action == "/say":
            if len(sys.argv) < 4:
                print(json.dumps({"error": "usage: /say <token> <message>", "ok": False}))
                return 1
            message = " ".join(sys.argv[3:])
            return cmd_say(conn, sys.argv[2], message)

        elif action == "/skip":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: /skip <token>", "ok": False}))
                return 1
            return cmd_skip(conn, sys.argv[2])

        elif action == "status":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: status <token>", "ok": False}))
                return 1
            return cmd_status(conn, sys.argv[2])

        elif action == "resolve":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "usage: resolve <token>", "ok": False}))
                return 1
            return cmd_resolve(conn, sys.argv[2])

        else:
            print(json.dumps({"error": f"unknown action '{action}'", "ok": False}))
            print(__doc__)
            return 1

    except sqlite3.Error as e:
        print(json.dumps({"error": f"database error: {e}", "ok": False}))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
