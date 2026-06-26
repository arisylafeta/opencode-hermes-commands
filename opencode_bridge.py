#!/usr/bin/env python3
"""
opencode_bridge.py — Bridge between Hermes and opencode sessions.

Hermes calls this script via the terminal tool to:
  - List pending opencode sessions waiting for input
  - Approve/reject permissions
  - Answer questions
  - Continue or dismiss sessions
  - List and interact with opencode sessions via short IDs

This script also understands slash-command replies:
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
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB_PATH = os.environ.get(
    "HERMES_RELAY_DB_PATH",
    str(Path.home() / ".hermes" / "plugins" / "opencode-hermes-commands" / "state.db"),
)
OPENCODE_DB_PATH = os.environ.get(
    "OPENCODE_DB_PATH",
    str(Path.home() / ".local" / "share" / "opencode" / "opencode.db"),
)

COMMAND_TTL_SECONDS = 300  # commands expire after 5 minutes
QUESTION_CORRELATION_TTL_SECONDS = 24 * 60 * 60  # unanswered questions stay visible for a day
SYSTEM_COMMAND_TOKEN = "__global__"


def format_separator() -> str:
    return "━━━━━━━━━━"


def format_oc_help() -> str:
    return "\n".join(
        [
            "🤖 OpenCode controls",
            format_separator(),
            "📂 Sessions",
            "• /oc list               (live/blocking only)",
            "• /oc list --all         (recent history)",
            "• /oc ps                 (actual RAM-using processes)",
            "• /oc show <id>",
            "• /oc reply <id> <message>",
            "• /oc kill <id> [id...]",
            "• /oc status <id>",
            "",
            "❓ Questions / approvals",
            "• /oc questions",
            "• /oc answer <id|token> <answer>",
            "• /oc ok <token>",
            "• /oc no <token> [reason]",
            "",
            "✨ New session",
            "• /oc new [--agent <name>] [--model <provider/model>] [--preset <name>] [--dir <path>] <prompt>",
            "",
            "💬 Examples",
            "• /oc reply 21 keep going, focus on the auth bug",
            "• /oc kill 21 24 28",
            "• /oc new --preset cheap-flex audit this repo for dead code",
            "• /oc new --agent fixer --dir projects/rebattery-enrich add a healthcheck endpoint",
        ]
    )


def compact_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def format_relative_age(timestamp_ms: int | None) -> str:
    if not timestamp_ms:
        return "unknown"
    seconds = max(0, int(time.time()) - int(timestamp_ms // 1000))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h {minutes % 60}m ago"
    days = hours // 24
    return f"{days}d {hours % 24}h ago"


def live_opencode_processes() -> list[dict]:
    """Return live OpenCode parent processes and child language servers.

    This is intentionally OS/process based, not OpenCode DB based. DB sessions are
    resumable history; processes are what consume RAM now.
    """
    processes: list[dict] = []
    proc_root = Path("/proc")
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace").strip()
            if not cmdline:
                continue
            if "opencode" not in cmdline:
                continue
            # Exclude this bridge/grep shell if its command text mentions opencode.
            comm = (entry / "comm").read_text(errors="replace").strip()
            if comm not in {"opencode", "node"} and "opencode run" not in cmdline:
                continue
            stat = (entry / "stat").read_text().split()
            ppid = int(stat[3])
            rss_mb = int(stat[23]) * os.sysconf("SC_PAGE_SIZE") // 1024 // 1024
            cwd = os.readlink(entry / "cwd")
            session_match = re.search(r"--session\s+(ses_[A-Za-z0-9]+)", cmdline)
            processes.append(
                {
                    "pid": pid,
                    "ppid": ppid,
                    "comm": comm,
                    "rss_mb": rss_mb,
                    "cwd": cwd,
                    "cmd": compact_whitespace(cmdline),
                    "session_id": session_match.group(1) if session_match else None,
                    "is_parent": comm == "opencode",
                }
            )
        except Exception:
            continue
    return sorted(processes, key=lambda p: (not p["is_parent"], -int(p["rss_mb"])))


def live_opencode_session_ids() -> set[str]:
    return {str(p["session_id"]) for p in live_opencode_processes() if p.get("session_id")}


def format_status_badge(status: str | None) -> str:
    value = (status or "unknown").lower()
    if value == "busy":
        return "🟡 busy"
    if value == "question":
        return "❓ question"
    if value == "idle":
        return "🟢 idle"
    return f"⚪ {value}"


def format_command_status_badge(status: str | None) -> str:
    value = (status or "unknown").lower()
    if value == "done":
        return "✅ done"
    if value == "pending":
        return "🕓 pending"
    if value == "claimed":
        return "🏃 claimed"
    if value == "failed":
        return "❌ failed"
    if value == "expired":
        return "⌛ expired"
    return f"⚪ {value}"


def open_opencode_db() -> sqlite3.Connection | None:
    """Open OpenCode's DB read-only. This DB is the source of truth."""
    path = Path(OPENCODE_DB_PATH)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1.0)
    conn.row_factory = sqlite3.Row
    return conn


def json_loads_maybe(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def get_question_message(details: dict) -> str:
    if details.get("message"):
        return str(details["message"])
    lines: list[str] = []
    questions = details.get("questions") if isinstance(details.get("questions"), list) else []
    for idx, q in enumerate(questions, 1):
        if not isinstance(q, dict):
            continue
        header = q.get("header") or f"Question {idx}"
        question = q.get("question") or ""
        lines.append(f"{idx}. {header}")
        if question:
            lines.append(str(question))
        options = q.get("options") if isinstance(q.get("options"), list) else []
        if options:
            lines.append("Options:")
            for opt in options:
                if isinstance(opt, dict):
                    label = opt.get("label") or ""
                    desc = opt.get("description") or ""
                    suffix = f" - {desc}" if desc else ""
                    lines.append(f"- {label}{suffix}")
                else:
                    lines.append(f"- {opt}")
    return "\n".join(lines) if lines else "Question asked"


def details_from_question_part(part_data: dict) -> dict:
    state = part_data.get("state") if isinstance(part_data.get("state"), dict) else {}
    input_data = state.get("input") if isinstance(state.get("input"), dict) else {}
    questions = input_data.get("questions") if isinstance(input_data.get("questions"), list) else []
    details = {
        "questions": questions,
        "tool": {
            "messageID": part_data.get("messageID") or part_data.get("message_id"),
            "callID": part_data.get("callID"),
        },
    }
    details["message"] = get_question_message(details)
    return details


def active_opencode_questions() -> list[dict]:
    """Return running OpenCode question tool parts from the real OpenCode DB.

    Avoid whole-DB JSON LIKE scans. OpenCode has an index on part.session_id, so
    scan recent sessions first, then inspect recent parts for each session.
    """
    oc = open_opencode_db()
    if not oc:
        return []
    try:
        sessions = oc.execute(
            """
            SELECT id, title, directory, time_updated AS session_updated
            FROM session
            WHERE (time_archived IS NULL OR time_archived = 0)
            ORDER BY time_updated DESC
            LIMIT 100
            """
        ).fetchall()
        rows = []
        for s in sessions:
            parts = oc.execute(
                """
                SELECT id AS part_id, message_id, session_id, time_created, time_updated, data
                FROM part
                WHERE session_id = ?
                ORDER BY time_updated DESC
                LIMIT 300
                """,
                (s["id"],),
            ).fetchall()
            for p in parts:
                rows.append((s, p))
    finally:
        oc.close()

    questions: list[dict] = []
    for session_row, row in rows:
        data = json_loads_maybe(row["data"])
        if data.get("type") != "tool" or data.get("tool") != "question":
            continue
        state = data.get("state") if isinstance(data.get("state"), dict) else {}
        if state.get("status") not in {"running", "pending"}:
            continue
        input_data = state.get("input") if isinstance(state.get("input"), dict) else {}
        if not input_data.get("questions"):
            # OpenCode briefly emits a pending empty shell before the question body arrives.
            continue
        details = details_from_question_part(data)
        questions.append(
            {
                "session_id": row["session_id"],
                "title": session_row["title"],
                "directory": session_row["directory"],
                "part_id": row["part_id"],
                "message_id": row["message_id"],
                "call_id": data.get("callID"),
                "time_created": row["time_created"],
                "time_updated": row["time_updated"],
                "session_updated": session_row["session_updated"],
                "details": details,
            }
        )
    return questions


def ensure_session_alias(conn: sqlite3.Connection, *, session_id: str, directory: str, title: str | None, status: str | None, last_activity_at: int | None, parent_id: str | None = None) -> int:
    now = int(time.time() * 1000)
    conn.execute(
        """
        INSERT OR IGNORE INTO sessions
        (session_id, short_id, directory, title, status, parent_id, is_child, deleted, last_activity_at, created_at, updated_at)
        VALUES (?, (SELECT COALESCE(MAX(short_id), 0) + 1 FROM sessions), ?, ?, ?, ?, ?, 0, ?, ?, ?)
        """,
        (session_id, directory or "", title, status or "unknown", parent_id, 1 if parent_id else 0, last_activity_at or now, now, now),
    )
    conn.execute(
        """
        UPDATE sessions
        SET directory = COALESCE(NULLIF(?, ''), directory), title = COALESCE(?, title),
            status = COALESCE(?, status), parent_id = COALESCE(?, parent_id),
            is_child = ?, deleted = 0, last_activity_at = COALESCE(?, last_activity_at), updated_at = ?
        WHERE session_id = ?
        """,
        (directory or "", title, status, parent_id, 1 if parent_id else 0, last_activity_at, now, session_id),
    )
    row = conn.execute("SELECT short_id FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
    return int(row["short_id"])


def ensure_question_correlation(conn: sqlite3.Connection, question: dict) -> sqlite3.Row | None:
    """Refresh or create relay token for a live OpenCode question.

    If OpenCode's event-created correlation exists, keep its token/request_id and
    extend expiry. If no request_id exists, create a visible token anyway, but it
    cannot be answered until a request_id-bearing event correlation appears.
    """
    now = int(time.time() * 1000)
    expires = now + QUESTION_CORRELATION_TTL_SECONDS * 1000
    session_id = question["session_id"]
    call_id = question.get("call_id")
    details = question.get("details") or {}
    details_json = json.dumps(details)

    candidates = conn.execute(
        """
        SELECT * FROM correlations
        WHERE opencode_session_id = ? AND event_type IN ('question.asked', 'question.v2.asked')
        ORDER BY created_at DESC
        """,
        (session_id,),
    ).fetchall()
    chosen = None
    for row in candidates:
        row_details = json_loads_maybe(row["details"])
        row_call = ((row_details.get("tool") or {}).get("callID") if isinstance(row_details.get("tool"), dict) else None)
        if call_id and row_call == call_id:
            chosen = row
            break
    if not chosen and candidates:
        # Prefer the newest request-id-bearing correlation for the session.
        chosen = next((r for r in candidates if r["request_id"]), candidates[0])

    if chosen:
        conn.execute(
            "UPDATE correlations SET details = COALESCE(?, details), expires_at = ? WHERE token = ?",
            (details_json, expires, chosen["token"]),
        )
        conn.commit()
        return conn.execute("SELECT * FROM correlations WHERE token = ?", (chosen["token"],)).fetchone()

    token_seed = question.get("part_id") or session_id
    token = "q_" + re.sub(r"[^A-Za-z0-9]", "", token_seed)[-10:]
    conn.execute(
        """
        INSERT OR REPLACE INTO correlations
        (token, opencode_session_id, directory, event_type, request_id, details, created_at, expires_at)
        VALUES (?, ?, ?, 'question.asked', NULL, ?, ?, ?)
        """,
        (token, session_id, question.get("directory") or "", details_json, now, expires),
    )
    conn.commit()
    return conn.execute("SELECT * FROM correlations WHERE token = ?", (token,)).fetchone()


def sync_active_questions(conn: sqlite3.Connection) -> list[dict]:
    questions = active_opencode_questions()
    synced: list[dict] = []
    for q in questions:
        short_id = ensure_session_alias(
            conn,
            session_id=q["session_id"],
            directory=q.get("directory") or "",
            title=q.get("title"),
            status="question",
            last_activity_at=q.get("time_updated") or q.get("session_updated"),
        )
        corr = ensure_question_correlation(conn, q)
        q["short_id"] = short_id
        q["token"] = corr["token"] if corr else None
        q["request_id"] = corr["request_id"] if corr else None
        synced.append(q)
    return synced


def recent_opencode_sessions(limit: int = 50) -> list[dict]:
    oc = open_opencode_db()
    if not oc:
        return []
    try:
        rows = oc.execute(
            """
            SELECT id, parent_id, title, directory, time_created, time_updated, time_archived,
                   agent, model, cost, tokens_input, tokens_output
            FROM session
            WHERE (time_archived IS NULL OR time_archived = 0)
            ORDER BY time_updated DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        oc.close()
    return [dict(r) for r in rows]


def infer_opencode_statuses(session_ids: list[str], question_session_ids: set[str]) -> dict[str, str]:
    """Infer session status from OpenCode parts when relay status is absent.

    OpenCode DB does not store a single status column. We infer the practical UI
    status from active tool parts and final step-finish markers.
    """
    statuses: dict[str, str] = {}
    oc = open_opencode_db()
    if not oc:
        return {sid: ("question" if sid in question_session_ids else "unknown") for sid in session_ids}
    try:
        for sid in session_ids:
            if sid in question_session_ids:
                statuses[sid] = "question"
                continue
            rows = oc.execute(
                """
                SELECT data
                FROM part
                WHERE session_id = ?
                ORDER BY time_updated DESC
                LIMIT 50
                """,
                (sid,),
            ).fetchall()
            status = "idle"
            for row in rows:
                data = json_loads_maybe(row["data"])
                typ = data.get("type")
                if typ == "tool":
                    raw_state = data.get("state")
                    state = raw_state if isinstance(raw_state, dict) else {}
                    tool_status = state.get("status")
                    tool = data.get("tool")
                    if tool == "question" and tool_status in {"running", "pending"}:
                        status = "question"
                        break
                    if tool_status in {"running", "pending"}:
                        status = "busy"
                        break
                if typ == "step-finish":
                    status = "idle"
                    break
            statuses[sid] = status
    finally:
        oc.close()
    return statuses


def sync_recent_sessions(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    questions_by_session = {q["session_id"]: q for q in sync_active_questions(conn)}
    rows = recent_opencode_sessions(limit)
    root_rows = [row for row in rows if not row.get("parent_id")]
    inferred_status = infer_opencode_statuses([row["id"] for row in root_rows], set(questions_by_session))
    synced: list[dict] = []
    for row in root_rows:
        status = inferred_status.get(row["id"], "unknown")
        relay = conn.execute("SELECT status FROM sessions WHERE session_id = ?", (row["id"],)).fetchone()
        if status == "unknown" and relay and relay["status"]:
            status = relay["status"]
        short_id = ensure_session_alias(
            conn,
            session_id=row["id"],
            directory=row.get("directory") or "",
            title=row.get("title"),
            status=status,
            last_activity_at=row.get("time_updated"),
            parent_id=row.get("parent_id"),
        )
        row["short_id"] = short_id
        row["status"] = status
        synced.append(row)
    conn.commit()
    return synced


def resolve_session_by_short_id(conn: sqlite3.Connection, short_id: int) -> sqlite3.Row | None:
    sync_recent_sessions(conn, 100)
    return conn.execute(
        "SELECT * FROM sessions WHERE short_id = ? AND deleted = 0",
        (short_id,),
    ).fetchone()


def latest_assistant_text_from_opencode(session_id: str) -> str | None:
    oc = open_opencode_db()
    if not oc:
        return None
    try:
        rows = oc.execute(
            """
            SELECT data
            FROM part
            WHERE session_id = ?
            ORDER BY time_updated DESC
            LIMIT 200
            """,
            (session_id,),
        ).fetchall()
    finally:
        oc.close()
    chunks: list[str] = []
    for row in rows:
        data = json_loads_maybe(row["data"])
        if data.get("type") == "text" and data.get("text"):
            chunks.append(str(data["text"]))
            if len("\n".join(chunks)) > 4000:
                break
    return "\n\n".join(reversed(chunks)) if chunks else None


def active_questions_for_session(conn: sqlite3.Connection, session_id: str) -> list[dict]:
    return [q for q in sync_active_questions(conn) if q.get("session_id") == session_id]


def parse_new_command_args(argv: list[str]) -> tuple[dict, str | None]:
    opts: dict[str, str | None] = {
        "agent": None,
        "model": None,
        "preset": None,
        "dir": None,
    }
    prompt_parts: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        if token in {"--agent", "--model", "--preset", "--dir"}:
            if i + 1 >= len(argv):
                return {}, f"missing value for {token}"
            opts[token[2:]] = argv[i + 1]
            i += 2
            continue
        prompt_parts = argv[i:]
        break

    prompt = compact_whitespace(" ".join(prompt_parts))
    if not prompt:
        return {}, "missing prompt"
    return opts, prompt


def infer_default_directory(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT directory
        FROM sessions
        WHERE deleted = 0
        ORDER BY last_activity_at DESC, updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row and row["directory"]:
        return str(row["directory"])
    return os.getcwd()


def launch_opencode_run(
    *,
    directory: str,
    message: str,
    session_id: str | None = None,
    agent: str | None = None,
    model: str | None = None,
    preset: str | None = None,
    title: str | None = None,
) -> tuple[int | None, str | None]:
    target_dir = str(Path(directory).expanduser())
    if not Path(target_dir).exists():
        return None, f"directory not found: {target_dir}"

    final_message = message
    if preset:
        final_message = f"/preset {preset}\n{message}"

    args = ["opencode", "run", "--dir", target_dir]
    if session_id:
        args.extend(["--session", session_id])
    elif title:
        args.extend(["--title", title])

    if agent:
        args.extend(["--agent", agent])
    if model:
        args.extend(["--model", model])

    args.append(final_message)

    try:
        child = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env={**os.environ},
        )
        return child.pid, None
    except Exception as exc:
        return None, str(exc)


def parse_kill_ids(argv: list[str]) -> tuple[list[int], str | None]:
    tokens: list[str] = []
    for arg in argv:
        pieces = [piece.strip() for piece in arg.split(",")]
        tokens.extend(piece for piece in pieces if piece)

    if not tokens:
        return [], "missing session ids"

    ids: list[int] = []
    invalid: list[str] = []
    for token in tokens:
        try:
            ids.append(int(token))
        except ValueError:
            invalid.append(token)

    if invalid:
        return [], f"invalid session id(s): {', '.join(invalid)}"

    deduped = list(dict.fromkeys(ids))
    return deduped, None


def get_db() -> sqlite3.Connection:
    db_path = Path(DB_PATH).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=3)
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
            details TEXT,
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

    # Migration: add question/permission details to correlations if missing
    try:
        conn.execute("SELECT details FROM correlations LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE correlations ADD COLUMN details TEXT")

    conn.commit()


def cmd_pending(conn: sqlite3.Connection) -> int:
    """List pending sessions/questions waiting for input.

    OpenCode DB is the source of truth for active questions; relay DB is used
    for tokens and command state.
    """
    active_questions = sync_active_questions(conn)
    active_question_tokens = {str(q.get("token")) for q in active_questions if q.get("token")}
    now = int(time.time() * 1000)
    params: list[object] = [now]
    active_clause = ""
    if active_question_tokens:
        placeholders = ",".join("?" for _ in active_question_tokens)
        active_clause = f" OR token IN ({placeholders})"
        params.extend(sorted(active_question_tokens))
    rows = conn.execute(
        f"""
        SELECT token, opencode_session_id, directory, event_type, request_id, details, created_at
        FROM correlations
        WHERE expires_at > ?{active_clause}
        ORDER BY created_at DESC
        """,
        params,
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

        details = None
        if r["details"]:
            try:
                details = json.loads(r["details"])
            except Exception:
                details = {"message": r["details"]}

        pending.append(
            {
                "token": r["token"],
                "session_id": r["opencode_session_id"],
                "directory": r["directory"],
                "event_type": r["event_type"],
                "request_id": r["request_id"],
                "details": details,
                "age_seconds": (now - r["created_at"]) // 1000,
            }
        )

    print(json.dumps({"pending": pending}, indent=2))
    return 0


def cmd_oc_questions(conn: sqlite3.Connection) -> int:
    """Show active question prompts from OpenCode DB, the source of truth."""
    visible = sync_active_questions(conn)

    # Hide questions that already have an unprocessed answer command queued.
    filtered = []
    for q in visible:
        token = q.get("token")
        if token:
            existing = conn.execute(
                "SELECT 1 FROM commands WHERE token = ? AND target_kind = 'correlation' AND status = 'pending'",
                (token,),
            ).fetchone()
            if existing:
                continue
        filtered.append(q)

    if not filtered:
        print("📭 No active OpenCode questions.")
        return 0

    print(format_separator())
    print(f"❓ OpenCode questions ({len(filtered)})")
    print(format_separator())
    for q in filtered:
        token = q.get("token") or "(no token)"
        print(f"Token: {token}")
        print(f"Session: #{q.get('short_id')}  {q.get('session_id')}")
        print(f"Title: {q.get('title') or 'untitled'}")
        print(f"Dir: {Path(q.get('directory') or '').name if q.get('directory') else 'unknown'}")
        print(f"Age: {format_relative_age(q.get('time_created'))}")
        if not q.get("request_id"):
            print("⚠️ Answer token has no request_id yet; visibility works, answer may need the live plugin event token.")
        message = get_question_message(q.get("details") or {})
        if message:
            print()
            print(message)
        print()
        print(f"Reply: /oc answer {q.get('short_id') or token} <answer>  (or token: {token})")
        print(format_separator())
    return 0


def cmd_oc_list(conn: sqlite3.Connection) -> int:
    """List OpenCode sessions.

    Default is live/question sessions only, so the command does not imply every
    historical DB session is an active process. Use --all for recent resumable
    history.
    """
    as_json = "--json" in sys.argv
    show_all = "--all" in sys.argv or "--history" in sys.argv
    rows = sync_recent_sessions(conn, 50)
    live_session_ids = live_opencode_session_ids()
    if not show_all:
        rows = [r for r in rows if r["status"] in {"question", "busy"} or r["id"] in live_session_ids]

    if as_json:
        sessions = [
            {
                "short_id": r["short_id"],
                "session_id": r["id"],
                "title": r["title"],
                "status": r["status"],
                "directory": r["directory"],
                "last_activity_at": r["time_updated"],
                "live_process": r["id"] in live_session_ids,
            }
            for r in rows
        ]
        print(json.dumps({"sessions": sessions, "source": "opencode.db", "mode": "all" if show_all else "live"}, indent=2))
        return 0

    if not rows:
        print("📭 No live/blocking OpenCode sessions. Use /oc list --all for recent resumable history.")
        return 0

    print(format_separator())
    print(f"📋 OpenCode sessions ({len(rows)})")
    print("Mode: recent history" if show_all else "Mode: live/blocking only")
    print("Source: opencode.db + /proc")
    print(format_separator())
    for r in rows:
        title = r["title"] or "untitled"
        directory_name = Path(r["directory"]).name if r["directory"] else "unknown"
        live = " live" if r["id"] in live_session_ids else " history"
        print(f"#{r['short_id']}  {format_status_badge(r['status'])}  {live}")
        print(f"Title: {title}")
        print(f"Dir: {directory_name}")
        print(f"Last active: {format_relative_age(r['time_updated'])}")
        print(format_separator())
    return 0


def cmd_oc_ps() -> int:
    """Show actual live OpenCode OS processes consuming memory."""
    processes = live_opencode_processes()
    parents = [p for p in processes if p["is_parent"]]
    children = [p for p in processes if not p["is_parent"]]
    if not processes:
        print("📭 No live OpenCode processes.")
        return 0
    total_mb = sum(int(p["rss_mb"]) for p in processes)
    print(format_separator())
    print(f"🧠 Live OpenCode processes: {len(parents)} parent, {len(children)} child, ~{total_mb} MB RSS")
    print(format_separator())
    for p in parents:
        session = f" session={p['session_id']}" if p.get("session_id") else " session=unknown"
        print(f"PID {p['pid']}  {p['rss_mb']} MB  cwd={Path(p['cwd']).name}{session}")
        print(p["cmd"][:220])
        print(format_separator())
    if children:
        child_mb = sum(int(p["rss_mb"]) for p in children)
        print(f"Child language servers: {len(children)}, ~{child_mb} MB RSS")
    return 0


def cmd_oc_show(conn: sqlite3.Connection, session_id: str) -> int:
    """Show the latest assistant text and any active question from OpenCode DB."""
    try:
        short_id = int(session_id)
    except ValueError:
        print(json.dumps({"error": f"invalid session id '{session_id}'", "ok": False}))
        return 1

    row = resolve_session_by_short_id(conn, short_id)
    if not row:
        print(json.dumps({"error": f"No session #{short_id} found.", "ok": False}))
        return 1

    title = row["title"] or "untitled"
    text = latest_assistant_text_from_opencode(row["session_id"])
    questions = active_questions_for_session(conn, row["session_id"])

    print(format_separator())
    print(f"🤖 OpenCode session #{short_id}")
    print(f"Title: {title}")
    print(f"Status: {row['status'] or 'unknown'}")
    print(f"Session ID: {row['session_id']}")
    print("Source: opencode.db")
    print(format_separator())

    if questions:
        print()
        print(f"❓ Active question(s): {len(questions)}")
        for q in questions:
            token = q.get("token") or "(no token)"
            print(f"Token: {token}")
            print(get_question_message(q.get("details") or {}))
            print(f"Reply: /oc answer {short_id} <answer>  (or token: {token})")
            if not q.get("request_id"):
                print("⚠️ This token has no request_id; answering may not work until the live event token is captured.")
            print(format_separator())

    if text:
        print()
        print(text)
        print()
        print(format_separator())
    elif not questions:
        print(f"Session #{short_id} ({title}) has no assistant text yet.")
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


def create_system_command(
    conn: sqlite3.Connection, action: str, payload: dict, scope_token: str
) -> int:
    """Write a system-level command to the commands table."""
    now = int(time.time() * 1000)

    existing = conn.execute(
        "SELECT id FROM commands WHERE token = ? AND target_kind = 'system' AND status = 'pending'",
        (scope_token,),
    ).fetchone()
    if existing:
        print(
            json.dumps(
                {
                    "error": "a system command is already pending",
                    "command_id": existing["id"],
                    "ok": False,
                }
            )
        )
        return 1

    conn.execute(
        """
        INSERT INTO commands (target_kind, token, action, payload, created_at, status, expires_at)
        VALUES ('system', ?, ?, ?, ?, 'pending', ?)
        """,
        (
            scope_token,
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
                "token": scope_token,
                "action": action,
                "message": f"✨ Queued {action} for {scope_token}. OpenCode should pick it up in a few seconds.",
            }
        )
    )
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

    pid, error = launch_opencode_run(
        directory=row["directory"],
        session_id=row["session_id"],
        message=message,
    )
    if error:
        print(json.dumps({"error": error, "ok": False}))
        return 1

    print(
        f"💬 Sent reply to session #{short_id}. OpenCode run started as PID {pid}."
    )
    return 0


def cmd_oc_new(conn: sqlite3.Connection, argv: list[str]) -> int:
    opts, prompt_or_error = parse_new_command_args(argv)
    if not prompt_or_error:
        print(json.dumps({"error": "missing prompt", "ok": False}))
        return 1
    if not opts and prompt_or_error.startswith("missing "):
        print(
            json.dumps(
                {
                    "error": f"usage: /oc new [--agent <name>] [--model <provider/model>] [--preset <name>] [--dir <path>] <prompt> ({prompt_or_error})",
                    "ok": False,
                }
            )
        )
        return 1

    target_dir = opts.get("dir") or infer_default_directory(conn)
    title = compact_whitespace(prompt_or_error)[:60]
    pid, error = launch_opencode_run(
        directory=target_dir,
        message=prompt_or_error,
        agent=opts.get("agent"),
        model=opts.get("model"),
        preset=opts.get("preset"),
        title=title,
    )
    if error:
        print(json.dumps({"error": error, "ok": False}))
        return 1

    print(
        f"✨ Started a new OpenCode run in {target_dir} as PID {pid}. It should appear in /oc list shortly."
    )
    return 0


def cmd_oc_kill(conn: sqlite3.Connection, argv: list[str]) -> int:
    ids, error = parse_kill_ids(argv)
    if error:
        print(json.dumps({"error": f"usage: /oc kill <id> [id...] ({error})", "ok": False}))
        return 1

    rows = conn.execute(
        f"""
        SELECT short_id, session_id, title, status
               , directory
        FROM sessions
        WHERE deleted = 0 AND short_id IN ({','.join('?' for _ in ids)})
        ORDER BY short_id ASC
        """,
        ids,
    ).fetchall()

    found_by_id = {int(row["short_id"]): row for row in rows}
    missing = [str(short_id) for short_id in ids if short_id not in found_by_id]
    if missing:
        print(
            json.dumps(
                {
                    "error": f"no active session(s) found for: {', '.join(missing)}",
                    "ok": False,
                }
            )
        )
        return 1

    sessions = [
        {
            "short_id": int(row["short_id"]),
            "session_id": row["session_id"],
            "title": row["title"],
            "status": row["status"],
        }
        for row in rows
    ]

    session_directories = {str(row["directory"]) for row in rows}
    if len(session_directories) != 1:
        print(
            json.dumps(
                {
                    "error": "kill only supports sessions from a single directory at a time",
                    "ok": False,
                }
            )
        )
        return 1

    return create_system_command(
        conn,
        "kill_sessions",
        {"sessions": sessions},
        next(iter(session_directories)),
    )


def create_command(
    conn: sqlite3.Connection, token: str, action: str, payload: dict
) -> int:
    """Write a command to the commands table."""
    now = int(time.time() * 1000)

    # Refresh OpenCode DB-backed active questions before resolving tokens.
    sync_active_questions(conn)

    # Verify the correlation exists. Questions are allowed past their old TTL if
    # OpenCode DB still shows a live question and sync_active_questions refreshed it.
    corr = conn.execute(
        "SELECT * FROM correlations WHERE token = ? AND (expires_at > ? OR event_type IN ('question.asked', 'question.v2.asked'))",
        (token, now),
    ).fetchone()
    if not corr:
        print(
            json.dumps(
                {"error": f"no active correlation for token '{token}'", "ok": False}
            )
        )
        return 1

    if action == "answer" and corr["event_type"] in {"question.asked", "question.v2.asked"} and not corr["request_id"]:
        print(
            json.dumps(
                {
                    "error": f"question token '{token}' is visible from OpenCode DB but has no request_id, so it cannot be answered via pluginClient.question.reply yet",
                    "ok": False,
                }
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
        "question.v2.asked": {"answer"},
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


def resolve_question_answer_target(conn: sqlite3.Connection, target: str) -> tuple[str | None, str | None]:
    """Resolve /oc answer target.

    Supports either the original correlation token or a session identifier:
    short numeric id (e.g. 60) or full OpenCode session id. Session-id answers
    only work when that session has exactly one active question.
    """
    sync_active_questions(conn)

    token_row = conn.execute(
        "SELECT token FROM correlations WHERE token = ? AND event_type IN ('question.asked', 'question.v2.asked')",
        (target,),
    ).fetchone()
    if token_row:
        return str(token_row["token"]), None

    session_row = None
    if target.isdigit():
        session_row = resolve_session_by_short_id(conn, int(target))
    elif target.startswith("ses_"):
        session_row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ? AND deleted = 0",
            (target,),
        ).fetchone()
        if not session_row:
            # Seed the relay alias table from OpenCode DB if needed.
            sync_recent_sessions(conn, 100)
            session_row = conn.execute(
                "SELECT * FROM sessions WHERE session_id = ? AND deleted = 0",
                (target,),
            ).fetchone()

    if not session_row:
        return None, f"no question token or session found for '{target}'"

    questions = active_questions_for_session(conn, session_row["session_id"])
    if not questions:
        return None, f"session #{session_row['short_id']} has no active OpenCode question"
    if len(questions) > 1:
        tokens = [str(q.get("token")) for q in questions if q.get("token")]
        return None, f"session #{session_row['short_id']} has {len(questions)} active questions; use one token: {', '.join(tokens)}"
    token = questions[0].get("token")
    if not token:
        return None, f"session #{session_row['short_id']} has an active question but no answer token"
    return str(token), None


def cmd_answer(conn: sqlite3.Connection, token: str, answer: str) -> int:
    resolved_token, error = resolve_question_answer_target(conn, token)
    if error or not resolved_token:
        print(json.dumps({"error": error or f"could not resolve '{token}'", "ok": False}))
        return 1
    return create_command(conn, resolved_token, "answer", {"answer": answer})


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
        print(f"📭 No command found for {token}.")
        return 0

    result = None
    if row["result"]:
        try:
            result = json.loads(row["result"])
        except Exception:
            result = row["result"]

    print(format_separator())
    print(f"📡 Command status for {token}")
    print(format_separator())
    print(f"ID: {row['id']}")
    print(f"Action: {row['action']}")
    print(f"Status: {format_command_status_badge(row['status'])}")
    print(f"Created: {format_relative_age(row['created_at'])}")
    if row["claimed_at"]:
        print(f"Claimed: {format_relative_age(row['claimed_at'])}")
    if result is not None:
        if isinstance(result, (dict, list)):
            result_text = json.dumps(result, indent=2)
        else:
            result_text = str(result)
        print()
        print("Result:")
        print(result_text)
    print(format_separator())
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

    conn = get_db()
    ensure_tables(conn)

    try:
        if action == "/oc":
            if len(sys.argv) < 3:
                print(json.dumps({"error": format_oc_help(), "ok": False}))
                return 1

            subcommand = sys.argv[2]

            if subcommand == "help":
                print(format_oc_help())
                return 0

            if subcommand == "list":
                return cmd_oc_list(conn)

            elif subcommand in {"ps", "processes"}:
                return cmd_oc_ps()

            elif subcommand in {"questions", "pending"}:
                return cmd_oc_questions(conn)

            elif subcommand == "answer":
                if len(sys.argv) < 5:
                    print(json.dumps({"error": "usage: /oc answer <id|token> <answer>", "ok": False}))
                    return 1
                answer = " ".join(sys.argv[4:])
                return cmd_answer(conn, sys.argv[3], answer)

            elif subcommand in {"ok", "approve"}:
                if len(sys.argv) < 4:
                    print(json.dumps({"error": f"usage: /oc {subcommand} <token>", "ok": False}))
                    return 1
                return cmd_approve(conn, sys.argv[3])

            elif subcommand in {"no", "reject"}:
                if len(sys.argv) < 4:
                    print(json.dumps({"error": f"usage: /oc {subcommand} <token> [reason]", "ok": False}))
                    return 1
                message = " ".join(sys.argv[4:]) if len(sys.argv) > 4 else ""
                return cmd_reject(conn, sys.argv[3], message)

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

            elif subcommand in {"reply", "say"}:
                if len(sys.argv) < 5:
                    print(json.dumps({"error": f"usage: /oc {subcommand} <id> <message>", "ok": False}))
                    return 1
                return cmd_oc_prompt(conn, sys.argv[3], " ".join(sys.argv[4:]))

            elif subcommand == "kill":
                if len(sys.argv) < 4:
                    print(json.dumps({"error": "usage: /oc kill <id> [id...]", "ok": False}))
                    return 1
                return cmd_oc_kill(conn, sys.argv[3:])

            elif subcommand == "new":
                if len(sys.argv) < 4:
                    print(
                        json.dumps(
                            {
                                "error": "usage: /oc new [--agent <name>] [--model <provider/model>] [--preset <name>] [--dir <path>] <prompt>",
                                "ok": False,
                            }
                        )
                    )
                    return 1
                return cmd_oc_new(conn, sys.argv[3:])

            else:
                print(json.dumps({"error": f"unknown /oc subcommand '{subcommand}'\n\n{format_oc_help()}", "ok": False}))
                return 1

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
                print(json.dumps({"error": "usage: answer <id|token> <answer>", "ok": False}))
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
