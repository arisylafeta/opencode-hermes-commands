# Deepwork: OpenCode Hermes Commands Enhancement

## Goal

Add `/oc` command interface for interacting with OpenCode sessions from WhatsApp/Hermes:
- `/oc list` — list all running opencode sessions with titles and short IDs
- `/oc show <id>` — show the final/last message of an AI session
- `/oc <id> "prompt"` — send a prompt to a specific session
- Session IDs: short, readable identifiers instead of full UUIDs

## Current Architecture (Confirmed Research)

### Two plugin versions

**TS source** (`opencode-hermes-commands.ts`, 1610 lines):
- Full SQLite persistent queue with 7 tables (queue, hermes_queue, dedup, sent_log, idle_debounce, correlations, commands)
- Command drain loop reads from `commands` table, executes via `input.client` SDK
- Bridge script integration via `/usr/local/bin/opencode_bridge.py`
- Stores `pluginClient` (input.client) reference for command execution
- Session tracking via in-memory Maps (sessionTitles, sessionLastAssistantText, subagentSessions)
- Supports: approve, reject, answer, continue, say, skip commands

**JS active** (`opencode-hermes-commands.js`, 751 lines):
- Simplified, in-memory only (NO SQLite, NO command drain, NO bridge integration)
- Has `SessionTracker` class: tracks sessions with title, status, parentId, children, pendingPermission, pendingQuestion
- Has `WhatsAppNotifier` for notifications
- Has `HermesRelayRuntime` tying it together with quiescence timers
- Does NOT store `input.client` reference
- Does NOT support any commands (no /oc, no bridge, no prompt sending)
- Session IDs are raw opencode UUIDs (no short IDs)

### Bridge script (`/usr/local/bin/opencode_bridge.py`, 477 lines)
- Python script that reads/writes the shared SQLite DB
- Supports: pending, approve, reject, answer, continue, /ok, /no, /say, /skip, status, resolve
- Still references OLD DB path: `~/.config/opencode/hermes-relay-state.db`
- Only works with TS version (JS version doesn't have SQLite or command drain)

### OpenCode SDK client methods available
From `PluginInput.client`:
- `session.get({ sessionID })` — get session info
- `session.messages({ sessionID })` — get messages (returns role + parts)
- `session.prompt({ sessionID, parts })` — send a prompt to a session
- `permission.reply({ requestID, reply, message })` — reply to permission
- `question.reply({ requestID, answers })` — reply to question
- NO `session.list` method — can't list sessions from SDK

### Key gap
The active JS plugin has no way to receive or execute commands. It doesn't store `input.client`, doesn't use SQLite, and has no command drain loop. To support `/oc` commands, we need to add these capabilities.

## Revised Plan (after @oracle review)

Key decisions from oracle:
- Do NOT switch to TS as base. Port minimum proven pieces from TS into JS.
- Short IDs MUST persist across restarts (SQLite-backed integers, autoincrement).
- Collapse session IDs into core schema, not a separate phase.
- Commands need two target types: correlation tokens (existing) and session short IDs (new).
- WhatsApp notifications must include `#short_id` so user knows what to type.
- `/oc list` output should be human-first, not JSON-only.
- Hide child/subagent sessions by default.

### Phase 1: Persistent session registry + command queue in JS plugin
- Store `input.client` reference (like TS `pluginClient`)
- Add SQLite init with `sessions` table:
  ```sql
  sessions(
    session_id TEXT PRIMARY KEY,
    short_id INTEGER UNIQUE NOT NULL,  -- autoincrement, persists across restarts
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
  ```
- Add `commands` table (same as TS but add `target_kind` column):
  ```sql
  commands(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_kind TEXT NOT NULL DEFAULT 'correlation',  -- 'correlation' | 'session'
    token TEXT NOT NULL,  -- correlation token or session short_id
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_at INTEGER,
    result TEXT,
    expires_at INTEGER NOT NULL
  )
  ```
- Keep existing `correlations` table for old approve/reject/answer compatibility
- On session.created/updated: insert into sessions table, allocate short_id
- On session.status: update status + last_activity_at
- On message.part.updated (assistant): track role via messageRoles map, store last_assistant_text
- On message.updated: track role
- On session.deleted: mark deleted=1
- Port message role tracking from TS (messageID→role map) for reliable assistant text capture
- Add command drain loop (5s interval): claim pending commands, resolve target (correlation or session), execute via SDK, mark done/failed
- Claim logic: join on directory for ownership safety (same as TS)
- Update WhatsApp notifications to include `#short_id` in the title line

### Phase 2: Bridge script updates
- Fix DB path to `~/.hermes/plugins/opencode-hermes-commands/state.db`
- Add `sessions` table to `ensure_tables()`
- Add `commands.target_kind` column to `ensure_tables()` (ALTER TABLE if missing)
- Add `/oc list` command: human-readable output of non-deleted, non-child sessions
- Add `/oc show <id>` command: fetch last_assistant_text by short_id
- Add `/oc <id> <prompt>` command: write command with target_kind='session', action='continue'
- Keep all existing commands working

### Phase 3: Notifications, docs, commit
- Update WhatsApp notification format to show `#short_id` next to title
- Update reference doc (`opencode-hermes-relay.md`)
- Commit throughout

## Resolved Questions
- Short IDs persist across restarts: YES (SQLite autoincrement)
- `/oc show`: show full last assistant message (truncated to 1200 chars max)
- Child/subagent sessions: hidden from `/oc list` by default

## Implementation Results

### Phase 2a: JS plugin (commit fcf1d26)
- Added `node:sqlite` DatabaseSync with WAL mode
- Sessions table with autoincrement short_id (persists across restarts)
- Commands table with target_kind column for both correlation and session targets
- Correlations table preserved for old approve/reject/answer compatibility
- Store input.client reference (pluginClient) for SDK calls
- Ported message role tracking (messageRoles map) from TS source
- Persist session.created/updated/status/deleted and assistant text to SQLite
- WhatsApp notifications now show #short_id in title (buildNotification, buildDoneNotification)
- Command drain loop (5s) with atomic claim, session/correlation target resolution
- Supports: approve, reject, answer, continue, say, show, skip actions
- Syntax verified: `node --check` passed

### Phase 2b: Bridge script (commit c528334)
- Fixed DB path to ~/.hermes/plugins/opencode-hermes-commands/state.db
- Added sessions table + target_kind migration to ensure_tables()
- /oc list: human-readable, supports --json flag
- /oc show <id>: displays title, status, last_assistant_text (truncated 1200 chars)
- /oc <id> <prompt>: queues command with target_kind='session', action='continue'
- /oc status <id>: command status check
- All existing commands unchanged
- Bridge script now versioned in plugin repo at opencode_bridge.py
- Syntax verified: `python3 -m py_compile` passed

### Phase 3: Oracle review fixes (commit 08e08ee)
- Added createCorrelation() for permission.asked/question.asked events (old /ok /no /say now work)
- Fixed command execution to use correlation's request_id instead of payload.requestID
- Added stale-claim recovery (claimed > 60s → back to pending)
- Added expired pending command cleanup
- Removed dead 'show' action from executeCommand
- Added short_id allocation retry on UNIQUE constraint conflict
- Fixed bridge duplicate pending check to include target_kind
- Fixed /oc status to filter by target_kind='session'
- Fixed /oc status docstring
- Both files syntax-verified
