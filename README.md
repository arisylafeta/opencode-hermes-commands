# opencode-hermes-commands

Control [OpenCode](https://opencode.ai) sessions from WhatsApp via the Hermes agent gateway.

Send prompts, start new sessions, list active sessions, check status, and kill sessions, all from your phone.

## How it works

```
WhatsApp message          Hermes gateway            OpenCode
────────────────          ──────────────            ────────
/oc reply 31 keep going   → handler.py             → opencode run --session <id> "keep going"
/oc new fix the auth bug  → handler.py             → opencode run --dir <dir> "fix the auth bug"
/oc list                  → opencode_bridge.py     → reads shared SQLite DB
```

The Hermes gateway plugin registers the `/oc` slash command. When you send a message like `/oc reply 31 hello`, the handler shells out to `opencode_bridge.py`, which either queries the shared SQLite database (for listings/status) or launches `opencode run` directly (for new sessions and replies).

## Commands

| Command | Description |
|---|---|
| `/oc help` | Show all available commands |
| `/oc list` | List active OpenCode sessions |
| `/oc show <id>` | Show the last assistant message for a session |
| `/oc reply <id> <message>` | Send a message to an existing session |
| `/oc <id> <message>` | Shorthand for reply |
| `/oc new [options] <prompt>` | Start a new OpenCode session |
| `/oc kill <id> [id...]` | Kill one or more sessions |
| `/oc status <id>` | Check command status for a session |

### `/oc new` options

| Flag | Description |
|---|---|
| `--agent <name>` | Agent to use (e.g. `fixer`, `build`) |
| `--model <provider/model>` | Model in provider/model format (e.g. `anthropic/claude-sonnet-4-6`) |
| `--preset <name>` | Preset to apply before the prompt |
| `--dir <path>` | Working directory (defaults to most recent active session's directory) |

### Examples

```
/oc list
/oc show 31
/oc reply 31 keep going, focus on the auth bug
/oc 31 continue from the last failing test
/oc new audit this repo for dead code
/oc new --agent fixer --dir projects/rebattery-enrich add a healthcheck endpoint
/oc new --model anthropic/claude-sonnet-4-6 --preset cheap-flex review the auth flow
/oc kill 21 24 28
/oc status 31
```

## Setup

### 1. Install the OpenCode plugin

The OpenCode plugin (`opencode-hermes-commands.js`) needs to be discoverable by OpenCode. Symlink it:

```bash
mkdir -p ~/.config/opencode/plugins
ln -s /path/to/opencode-hermes-commands/opencode-hermes-commands.js \
      ~/.config/opencode/plugins/opencode-hermes-commands.js
```

This plugin listens to OpenCode events and writes session metadata (short IDs, titles, status, last assistant text) to a shared SQLite database. It also polls for queued commands (used by `/oc kill` and permission replies).

### 2. Install the Hermes gateway plugin

The Hermes gateway plugin (`__init__.py`, `handler.py`, `plugin.yaml`) registers the `/oc` slash command. Place this repo (or a symlink) in your Hermes plugins directory.

### 3. Link the bridge script

```bash
ln -s /path/to/opencode-hermes-commands/opencode_bridge.py /usr/local/bin/opencode_bridge.py
```

### 4. Restart OpenCode

OpenCode loads plugins on startup. Restart any running instances to pick up the plugin.

## File layout

```
opencode-hermes-commands/
├── __init__.py                  # Hermes gateway plugin entrypoint
├── handler.py                   # /oc slash-command handler (called by Hermes)
├── plugin.yaml                  # Hermes plugin metadata
├── opencode-hermes-commands.ts  # OpenCode plugin source (TypeScript)
├── opencode-hermes-commands.js  # OpenCode plugin runtime (compiled, active)
├── opencode_bridge.py           # Bridge CLI: queries DB, launches opencode run
├── state.db                     # Shared SQLite database (auto-created)
├── errors.log                   # Plugin error log
└── README.md
```

## Architecture

### Direct execution path (new sessions and replies)

`/oc new` and `/oc reply` launch `opencode run` directly as a detached subprocess. This is reliable and does not depend on the OpenCode plugin being loaded or the command queue working.

### Queue path (kill, approve, reject, answer)

`/oc kill`, permission replies (`/ok`, `/no`), and question answers use a shared SQLite queue. The bridge writes a command row, and the OpenCode plugin polls every 5 seconds to claim and execute it.

### Shared SQLite database

`state.db` is the single source of truth for session metadata and queued commands. Both the bridge (Python) and the OpenCode plugin (Bun/JavaScript) read and write to it using WAL mode with a 3-second busy timeout.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `HERMES_RELAY_DB_PATH` | `~/.hermes/plugins/opencode-hermes-commands/state.db` | Path to shared SQLite database |
| `HERMES_RELAY_ENABLED` | `true` | Enable/disable the OpenCode plugin |
| `HERMES_WHATSAPP_BRIDGE_URL` | `http://127.0.0.1:3000` | WhatsApp bridge URL for direct notifications |
| `HERMES_WHATSAPP_CHAT_ID` | (Ari's WhatsApp LID) | Default WhatsApp chat to notify |
| `HERMES_API_URL` | `http://127.0.0.1:8642` | Hermes orchestrator API URL |
| `HERMES_PROFILE` | `coder` | Hermes profile to use |

## Requirements

- [OpenCode](https://opencode.ai) (with plugin support)
- [Bun](https://bun.sh) (for the OpenCode plugin runtime)
- Python 3.10+
- Hermes agent gateway (for WhatsApp integration)

## License

Private.
