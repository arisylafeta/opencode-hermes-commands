# hermes-commands

Slash commands for Hermes Agent. Provides `/oc` for interacting with OpenCode sessions from WhatsApp.

## Usage

```
/oc list                     List all active opencode sessions
/oc show <id>                Show the last AI message for a session
/oc <id> <prompt>            Send a prompt to a session
/oc status <id>              Check status of the latest command for a session
```

## How It Works

1. User types `/oc list` in WhatsApp
2. Hermes gateway intercepts the slash command
3. Plugin handler calls `opencode_bridge.py /oc ...` via subprocess
4. Bridge script reads/writes the shared SQLite DB
5. OpenCode plugin polls the DB and executes commands via the OpenCode SDK

## Installation

Already installed at `~/.hermes/plugins/hermes-commands/`. The gateway auto-discovers plugins on startup.

Restart the gateway after changes:

```bash
hermes gateway restart
```
