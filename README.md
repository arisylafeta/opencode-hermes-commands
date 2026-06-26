# opencode-hermes-commands

Single source of truth for the Hermes <-> OpenCode relay.

This repo contains both runtime entrypoints:

- Hermes gateway plugin: registers `/oc` for WhatsApp-side session control
- OpenCode plugin: polls the shared SQLite queue and executes commands inside OpenCode

## Layout

```text
opencode-hermes-commands/
├── __init__.py                  # Hermes gateway plugin entrypoint
├── handler.py                   # `/oc` slash-command handler
├── plugin.yaml                  # Hermes plugin metadata
├── opencode-hermes-commands.ts  # OpenCode plugin source
├── opencode-hermes-commands.js  # OpenCode plugin runtime
├── opencode_bridge.py           # Shared bridge / queue CLI
└── state.db                     # Shared SQLite queue
```

## Runtime wiring

- Hermes gateway discovers this repo as the `hermes-commands` plugin via `plugin.yaml`
- OpenCode loads `opencode-hermes-commands.js` via the symlink in `~/.config/opencode/plugins/`
- `/usr/local/bin/opencode_bridge.py` should point at `opencode_bridge.py` in this repo

## Usage

```text
/oc help
/oc list
/oc show <id>
/oc reply <id> <message>
/oc <id> <prompt>
/oc status <id>
/oc new [--agent <name>] [--model <provider/model>] [--preset <name>] [--dir <path>] <prompt>
```
