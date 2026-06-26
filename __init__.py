import importlib.util
import logging
from pathlib import Path

try:
    from .handler import handle_oc
except ImportError:
    handler_path = Path(__file__).with_name("handler.py")
    spec = importlib.util.spec_from_file_location("opencode_hermes_commands_handler", handler_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    handle_oc = module.handle_oc

logger = logging.getLogger(__name__)


def register(ctx):
    ctx.register_command(
        name="oc",
        handler=handle_oc,
        description="Control OpenCode sessions: list, show, prompt, status.",
        args_hint="[list|show <id>|<id> <prompt>|status <id>]",
    )
