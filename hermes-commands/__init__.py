import logging

try:
    from .handler import handle_oc
except ImportError:
    from handler import handle_oc

logger = logging.getLogger(__name__)


def register(ctx):
    ctx.register_command(
        name="oc",
        handler=handle_oc,
        description="Control OpenCode sessions: list, show, prompt, status.",
        args_hint="[list|show <id>|<id> <prompt>|status <id>]",
    )
