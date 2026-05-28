"""Shared Rich console and logging setup for pipeline CLIs."""

from __future__ import annotations

import logging
import sys

from rich.console import Console
from rich.logging import RichHandler

_console: Console | None = None


def get_console() -> Console:
    global _console
    if _console is None:
        _console = Console(stderr=True)
    return _console


def setup_logging(*, verbose: bool = False) -> None:
    """Configure root logger with Rich when stderr is a TTY."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers: list[logging.Handler]
    if sys.stderr.isatty():
        handlers = [
            RichHandler(
                console=get_console(),
                rich_tracebacks=True,
                show_time=False,
                show_path=verbose,
            )
        ]
    else:
        handlers = [logging.StreamHandler(sys.stderr)]
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        handlers=handlers,
        force=True,
    )
