"""Unified CLI: dub trigger | dub worker | dub download."""

from __future__ import annotations

import typer

from pipeline.download import app as download_app

app = typer.Typer(
    name="dub",
    help="Video dubbing pipeline — Hatchet + Nebius serverless.",
    no_args_is_help=True,
)

app.add_typer(download_app, name="download")

from hatchet.trigger import app as trigger_app  # noqa: E402

app.add_typer(trigger_app, name="trigger")


@app.command("worker")
def cmd_worker() -> None:
    """Start the Hatchet worker (long-running)."""
    from hatchet.worker import run_worker

    run_worker()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
