"""
Start the Hatchet worker.

Usage:
    python -m hatchet.worker

The worker connects to Hatchet (via HATCHET_CLIENT_TOKEN) and waits for batch
runs (`video-dubbing-batch-pipeline`). Keep this running while you trigger
pipeline runs from `python -m hatchet.trigger`.
"""

from __future__ import annotations

import asyncio
import logging

from rich.panel import Panel

from pipeline.console import get_console, setup_logging
from hatchet.workflow import hatchet, workflow

logger = logging.getLogger(__name__)


async def _validate_nebius_credentials() -> None:
    """Cheap Nebius auth-check before the worker registers — fails fast on
    expired/wrong IAM tokens.

    Without this, an expired token surfaces only when Hatchet schedules the
    first task (~30-60 s in), and the failure mode (UNAUTHENTICATED from the
    Nebius SDK deep in a worker callback) is opaque. We've hit this bug
    three times during testing; the diagnostic block below covers all three.
    """
    from pipeline.config import secrets
    from nebius.sdk import SDK
    from nebius.api.nebius.ai.v1 import JobServiceClient, ListJobsRequest

    if not secrets.nebius_iam_token:
        raise SystemExit(
            "✗ NEBIUS_IAM_TOKEN is empty.\n"
            "  Set it in .env (see .env.example) and restart the worker."
        )

    sdk = SDK(credentials=secrets.nebius_iam_token)
    try:
        await JobServiceClient(sdk).list(
            ListJobsRequest(parent_id=secrets.nebius_project_id, page_size=1)
        )
    except Exception as e:
        msg = str(e)
        if "UNAUTHENTICATED" in msg or "unauthenticated" in msg:
            raise SystemExit(
                "✗ Nebius IAM auth failed at worker startup.\n"
                "  Three things to check:\n"
                "    1. NEBIUS_IAM_TOKEN in .env may be expired (default TTL ~12h).\n"
                "       Refresh via the Nebius console / CLI and update .env.\n"
                "    2. Your shell may have an old exported NEBIUS_IAM_TOKEN that\n"
                "       overrides .env (pydantic-settings reads shell env first).\n"
                "       Fix: `unset NEBIUS_IAM_TOKEN` then restart the worker.\n"
                "    3. NEBIUS_PROJECT_ID may be wrong for the IAM token's tenant.\n"
                f"  Original error: {msg[:200]}"
            )
        raise
    finally:
        await sdk.close()


def run_worker() -> None:
    """Register the batch workflow and block until interrupted."""
    setup_logging(verbose=False)

    # Pre-flight: validate Nebius credentials before any task can be scheduled.
    # ~1 second of latency at boot, saves a ~60s cold-start-to-cryptic-error
    # cycle when credentials are stale.
    asyncio.run(_validate_nebius_credentials())

    console = get_console()
    console.print(
        Panel(
            "[bold]video-dubbing-batch-pipeline[/bold]\n"
            "Waiting for batch runs…\n"
            "[link=https://cloud.hatchet.run]Hatchet dashboard[/link]",
            title="Dubbing worker",
            border_style="cyan",
        )
    )
    worker = hatchet.worker("dubbing-batch-worker")
    worker.register_workflow(workflow)
    logger.info("Worker registered; listening for events")
    worker.start()


def main() -> None:
    run_worker()


if __name__ == "__main__":
    main()
