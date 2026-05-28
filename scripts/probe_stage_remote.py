#!/usr/bin/env python3
"""Launch ONE Nebius job for ONE stage and report timeline + cost.

Bypasses Hatchet entirely — use this to verify a container image works on
Nebius before subjecting it to the full orchestrated workflow. The same
``--source`` syntax as ``trigger.py`` is supported, so you can probe a stage
against either a single video or a whole folder prefix:

    # Single file
    python scripts/probe_stage_remote.py extract --source sample_file/sample.mp4
    python scripts/probe_stage_remote.py extract                              # default: sample_file/sample.mp4

    # Folder / batch — same shape as trigger.py
    python scripts/probe_stage_remote.py extract --source sample_batch/ --run-id probe-batch

    # Downstream stages — re-use the same --run-id that extract just wrote
    python scripts/probe_stage_remote.py transcribe --run-id <id-from-extract>
    python scripts/probe_stage_remote.py translate  --run-id <same>
    python scripts/probe_stage_remote.py tts        --run-id <same>
    python scripts/probe_stage_remote.py remux      --run-id <same>

To run all 5 stages in dependency order with a single command, see
``scripts/probe_pipeline_remote.py`` — it calls ``probe_stage()`` 5×.

Pre-flight catches the common typos before we burn a Nebius cold start:
    - extract: the input video(s) must exist in the bucket at the resolved key(s)
    - downstream stages: the upstream report must exist under --run-id
"""
from __future__ import annotations

import argparse
import asyncio
import time

from pipeline.config import get_config
from pipeline.metadata import _job_run_s, read_task_report, write_task_manifest
from pipeline.nebius import NebiusJobError, create_and_wait
from pipeline.paths import resolve_video_keys, task_manifest_container_path
from pipeline.run import PipelineRun
from pipeline.storage import list_existing

STAGES = ("extract", "transcribe", "translate", "tts", "remux")

DEFAULT_SOURCE = "sample_file/sample.mp4"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Launch one Nebius job for one stage (no Hatchet)."
    )
    p.add_argument("stage", choices=STAGES)
    p.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=(
            "Bucket-relative video file or folder prefix. "
            "File: 'sample_file/sample.mp4'. Folder: 'sample_batch/'. "
            f"Only used by extract; downstream stages use --run-id. Default: {DEFAULT_SOURCE}"
        ),
    )
    p.add_argument("--run-id", default=None,
                   help="Run label (output namespace). Default: probe-<unix-ts>")
    p.add_argument("--target-lang", default=None,
                   help="NLLB target language code. Default: config.pipeline.target_lang")
    p.add_argument("--preflight-only", action="store_true",
                   help="Run pre-flight checks and exit without launching a Nebius job")
    return p.parse_args()


def _preflight(stage: str, run_id: str, video_keys: list[str]) -> str | None:
    """Return an error message if a required cloud-side input is missing.

    Avoids burning a Nebius cold start on a missing input:
      - extract: every resolved video key must exist in the bucket
      - downstream stages: the upstream report must exist for run_id
    """
    upstream_for = {
        "transcribe": "extract",
        "translate":  "transcribe",
        "tts":        "translate",
        "remux":      "tts",
    }
    if stage == "extract":
        from pathlib import PurePosixPath
        if video_keys:
            parent = str(PurePosixPath(video_keys[0]).parent)
            prefix = (parent + "/") if parent != "." else ""
            existing = list_existing(prefix)
            missing = [k for k in video_keys if k not in existing]
        else:
            missing = []
        if missing:
            sample = ", ".join(missing[:3]) + ("…" if len(missing) > 3 else "")
            return f"{len(missing)}/{len(video_keys)} input video(s) missing in bucket: {sample}"
        return None
    upstream = upstream_for.get(stage)
    if upstream is None:
        return None
    if read_task_report(run_id, upstream) is None:
        return (
            f"upstream report missing: runs/{run_id}/reports/{upstream}.json — "
            f"run probe for '{upstream}' first (same --run-id)"
        )
    return None


async def probe_stage(
    stage: str,
    run_id: str,
    *,
    source: str | None = None,
    target_lang: str | None = None,
) -> int:
    """Launch one Nebius job for ``stage`` under ``run_id``. Return shell-style exit code.

    Reusable helper — used by both the per-stage CLI here and the chained driver
    in ``probe_pipeline_remote.py``. Side effects: writes the task manifest +
    orch report to the bucket; prints timeline / job-id to stdout.

    Args:
        stage: one of STAGES.
        run_id: namespace for outputs under ``runs/<run_id>/`` in the bucket.
        source: bucket-relative video file or folder prefix. Only used when
            ``stage == "extract"``; downstream stages derive inputs from the
            upstream report. Defaults to ``DEFAULT_SOURCE`` for extract.
        target_lang: NLLB target language code; defaults to config.pipeline.target_lang.

    Returns:
        0 on success, 2 on pre-flight failure, 1 on any other failure.
    """
    cfg_root = get_config()
    cfg = getattr(cfg_root.pipeline, stage)
    image = f"{cfg.image_name}:{cfg_root.pipeline.image_tag}"
    target_lang = target_lang or cfg_root.pipeline.target_lang

    # Resolve --source into a list of bucket-relative video keys. Folder
    # prefixes are scanned via S3 LIST; missing/empty prefixes raise here.
    if stage == "extract":
        try:
            video_keys = resolve_video_keys(source or DEFAULT_SOURCE)
        except ValueError as e:
            print(f"✗ source resolution failed: {e}")
            return 2
    else:
        video_keys = []  # downstream stages derive inputs from the upstream report

    err = _preflight(stage, run_id, video_keys)
    if err:
        print(f"✗ pre-flight failed: {err}")
        return 2

    run = PipelineRun(
        video_keys=video_keys,
        run_id=run_id,
        batch_id="probe",
        target_lang=target_lang,
    )

    manifest_path = write_task_manifest(run, stage, executor="nebius")
    n_files = len(video_keys) if stage == "extract" else "<from upstream>"
    print(f"stage:     {stage}")
    print(f"run_id:    {run_id}")
    print(f"image:     {image}")
    print(f"compute:   {cfg.compute.platform}/{cfg.compute.preset}, "
          f"preemptible={cfg.compute.preemptible}, "
          f"timeout={cfg.compute.job_timeout_min}m, "
          f"disk={cfg.compute.job_disk_gb}GB")
    print(f"manifest:  {manifest_path}")
    print(f"inputs:    {n_files} file(s)")
    print("---")

    t0 = time.time()
    try:
        import logging
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        rec = await create_and_wait(
            name=f"probe-{stage}-{run_id}"[:50],
            image=image,
            args=manifest_path,
            job=cfg.compute,
        )
        success = True
    except NebiusJobError as e:
        rec = e.record
        print(f"\n✗ NebiusJobError: {e}")
        success = False
    except Exception as e:
        print(f"\n✗ {type(e).__name__}: {e}")
        return 1

    if success:
        print("\n✓ SUCCESS")

    elapsed = time.time() - t0
    print(f"\nTotal wall time:           {elapsed:6.1f}s")
    print(f"Terminal state:            {rec.get('terminal_state')}")
    print(f"Run time (RUNNING→term):   {_job_run_s(rec):6.1f}s")
    print("\nState-transition timeline:")
    created = rec.get("created_at_s", 0)
    for t in rec.get("state_transitions", []):
        dt = t["observed_at_s"] - created
        print(f"  +{dt:7.1f}s  {t['state']}")
    print(f"\njob_id: {rec.get('job_id')}")
    return 0 if success else 1


async def _cli_main() -> int:
    args = parse_args()
    run_id = args.run_id or f"probe-{int(time.time())}"

    if args.preflight_only:
        if args.stage == "extract":
            try:
                video_keys = resolve_video_keys(args.source or DEFAULT_SOURCE)
            except ValueError as e:
                print(f"✗ source resolution failed: {e}")
                return 2
        else:
            video_keys = []
        err = _preflight(args.stage, run_id, video_keys)
        if err:
            print(f"✗ pre-flight failed: {err}")
            return 2
        n = len(video_keys) if args.stage == "extract" else "upstream report found"
        print(f"✓ pre-flight passed  stage={args.stage}  inputs={n}")
        return 0

    return await probe_stage(
        args.stage,
        run_id,
        source=args.source,
        target_lang=args.target_lang,
    )


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_cli_main()))
