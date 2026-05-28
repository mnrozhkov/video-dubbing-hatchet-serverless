"""
Hatchet workflow — one task per pipeline stage; each launches one or more
Nebius jobs depending on the stage's ``max_concurrent`` setting.

Per-task flow:
    1. Pre-flight: scan S3 for expected outputs. If all present (and not forced),
       skip Nebius launch and write a status="skipped" report.
    2. Otherwise, compute missing inputs, chunk by ``batch_size``, dispatch up to
       ``max_concurrent`` Nebius jobs in parallel via ``asyncio.gather`` under a
       ``Semaphore``. ``max_concurrent == 1`` short-circuits to a single job
       processing all missing inputs (preserves pre-fan-out behavior for CPU
       stages).
    3. Post-flight: re-scan S3; raise if any expected output is still missing.

Fan-out failure model: ``asyncio.gather(*coros, return_exceptions=True)`` lets
all in-flight chunks finish (success or fail) before propagating. On Hatchet
retry, the pre-flight rescan finds succeeded chunks' outputs and re-chunks only
the failed files. Per-chunk preemption recovers via the same mechanism — Nebius
ERROR → ``RuntimeError`` from ``create_and_wait`` → caught by ``gather`` →
aggregate raise → Hatchet retries the task → S3 idempotency does the rest.
"""

from __future__ import annotations

import asyncio

from hatchet_sdk import Context, Hatchet

from pipeline.batch import chunk
from pipeline.config import StageConfig, config
from pipeline.metadata import (
    STAGE_TASKS,
    expected_output_keys,
    missing_inputs,
    write_fan_out_task_report,
    write_run_summary,
    write_skipped_report,
    write_task_chunk_manifest,
    write_task_manifest,
    write_task_orchestration_report,
)
from pipeline.nebius import NebiusJobError, create_and_wait
from pipeline.paths import (
    run_summary_key,
    task_artifacts_prefix,
    task_manifest_container_path,
    task_report_key,
)
from pipeline.run import PipelineRun
from pipeline.storage import list_existing


def _hatchet_timeout(stage: StageConfig) -> str:
    """Hatchet task timeout = 4× Nebius job_timeout_min (covers 3 retries + overhead).
    job_timeout_min is the real limit; this is just a safety net above it."""
    return f"{stage.compute.job_timeout_min * 60 * 4}s"


hatchet = Hatchet(debug=True)

workflow = hatchet.workflow(
    name=config.workflow_name,
    on_events=["dubbing:batch"],
    input_validator=PipelineRun,
)


async def _launch_single_job(task: str, run: PipelineRun, ctx: Context) -> list[dict]:
    """One Nebius job, full input — the ``max_concurrent == 1`` path.

    Returns a 1-element list of job records (success path) or raises with the
    record already attached to the ``NebiusJobError`` (failure path).
    """
    cfg = getattr(config.pipeline, task)
    ctx.log(
        f"[{task}] launching 1 Nebius job ({cfg.compute.platform}/{cfg.compute.preset})"
    )
    write_task_manifest(run, task, executor="nebius")
    attempt = getattr(ctx, "retry_count", 0)
    record = await create_and_wait(
        name=f"{task}-{run.run_id}-{attempt}"[:50],
        image=f"{cfg.image_name}:{config.pipeline.image_tag}",
        args=task_manifest_container_path(run.run_id, task),
        job=cfg.compute,
    )
    return [{**record, "chunk_index": 0}]


async def _launch_fanout(
    task: str,
    run: PipelineRun,
    ctx: Context,
    *,
    video_key_chunks: list[list[str]] | None,
    stem_chunks: list[list[str]] | None,
    max_concurrent: int,
) -> list[dict]:
    """Chunked fan-out: K Nebius jobs in parallel, capped by ``max_concurrent``.

    Uses ``asyncio.gather(..., return_exceptions=True)`` so all in-flight chunks
    complete (success or fail) before propagating. Returns per-chunk job records
    (collected from both successful returns AND ``NebiusJobError.record`` on
    failure paths). Raises an aggregate ``RuntimeError`` if any chunk failed,
    triggering Hatchet retry; the records are still written by the caller before
    the raise propagates.
    """
    cfg = getattr(config.pipeline, task)
    chunks = video_key_chunks if task == "extract" else stem_chunks
    assert chunks is not None
    n = len(chunks)
    ctx.log(
        f"[{task}] launching {n} Nebius jobs ({cfg.compute.platform}/{cfg.compute.preset}), "
        f"up to {max_concurrent} in parallel"
    )

    sem = asyncio.Semaphore(max_concurrent)
    attempt = getattr(ctx, "retry_count", 0)

    async def _run_chunk(i: int, chunk_items: list[str]) -> dict:
        if task == "extract":
            manifest_path = write_task_chunk_manifest(
                run, task, i, video_keys=chunk_items, executor="nebius"
            )
        else:
            manifest_path = write_task_chunk_manifest(
                run, task, i, stems=chunk_items, executor="nebius"
            )
        async with sem:
            ctx.log(f"[{task}] chunk {i}/{n - 1} ({len(chunk_items)} files) → Nebius")
            record = await create_and_wait(
                name=f"{task}-{run.run_id}-{attempt}-c{i}"[:50],
                image=f"{cfg.image_name}:{config.pipeline.image_tag}",
                args=manifest_path,
                job=cfg.compute,
            )
            ctx.log(f"[{task}] chunk {i}/{n - 1} done")
            return {**record, "chunk_index": i}

    results = await asyncio.gather(
        *(_run_chunk(i, c) for i, c in enumerate(chunks)),
        return_exceptions=True,
    )

    records: list[dict] = []
    failures: list[tuple[int, BaseException]] = []
    for i, r in enumerate(results):
        if isinstance(r, NebiusJobError):
            records.append({**r.record, "chunk_index": i, "error": str(r)})
            failures.append((i, r))
        elif isinstance(r, BaseException):
            # CancelledError, network, timeout — no Nebius record available.
            records.append({"chunk_index": i, "error": type(r).__name__})
            failures.append((i, r))
        else:
            records.append(r)

    if failures:
        sample = ", ".join(f"chunk-{i}: {type(e).__name__}" for i, e in failures[:5])
        # Stash records on the exception so the caller can write the orch report
        # before re-raising to Hatchet.
        exc = RuntimeError(f"[{task}] {len(failures)}/{n} chunks failed: {sample}")
        exc.records = records  # type: ignore[attr-defined]
        raise exc
    return records


async def _run_remote(task: str, run: PipelineRun, ctx: Context) -> dict:
    cfg = getattr(config.pipeline, task)
    mc = getattr(config.stages, task).max_concurrent
    bs = cfg.batch_size

    expected = expected_output_keys(run, task)
    existing = list_existing(task_artifacts_prefix(run.run_id, task))
    missing = [k for k in expected if k not in existing]

    if expected and not missing and not run.force:
        ctx.log(f"[{task}] all {len(expected)} outputs present; skipping Nebius launch")
        write_skipped_report(run, task, expected)
        return {
            "task": task,
            "report_key": task_report_key(run.run_id, task),
            "status": "skipped",
            "processed": 0,
            "skipped": len(expected),
        }

    # Pre-flight: confirm any model(s) this stage needs are in the bucket.
    # HF auto-download is broken on FUSE (see .dev/talk.md bug #5), so a missing
    # remote model means we must fail BEFORE burning ~60s of cold-start time.
    # CPU stages (extract, remux) have no deps; pre_flight_check returns True
    # for them.
    from models.preflight import pre_flight_check
    present, missing_models = pre_flight_check(task, location="remote")
    if not present:
        raise RuntimeError(
            f"[{task}] required model(s) missing in bucket: {missing_models}\n"
            f"  Cold start would try to download from HF and fail on FUSE.\n"
            f"  Fix: python scripts/sync_models.py"
        )

    # Branch on max_concurrent. mc==1 keeps the legacy single-job path so the
    # behavior of CPU stages (and any stage the user pins to 1) is unchanged.
    job_records: list[dict] = []
    raise_after_report: BaseException | None = None
    try:
        if mc <= 1:
            job_records = await _launch_single_job(task, run, ctx)
        else:
            # Compute the chunk roster. For a fresh run, this matches "all inputs";
            # for a retry it's only the still-missing inputs (S3 idempotency).
            if run.force:
                video_keys = list(run.video_keys) if task == "extract" else []
                stems: list[str] = []
                if task != "extract":
                    from pipeline.metadata import resolve_upstream_stems
                    stems = resolve_upstream_stems(run.run_id, task)
            else:
                inputs = missing_inputs(run, task)
                video_keys = inputs["video_keys"]
                stems = inputs["stems"]

            if task == "extract":
                chunks_vk = chunk(video_keys, bs)
                job_records = await _launch_fanout(
                    task, run, ctx,
                    video_key_chunks=chunks_vk,
                    stem_chunks=None,
                    max_concurrent=mc,
                )
            else:
                chunks_st = chunk(stems, bs)
                job_records = await _launch_fanout(
                    task, run, ctx,
                    video_key_chunks=None,
                    stem_chunks=chunks_st,
                    max_concurrent=mc,
                )
    except NebiusJobError as exc:
        # Single-job path failure — record is on the exception.
        job_records = [{**exc.record, "chunk_index": 0, "error": str(exc)}]
        raise_after_report = exc
    except RuntimeError as exc:
        # Fan-out aggregate failure — records are stashed on the exception.
        if hasattr(exc, "records"):
            job_records = exc.records  # type: ignore[attr-defined]
        raise_after_report = exc

    # Always write the orchestration report — it carries cost + timeline even
    # for failure paths (Hatchet retry will overwrite next attempt).
    orch_report = write_task_orchestration_report(
        run.run_id, task, jobs=job_records, chunk_count=len(job_records)
    )
    ctx.log(
        f"[{task}] orchestration report: {len(job_records)} job(s), "
        f"cost ≈ ${orch_report['cost_usd']:.4f} "
        f"(on-demand would be ${orch_report['cost_if_on_demand_usd']:.4f})"
    )
    if raise_after_report is not None:
        raise raise_after_report

    # Re-compute expected (extract populates it; downstream needs upstream report).
    expected = expected_output_keys(run, task)
    existing = list_existing(task_artifacts_prefix(run.run_id, task))
    still_missing = [k for k in expected if k not in existing]
    if still_missing:
        sample = ", ".join(still_missing[:5]) + ("…" if len(still_missing) > 5 else "")
        raise RuntimeError(
            f"[{task}] job(s) completed but {len(still_missing)}/{len(expected)} outputs missing: {sample}"
        )

    # Overwrite the partial container report with a consolidated one covering all
    # outputs — in fan-out mode each chunk container writes the same report key and
    # the last one wins, leaving downstream stages with only one chunk's stems.
    processed = len(missing) if missing else 0
    write_fan_out_task_report(run, task, expected, processed=processed)

    return {
        "task": task,
        "report_key": task_report_key(run.run_id, task),
        "status": "completed",
        "processed": len(missing) if missing else len(expected),
        "skipped": len(expected) - (len(missing) if missing else len(expected)),
    }


@workflow.task(execution_timeout=_hatchet_timeout(config.pipeline.extract), retries=config.stages.extract.retries)
async def extract(run: PipelineRun, ctx: Context) -> dict:
    return await _run_remote("extract", run, ctx)


@workflow.task(parents=[extract], execution_timeout=_hatchet_timeout(config.pipeline.transcribe), retries=config.stages.transcribe.retries)
async def transcribe(run: PipelineRun, ctx: Context) -> dict:
    return await _run_remote("transcribe", run, ctx)


@workflow.task(parents=[transcribe], execution_timeout=_hatchet_timeout(config.pipeline.translate), retries=config.stages.translate.retries)
async def translate(run: PipelineRun, ctx: Context) -> dict:
    return await _run_remote("translate", run, ctx)


@workflow.task(parents=[translate], execution_timeout=_hatchet_timeout(config.pipeline.tts), retries=config.stages.tts.retries)
async def tts(run: PipelineRun, ctx: Context) -> dict:
    return await _run_remote("tts", run, ctx)


@workflow.task(parents=[tts], execution_timeout=_hatchet_timeout(config.pipeline.remux), retries=config.stages.remux.retries)
async def remux(run: PipelineRun, ctx: Context) -> dict:
    return await _run_remote("remux", run, ctx)


@workflow.task(parents=[remux], execution_timeout="60s", retries=0)
async def summary(run: PipelineRun, ctx: Context) -> dict:
    """Aggregate per-stage orchestration reports into a single run summary.

    Reads ``runs/<id>/reports/<stage>__orch.json`` for each stage, sums cost +
    on-demand cost, writes ``runs/<id>/run_summary.json``, and logs the headline
    figures so the Hatchet trace view shows them.
    """
    payload = write_run_summary(run.run_id)
    ctx.log(
        f"[summary] run {run.run_id}: total ${payload['total_cost_usd']:.4f} "
        f"vs on-demand ${payload['cost_if_on_demand_usd']:.4f} "
        f"→ {payload['savings_pct']}% saved"
    )
    return {
        "run_id": run.run_id,
        "summary_key": run_summary_key(run.run_id),
        "total_cost_usd": payload["total_cost_usd"],
        "savings_pct": payload["savings_pct"],
    }
