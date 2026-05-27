"""Task manifests, reports, and upstream input discovery for pipeline runs."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from pipeline.config import config
from pipeline.cost import estimate_cost
from pipeline.paths import (
    RUNS_PREFIX,
    run_summary_key,
    task_chunk_manifest_container_path,
    task_chunk_manifest_key,
    task_manifest_container_path,
    task_manifest_key,
    task_orch_report_key,
    task_report_key,
)
from pipeline.storage import list_objects, object_exists, read_json, upload_json
from pipeline.utils import utc_now

if TYPE_CHECKING:
    from pipeline.run import PipelineRun

STAGE_TASKS: tuple[str, ...] = ("extract", "transcribe", "translate", "tts", "remux")

StageStatus = Literal["completed", "failed", "skipped"]

_MANIFEST_TOP_LEVEL_KEYS = ("task", "run_id", "config", "force", "target_lang")

_TASK_REQUIRED_CONFIG: dict[str, tuple[str, ...]] = {
    "extract": (),
    "transcribe": ("model", "device", "batch_size"),
    "translate": ("model", "device", "batch_size"),
    "tts": ("voice", "lang", "device", "repo", "batch_size"),
    "remux": (),
}

# Upstream task report field → artifact directory to scan as fallback.
_UPSTREAM: dict[str, tuple[str, str, str]] = {
    "transcribe": ("extract", "audio_keys", "extract"),
    "translate": ("transcribe", "transcript_keys", "transcribe"),
    "tts": ("translate", "translated_keys", "translate"),
    "remux": ("tts", "dubbed_keys", "tts"),
}


class _TaskManifest(BaseModel):
    """Config snapshot written before a stage runs."""

    task: str
    run_id: str
    batch_id: str
    input_count: int
    target_lang: str
    force: bool = False
    created_at: str
    executor: str | None = None
    config: dict[str, Any]
    video_keys: list[str] | None = None  # only populated for extract; downstream stages derive inputs from upstream reports
    stems: list[str] | None = None  # chunk-scoped input filter for downstream stages; None = use full upstream report
    chunk_index: int | None = None  # set when this manifest describes one chunk of a fan-out


def read_task_report(run_id: str, task: str) -> dict[str, Any] | None:
    return read_json(task_report_key(run_id, task))


def _stems_from_report(run_id: str, upstream_task: str, output_key: str) -> list[str] | None:
    report = read_task_report(run_id, upstream_task)
    if report is None:
        return None
    keys = report.get("outputs", {}).get(output_key)
    if not keys:
        return None
    return sorted(Path(str(key)).stem for key in keys)


def _stems_from_artifacts(run_id: str, artifact_task: str) -> list[str]:
    prefix = f"{RUNS_PREFIX}/{run_id}/{artifact_task}/"
    keys = list_objects(prefix)
    if artifact_task == "extract":
        return sorted(Path(k).stem for k in keys if k.endswith(".wav"))
    if artifact_task == "transcribe":
        return sorted(
            Path(k).stem
            for k in keys
            if k.endswith(".txt") and not k.endswith("_aligned.json")
        )
    if artifact_task == "translate":
        return sorted(Path(k).stem for k in keys if k.endswith(".txt"))
    if artifact_task == "tts":
        return sorted(Path(k).stem for k in keys if k.endswith(".wav"))
    return []


def resolve_upstream_stems(run_id: str, stage: str) -> list[str]:
    """Return ordered stems for a downstream stage, or ``[]`` if none found."""
    if stage == "extract":
        return []
    if stage not in _UPSTREAM:
        raise ValueError(f"resolve_upstream_stems does not support stage {stage!r}")

    upstream_task, output_key, artifact_task = _UPSTREAM[stage]
    stems = _stems_from_report(run_id, upstream_task, output_key)
    if stems is None:
        stems = _stems_from_artifacts(run_id, artifact_task)
    return stems


def resolve_manifest_stems(manifest: dict[str, Any]) -> list[str]:
    """Like ``resolve_upstream_stems`` but raises when inputs are missing.

    Chunk manifests carry an explicit ``stems`` filter — when present, it scopes
    the container to that chunk's subset. Non-chunk manifests fall back to the
    full upstream report.
    """
    chunk_stems = manifest.get("stems")
    if chunk_stems:
        return list(chunk_stems)
    task = manifest["task"]
    run_id = manifest["run_id"]
    stems = resolve_upstream_stems(run_id, task)
    if stems:
        return stems
    upstream_task, _, _ = _UPSTREAM[task]
    raise FileNotFoundError(
        f"[{task}] no inputs found for run {run_id!r}; "
        f"run `{upstream_task}` first or check runs/{run_id}/reports/{upstream_task}.json"
    )


# ── Container job helpers ─────────────────────────────────────────────────────


def manifest_path_from_argv() -> Path:
    """Return the manifest path passed as the container's first argv."""
    if len(sys.argv) < 2:
        raise SystemExit("usage: <job>.py <task_manifest.json>")
    return Path(sys.argv[1].strip())


def _manifest_object_key(path: Path) -> str:
    key = path.as_posix()
    if key.startswith("/data/"):
        return key[len("/data/") :]
    return key.lstrip("/")


def load_manifest(path: Path) -> dict[str, Any]:
    """Load a task manifest JSON from a container or host path under ``/data``."""
    data = read_json(_manifest_object_key(path))
    if data is None:
        raise FileNotFoundError(f"manifest not found: {path}")
    return data


def parse_task_runtime(manifest: dict[str, Any], task: str) -> dict[str, Any]:
    """Validate manifest fields required at container runtime."""
    missing_top = [key for key in _MANIFEST_TOP_LEVEL_KEYS if key not in manifest]
    if missing_top:
        raise KeyError(f"manifest missing required fields: {missing_top}")

    if manifest["task"] != task:
        raise ValueError(
            f"manifest task {manifest['task']!r} does not match expected {task!r}"
        )

    cfg = manifest["config"]
    if not isinstance(cfg, dict):
        raise TypeError("manifest config must be a dict")

    required = _TASK_REQUIRED_CONFIG.get(task)
    if required is None:
        raise ValueError(f"parse_task_runtime does not support task {task!r}")

    missing_cfg = [key for key in required if key not in cfg]
    if missing_cfg:
        raise KeyError(f"manifest config missing required fields: {missing_cfg}")

    return {
        "task": manifest["task"],
        "run_id": manifest["run_id"],
        "target_lang": manifest["target_lang"],
        "force": bool(manifest["force"]),
        "config": cfg,
    }


def config_str(cfg: dict[str, Any], key: str) -> str:
    value = cfg[key]
    if not isinstance(value, str):
        raise TypeError(f"manifest config[{key!r}] must be a string, got {type(value).__name__}")
    return value


def ensure_torch_device(device: str) -> str:
    if device not in {"cpu", "cuda"}:
        raise ValueError(f"unsupported manifest config.device {device!r}; expected 'cpu' or 'cuda'")
    if device == "cuda":
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError("manifest config.device='cuda' but CUDA is not available in container")
    return device


# ── Orchestrator manifest / report I/O ───────────────────────────────────────


def video_keys_by_stem(run_id: str) -> dict[str, str]:
    """Map stem → source ``video_key`` from the extract task report, when available."""
    report = read_task_report(run_id, "extract")
    if report is None:
        return {}
    outputs = report.get("outputs", {})
    stems = outputs.get("stems")
    video_keys = outputs.get("video_keys")
    if not stems or not video_keys or len(stems) != len(video_keys):
        return {}
    return dict(zip(stems, video_keys, strict=True))


def _resolve_task_device(task: str) -> str:
    """Runtime device for a task manifest (``cpu`` / ``cuda``)."""
    if task in ("extract", "remux"):
        return "cpu"
    task_cfg = getattr(config.pipeline, task)
    configured = getattr(task_cfg, "device", "cuda")
    return configured if task_cfg.compute.platform.startswith("gpu-") else "cpu"


def _task_config(task: str, *, cli_overrides: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    task_settings = {
        "extract": config.pipeline.extract,
        "transcribe": config.pipeline.transcribe,
        "translate": config.pipeline.translate,
        "tts": config.pipeline.tts,
        "remux": config.pipeline.remux,
    }
    if task not in task_settings:
        raise ValueError(f"Unknown task {task!r}")

    cfg = task_settings[task].model_dump()
    overrides = (cli_overrides or {}).get(task, {})
    cfg.update({key: value for key, value in overrides.items() if key != "device"})
    cfg["device"] = overrides["device"] if "device" in overrides else _resolve_task_device(task)
    return cfg


def _input_count(run: PipelineRun, task: str) -> int:
    if task == "extract":
        return len(run.video_keys)
    stems = resolve_upstream_stems(run.run_id, task)
    return len(stems) if stems else len(run.video_keys)


def build_task_manifest(
    run: PipelineRun,
    task: str,
    *,
    cli_overrides: dict[str, dict[str, Any]] | None = None,
    executor: str | None = None,
) -> _TaskManifest:
    return _TaskManifest(
        task=task,
        run_id=run.run_id,
        batch_id=run.batch_id,
        input_count=_input_count(run, task),
        target_lang=run.target_lang,
        force=run.force,
        created_at=utc_now(),
        executor=executor,
        config=_task_config(task, cli_overrides=cli_overrides),
        video_keys=list(run.video_keys) if task == "extract" else None,
    )


def write_task_manifest(
    run: PipelineRun,
    task: str,
    *,
    cli_overrides: dict[str, dict[str, Any]] | None = None,
    executor: str | None = None,
) -> str:
    """Write task manifest JSON; return container path under ``/data``."""
    manifest = build_task_manifest(
        run,
        task,
        cli_overrides=cli_overrides,
        executor=executor,
    )
    upload_json(manifest.model_dump(), task_manifest_key(run.run_id, task))
    return task_manifest_container_path(run.run_id, task)


def write_task_chunk_manifest(
    run: PipelineRun,
    task: str,
    chunk_index: int,
    *,
    video_keys: list[str] | None = None,
    stems: list[str] | None = None,
    cli_overrides: dict[str, dict[str, Any]] | None = None,
    executor: str | None = None,
) -> str:
    """Write a chunk-scoped manifest; return container path under ``/data``.

    ``video_keys`` scopes extract chunks; ``stems`` scopes downstream stage chunks
    (transcribe/translate/tts/remux). Exactly one should be set for the relevant stage.
    """
    base = build_task_manifest(
        run,
        task,
        cli_overrides=cli_overrides,
        executor=executor,
    )
    manifest = base.model_copy(
        update={
            "chunk_index": chunk_index,
            "input_count": len(video_keys) if video_keys is not None else (len(stems) if stems is not None else base.input_count),
            "video_keys": list(video_keys) if video_keys is not None else (list(run.video_keys) if task == "extract" else None),
            "stems": list(stems) if stems is not None else None,
        }
    )
    upload_json(manifest.model_dump(), task_chunk_manifest_key(run.run_id, task, chunk_index))
    return task_chunk_manifest_container_path(run.run_id, task, chunk_index)


# ── Orchestrator-side reporting (Nebius job records + cost) ──────────────────


def _job_run_s(record: dict[str, Any]) -> float:
    """Seconds between first ``RUNNING`` observation and the terminal observation.

    Returns 0 when the timeline never reached RUNNING (e.g. cancelled in QUEUED).
    """
    transitions = record.get("state_transitions") or []
    running_at = None
    terminal_at = None
    for t in transitions:
        state = t.get("state")
        ts = t.get("observed_at_s")
        if running_at is None and state == "RUNNING":
            running_at = ts
        if state in ("COMPLETED", "FAILED", "CANCELLED", "ERROR"):
            terminal_at = ts
    if running_at is None or terminal_at is None or terminal_at < running_at:
        return 0.0
    return float(terminal_at - running_at)


def _job_costs(record: dict[str, Any]) -> tuple[float, float, float]:
    """Return (run_s, actual_usd, on_demand_usd) for one job record."""
    platform = record.get("platform", "")
    preset = record.get("preset", "")
    run_s = _job_run_s(record)
    actual = estimate_cost(platform, preset, bool(record.get("preemptible", False)), run_s)
    on_demand = estimate_cost(platform, preset, False, run_s)
    return run_s, actual, on_demand


def write_task_orchestration_report(
    run_id: str,
    task: str,
    *,
    jobs: list[dict[str, Any]],
    chunk_count: int,
) -> dict[str, Any]:
    """Persist per-chunk Nebius job records for one stage + the cost rollup."""
    enriched: list[dict[str, Any]] = []
    total_actual = 0.0
    total_on_demand = 0.0
    for j in jobs:
        run_s, actual, on_demand = _job_costs(j)
        enriched.append({**j, "run_s": round(run_s, 2), "estimated_usd": round(actual, 4)})
        total_actual += actual
        total_on_demand += on_demand
    cost_usd = round(total_actual, 4)
    on_demand_usd = round(total_on_demand, 4)
    payload = {
        "task": task,
        "run_id": run_id,
        "completed_at": utc_now(),
        "chunk_count": chunk_count,
        "cost_usd": cost_usd,
        "cost_if_on_demand_usd": on_demand_usd,
        "jobs": enriched,
    }
    upload_json(payload, task_orch_report_key(run_id, task))
    return payload


def _read_task_orchestration_report(run_id: str, task: str) -> dict[str, Any] | None:
    return read_json(task_orch_report_key(run_id, task))


def write_run_summary(run_id: str) -> dict[str, Any]:
    """Aggregate per-stage orchestration reports into a single run summary."""
    stages_data: list[dict[str, Any]] = []
    for task in STAGE_TASKS:
        report = _read_task_orchestration_report(run_id, task)
        if report is None:
            continue
        stages_data.append({
            "task": task,
            "chunk_count": report.get("chunk_count", 0),
            "cost_usd": report.get("cost_usd", 0.0),
            "cost_if_on_demand_usd": report.get("cost_if_on_demand_usd", 0.0),
        })

    total = round(sum(s["cost_usd"] for s in stages_data), 4)
    on_demand = round(sum(s["cost_if_on_demand_usd"] for s in stages_data), 4)
    savings = round(on_demand - total, 4)
    pct = round((savings / on_demand * 100), 1) if on_demand > 0 else 0.0

    payload = {
        "run_id": run_id,
        "completed_at": utc_now(),
        "stages": stages_data,
        "total_cost_usd": total,
        "cost_if_on_demand_usd": on_demand,
        "savings_usd": savings,
        "savings_pct": pct,
    }
    upload_json(payload, run_summary_key(run_id))
    return payload


def _stage_status(result: dict[str, Any], *, failed: bool) -> StageStatus:
    if failed:
        return "failed"
    timing = result.get("timing")
    if not timing:
        return "completed"
    if (
        timing.get("processed_files", 0) == 0
        and timing.get("skipped_files", 0) == timing.get("total_files", 0)
        and timing.get("total_files", 0) > 0
    ):
        return "skipped"
    return "completed"


# Output key field per task (what each stage produces).
_OUTPUT_FIELDS: dict[str, tuple[str, ...]] = {
    "extract": ("audio_key",),
    "transcribe": ("transcript_key", "aligned_key"),
    "translate": ("translated_key",),
    "tts": ("dubbed_key",),
    "remux": ("output_key",),
}


def _items_for(run: PipelineRun, task: str) -> list[dict[str, str]]:
    """Build per-file item dicts (stem, video_key, output paths) for this stage.

    Shared by ``expected_output_keys`` and ``missing_inputs`` so fan-out chunking
    walks the exact same items the pre-flight scan uses.
    """
    from pipeline.paths import build_run_items, build_run_items_from_stems

    if task not in _OUTPUT_FIELDS:
        raise ValueError(f"Unknown task {task!r}")

    if task == "extract":
        return build_run_items(list(run.video_keys), run.run_id)
    stems = resolve_upstream_stems(run.run_id, task)
    if not stems:
        return []
    return build_run_items_from_stems(
        stems,
        run.run_id,
        video_keys_by_stem=video_keys_by_stem(run.run_id) if task == "remux" else None,
    )


def expected_output_keys(run: PipelineRun, task: str) -> list[str]:
    """All artifact object keys this stage is expected to produce for *run*.

    Used by Hatchet pre-flight to decide whether work is needed: scan upstream
    report (or ``run.video_keys`` for extract) to derive stems, then build the
    full output-key list. Returns an empty list when upstream hasn't run.
    """
    items = _items_for(run, task)
    keys: list[str] = []
    for item in items:
        for field in _OUTPUT_FIELDS[task]:
            keys.append(item[field])
    return keys


def missing_inputs(run: PipelineRun, task: str) -> dict[str, list[str]]:
    """Inputs that still need processing for *task* — outputs not yet in S3.

    Returns ``{"video_keys": [...], "stems": []}`` for extract, or
    ``{"video_keys": [], "stems": [...]}`` for downstream stages. Empty values
    on both keys mean nothing to do.
    """
    items = _items_for(run, task)
    output_fields = _OUTPUT_FIELDS[task]

    if task == "extract":
        video_keys = [
            item["video_key"]
            for item in items
            if any(not object_exists(item[f]) for f in output_fields)
        ]
        return {"video_keys": video_keys, "stems": []}

    stems = [
        item["stem"]
        for item in items
        if any(not object_exists(item[f]) for f in output_fields)
    ]
    return {"video_keys": [], "stems": stems}


def write_skipped_report(run: PipelineRun, task: str, expected: list[str]) -> dict[str, Any]:
    """Emit a status='skipped' report when Hatchet pre-flight finds nothing to do.

    Shape mirrors the report a successful ``run_task`` would write, so downstream
    pre-flight (which reads upstream report outputs) keeps working.
    """
    started = utc_now()
    field = _OUTPUT_FIELDS[task][0]
    outputs_key = {
        "extract": "audio_keys",
        "transcribe": "transcript_keys",
        "translate": "translated_keys",
        "tts": "dubbed_keys",
        "remux": "output_keys",
    }[task]
    # For transcribe we also expose aligned_keys (paired with transcript_keys).
    extra: dict[str, list[str]] = {}
    if task == "transcribe":
        n = len(expected) // 2
        # expected alternates [transcript, aligned] per stem
        extra["aligned_keys"] = expected[1::2]
        result_keys = expected[0::2]
    else:
        result_keys = expected

    result: dict[str, Any] = {outputs_key: result_keys, **extra}
    if task == "extract":
        result["video_keys"] = list(run.video_keys)
        result["stems"] = [Path(k).stem for k in result_keys]

    timing = {
        "task": task,
        "total_files": len(result_keys),
        "processed_files": 0,
        "skipped_files": len(result_keys),
        "wall_s": 0.0,
        "per_file_s": 0.0,
    }
    result["timing"] = timing
    _write_task_report(run.run_id, run.batch_id, task, result, started_at=started)
    return result


def make_timing(task: str, *, total: int, processed: int, skipped: int, t0: float) -> dict[str, Any]:
    """Build the per-task timing dict embedded in a report."""
    wall_s = round(time.perf_counter() - t0, 2)
    per_file_s = round(wall_s / processed, 2) if processed else 0.0
    return {
        "task": task,
        "total_files": total,
        "processed_files": processed,
        "skipped_files": skipped,
        "wall_s": wall_s,
        "per_file_s": per_file_s,
    }


def record_task_result(
    config: dict[str, Any],
    result: dict[str, Any],
    *,
    started_at: str,
    failed: bool = False,
    error: str | None = None,
) -> None:
    """Convenience: pull run_id/batch_id/task from a manifest dict and write the report."""
    _write_task_report(
        config["run_id"],
        config["batch_id"],
        config["task"],
        result,
        started_at=started_at,
        failed=failed,
        error=error,
    )


def _write_task_report(
    run_id: str,
    batch_id: str,
    task: str,
    result: dict[str, Any],
    *,
    started_at: str,
    failed: bool = False,
    error: str | None = None,
) -> None:
    """Write one task report JSON (overwrites prior report for this task)."""
    timing = result.get("timing")
    outputs = {key: value for key, value in result.items() if key != "timing"}
    status = _stage_status(result, failed=failed)

    manifest_key = task_manifest_key(run_id, task)
    manifest = read_json(manifest_key)
    device = manifest.get("config", {}).get("device") if manifest else None

    report: dict[str, Any] = {
        "task": task,
        "run_id": run_id,
        "batch_id": batch_id,
        "status": status,
        "device": device,
        "started_at": started_at,
        "completed_at": utc_now(),
        "manifest_key": manifest_key,
        "wall_s": timing.get("wall_s") if timing else None,
        "timing": timing,
        "outputs": outputs,
        "error": error,
    }
    upload_json(report, task_report_key(run_id, task))
