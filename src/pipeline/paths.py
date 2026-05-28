"""Bucket-relative paths under the ``/data`` mount — input sources and run outputs."""

from __future__ import annotations

from pathlib import Path

VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v"}

RUNS_PREFIX = "runs"


def repo_data_dir() -> Path:
    """Host ``data/`` directory at the repository root."""
    return Path(__file__).resolve().parents[2] / "data"


# ── Input keys (video sources) ───────────────────────────────────────────────


def _normalize_bucket_key(path: str) -> str:
    """Normalize to a bucket-relative key (no leading slash, no ``data/`` host prefix)."""
    key = path.strip().replace("\\", "/")
    if key.startswith("./"):
        key = key[2:]
    if key.startswith("/data/"):
        key = key[len("/data/") :]
    elif key.startswith("data/"):
        key = key[len("data/") :]
    return key.lstrip("/")


def _is_video_key(key: str) -> bool:
    return Path(key).suffix.lower() in VIDEO_EXTS


def _scan_bucket_prefix(prefix: str) -> list[str]:
    """List video keys under a bucket prefix."""
    from pipeline.storage import list_objects

    normalized = _normalize_bucket_key(prefix)
    if normalized and not normalized.endswith("/"):
        normalized = f"{normalized}/"
    keys = list_objects(normalized)
    return sorted(k for k in keys if _is_video_key(k))


def _scan_local_prefix(prefix: str, data_root: Path) -> list[str]:
    """List video keys under a host ``data/`` directory."""
    key = _normalize_bucket_key(prefix)
    prefix_path = data_root / key
    if not prefix_path.exists():
        raise ValueError(f"No video files found under prefix {key!r}")
    scan_root = prefix_path if prefix_path.is_dir() else data_root / key
    keys = sorted(
        p.relative_to(data_root).as_posix()
        for p in scan_root.rglob("*")
        if p.is_file() and _is_video_key(p.name)
    )
    if not keys:
        raise ValueError(f"No video files found under prefix {key!r}")
    return keys


def resolve_video_keys(source: str, *, data_root: Path | None = None) -> list[str]:
    """
    Resolve a single video path or folder prefix to ``video_keys``.

    Paths are bucket-relative (same layout as inside a job at ``/data/…``):

        sample.mp4                    → one file
        sample_batch/001_sample.mp4   → one file
        sample_batch/                 → all videos under prefix
        data/sample_batch/            → same (host ``data/`` stripped)

    When ``data_root`` is set, scan the host ``data/`` tree instead of the bucket.
    """
    key = _normalize_bucket_key(source)
    if not key:
        raise ValueError("Input path is empty")

    if _is_video_key(key):
        if data_root is not None and not (data_root / key).is_file():
            raise ValueError(f"Video file not found: {data_root / key}")
        return [key]

    if data_root is not None:
        return _scan_local_prefix(key, data_root)

    keys = _scan_bucket_prefix(key)
    if not keys:
        raise ValueError(f"No video files found under prefix {key!r}")
    return keys


# ── Output keys (run artifacts) ──────────────────────────────────────────────


def _artifact_key(run_id: str, task: str, filename: str) -> str:
    """Object key: ``runs/{run_id}/{task}/{filename}``."""
    return f"{RUNS_PREFIX}/{run_id}/{task}/{filename}"


def task_manifest_key(run_id: str, task: str) -> str:
    return f"{RUNS_PREFIX}/{run_id}/manifests/{task}.json"


def task_manifest_container_path(run_id: str, task: str) -> str:
    return f"/data/{task_manifest_key(run_id, task)}"


def task_chunk_manifest_key(run_id: str, task: str, chunk_index: int) -> str:
    return f"{RUNS_PREFIX}/{run_id}/manifests/{task}__chunk-{chunk_index:03d}.json"


def task_chunk_manifest_container_path(run_id: str, task: str, chunk_index: int) -> str:
    return f"/data/{task_chunk_manifest_key(run_id, task, chunk_index)}"


def task_report_key(run_id: str, task: str) -> str:
    return f"{RUNS_PREFIX}/{run_id}/reports/{task}.json"


def task_orch_report_key(run_id: str, task: str) -> str:
    """Orchestrator-side sidecar report — per-chunk Nebius job records + cost."""
    return f"{RUNS_PREFIX}/{run_id}/reports/{task}__orch.json"


def run_summary_key(run_id: str) -> str:
    """Final aggregated report for one batch run — cost rollup across all stages."""
    return f"{RUNS_PREFIX}/{run_id}/run_summary.json"


def task_artifacts_prefix(run_id: str, task: str) -> str:
    """Prefix covering all artifact outputs for one stage: ``runs/{run_id}/{task}/``."""
    return f"{RUNS_PREFIX}/{run_id}/{task}/"


def _build_run_item(stem: str, run_id: str, *, video_key: str = "") -> dict[str, str]:
    return {
        "video_key": video_key,
        "stem": stem,
        "audio_key": _artifact_key(run_id, "extract", f"{stem}.wav"),
        "transcript_key": _artifact_key(run_id, "transcribe", f"{stem}.txt"),
        "aligned_key": _artifact_key(run_id, "transcribe", f"{stem}_aligned.json"),
        "translated_key": _artifact_key(run_id, "translate", f"{stem}.txt"),
        "dubbed_key": _artifact_key(run_id, "tts", f"{stem}.wav"),
        "output_key": _artifact_key(run_id, "remux", f"{stem}.mp4"),
    }


def build_run_items_from_stems(
    stems: list[str],
    run_id: str,
    *,
    video_keys_by_stem: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    vk = video_keys_by_stem or {}
    return [_build_run_item(stem, run_id, video_key=vk.get(stem, "")) for stem in stems]


def build_run_items(video_keys: list[str], run_id: str) -> list[dict[str, str]]:
    """Map each input video to per-task output keys under ``runs/{run_id}/``."""
    items: list[dict[str, str]] = []
    for video_key in video_keys:
        stem = Path(video_key).stem
        items.append(_build_run_item(stem, run_id, video_key=video_key))
    return items
