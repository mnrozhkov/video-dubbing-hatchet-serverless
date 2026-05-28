#!/usr/bin/env python3
"""
Transcription + alignment job (Whisper ASR + WhisperX word-level timestamps).

Invocation:
    python -m jobs.transcribe /data/runs/<run_id>/manifests/transcribe.json

The manifest carries ``run_id``, task ``config``, and ``force``; per-file paths are
derived from the upstream extract report (``runs/{run_id}/reports/extract.json``).
"""

import json
import time
from pathlib import Path

# IMPORTANT: import model_cache BEFORE whisperx/faster_whisper. Both
# transitively import huggingface_hub, which captures HF_HUB_DISABLE_XET at
# its own module-import time. model_cache sets that env var at *its* module
# top, so it must be imported first to take effect.
try:
    import model_cache
except ImportError:
    from models import model_cache

import whisperx
from faster_whisper import WhisperModel

from pipeline.metadata import (
    config_str,
    ensure_torch_device,
    load_manifest,
    make_timing,
    manifest_path_from_argv,
    parse_task_runtime,
    record_task_result,
    resolve_manifest_stems,
)
from pipeline.paths import build_run_items_from_stems
from pipeline.storage import data_root
from pipeline.utils import utc_now

model_cache.configure()


def _transcribe_one(
    whisper_model: WhisperModel,
    audio_path: Path,
    transcript_path: Path,
) -> tuple[list[dict], str]:
    raw_segments, info = whisper_model.transcribe(str(audio_path), beam_size=5)
    segments = list(raw_segments)
    print(
        f"  language: {info.language} ({info.language_probability:.2f}) | {len(segments)} segments",
        flush=True,
    )
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    transcript_path.unlink(missing_ok=True)  # S3 FUSE: unlink before write to avoid FileExistsError
    transcript_path.write_text(
        "\n".join(s.text.strip() for s in segments if s.text.strip()) + "\n",
        encoding="utf-8",
    )
    wx_segments = [
        {"text": s.text, "start": s.start, "end": s.end}
        for s in segments
        if s.text.strip()
    ]
    return wx_segments, info.language


def _align_one(
    align_cache: dict,
    wx_segments: list[dict],
    audio_path: Path,
    language: str,
    device: str,
    aligned_path: Path,
) -> None:
    if language not in align_cache:
        print(f"  loading align model for language={language}", flush=True)
        model_a, metadata = whisperx.load_align_model(
            language_code=language,
            device=device,
            model_dir=str(model_cache.hf_hub_cache()),
        )
        align_cache[language] = (model_a, metadata)
    model_a, metadata = align_cache[language]
    audio = whisperx.load_audio(str(audio_path))
    result = whisperx.align(wx_segments, model_a, metadata, audio, device)
    aligned_path.parent.mkdir(parents=True, exist_ok=True)
    aligned_path.unlink(missing_ok=True)  # S3 FUSE: unlink before write to avoid FileExistsError
    aligned_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))


def _process_file(
    whisper_model: WhisperModel,
    align_cache: dict,
    device: str,
    audio_path: Path,
    transcript_path: Path,
    aligned_path: Path,
    *,
    force: bool = False,
) -> bool:
    """Returns True if processed, False if skipped."""
    if not force and transcript_path.exists() and aligned_path.exists():
        print(f"SKIP (already done): {audio_path.name}", flush=True)
        return False
    print(f"FILE: {audio_path.name}", flush=True)
    wx_segments, language = _transcribe_one(whisper_model, audio_path, transcript_path)
    _align_one(align_cache, wx_segments, audio_path, language, device, aligned_path)
    print(f"  done: {transcript_path.name}, {aligned_path.name}", flush=True)
    return True


def _load_whisper(model_size: str, device: str, compute_type: str) -> WhisperModel:
    cached = model_cache.whisper_model_cached(model_size)
    if cached:
        print(f"Using cached faster-whisper {model_size}", flush=True)
    else:
        print(f"Downloading faster-whisper {model_size} → {model_cache.faster_whisper_root()}", flush=True)
    return WhisperModel(
        model_size,
        device=device,
        compute_type=compute_type,
        download_root=str(model_cache.faster_whisper_root()),
        local_files_only=cached,
    )


def run_task(config: dict) -> dict:
    """Process all files described by the manifest dict. Writes report; returns payload."""
    started_at = utc_now()
    t0 = time.perf_counter()
    try:
        model_cache.configure()
        runtime = parse_task_runtime(config, "transcribe")
        cfg = runtime["config"]
        run_id = runtime["run_id"]
        model_size = config_str(cfg, "model")
        device = ensure_torch_device(config_str(cfg, "device"))
        force = runtime["force"]
        compute_type = "float16" if device == "cuda" else "int8"

        stems = resolve_manifest_stems(config)
        items = build_run_items_from_stems(stems, run_id)

        print(
            f"TASK: transcribe run_id={run_id} | {len(items)} files | "
            f"model={model_size} device={device} force={force}",
            flush=True,
        )
        print(f"Loading model {model_size} on {device}", flush=True)
        whisper_model = _load_whisper(model_size, device, compute_type)
        align_cache: dict = {}

        data = data_root()
        processed = 0
        for idx, item in enumerate(items, 1):
            print(f"\n[{idx}/{len(items)}]", flush=True)
            if _process_file(
                whisper_model,
                align_cache,
                device,
                data / item["audio_key"],
                data / item["transcript_key"],
                data / item["aligned_key"],
                force=force,
            ):
                processed += 1

        skipped = len(items) - processed
        print(f"\nTask complete: {processed} processed, {skipped} skipped", flush=True)
        result = {
            "transcript_keys": [i["transcript_key"] for i in items],
            "aligned_keys": [i["aligned_key"] for i in items],
            "timing": make_timing(
                "transcribe", total=len(items), processed=processed, skipped=skipped, t0=t0
            ),
        }
        record_task_result(config, result, started_at=started_at)
        return result
    except Exception as exc:
        record_task_result(config, {}, started_at=started_at, failed=True, error=str(exc))
        raise


def main() -> None:
    config = load_manifest(manifest_path_from_argv())
    run_task(config)


if __name__ == "__main__":
    main()
