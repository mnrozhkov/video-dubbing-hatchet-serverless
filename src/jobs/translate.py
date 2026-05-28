#!/usr/bin/env python3
"""
NLLB-200 translation job.

Invocation:
    python -m jobs.translate /data/runs/<run_id>/manifests/translate.json
"""

import time
from pathlib import Path

# IMPORTANT: import model_cache BEFORE transformers/torch. transformers
# transitively imports huggingface_hub, which captures HF_HUB_DISABLE_XET at
# its own module-import time. model_cache sets that env var at *its* module
# top, so it must be imported first to take effect.
try:
    import model_cache
except ImportError:
    from models import model_cache

import torch
from transformers import AutoModelForSeq2SeqLM, NllbTokenizer

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

_NLLB_LANG: dict[str, str] = {
    "de": "deu_Latn", "fr": "fra_Latn", "es": "spa_Latn",
    "it": "ita_Latn", "pt": "por_Latn", "nl": "nld_Latn",
    "pl": "pol_Latn", "ru": "rus_Cyrl", "zh": "zho_Hans",
    "ja": "jpn_Jpan", "ko": "kor_Hang", "ar": "arb_Arab",
    "hi": "hin_Deva", "tr": "tur_Latn",
}


def _split_text(text: str, max_chars: int = 700) -> list[str]:
    text = " ".join(text.split())
    chunks: list[str] = []
    cur = ""
    for sent in text.split(". "):
        s = sent.strip()
        if not s:
            continue
        s = s if s.endswith(".") else s + "."
        if len(cur) + len(s) + 1 > max_chars:
            if cur:
                chunks.append(cur.strip())
            cur = s
        else:
            cur = (cur + " " + s).strip()
    if cur:
        chunks.append(cur)
    return chunks


def _translate_one(
    tokenizer: NllbTokenizer,
    model: AutoModelForSeq2SeqLM,
    forced_bos_token_id: int,
    input_path: Path,
    output_path: Path,
    *,
    force: bool = False,
) -> bool:
    """Returns True if processed, False if skipped."""
    if not force and output_path.exists():
        print(f"SKIP (already done): {input_path.name}", flush=True)
        return False
    print(f"FILE: {input_path.name} -> {output_path.name}", flush=True)
    text = input_path.read_text(encoding="utf-8")
    chunks = _split_text(text)
    print(f"  chunks: {len(chunks)}", flush=True)

    translated: list[str] = []
    for i, chunk in enumerate(chunks):
        inputs = tokenizer(
            chunk, return_tensors="pt", truncation=True, max_length=1024
        ).to(model.device)
        out = model.generate(
            **inputs, forced_bos_token_id=forced_bos_token_id, max_new_tokens=512
        )
        translated.append(tokenizer.decode(out[0], skip_special_tokens=True))
        print(f"  chunk {i + 1}/{len(chunks)} done", flush=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)  # S3 FUSE: unlink before write to avoid FileExistsError
    output_path.write_text(" ".join(translated).strip(), encoding="utf-8")
    return True


def _load_model(
    model_name: str,
    target_lang: str,
    device: str,
) -> tuple[NllbTokenizer, AutoModelForSeq2SeqLM, int]:
    ensure_torch_device(device)
    tgt = _NLLB_LANG.get(target_lang, target_lang)
    cache = str(model_cache.hf_hub_cache())
    cached = model_cache.hf_model_cached(model_name)
    if cached:
        print(f"Using cached {model_name}", flush=True)
    else:
        print(f"Downloading {model_name} → {cache}", flush=True)
    print(f"Loading {model_name} on {device} (target={target_lang} → {tgt})", flush=True)
    tokenizer = NllbTokenizer.from_pretrained(
        model_name,
        src_lang="eng_Latn",
        cache_dir=cache,
        local_files_only=cached,
    )
    dtype = torch.float16 if device == "cuda" else torch.float32
    model = AutoModelForSeq2SeqLM.from_pretrained(
        model_name,
        cache_dir=cache,
        local_files_only=cached,
        torch_dtype=dtype,
    ).to(device)
    model.eval()
    forced_bos_token_id = tokenizer.convert_tokens_to_ids(tgt)
    return tokenizer, model, forced_bos_token_id


def run_task(config: dict) -> dict:
    """Process all files described by the manifest dict. Writes report; returns payload."""
    started_at = utc_now()
    t0 = time.perf_counter()
    try:
        model_cache.configure()
        runtime = parse_task_runtime(config, "translate")
        cfg = runtime["config"]
        run_id = runtime["run_id"]
        target_lang = runtime["target_lang"]
        model_name = config_str(cfg, "model")
        device = ensure_torch_device(config_str(cfg, "device"))
        force = runtime["force"]

        stems = resolve_manifest_stems(config)
        items = build_run_items_from_stems(stems, run_id)

        print(
            f"TASK: translate run_id={run_id} | {len(items)} files | "
            f"model={model_name} device={device} target={target_lang} force={force}",
            flush=True,
        )
        tokenizer, model, forced = _load_model(model_name, target_lang, device)

        data = data_root()
        processed = 0
        for idx, item in enumerate(items, 1):
            print(f"\n[{idx}/{len(items)}]", flush=True)
            if _translate_one(
                tokenizer, model, forced,
                data / item["transcript_key"],
                data / item["translated_key"],
                force=force,
            ):
                processed += 1

        skipped = len(items) - processed
        print(f"\nTask complete: {processed} processed, {skipped} skipped", flush=True)
        result = {
            "translated_keys": [i["translated_key"] for i in items],
            "timing": make_timing(
                "translate", total=len(items), processed=processed, skipped=skipped, t0=t0
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
