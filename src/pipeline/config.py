"""
Two-tier configuration:

- ``config`` (``HatchetConfig``) — top-level user-facing config. The orchestrator
  (Hatchet) sits at the root and contains the pipeline it runs:

      HatchetConfig                          ← top-level, BaseSettings (env-aware)
      ├── workflow_name, timeout_buffer_s
      ├── stages: HatchetStages              ← per-stage orchestration (max_concurrent, retries)
      └── pipeline: PipelineConfig           ← what gets orchestrated
          ├── target_lang, image_tag
          └── extract, transcribe, …         ← per-stage algorithm + compute (StageConfig)

- ``secrets`` (``Secrets``) — cloud credentials and bindings. Loaded from ``.env``
  (gitignored) by convention.

Override nested fields via double-underscore env vars, e.g.:

    PIPELINE__IMAGE_TAG=v0.2.0
    PIPELINE__TARGET_LANG=de
    PIPELINE__TRANSCRIBE__COMPUTE__PLATFORM=gpu-h200-sxm
    PIPELINE__TRANSCRIBE__MODEL=large-v3
    STAGES__TRANSCRIBE__MAX_CONCURRENT=8
    STAGES__TRANSCRIBE__RETRIES=5
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Nebius platform reference (verify in console):
# | GPU / CPU | platform     | example preset     |
# | L40S      | gpu-l40s-d   | 1gpu-16vcpu-96gb   |
# | H100/H200 | gpu-h100-sxm | 1gpu-16vcpu-200gb  |
# | CPU       | cpu-e2       | 4vcpu-16gb …       |


class Compute(BaseModel):
    """Cloud machine spec for one pipeline stage — feeds directly into Nebius ``JobSpec``.

    ``job_timeout_min`` is the single source of truth for "how long this stage may
    run". The Hatchet ``execution_timeout`` is derived from it plus
    ``HatchetConfig.timeout_buffer_s``.
    """

    platform: str = "cpu-e2"
    preset: str = "4vcpu-16gb"
    preemptible: bool = False
    job_disk_gb: int = 450
    job_timeout_min: int = 60


class StageConfig(BaseModel):
    image_name: str  # registry/repo without tag; tag from PipelineConfig.image_tag
    batch_size: int = 100  # files per Nebius job (chunk size)
    compute: Compute = Field(default_factory=Compute)


class ExtractConfig(StageConfig):
    image_name: str = "mnrozhkov/video-dubbing-extract"
    batch_size: int = 1000
    compute: Compute = Field(default_factory=lambda: Compute(job_timeout_min=20))


class TranscribeConfig(StageConfig):
    image_name: str = "mnrozhkov/video-dubbing-transcribe"
    model: str = "distil-large-v3"
    device: str = "cuda"
    align_lang: str = "en"  # WhisperX align weights for English source audio
    compute: Compute = Field(
        default_factory=lambda: Compute(
            platform="gpu-l40s-d",
            preset="1gpu-16vcpu-96gb",
            preemptible=True,
            job_timeout_min=90,
        )
    )


class TranslateConfig(StageConfig):
    image_name: str = "mnrozhkov/video-dubbing-translate"
    model: str = "facebook/nllb-200-distilled-1.3B"
    device: str = "cuda"
    compute: Compute = Field(
        default_factory=lambda: Compute(
            platform="gpu-l40s-d",
            preset="1gpu-16vcpu-96gb",
            preemptible=True,
        )
    )


class TtsConfig(StageConfig):
    image_name: str = "mnrozhkov/video-dubbing-tts"
    voice: str = "af_bella"
    lang: str = "e"  # Kokoro Spanish pipeline (EN source → ES dub)
    repo: str = "hexgrad/Kokoro-82M"
    device: str = "cuda"
    compute: Compute = Field(
        default_factory=lambda: Compute(
            platform="gpu-l40s-d",
            preset="1gpu-16vcpu-96gb",
            preemptible=True,
        )
    )


class RemuxConfig(StageConfig):
    image_name: str = "mnrozhkov/video-dubbing-remux"
    batch_size: int = 1000
    compute: Compute = Field(default_factory=lambda: Compute(job_timeout_min=20))


class PipelineConfig(BaseModel):
    """What gets orchestrated — pure pipeline definition. No env loading here;
    that happens at ``HatchetConfig`` (the top-level ``BaseSettings``)."""

    target_lang: str = "es"  # NLLB translate target (EN → ES); override via PIPELINE__TARGET_LANG
    image_tag: str = "v0.2.0"  # applied to every stage's image; override via PIPELINE__IMAGE_TAG

    extract:    ExtractConfig    = Field(default_factory=ExtractConfig)
    transcribe: TranscribeConfig = Field(default_factory=TranscribeConfig)
    translate:  TranslateConfig  = Field(default_factory=TranslateConfig)
    tts:        TtsConfig        = Field(default_factory=TtsConfig)
    remux:      RemuxConfig      = Field(default_factory=RemuxConfig)


class StageOrchestration(BaseModel):
    """Hatchet orchestration knobs for one stage.

    ``max_concurrent`` is the cap on parallel Nebius jobs for this stage when
    fan-out lands (Phase 4 / .dev/spec.md). Today it's declared but unwired.
    ``retries`` IS wired and feeds ``@workflow.task(retries=…)`` — primary use is
    preemption recovery (Nebius ERROR → RuntimeError → Hatchet retries).
    """

    max_concurrent: int = 1
    retries: int = 3


class HatchetStages(BaseModel):
    """Per-stage Hatchet orchestration — mirrors PipelineConfig stage names."""

    extract:    StageOrchestration = Field(default_factory=lambda: StageOrchestration(max_concurrent=1))
    transcribe: StageOrchestration = Field(default_factory=lambda: StageOrchestration(max_concurrent=10))
    translate:  StageOrchestration = Field(default_factory=lambda: StageOrchestration(max_concurrent=10))
    tts:        StageOrchestration = Field(default_factory=lambda: StageOrchestration(max_concurrent=10))
    remux:      StageOrchestration = Field(default_factory=lambda: StageOrchestration(max_concurrent=1))


class HatchetConfig(BaseSettings):
    """Top-level config: Hatchet orchestrator + the pipeline it runs.

    Loaded lazily from process env + ``.env`` via ``get_config()``. Cloud
    credentials default to empty strings so local-only runs work without
    Hatchet/Nebius configuration.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # ── Workflow-level ─────────────────────────────────────────────────
    workflow_name: str = "video-dubbing-batch-pipeline"
    timeout_buffer_s: int = 600  # cold start + SDK overhead; added to job_timeout_min

    # ── Per-stage orchestration ────────────────────────────────────────
    stages: HatchetStages = Field(default_factory=HatchetStages)

    # ── What's being orchestrated ──────────────────────────────────────
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)


class Secrets(BaseSettings):
    """Cloud credentials and bindings — loaded from ``.env`` (never committed)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    hatchet_client_token: str = ""
    nebius_iam_token: str = ""
    nebius_project_id: str = ""
    nebius_subnet_id: str = ""
    nebius_bucket_id: str = ""
    nebius_bucket_name: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_endpoint_url: str = "https://storage.eu-north1.nebius.cloud"


_config: HatchetConfig | None = None
_secrets: Secrets | None = None


def get_config() -> HatchetConfig:
    """Return cached config, loading ``.env`` on first call."""
    global _config
    if _config is None:
        _config = HatchetConfig()
    return _config


def _get_secrets() -> Secrets:
    """Return cached secrets, loading ``.env`` on first call."""
    global _secrets
    if _secrets is None:
        _secrets = Secrets()
    return _secrets


def require_cloud_setting(name: str, value: str) -> str:
    """Validate a cloud credential is configured before Nebius/S3 use."""
    if not value:
        raise RuntimeError(
            f"{name} is not set. Add it to .env (see .env.example) for cloud pipeline runs."
        )
    return value


class _ConfigProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(get_config(), name)


class _SecretsProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(_get_secrets(), name)


config = _ConfigProxy()
secrets = _SecretsProxy()
