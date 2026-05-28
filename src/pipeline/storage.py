"""
Object storage helpers — S3-compatible interface for Nebius Object Storage.

Pure boto3 functions, no framework dependency. Used by the workflow to verify
job outputs and to filter already-completed inputs (resume on retry).

When ``use_local_artifacts(root)`` is active, reads/writes map to ``root / key``
instead of the bucket (local Docker pipeline runs).
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Callable, Generator, Iterable, TypeVar

import boto3
from botocore.exceptions import ClientError

from pipeline.config import require_cloud_setting, secrets

logger = logging.getLogger(__name__)

T = TypeVar("T")

OutputKeyFn = Callable[[T], str | Iterable[str]]

_local_root: ContextVar[Path | None] = ContextVar("local_artifact_root", default=None)
_default_data_root: Path | None = None


def configure_data_root(root: Path) -> None:
    """Default filesystem root for artifact I/O (container ``/data`` mount)."""
    global _default_data_root
    _default_data_root = root.resolve()


def auto_configure_data_root() -> bool:
    """If ``/data`` exists (container has the bucket FUSE-mounted), point storage
    helpers at it so manifest/report I/O goes through the filesystem rather than
    the S3 client. Returns True when applied, False on host machines without /data.
    Safe to call multiple times; idempotent.
    """
    mount = Path("/data")
    if mount.is_dir():
        configure_data_root(mount)
        return True
    return False


@contextmanager
def staged_write(final_path: Path) -> Generator[Path, None, None]:
    """Write to a seekable local tmp file, then move to ``final_path`` on success.

    Object-storage FUSE mounts at ``/data`` don't support seeking, but ffmpeg
    (WAV RIFF header, MP4 moov atom) and soundfile (RIFF size field) need
    seek-back to finalize file headers. Writing direct to ``/data/...`` fails
    with cryptic non-zero exit codes. Workaround: stage in /tmp (local disk,
    seekable), then atomic move into the bucket once the writer is done.

    No-op when ``final_path`` is not under ``/data`` (local mode) — writes
    straight there. Cleans up the tmp file/dir on success and on exception.
    """
    final_path.parent.mkdir(parents=True, exist_ok=True)
    needs_staging = (
        final_path.is_absolute() and len(final_path.parts) > 1 and final_path.parts[1] == "data"
    )
    if not needs_staging:
        yield final_path
        return

    tmp_dir = Path(tempfile.mkdtemp(prefix="dub-stage-"))
    tmp_path = tmp_dir / final_path.name
    try:
        yield tmp_path
        # Writer succeeded — copy the finalized file into the bucket.
        shutil.copyfile(tmp_path, final_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _effective_local_root() -> Path | None:
    ctx = _local_root.get()
    if ctx is not None:
        return ctx
    return _default_data_root


def data_root() -> Path:
    """Effective filesystem root for the /data mount.

    Returns the host directory when ``use_local_artifacts`` (or ``configure_data_root``)
    is active — typically ``./data`` for in-process Python runs. Otherwise returns
    ``/data``, which is the bucket mount path inside a container.
    """
    local = _effective_local_root()
    return local if local is not None else Path("/data")


@contextmanager
def use_local_artifacts(root: Path) -> Generator[None, None, None]:
    """Route artifact helpers to a host directory (``./data`` → ``/data`` in containers)."""
    token = _local_root.set(root.resolve())
    try:
        yield
    finally:
        _local_root.reset(token)


def _local_path(object_key: str) -> Path | None:
    root = _effective_local_root()
    if root is None:
        return None
    return root / object_key


def output_keys(key_fn: OutputKeyFn[T], item: T) -> list[str]:
    """Normalize a single key or list of keys for one pipeline item."""
    keys = key_fn(item)
    return [keys] if isinstance(keys, str) else list(keys)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=secrets.aws_endpoint_url,
        aws_access_key_id=require_cloud_setting("AWS_ACCESS_KEY_ID", secrets.aws_access_key_id),
        aws_secret_access_key=require_cloud_setting(
            "AWS_SECRET_ACCESS_KEY", secrets.aws_secret_access_key
        ),
    )


def _bucket_name() -> str:
    return require_cloud_setting("NEBIUS_BUCKET_NAME", secrets.nebius_bucket_name)


def upload_to_storage(local_path: Path, object_key: str) -> None:
    bucket = _bucket_name()
    logger.info(f"Uploading {local_path} → s3://{bucket}/{object_key}")
    _s3_client().upload_file(str(local_path), bucket, object_key)


def _upload_bytes(data: bytes, object_key: str, content_type: str = "application/octet-stream") -> None:
    """Upload raw bytes (e.g. a JSON manifest) directly without writing a temp file."""
    local = _local_path(object_key)
    if local is not None:
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(data)
        logger.info(f"Writing {len(data)} bytes → {local}")
        return
    bucket = _bucket_name()
    logger.info(f"Uploading {len(data)} bytes → s3://{bucket}/{object_key}")
    _s3_client().put_object(
        Bucket=bucket,
        Key=object_key,
        Body=data,
        ContentType=content_type,
    )


def upload_json(payload: dict, object_key: str) -> None:
    _upload_bytes(json.dumps(payload, ensure_ascii=False).encode("utf-8"), object_key, "application/json")


def read_json(object_key: str) -> dict[str, Any] | None:
    """Return parsed JSON for *object_key*, or ``None`` if missing."""
    local = _local_path(object_key)
    if local is not None:
        if not local.is_file():
            return None
        return json.loads(local.read_text(encoding="utf-8"))
    try:
        resp = _s3_client().get_object(Bucket=_bucket_name(), Key=object_key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return None
        raise


def object_exists(object_key: str) -> bool:
    """True if the key exists. Network/auth errors propagate (not silenced as 'missing')."""
    local = _local_path(object_key)
    if local is not None:
        return local.is_file()
    try:
        _s3_client().head_object(Bucket=_bucket_name(), Key=object_key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False
        raise


def list_existing(prefix: str) -> frozenset[str]:
    """All keys under *prefix* as a frozenset — one LIST call instead of N HEADs.

    Use for bulk existence checks (pre-flight / post-flight scans). Membership
    lookup is O(1) after the single round-trip, vs O(N) sequential HEAD requests.
    """
    return frozenset(list_objects(prefix))


def list_objects(prefix: str) -> list[str]:
    """List all keys under a prefix (paginated; handles > 1000 objects)."""
    root = _effective_local_root()
    if root is not None:
        base = root / prefix if prefix else root
        if not base.exists():
            return []
        if base.is_file():
            rel = base.relative_to(root).as_posix()
            return [rel]
        keys: list[str] = []
        for path in sorted(base.rglob("*")):
            if path.is_file():
                keys.append(path.relative_to(root).as_posix())
        return keys
    paginator = _s3_client().get_paginator("list_objects_v2")
    keys = []
    bucket = _bucket_name()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


