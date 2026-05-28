"""Download sample videos for local / batch pipeline testing."""

from __future__ import annotations

import logging
import random
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

import requests
import typer
from rich.panel import Panel
from rich.progress import BarColumn, DownloadColumn, MofNCompleteColumn, Progress, TransferSpeedColumn

from pipeline.console import get_console, setup_logging

app = typer.Typer(
    name="download",
    help="Download sample videos to data/ for pipeline testing.",
    no_args_is_help=True,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "data"
SAMPLE_NAME = "sample.mp4"
BATCH_DIR_NAME = "sample_batch"
MANIFEST_NAME = "manifest.txt"

NASA_CORONAGRAPH_2MIN_URL = (
    "https://assets.science.nasa.gov/content/dam/science/astro/"
    "programs/exep/technology/videos/coronagraph_2min.mp4"
)
TEARS_OF_STEEL_YOUTUBE_URL = "https://www.youtube.com/watch?v=R6MlUcmOul8"

logger = logging.getLogger(__name__)


def _require_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise RuntimeError(f"Required tool not found on PATH: {name}")
    return path


def _run(cmd: Sequence[str]) -> None:
    logger.debug("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, text=True)


def _download_url(url: str, dest: Path, *, timeout: float = 120.0) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    console = get_console()
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0)) or None
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Downloading {dest.name}", total=total)
            with dest.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    if chunk:
                        handle.write(chunk)
                        progress.update(task, advance=len(chunk))


def _trim_video(src: Path, dest: Path, *, duration: float) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        logger.warning("ffmpeg not found — copying full file to %s", dest.name)
        shutil.copy2(src, dest)
        return
    _run(
        [
            ffmpeg,
            "-y",
            "-i",
            str(src),
            "-t",
            str(duration),
            "-c",
            "copy",
            str(dest),
        ]
    )


def download_nasa_sample(data_dir: Path, *, duration: float) -> Path:
    """NASA coronagraph narrated clip (public domain) → data/sample.mp4."""
    sample = data_dir / SAMPLE_NAME
    with tempfile.TemporaryDirectory(prefix="nasa-sample-") as tmp:
        raw = Path(tmp) / "coronagraph_2min.mp4"
        trimmed = Path(tmp) / "trimmed.mp4"
        _download_url(NASA_CORONAGRAPH_2MIN_URL, raw)
        _trim_video(raw, trimmed, duration=duration)
        sample.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(trimmed, sample)
    logger.info("Sample ready: %s (%.0fs)", sample, duration)
    return sample


def download_tears_of_steel_sample(
    data_dir: Path,
    *,
    start: int,
    duration: int,
) -> Path:
    """Blender *Tears of Steel* segment via YouTube (CC BY) → data/sample.mp4."""
    _require_tool("yt-dlp")
    sample = data_dir / SAMPLE_NAME
    with tempfile.TemporaryDirectory(prefix="tos-sample-") as tmp:
        clip = Path(tmp) / "clip.mp4"
        end = start + duration
        get_console().print(f"[dim]Fetching Tears of Steel via yt-dlp (t={start}s, {duration}s)…[/dim]")
        _run(
            [
                "yt-dlp",
                TEARS_OF_STEEL_YOUTUBE_URL,
                "--download-sections",
                f"*{start}-{end}",
                "-f",
                "bv*+ba/b",
                "--merge-output-format",
                "mp4",
                "-o",
                str(clip),
            ]
        )
        sample.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(clip, sample)
    logger.info("Sample ready: %s (%ds from t=%ds)", sample, duration, start)
    return sample


def populate_mixed_batch(
    sources: list[Path],
    data_dir: Path,
    count: int,
    *,
    weights: list[float] | None = None,
    seed: int | None = None,
) -> list[Path]:
    """Copy *count* files into data/sample_batch/, picking source randomly by *weights*.

    *weights* are relative (need not sum to 1); defaults to equal probability.
    *seed* makes the draw reproducible.
    """
    if count < 1:
        raise ValueError("--sample-size must be >= 1")
    if not sources:
        raise ValueError("at least one source file required")

    rng = random.Random(seed)
    batch_dir = data_dir / BATCH_DIR_NAME
    batch_dir.mkdir(parents=True, exist_ok=True)

    width = max(3, len(str(count)))
    copies: list[Path] = []
    for i in range(1, count + 1):
        src = rng.choices(sources, weights=weights, k=1)[0]
        dest = batch_dir / f"{i:0{width}d}_{SAMPLE_NAME}"
        shutil.copy2(src, dest)
        copies.append(dest)
        logger.debug("Batch copy %d/%d: %s → %s", i, count, src.name, dest.name)

    manifest = batch_dir / MANIFEST_NAME
    keys = [f"{BATCH_DIR_NAME}/{p.name}" for p in copies]
    manifest.write_text("\n".join(keys) + "\n", encoding="utf-8")
    logger.info("Mixed batch: %d files under %s", count, batch_dir)
    return copies


def upload_batch_to_s3(copies: list[Path], s3_prefix: str) -> None:
    """Upload local batch files to S3 under *s3_prefix*."""
    from pipeline.storage import upload_to_storage

    prefix = s3_prefix.strip("/")
    console = get_console()
    console.print(f"[dim]Uploading {len(copies)} file(s) → s3 prefix [bold]{prefix}/[/bold][/dim]")
    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Uploading", total=len(copies))
        for path in copies:
            upload_to_storage(path, f"{prefix}/{path.name}")
            progress.advance(task)
    logger.info("Uploaded %d file(s) to s3 prefix %s/", len(copies), prefix)


def populate_batch(sample: Path, data_dir: Path, count: int) -> list[Path]:
    """Copy sample into data/sample_batch/ as 001_sample.mp4, 002_sample.mp4, …"""
    if count < 1:
        raise ValueError("--sample-size must be >= 1")

    batch_dir = data_dir / BATCH_DIR_NAME
    batch_dir.mkdir(parents=True, exist_ok=True)

    width = max(3, len(str(count)))
    copies: list[Path] = []
    for i in range(1, count + 1):
        dest = batch_dir / f"{i:0{width}d}_{SAMPLE_NAME}"
        shutil.copy2(sample, dest)
        copies.append(dest)
        logger.info("Batch copy: %s", dest.relative_to(data_dir))

    manifest = batch_dir / MANIFEST_NAME
    keys = [f"{BATCH_DIR_NAME}/{path.name}" for path in copies]
    manifest.write_text("\n".join(keys) + "\n", encoding="utf-8")
    logger.info("Wrote manifest (%d keys): %s", len(keys), manifest.relative_to(data_dir))
    return copies


def _finish(data_dir: Path, sample: Path, sample_size: int | None, s3_prefix: str | None = None) -> None:
    lines = [f"[green]✓[/green] Sample: [bold]{sample}[/bold]"]
    if sample_size is not None:
        manifest = data_dir / BATCH_DIR_NAME / MANIFEST_NAME
        lines.append(f"[green]✓[/green] Batch: {sample_size} copies under {data_dir / BATCH_DIR_NAME}")
        lines.append(f"[green]✓[/green] Manifest: {manifest}")
    if s3_prefix is not None:
        lines.append(f"[green]✓[/green] Uploaded to S3 prefix: [bold]{s3_prefix.strip('/')}/[/bold]")
    get_console().print(Panel("\n".join(lines), title="Download complete", border_style="green"))


def _common_options(
    data_dir: Path,
    sample_size: int | None,
    verbose: bool,
) -> Path:
    setup_logging(verbose=verbose)
    resolved = data_dir.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


@app.command("nasa")
def cmd_nasa(
    duration: float = typer.Option(75.0, "--duration", help="Trim length in seconds"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, "--data-dir", help="Output directory"),
    sample_size: int | None = typer.Option(
        None, "--sample-size", help="Duplicate sample into sample_batch/"
    ),
    s3_prefix: str | None = typer.Option(
        None, "--s3-prefix", help="Upload batch to this S3 prefix after local creation (e.g. demo-100)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """NASA coronagraph narrated clip (public domain)."""
    data_dir = _common_options(data_dir, sample_size, verbose)
    try:
        sample = download_nasa_sample(data_dir, duration=duration)
        copies: list[Path] = []
        if sample_size is not None:
            copies = populate_batch(sample, data_dir, sample_size)
        if s3_prefix is not None:
            if not copies:
                raise typer.BadParameter("--s3-prefix requires --sample-size", param_hint="--s3-prefix")
            upload_batch_to_s3(copies, s3_prefix)
        _finish(data_dir, sample, sample_size, s3_prefix)
    except Exception as exc:
        get_console().print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("tears-of-steel")
def cmd_tears_of_steel(
    start: int = typer.Option(300, "--start", help="Start time in seconds"),
    duration: int = typer.Option(60, "--duration", help="Clip length in seconds"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, "--data-dir", help="Output directory"),
    sample_size: int | None = typer.Option(
        None, "--sample-size", help="Duplicate sample into sample_batch/"
    ),
    s3_prefix: str | None = typer.Option(
        None, "--s3-prefix", help="Upload batch to this S3 prefix after local creation (e.g. demo-100)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Blender Tears of Steel clip (CC BY, via yt-dlp)."""
    data_dir = _common_options(data_dir, sample_size, verbose)
    try:
        sample = download_tears_of_steel_sample(data_dir, start=start, duration=duration)
        copies: list[Path] = []
        if sample_size is not None:
            copies = populate_batch(sample, data_dir, sample_size)
        if s3_prefix is not None:
            if not copies:
                raise typer.BadParameter("--s3-prefix requires --sample-size", param_hint="--s3-prefix")
            upload_batch_to_s3(copies, s3_prefix)
        _finish(data_dir, sample, sample_size, s3_prefix)
    except Exception as exc:
        get_console().print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("mix")
def cmd_mix(
    sample_size: int = typer.Option(10, "--sample-size", help="Total number of files in the batch"),
    nasa_ratio: float = typer.Option(0.5, "--nasa-ratio", help="Fraction from NASA source (0.0–1.0)"),
    nasa_duration: float = typer.Option(75.0, "--nasa-duration", help="NASA clip trim length in seconds"),
    tos_start: int = typer.Option(300, "--tos-start", help="Tears of Steel start time in seconds"),
    tos_duration: int = typer.Option(60, "--tos-duration", help="Tears of Steel clip length in seconds"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, "--data-dir", help="Output directory"),
    s3_prefix: str | None = typer.Option(
        None, "--s3-prefix", help="Upload batch to this S3 prefix after local creation (e.g. demo-100)"
    ),
    seed: int | None = typer.Option(None, "--seed", help="Random seed for reproducible source assignment"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Mixed batch: random files drawn from NASA and Tears of Steel sources."""
    if not 0.0 <= nasa_ratio <= 1.0:
        raise typer.BadParameter("must be between 0.0 and 1.0", param_hint="--nasa-ratio")
    data_dir = _common_options(data_dir, sample_size, verbose)
    try:
        console = get_console()
        sample_dir = data_dir / "sample_file"

        console.print("[dim]Downloading NASA source…[/dim]")
        nasa_sample = download_nasa_sample(sample_dir, duration=nasa_duration)
        nasa_dest = sample_dir / "nasa_sample.mp4"
        shutil.move(str(nasa_sample), nasa_dest)

        sources = [nasa_dest]
        effective_weights = [1.0]

        if shutil.which("yt-dlp") is None:
            console.print("[yellow]yt-dlp not found — using NASA source only[/yellow]")
        else:
            console.print("[dim]Downloading Tears of Steel source…[/dim]")
            tos_sample = download_tears_of_steel_sample(sample_dir, start=tos_start, duration=tos_duration)
            tos_dest = sample_dir / "tos_sample.mp4"
            shutil.move(str(tos_sample), tos_dest)
            sources = [nasa_dest, tos_dest]
            effective_weights = [nasa_ratio, 1.0 - nasa_ratio]

        copies = populate_mixed_batch(
            sources,
            data_dir,
            sample_size,
            weights=effective_weights,
            seed=seed,
        )
        if s3_prefix is not None:
            upload_batch_to_s3(copies, s3_prefix)
        _finish(data_dir, nasa_dest, sample_size, s3_prefix)
    except Exception as exc:
        get_console().print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def main() -> None:
    app()


if __name__ == "__main__":
    main()
    sys.exit(0)
