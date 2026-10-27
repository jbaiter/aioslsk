#!/usr/bin/env python3
"""Synthetic benchmark for shares scan RAM usage.

Creates (or reuses) a synthetic or real-audio dataset and compares share
backends. Run each backend in a fresh process for meaningful RSS comparisons.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack, suppress
import json
import os
from pathlib import Path
import shutil
import statistics
import time
from unittest.mock import AsyncMock, Mock
from unittest.mock import patch

from aioslsk.database.sqlite import SQLiteDatabase
from aioslsk.events import EventBus
from aioslsk.settings import Settings
from aioslsk.shares.manager import SharesManager


def _read_proc_status_kb(key: str) -> int:
    with open("/proc/self/status", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith(key):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1])
    return 0


def current_rss_kb() -> int:
    return _read_proc_status_kb("VmRSS:")


def current_hwm_kb() -> int:
    return _read_proc_status_kb("VmHWM:")


async def monitor_peak_rss_kb(stop_event: asyncio.Event, interval_s: float) -> int:
    peak = 0
    while not stop_event.is_set():
        peak = max(peak, current_rss_kb())
        await asyncio.sleep(interval_s)
    peak = max(peak, current_rss_kb())
    return peak


def _format_rate(count: int, elapsed_s: float) -> float:
    if elapsed_s <= 0:
        return 0.0
    return count / elapsed_s


def build_dataset(
    dataset_root: Path,
    directories: int,
    files_per_directory: int,
    nested_levels: int,
    file_size_bytes: int,
    recreate: bool,
    audio_source: Path | None = None,
) -> int:
    if recreate and dataset_root.exists():
        shutil.rmtree(dataset_root)

    dataset_root.mkdir(parents=True, exist_ok=True)
    payload = audio_source.read_bytes() if audio_source else b"\0" * file_size_bytes
    extension = audio_source.suffix if audio_source else ".bin"
    total_files = 0

    for d_idx in range(directories):
        parts = [f"dir_{d_idx:04d}"]
        for n_idx in range(nested_levels):
            parts.append(f"sub_{(d_idx + n_idx) % 17:02d}_{n_idx:02d}")
        dir_path = dataset_root.joinpath(*parts)
        dir_path.mkdir(parents=True, exist_ok=True)

        for f_idx in range(files_per_directory):
            fp = dir_path / f"file_{d_idx:04d}_{f_idx:05d}{extension}"
            if not fp.exists() or recreate:
                with open(fp, "wb") as fh:
                    fh.write(payload)
            total_files += 1

    return total_files


async def run_scan(
    dataset_root: Path,
    db_path: Path,
    monitor_interval_s: float,
    progress_interval_s: float,
    directories: int,
    files_per_directory: int,
    virtual: bool,
    virtual_file_size_bytes: int,
    skip_attributes: bool,
    backend: str,
    scan_runs: int,
) -> dict:
    settings = Settings(
        **{
            "credentials": {"username": "bench_user", "password": "bench_pass"},
            "shares": {
                "directories": [{"path": str(dataset_root), "share_mode": "everyone"}]
            },
            "users": {"friends": []},
        }
    )

    db = SQLiteDatabase(str(db_path)) if backend == "sqlite" else None
    if db:
        await db.connect()
    manager = SharesManager(settings, EventBus(), AsyncMock(), database=db)

    await manager.load_from_settings()

    scanned_counter = 0
    attrs_counter = 0
    prebake_seconds = []
    scan_seconds = []
    original_prebake = manager.update_prebaked_listings

    async def timed_prebake():
        started = time.perf_counter()
        await original_prebake()
        prebake_seconds.append(time.perf_counter() - started)

    manager.update_prebaked_listings = timed_prebake  # type: ignore[method-assign]

    original_persist_batch = manager.index.persist_scanned_items_batch
    original_persist_attr_batch = manager.index.persist_attribute_updates_batch

    async def wrapped_persist_batch(shared_directory, items):
        nonlocal scanned_counter
        scanned_counter += len(items)
        await original_persist_batch(shared_directory, items)

    async def wrapped_persist_attr_batch(items):
        nonlocal attrs_counter
        attrs_counter += len(items)
        await original_persist_attr_batch(items)

    manager.index.persist_scanned_items_batch = wrapped_persist_batch  # type: ignore[assignment]
    manager.index.persist_attribute_updates_batch = wrapped_persist_attr_batch  # type: ignore[assignment]

    stop_event = asyncio.Event()
    monitor_task = asyncio.create_task(
        monitor_peak_rss_kb(stop_event, interval_s=monitor_interval_s)
    )

    async def progress_reporter(scan_start_time: float):
        while not stop_event.is_set():
            elapsed = time.perf_counter() - scan_start_time
            print(
                (
                    f"[progress] elapsed={elapsed:.1f}s "
                    f"scanned={scanned_counter} "
                    f"scanned_rate={_format_rate(scanned_counter, elapsed):.1f}/s "
                    f"attrs={attrs_counter} "
                    f"rss_mb={current_rss_kb() / 1024:.1f}"
                ),
                flush=True,
            )
            await asyncio.sleep(progress_interval_s)

    rss_before = current_rss_kb()
    hwm_before = current_hwm_kb()
    start = time.perf_counter()
    progress_task = asyncio.create_task(progress_reporter(start))

    class _FakeStat:
        def __init__(self, size: int):
            self.st_size = size
            self.st_mtime = 1_700_000_000.0

    fake_stat = _FakeStat(virtual_file_size_bytes)

    def _virtual_walk(_root: str):
        root = str(dataset_root)
        for d_idx in range(directories):
            dir_path = os.path.join(root, f"virt_dir_{d_idx:07d}")
            filenames = [
                f"file_{d_idx:07d}_{f_idx:06d}.bin"
                for f_idx in range(files_per_directory)
            ]
            yield dir_path, [], filenames

    def _virtual_stat(_path: str):
        return fake_stat

    patchers = []
    if skip_attributes:
        patchers.append(
            patch(
                "aioslsk.shares.manager.extract_attributes",
                new=lambda _filepath: [],
            )
        )
    if virtual:
        patchers.append(
            patch("aioslsk.shares.manager.os.walk", new=_virtual_walk)
        )
        patchers.append(
            patch("aioslsk.shares.manager.os.stat", new=_virtual_stat)
        )

    with ExitStack() as patches:
        for patcher in patchers:
            patches.enter_context(patcher)
        for _ in range(scan_runs):
            started = time.perf_counter()
            await manager.scan()
            scan_seconds.append(time.perf_counter() - started)
    elapsed = time.perf_counter() - start
    stop_event.set()
    with suppress(asyncio.CancelledError):
        progress_task.cancel()
        await progress_task
    peak_rss_kb = await monitor_task
    rss_after = current_rss_kb()
    hwm_after = current_hwm_kb()
    folder_count, file_count = await manager.get_stats()

    first_file = "file_0000000_000000" if virtual else "file_0000_00000"
    query_metrics = {}
    for query in ["file", "*ile", first_file]:
        await manager.query(query)  # warmup
        durations = []
        for _ in range(5):
            started = time.perf_counter()
            visible, locked = await manager.query(query)
            durations.append(time.perf_counter() - started)
        query_metrics[query] = {
            "median_ms": round(statistics.median(durations) * 1000, 3),
            "results": len(visible) + len(locked),
        }

    from aioslsk.shares.model import DirectoryShareMode

    listing = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
    writer = Mock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    started = time.perf_counter()
    copied = await manager.index.copy_prebaked_listing_to(writer, DirectoryShareMode.EVERYONE)
    stream_seconds = time.perf_counter() - started
    assert copied == len(listing)
    assert file_count == directories * files_per_directory
    if db:
        await db.close()
    db_bytes = db_path.stat().st_size if db else 0

    return {
        "backend": backend,
        "scan_seconds": [round(value, 3) for value in scan_seconds],
        "prebake_seconds": [round(value, 3) for value in prebake_seconds],
        "queries": query_metrics,
        "listing_bytes": len(listing),
        "listing_stream_ms": round(stream_seconds * 1000, 3),
        "database_bytes": db_bytes,
        "elapsed_seconds": round(elapsed, 3),
        "rss_before_kb": rss_before,
        "rss_after_kb": rss_after,
        "peak_rss_kb": peak_rss_kb,
        "hwm_before_kb": hwm_before,
        "hwm_after_kb": hwm_after,
        "hwm_delta_kb": max(0, hwm_after - hwm_before),
        "scanned_items_persisted": scanned_counter,
        "attribute_items_persisted": attrs_counter,
        "reported_folder_count": folder_count,
        "reported_file_count": file_count,
    }


async def amain() -> int:
    parser = argparse.ArgumentParser(description="Benchmark shares scanning, search and listings")
    parser.add_argument("--backend", choices=["memory", "sqlite"], default="sqlite")
    parser.add_argument("--scan-runs", type=int, default=1)
    parser.add_argument(
        "--audio-source", type=Path,
        help="Copy this audio file's bytes/extension instead of generating .bin files",
    )
    parser.add_argument(
        "--dataset-root",
        default="/tmp/aioslsk-scan-bench-dataset",
        help="Path to synthetic dataset directory",
    )
    parser.add_argument(
        "--db-path",
        default="/tmp/aioslsk-scan-bench.db",
        help="Path to benchmark SQLite DB file",
    )
    parser.add_argument("--directories", type=int, default=60)
    parser.add_argument("--files-per-directory", type=int, default=500)
    parser.add_argument("--nested-levels", type=int, default=2)
    parser.add_argument("--file-size-bytes", type=int, default=1)
    parser.add_argument(
        "--monitor-interval-ms",
        type=int,
        default=20,
        help="RSS sampling interval in milliseconds",
    )
    parser.add_argument(
        "--progress-interval-s",
        type=float,
        default=5.0,
        help="Live progress print interval in seconds",
    )
    parser.add_argument(
        "--recreate-dataset",
        action="store_true",
        help="Delete and recreate synthetic dataset before benchmark",
    )
    parser.add_argument(
        "--virtual",
        action="store_true",
        help="Use a virtual synthetic tree (no real files created).",
    )
    parser.add_argument(
        "--virtual-file-size-bytes",
        type=int,
        default=1,
        help="Synthetic file size used in virtual mode.",
    )
    parser.add_argument(
        "--skip-attributes",
        action="store_true",
        help="Skip mutagen attribute extraction to benchmark file scan/index path.",
    )
    args = parser.parse_args()

    dataset_root = Path(args.dataset_root)
    db_path = Path(args.db_path)
    if args.backend == "sqlite" and db_path.exists():
        db_path.unlink()

    if args.virtual:
        dataset_root.mkdir(parents=True, exist_ok=True)
        created_files = args.directories * args.files_per_directory
    else:
        created_files = build_dataset(
            dataset_root=dataset_root,
            directories=args.directories,
            files_per_directory=args.files_per_directory,
            nested_levels=args.nested_levels,
            file_size_bytes=args.file_size_bytes,
            recreate=args.recreate_dataset,
            audio_source=args.audio_source,
        )

    results = await run_scan(
        dataset_root=dataset_root,
        db_path=db_path,
        monitor_interval_s=args.monitor_interval_ms / 1000.0,
        progress_interval_s=args.progress_interval_s,
        directories=args.directories,
        files_per_directory=args.files_per_directory,
        virtual=args.virtual,
        virtual_file_size_bytes=args.virtual_file_size_bytes,
        skip_attributes=args.skip_attributes,
        backend=args.backend,
        scan_runs=args.scan_runs,
    )
    results.update(
        {
            "dataset_root": str(dataset_root),
            "db_path": str(db_path),
            "directories": args.directories,
            "files_per_directory": args.files_per_directory,
            "nested_levels": args.nested_levels,
            "synthetic_file_count": created_files,
            "virtual_mode": args.virtual,
            "skip_attributes": args.skip_attributes,
            "audio_source": str(args.audio_source) if args.audio_source else None,
        }
    )

    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
