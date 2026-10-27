# SQLite shares: behavior and performance

The memory index remains the default. Passing a `SQLiteDatabase` to `SoulSeekClient`
selects the SQLite shares index; the client manages its connection lifecycle.

```python
from aioslsk.client import SoulSeekClient
from aioslsk.database.sqlite import SQLiteDatabase

client = SoulSeekClient(settings, database=SQLiteDatabase("shares.db"))
```

SQLite shares require JSON1 and FTS5. Database migrations are packaged with the library.
Transfer persistence is not switched to SQLite by this change.

## Correctness guarantees

- Transactions own the shared connection through commit or rollback, including
  cancellation. Wrapper operations wait instead of joining another task's transaction.
- Scans write file data, not captured sharing configuration. Directory ownership checks
  prevent old scans from recreating removed/replaced roots or bypassing new child shares.
- Standalone scans have directory-scoped seen-file bookkeeping. Failed scans preserve
  previous entries, overlapping scans of a root are serialized, and cancelled sibling
  scans are awaited.
- Sharing-change notifications follow index mutations. File, directory, permission, and
  settings changes invalidate prebaked listings; absent caches use dynamic generation.
- Streamed browse replies and ordinary messages share a whole-frame send lock. Timeout,
  cancellation, or partial-frame failure closes the connection instead of sending a
  second response inside the unfinished frame.
- SQLite stages listings to a bounded-memory temporary-file snapshot before network
  writes, so peer backpressure does not occupy the shared database worker.
- Prebake counts, tree-ordered serialization, and publication share one transaction.
- Search terms are quoted as literal FTS text, and query results use native local paths.
- Upload setup checks the live filesystem size. Download destinations remain deferred
  until transfer start, preserving sequential duplicate naming.

Manager regressions exercise memory and SQLite; backend-specific cases target the index
itself. End-to-end coverage uses the project's local mock server, not live Soulseek.

## Scan optimizations

The existing walker thread performs stats and sends completed item batches through a
bounded queue. There is no coroutine/executor submission per stat call. Per-file errors,
child-share exclusion, and worker shutdown are covered by regressions.

`stat_concurrency` remains accepted for compatibility, but no longer controls per-file
jobs. Stats are serial within each root's walker; high-latency network filesystems were
not benchmarked and may trade their former concurrency for lower scheduling overhead.

Completed attributes are reused only when root/path, size, and stored mtime match:

- Memory uses a temporary per-directory lookup during the scan.
- SQLite looks up the incoming batch through JSON1 and the existing unique file index,
  in the same transaction as persistence. It does not materialize the whole library.
- Pending attributes remain distinct from completed-empty metadata. Raised extraction
  errors leave pending state and retry. The existing extractor API returns `[]` for
  unsupported or Mutagen-rejected files; those attempts are cached until the file changes.
- Migration 0003 resets legacy ambiguous empty strings to pending once. Existing nonempty
  metadata is retained. Subsequent writes distinguish `NULL` (pending) from `''` (empty).

A size/mtime cache cannot detect edits that preserve both values. The existing float mtime
representation is retained; this is not content hashing.

### Measured scan results

Ryzen 7 PRO 6850U, Python 3.12.12, fresh process per backend, warm filesystem. First scans
start with empty indexes; rescans use unchanged data. Times include prebake generation.
"Before" is the correctness-fixed implementation before stat batching/attribute reuse.
These are local samples, not confidence intervals or cold-disk throughput estimates.

| Dataset | Backend | Before first / rescan | After first / rescan |
|---|---|---:|---:|
| 100k virtual files, no-op attributes | Memory | 19.098 / 17.840 s | 12.599 / 5.163 s |
| 100k virtual files, no-op attributes | SQLite | 22.516 / 17.364 s | 15.771 / 5.967 s |
| 30k real binary files, no-op attributes | Memory | 6.318 / 7.124 s | 3.443 / 2.165 s |
| 30k real binary files, no-op attributes | SQLite | 5.627 / 5.066 s | 3.743 / 1.537 s |
| 600 real MP3 copies | Memory | 0.360 / 0.351 s | 0.316 / 0.034 s |
| 600 real MP3 copies | SQLite | 0.422 / 0.362 s | 0.358 / 0.038 s |
| 600 real FLAC copies | Memory | 0.285 / 0.312 s | 0.251 / 0.033 s |
| 600 real FLAC copies | SQLite | 0.349 / 0.288 s | 0.280 / 0.043 s |

The audio fixtures repeat one recording per format, not a diverse library. Each audio
case extracted all 600 files initially and none on the unchanged rescan. At 100k files,
peak RSS changed from 201.6 to 204.5 MiB for memory and 96.3 to 96.6 MiB for SQLite.

Correctness checks added a real persistence cost before optimization: paired synthetic
SQLite runs were about 18% slower than the pre-fix scanner, while an instrumented pair
showed 6%. Stat batching and attribute reuse recovered that cost in the measured workloads.
Upgrading a copy of a generated 100k-file legacy DB took 0.395 s; reopening took 0.005 s.
No five-million-file migration or new full scan was measured.

## Search selection and ordering

SQLite search no longer requests FTS rank or sorts by it. Matching, exclusions, visibility,
paging, response caps, and cap defaults are preserved. Selection under a cap is not a
relevance guarantee, consistent with the memory backend's unordered matching contract.

Outgoing peer replies sort the already-selected visible and locked lists independently
by full remote path using ordinary case-sensitive string order. This matches Nicotine+'s
[`_create_file_info_list()`](https://github.com/nicotine-plus/nicotine-plus/blob/master/pynicotine/search.py):
cap candidates, then sort each list by path. It does not improve which matches survive a cap.

Incoming replies remain unranked and unmodified. Relevance, format/quality preferences,
availability priorities, and presentation belong to client implementers.

### Measured query results

Actual ranked and rank-free production classes were compared on a read-only five-million-
file synthetic fixture, without migrations or rebuilds. Seven samples per query followed
warmup. Broad queries returned 100 matches; the selective query returned one.

| Query | Ranked median | Rank-free median |
|---|---:|---:|
| `bin` | 302.169 ms | 3.211 ms |
| `file` | 305.399 ms | 2.977 ms |
| `*ile` | 305.969 ms | 3.170 ms |
| `file_0000000_000000` | 0.811 ms | 0.607 ms |

Broad searches were about 100x faster in this run. The rank-free plan has no temporary
ordering B-tree. These are warm query-method latencies, not network response times.
Moving ranking inside the CTE was also tested and took about 6.5 s for broad terms;
it is not a useful substitute for removing rank.

Copying/sorting already-converted records in an 80/20 visible/locked split took 0.0181 ms
for 100 records, 0.2866 ms for 1,000, and 5.6161 ms for 10,000 (five-repeat medians).
Production sorts freshly converted lists without those extra copies.

## Reproduction

Run tests and aiosqlite benchmarks in a temporary window of the existing tmux session.
From a working Poetry environment:

```sh
poetry run pytest tests/unit tests/e2e -q

poetry run python scripts/benchmark_scan_memory.py \
  --backend sqlite --scan-runs 2 --virtual \
  --directories 200 --files-per-directory 500 --skip-attributes \
  --db-path /tmp/aioslsk-benchmark.db

poetry run python scripts/benchmark_scan_memory.py \
  --backend memory --scan-runs 2 --directories 6 --files-per-directory 100 \
  --audio-source tests/unit/resources/shared/Kevin_MacLeod-Galway.mp3 \
  --dataset-root /tmp/aioslsk-audio-benchmark
```

Use a fresh process for each backend. The benchmark reports Linux `/proc` RSS, query and
listing-delivery timings, attribute counts, and database size. The virtual case bypasses
real filesystem stats and is not an audio throughput estimate. Audio datasets should use
separate roots by format. `--recreate-dataset` deletes the specified generated dataset.
The script recreates its specified SQLite database: **never point it at a real library DB**.

## Remaining performance work

- Initial tag extraction still submits one executor job per file. A warm 1,200-file
  microbenchmark showed no gain with four threads and about 2.1x with warmed spawned
  processes. SQLite's process-pool restriction remains; changing it needs lifecycle tests.
- Large prebakes still allocate final compressed buffers. Snapshot staging adds local I/O;
  the measurements used tmpfs, and slow peers do not hold the database worker.
- The 128-byte rate-limiter granularity creates substantial per-chunk work. Adaptive
  sizing needs separate evaluation; simply raising the minimum can break low limits.
- Multi-root scaling, cold storage, diverse audio libraries, and high-latency network
  filesystems need representative measurements before drawing broader conclusions.
