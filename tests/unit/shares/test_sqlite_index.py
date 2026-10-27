from __future__ import annotations

from aioslsk.database.sqlite import SQLiteDatabase
from aioslsk.exceptions import SharedDirectoryError
from aioslsk.protocol.primitives import DirectoryData
from aioslsk.shares.index import SQLiteShareIndex
from aioslsk.shares.index.memory import MemoryShareIndex
from aioslsk.shares.model import DirectoryShareMode, SharedDirectory, SharedItem
from aioslsk.settings import Settings

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import os
import random
import re
import sqlite3
import asyncio
import io
import pytest
from unittest.mock import AsyncMock, Mock, patch


DEFAULT_SETTINGS = {
    "credentials": {"username": "user0", "password": "pass0"},
    "shares": {"directories": []},
    "users": {"friends": []},
}


async def _make_index(tmp_path, settings: Settings | None = None) -> tuple[SQLiteDatabase, SQLiteShareIndex]:
    db_path = str(tmp_path / "test_shares.db")
    db = SQLiteDatabase(db_path)
    await db.connect()
    index = SQLiteShareIndex(db, settings or Settings(**DEFAULT_SETTINGS))
    return db, index


@pytest.mark.asyncio
async def test_query_does_not_read_fts_rank(tmp_path):
    db, index = await _make_index(tmp_path)
    try:
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        shared.items.add(SharedItem(shared, "", "song.mp3", 0.0, 1))
        await index.add_shared_directory(shared)

        def deny_ranking(action, table, column, database, trigger):
            if action == sqlite3.SQLITE_READ and table == "files_fts" and column == "rank":
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_FUNCTION and column == "bm25":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        await db.connection._execute(db.connection._conn.set_authorizer, deny_ranking)
        visible, locked = await index.query("song")
        assert [item.filename for item in visible] == ["song.mp3"]
        assert locked == []
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("username", [None, "friend", "allowed", "stranger"])
@pytest.mark.parametrize("limit", [None, 2, 10])
async def test_query_pages_after_exclusions_and_preserves_cap_and_visibility(tmp_path, username, limit):
    db, index = await _make_index(tmp_path)
    try:
        index._settings.searches.receive.max_results = 3
        index._settings.users.friends = ["friend"]
        public = SharedDirectory("public", str(tmp_path / "public"), "public")
        friends = SharedDirectory(
            "friends", str(tmp_path / "friends"), "friends", share_mode=DirectoryShareMode.FRIENDS
        )
        users = SharedDirectory(
            "users", str(tmp_path / "users"), "users", share_mode=DirectoryShareMode.USERS, users=["allowed"]
        )
        for directory in [public, friends, users]:
            await index.add_shared_directory(directory)
        # Ensure rejected candidates fill early pages before any eligible files.
        items = [SharedItem(public, "", f"song skip {idx}.mp3", 0.0, 1) for idx in range(24)]
        items += [SharedItem(public, "", f"song banned phrase {idx}.mp3", 0.0, 1) for idx in range(16)]
        eligible = [SharedItem(directory, "", "song keep.mp3", 0.0, 1) for directory in [public, friends, users]]
        await index.save_shared_items(items + eligible)

        with patch.object(db, "execute", wraps=db.execute) as execute:
            visible, locked = await index.query(
                "song -skip", username=username, excluded_search_phrases=["banned phrase"], limit=limit
            )
        returned = {item.get_remote_path() for item in visible + locked}
        assert len(visible) + len(locked) == len(returned) == min(limit if limit is not None else 3, 3)
        assert returned <= {item.get_remote_path() for item in eligible}
        expected_locked = set()
        if username is not None and username != "friend":
            expected_locked.add(eligible[1].get_remote_path())
        if username is not None and username != "allowed":
            expected_locked.add(eligible[2].get_remote_path())
        assert {item.get_remote_path() for item in locked} == returned & expected_locked
        assert {item.get_remote_path() for item in visible} == returned - expected_locked
        assert index._settings.searches.receive.max_results == 3
        offsets = [call.args[1]["offset"] for call in execute.call_args_list]
        assert offsets[0] == 0
        assert len(offsets) > 1
        assert offsets == list(range(0, len(offsets) * 2 * (limit or 3), 2 * (limit or 3)))
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_validate_executor_compatibility_process_pool_raises(tmp_path):
    db, index = await _make_index(tmp_path)
    with pytest.raises(SharedDirectoryError) as exc_info:
        index.validate_executor_compatibility(executor=None, executor_factory=ProcessPoolExecutor)
    assert "ProcessPoolExecutor cannot be used with SQLite" in str(exc_info.value)
    await db.close()


@pytest.mark.asyncio
async def test_validate_executor_compatibility_thread_pool_ok(tmp_path):
    db, index = await _make_index(tmp_path)
    index.validate_executor_compatibility(executor=None, executor_factory=ThreadPoolExecutor)
    await db.close()


@pytest.mark.asyncio
async def test_sqlite_inclusion_queries_are_superset_of_memory(tmp_path):
    seed = int(os.environ.get("AIO_SQ_SHARE_PROP_SEED", "1337"))
    item_count = int(os.environ.get("AIO_SQ_SHARE_PROP_ITEMS", "200"))
    query_count = int(os.environ.get("AIO_SQ_SHARE_PROP_QUERIES", "200"))
    rng = random.Random(seed)
    settings = Settings(**DEFAULT_SETTINGS)
    # Ensure we can return all matches so set comparisons are meaningful.
    settings.searches.receive.max_results = item_count

    db, sqlite_index = await _make_index(tmp_path, settings=settings)
    memory_index = MemoryShareIndex(settings)

    shared_dir = SharedDirectory("music", str(tmp_path / "music"), "alias1")

    alnum = "abcdefghijklmnopqrstuvwxyz0123456789"
    punct = "-_.()"

    def _random_segment(min_len: int = 3, max_len: int = 10) -> str:
        length = rng.randint(min_len, max_len)
        chars = [rng.choice(alnum)]
        for _ in range(length - 1):
            if rng.random() < 0.2:
                chars.append(rng.choice(punct))
            else:
                chars.append(rng.choice(alnum))
        # Randomly uppercase some letters to validate case-insensitivity
        for i, ch in enumerate(chars):
            if ch.isalpha() and rng.random() < 0.15:
                chars[i] = ch.upper()
        return "".join(chars)

    def _random_subdir() -> str:
        parts = [_random_segment() for _ in range(rng.randint(0, 3))]
        return "\\".join(parts)

    def _random_filename() -> str:
        base = _random_segment()
        ext = rng.choice(["mp3", "flac", "txt", "jpg", "mkv"])
        return f"{base}.{ext}"

    for _ in range(item_count):
        subdir = _random_subdir()
        filename = _random_filename()
        shared_dir.items.add(SharedItem(shared_dir, subdir, filename, 0.0, 1))

    await sqlite_index.add_shared_directory(shared_dir)
    await memory_index.add_shared_directory(shared_dir)
    memory_index.build_term_map(shared_dir)
    memory_index.cleanup_term_map()

    items = list(shared_dir.items)

    def _build_query_from_path(path: str) -> str:
        tokens = [t for t in re.split(r"[\W_]", path) if t]
        segments = path.split("\\")
        literal_candidates = [seg for seg in segments if any(c in seg for c in punct)]

        terms = []
        for _ in range(rng.randint(1, 3)):
            if literal_candidates and rng.random() < 0.3:
                term = rng.choice(literal_candidates)
            else:
                term = rng.choice(tokens)
            terms.append(term)

        return " ".join(terms).lower()

    for _ in range(query_count):
        item = rng.choice(items)
        query = _build_query_from_path(item.get_query_path())

        mem_items, _ = await memory_index.query(query)
        sqlite_items, _ = await sqlite_index.query(query)

        mem_paths = {it.get_query_path().lower() for it in mem_items}
        sqlite_paths = {it.get_query_path().lower() for it in sqlite_items}

        assert mem_paths, f"query={query!r} produced no matches in memory index"
        assert mem_paths <= sqlite_paths, (
            f"query={query!r} missing={sorted(mem_paths - sqlite_paths)[:5]!r}"
        )

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_fts_updates_on_directory_move(tmp_path):
    db, index = await _make_index(tmp_path)

    parent_path = str(tmp_path / "music")
    child_path = str(tmp_path / "music" / "rock")

    parent = SharedDirectory("music", parent_path, "alias1")
    child = SharedDirectory("rock", child_path, "alias2")
    child.items.add(SharedItem(child, "", "song.mp3", 0.0, 1))

    await index.add_shared_directory(parent)
    await index.add_shared_directory(child)
    await index.delete_shared_directory(child)

    results, locked = await index.query("rock", limit=10)
    assert locked == []
    assert any(item.filename == "song.mp3" for item in results)

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_prebaked_listing_fallback_blobopen(tmp_path, monkeypatch):
    db, index = await _make_index(tmp_path)

    monkeypatch.setattr("aioslsk.shares.index.sqlite._has_blobopen", lambda _conn: False)

    visible = [DirectoryData(name="@@alias1", files=[])]
    locked: list[DirectoryData] = []

    await index.save_prebaked_listing(DirectoryShareMode.EVERYONE, visible, locked)
    data = await index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
    assert data

    buffer = io.BytesIO()
    bytes_written = await index.copy_prebaked_listing_to(buffer, DirectoryShareMode.EVERYONE)
    assert bytes_written > 0
    buffer.seek(0)
    assert len(buffer.read()) == bytes_written

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_rebuild_prebaked_listing_does_not_use_get_shared_items_for_user(
    tmp_path, monkeypatch
):
    db, index = await _make_index(tmp_path)

    shared_dir = SharedDirectory("music", "/tmp/music", "alias1")
    shared_dir.items.add(SharedItem(shared_dir, "", "song.mp3", 0.0, 1))
    await index.add_shared_directory(shared_dir)

    monkeypatch.setattr(
        index,
        "get_shared_items_for_user",
        AsyncMock(side_effect=AssertionError("materialized path should not be used")),
    )

    await index.rebuild_prebaked_listing(DirectoryShareMode.EVERYONE, username="notfriend")
    assert await index.get_prebaked_listing(DirectoryShareMode.EVERYONE)

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_get_prebaked_listing_resolves_friends_to_everyone_without_friends(
    tmp_path,
):
    db, index = await _make_index(tmp_path)

    visible = [DirectoryData(name="@@alias1", files=[])]
    locked: list[DirectoryData] = []
    await index.save_prebaked_listing(DirectoryShareMode.EVERYONE, visible, locked)
    assert (
        await index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        == await index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
    )

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_persist_scanned_items_batch_write_failure_should_not_hang(
    tmp_path, monkeypatch
):
    db, index = await _make_index(tmp_path)

    shared_dir = SharedDirectory("shared", "/tmp/shared", "alias1")
    item = SharedItem(shared_dir, "", "a.txt", 0.0, 1)

    async def _fail_save(*_args, **_kwargs):
        raise RuntimeError("simulated db failure")

    monkeypatch.setattr(index, "save_shared_items", _fail_save)

    with pytest.raises(RuntimeError, match="simulated db failure"):
        await asyncio.wait_for(
            index.persist_scanned_items_batch(shared_dir, [item]),
            timeout=1.0,
        )

    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("blobopen", [False, True])
async def test_slow_stream_does_not_block_database_or_change_its_snapshot(
    tmp_path, monkeypatch, blobopen
):
    db, index = await _make_index(tmp_path)
    release = asyncio.Event()
    streaming = None
    try:
        if not blobopen:
            monkeypatch.setattr("aioslsk.shares.index.sqlite._has_blobopen", lambda _: False)
        payload = b"A" * (131072 * 2)
        await index._save_prebaked_listing_bytes(DirectoryShareMode.EVERYONE, payload)
        entered = asyncio.Event()
        writer = Mock(spec=asyncio.StreamWriter)
        writes = []
        writer.write.side_effect = writes.append

        async def drain():
            entered.set()
            await release.wait()

        writer.drain = drain
        streaming = asyncio.create_task(
            index.copy_prebaked_listing_to(writer, DirectoryShareMode.EVERYONE)
        )
        await asyncio.wait_for(entered.wait(), 2)
        assert await asyncio.wait_for(index.get_stats(), 0.5) == (0, 0)
        await asyncio.wait_for(
            index._save_prebaked_listing_bytes(DirectoryShareMode.EVERYONE, b"replacement"),
            0.5,
        )
        release.set()
        assert await streaming == len(payload)
        assert b"".join(writes) == payload
    finally:
        release.set()
        if streaming:
            await streaming
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["why?", "C++", "[Live]", "name@home", "a&b", "*why?", "*NOT"])
async def test_literal_search_punctuation_matches_memory(tmp_path, query):
    db, index = await _make_index(tmp_path)
    try:
        memory = MemoryShareIndex(index._settings)
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        for filename in ["Why?.mp3", "C++.flac", "[Live] demo.mp3", "name@home.mp3", "a&b.flac", "NOT.mp3"]:
            shared.items.add(SharedItem(shared, "", filename, 0.0, 1))
        await index.add_shared_directory(shared)
        await memory.add_shared_directory(shared)
        memory.build_term_map(shared)
        expected, _ = await memory.query(query)
        actual, _ = await index.query(query)
        assert expected
        assert {item.filename for item in actual} == {item.filename for item in expected}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_search_results_have_native_local_paths(tmp_path):
    db, index = await _make_index(tmp_path)
    try:
        root = tmp_path / "music"
        folder = root / "Artist" / "Album"
        folder.mkdir(parents=True)
        path = folder / "song.mp3"
        path.write_bytes(b"song")
        shared = SharedDirectory("music", str(root), "alias")
        shared.items.add(SharedItem(shared, os.path.join("Artist", "Album"), path.name, 0.0, 4))
        await index.add_shared_directory(shared)
        results, _ = await index.query("song")
        assert len(results) == 1
        assert results[0].get_absolute_path() == str(path)
        direct = await index.get_shared_item(results[0].get_remote_path())
        assert results[0] == direct
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_prebake_tree_order_does_not_repeat_parent_directories(tmp_path):
    from aioslsk.protocol.messages import PeerSharesReply

    db, index = await _make_index(tmp_path)
    try:
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        for subdir in ["Artist", "Artist - Album", "Artist\\Album", "Artist!", "Artist\\Album\\Live"]:
            shared.items.add(SharedItem(shared, subdir, "song.mp3", 0.0, 1))
        await index.add_shared_directory(shared)
        await index.rebuild_prebaked_listing(DirectoryShareMode.EVERYONE, "stranger")
        reply = PeerSharesReply.Request.deserialize(
            0, await index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        )
        dynamic, _ = await index.get_shared_items_for_user("stranger")
        assert len({directory.name for directory in reply.directories}) == len(reply.directories)
        assert sorted(reply.directories, key=lambda directory: directory.name) == dynamic
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_prebake_cannot_save_stale_snapshot_after_sharing_change(tmp_path, monkeypatch):
    from aioslsk.exceptions import FileNotFoundError

    db, index = await _make_index(tmp_path)
    mutation = None
    try:
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        await index.add_shared_directory(shared)
        original = index._serialize_prebaked_group_into
        blocked = []

        async def serialize(*args, **kwargs):
            nonlocal mutation
            if mutation is None:
                other = SharedDirectory("other", str(tmp_path / "other"), "other")
                mutation = asyncio.create_task(index.add_shared_directory(other))
                ready, _ = await asyncio.wait([mutation], timeout=0.05)
                blocked.append(not ready)
            await original(*args, **kwargs)

        monkeypatch.setattr(index, "_serialize_prebaked_group_into", serialize)
        await index.rebuild_prebaked_listing(DirectoryShareMode.EVERYONE, "stranger")
        await mutation
        assert blocked == [True]
        with pytest.raises(FileNotFoundError):
            await index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
    finally:
        if mutation:
            await mutation
        await db.close()


@pytest.mark.asyncio
async def test_scanned_attributes_reused_after_reconnect_in_bounded_batch(tmp_path):
    db, index = await _make_index(tmp_path)
    try:
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        expected = [None if idx % 3 == 0 else [] if idx % 3 == 1 else [(1, idx)] for idx in range(700)]
        for idx, attributes in enumerate(expected):
            item = SharedItem(shared, os.path.join("album", str(idx)), "song.mp3", float(idx), idx + 1)
            item.attributes = attributes
            shared.items.add(item)
        await index.add_shared_directory(shared)
        await db.close()
        await db.connect()
        index = SQLiteShareIndex(db, Settings(**DEFAULT_SETTINGS))
        shared = await index.get_shared_directory(shared.absolute_path)
        assert shared is not None
        if hasattr(db.connection._conn, "setlimit"):
            await db.connection._execute(
                db.connection._conn.setlimit, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999
            )
        scanned = [
            SharedItem(shared, os.path.join("album", str(idx)), "song.mp3", float(idx), idx + 1)
            for idx in range(700)
        ]
        await index.begin_directory_scan(shared)
        await index.persist_scanned_items_batch(shared, scanned)
        await index.end_directory_scan(shared)
        assert [item.attributes for item in scanned] == expected
        for item in scanned:
            stored = await index.get_shared_item(item.get_remote_path())
            assert stored.attributes == item.attributes
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_attribute_state_migration_retries_ambiguous_entries_once(tmp_path):
    db, index = await _make_index(tmp_path)
    try:
        shared = SharedDirectory("music", str(tmp_path / "music"), "alias")
        for filename, attributes in [("pending", None), ("empty", []), ("known", [(1, 15)])]:
            item = SharedItem(shared, "", filename, 0.0, 1)
            item.attributes = attributes
            shared.items.add(item)
        await index.add_shared_directory(shared)
        async with db.transaction() as conn:
            await conn.execute("DELETE FROM schema_version WHERE version = 3")
            await conn.execute("UPDATE files SET attributes = '' WHERE filename IN ('pending', 'empty')")
        await db.close()
        await db.connect()
        index = SQLiteShareIndex(db, Settings(**DEFAULT_SETTINGS))
        prefix = shared.get_remote_path() + "\\"
        assert (await index.get_shared_item(prefix + "pending")).attributes is None
        assert (await index.get_shared_item(prefix + "empty")).attributes is None
        assert (await index.get_shared_item(prefix + "known")).attributes == [(1, 15)]

        empty = await index.get_shared_item(prefix + "empty")
        empty.attributes = []
        await index.update_item_attributes([empty])
        await db.close()
        await db.connect()
        assert (await index.get_shared_item(prefix + "empty")).attributes == []
    finally:
        await db.close()
