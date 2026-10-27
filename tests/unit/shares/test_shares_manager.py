from aioslsk.database.sqlite import SQLiteDatabase
from aioslsk.events import EventBus, SharedDirectoryChangeEvent
from aioslsk.transfer.manager import TransferManager
from aioslsk.transfer.state import TransferState
from aioslsk.exceptions import (
    FileNotFoundError,
    FileNotSharedError,
    SharedDirectoryError,
)
from aioslsk.shares.manager import (
    SharesManager,
    extract_attributes,
    iter_scanned_item_batches,
)
from aioslsk.shares.model import DirectoryShareMode, SharedDirectory, SharedItem
from aioslsk.naming import DefaultNamingStrategy, KeepDirectoryStrategy
from aioslsk.settings import Settings, SharedDirectorySettingEntry

from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
import asyncio
import multiprocessing as mp
import mutagen
import pytest
import pytest_asyncio
from pytest_unordered import unordered
import os
import sys
import threading
import time
from typing import Optional
from typing import Callable
from unittest.mock import patch, AsyncMock

from aioslsk.protocol.primitives import DirectoryData


RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources')
SHARED_DIR_PATH = os.path.join(RESOURCES, 'shared')

FRIEND = 'friend0'
USER = 'user0'

MP3_FILENAME = 'Kevin_MacLeod-Galway.mp3'
FLAC_FILENAME = 'Kevin_MacLeod-Galway.flac'
SUBDIR_FILENAME = 'Strange_Drone_Impact.mp3'
SUBDIR_PATH = os.path.join(SHARED_DIR_PATH, 'Cool_Test_Album')
ROOT_FILE_1_PATH = os.path.join(SHARED_DIR_PATH, MP3_FILENAME)
ROOT_FILE_2_PATH = os.path.join(SHARED_DIR_PATH, FLAC_FILENAME)
SUBDIR_FILE_1_PATH = os.path.join(SHARED_DIR_PATH, SUBDIR_FILENAME)
TOTAL_FILES = 4
TOTAL_DIRECTORIES = 4

DEFAULT_SETTINGS = {
    'credentials': {'username': 'user0', 'password': 'pass0'},
    'shares': {
        'directories': [
            {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'}
        ]
    },
    'users': {'friends': []}
}

SETTINGS_SUBDIR_FRIENDS = Settings(**{
    'credentials': {'username': 'user0', 'password': 'pass0'},
    'shares': {
        'directories': [
            {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'},
            {'path': SUBDIR_PATH, 'share_mode': 'friends'}
        ]
    },
    'users': {'friends': [FRIEND]}
})
SETTINGS_SUBDIR_USERS = Settings(**{
    'credentials': {'username': 'user0', 'password': 'pass0'},
    'shares': {
        'directories': [
            {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'},
            {'path': SUBDIR_PATH, 'share_mode': 'users', 'users': [USER]}
        ]
    },
    'users': {'friends': []}
})
SETTINGS_SUBDIR_USERS_WITH_FRIEND = Settings(**{
    'credentials': {'username': 'user0', 'password': 'pass0'},
    'shares': {
        'directories': [
            {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'},
            {'path': SUBDIR_PATH, 'share_mode': 'users', 'users': [USER]}
        ]
    },
    'users': {'friends': [FRIEND]}
})
SETTINGS_SUBDIR_USERS_WITH_PRIVILEGED_FRIEND = Settings(**{
    'credentials': {'username': 'user0', 'password': 'pass0'},
    'shares': {
        'directories': [
            {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'},
            {'path': SUBDIR_PATH, 'share_mode': 'users', 'users': [USER]}
        ]
    },
    'users': {'friends': [FRIEND, USER]}
})

SHARED_DIRECTORY = SharedDirectory('music', 'C:\\music', 'abcdef')
SHARED_ITEMS = {
    'item1': SharedItem(
        SHARED_DIRECTORY,
        os.path.join('folk', 'folkalbum, release'),
        "simple band(contrib. singer) - isn't easy song.mp3",
        0.0,
        0,
    ),
    'item2': SharedItem(
        SHARED_DIRECTORY,
        os.path.join('metal', 'metalalbum release'),
        "simple band (contrib. singer)_-_don't easy song 片仮名.flac",
        0.0,
        0,
    ),
    'item3': SharedItem(
        SHARED_DIRECTORY,
        os.path.join('rap', 'rapalbum_release'),
        "simple_band(contributer. singer)-don't_easy_song 片仮名.mp3",
        0.0,
        0,
    ),
}
SHARED_DIRECTORY.items = set(SHARED_ITEMS.values())


async def _load_and_scan(manager: SharesManager):
    await manager.load_from_settings()
    await manager.scan()


def _spawn_process_pool_executor() -> ProcessPoolExecutor:
    # Using spawn avoids fork()-in-multithreaded-process deprecation warnings.
    return ProcessPoolExecutor(mp_context=mp.get_context("spawn"))


@pytest_asyncio.fixture(params=["memory", "sqlite"])
async def manager(request, tmp_path):
    """Parametrized fixture that tests both memory and SQLite implementations"""
    if request.param == "sqlite":
        db_path = str(tmp_path / "test_shares.db")
        db = SQLiteDatabase(db_path)
        await db.connect()
        manager = SharesManager(Settings(**DEFAULT_SETTINGS), EventBus(), AsyncMock(), database=db)
        yield manager
        await db.close()
    else:
        yield SharesManager(Settings(**DEFAULT_SETTINGS), EventBus(), AsyncMock())


@pytest_asyncio.fixture(params=["memory", "sqlite"])
async def manager_query(request, tmp_path):
    """Parametrized fixture for query tests"""
    if request.param == "sqlite":
        db_path = str(tmp_path / "test_shares_query.db")
        db = SQLiteDatabase(db_path)
        await db.connect()
        manager = SharesManager(Settings(**DEFAULT_SETTINGS), EventBus(), AsyncMock(), database=db)
        # Add the shared directory to the index
        await manager.index.add_shared_directory(SHARED_DIRECTORY)
        # For SQLite, we need to add items to the directory
        for item in SHARED_ITEMS.values():
            SHARED_DIRECTORY.items.add(item)
        await manager.index.update_shared_directory(SHARED_DIRECTORY)
        # SQLite uses FTS5, no need to build term map manually
        yield manager
        await db.close()
    else:
        manager = SharesManager(
            Settings(**DEFAULT_SETTINGS), EventBus(), AsyncMock())
        # For memory, directly add to the index's internal list
        await manager.index.add_shared_directory(SHARED_DIRECTORY)
        manager.index.build_term_map(SHARED_DIRECTORY)
        yield manager


class TestFunctions:

    def test_extractAttributes_MP3File_shouldReturnAttributes(self):
        filepath = os.path.join(SHARED_DIR_PATH, MP3_FILENAME)

        attributes = extract_attributes(filepath)

        assert attributes == [(0, 128), (1, 15)]

    def test_extractAttributes_FLACFile_shouldReturnAttributes(self):
        filepath = os.path.join(SHARED_DIR_PATH, FLAC_FILENAME)

        attributes = extract_attributes(filepath)

        assert attributes == [(1, 15), (4, 44100), (5, 16)]

    def test_extractAttributes_exception_shouldReturnEmpty(self):
        filepath = os.path.join(SHARED_DIR_PATH, FLAC_FILENAME)
        with patch('mutagen.File', side_effect=mutagen.MutagenError):
            attributes = extract_attributes(filepath)

        assert [] == attributes

    @pytest.mark.asyncio
    async def test_iter_scanned_batches_stats_run_in_one_worker(self, tmp_path, monkeypatch):
        root = tmp_path / "root"
        root.mkdir()
        for idx in range(13):
            (root / f"file-{idx}").write_text("data")
        directory = SharedDirectory(str(root), str(root), "alias")
        real_stat = os.stat
        stat_threads = []

        def record_stat(path, *args, **kwargs):
            if isinstance(path, (str, os.PathLike)) and os.path.dirname(os.fspath(path)) == str(root):
                stat_threads.append(threading.get_ident())
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr("aioslsk.shares.manager.os.stat", record_stat)
        with patch("aioslsk.shares.manager.asyncio.to_thread", wraps=asyncio.to_thread) as dispatch:
            batches = [batch async for batch in iter_scanned_item_batches(directory, batch_size=5)]

        assert sorted(len(batch) for batch in batches) == [3, 5, 5]
        assert len(stat_threads) == 13
        assert len(set(stat_threads)) == 1
        assert threading.get_ident() not in stat_threads
        assert dispatch.call_count == 1
        assert all(item.size_bytes == 4 for batch in batches for item in batch)

    @pytest.mark.asyncio
    async def test_iter_scanned_batches_skip_stat_errors_and_child_roots(self, tmp_path, monkeypatch):
        root = tmp_path / "root"
        root.mkdir()
        child = root / "private"
        child.mkdir()
        (root / "good").write_text("data")
        (child / "secret").write_text("secret")
        directory = SharedDirectory(str(root), str(root), "alias")
        child_directory = SharedDirectory(str(child), str(child), "child")
        real_stat = os.stat

        def stat(path, *args, **kwargs):
            if path == str(root / "denied"):
                raise PermissionError(path)
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr("aioslsk.shares.manager.os.stat", stat)
        monkeypatch.setattr("aioslsk.shares.manager.os.walk", lambda _: iter([
            (str(root), ["private"], ["missing", "denied", "good"]),
            (str(child), [], ["secret"]),
        ]))
        batches = [batch async for batch in iter_scanned_item_batches(
            directory, children=[child_directory], batch_size=2
        )]

        assert [[item.filename for item in batch] for batch in batches] == [["good"]]

    @pytest.mark.asyncio
    async def test_iter_scanned_batches_close_stops_stat_worker(self, tmp_path, monkeypatch):
        for idx in range(100):
            (tmp_path / f"file-{idx}").write_text("data")
        directory = SharedDirectory(str(tmp_path), str(tmp_path), "alias")
        real_stat = os.stat
        stat_calls = []

        def slow_stat(path, *args, **kwargs):
            if isinstance(path, (str, os.PathLike)) and os.path.dirname(os.fspath(path)) == str(tmp_path):
                stat_calls.append(path)
                time.sleep(0.001)
            return real_stat(path, *args, **kwargs)

        monkeypatch.setattr("aioslsk.shares.manager.os.stat", slow_stat)
        batches = iter_scanned_item_batches(directory, batch_size=2)
        await anext(batches)
        await asyncio.wait_for(batches.aclose(), timeout=2)
        completed_count = len(stat_calls)
        await asyncio.sleep(0.01)
        assert len(stat_calls) == completed_count
        assert completed_count < 100

    @pytest.mark.asyncio
    async def test_iterScannedItemBatches_consumerStopsEarly_shouldNotHang(
        self, tmp_path, monkeypatch
    ):
        shared_path = tmp_path / "scan-root"
        shared_path.mkdir()
        for idx in range(32):
            (shared_path / f"file-{idx}.txt").write_text("x")

        shared_directory = SharedDirectory("scan-root", str(shared_path), "alias")

        real_stat = os.stat

        def slow_stat(filepath: str, *args, **kwargs) -> os.stat_result:
            if os.fspath(filepath).startswith(os.fspath(shared_path)):
                time.sleep(0.02)
            return real_stat(filepath, *args, **kwargs)

        monkeypatch.setattr("aioslsk.shares.manager.os.stat", slow_stat)

        async def consume_single_batch():
            async for _ in iter_scanned_item_batches(
                shared_directory,
                stat_concurrency=1,
                batch_size=1,
            ):
                break

        await asyncio.wait_for(consume_single_batch(), timeout=2.0)


class TestSharedDirectory:

    def test_getRemotePath_shouldReturnPath(self):
        alias = 'abcdef'
        shared_directory = SharedDirectory('music', r"C:\\music", alias)
        assert '@@' + alias == shared_directory.get_remote_path()

    def test_getItemByRemotePath_itemExists_shouldReturnItem(self):
        shared_directory = SharedDirectory('music', r"C:\\music", 'abcdef')
        item = SharedItem(shared_directory, 'author', 'song.mp3', 1.0, 1000)
        shared_directory.items = {item}

        remote_path = '@@abcdef\\author\\song.mp3'
        assert item == shared_directory.get_item_by_remote_path(remote_path)

    def test_getItemByRemotePath_itemNotExists_shouldRaise(self):
        shared_directory = SharedDirectory('music', r"C:\\music", 'abcdef')
        item = SharedItem(shared_directory, 'author', 'song.mp3', 1.0, 1000)
        shared_directory.items = {item}

        remote_path = '@@abcdef\\author\\song2.mp3'
        with pytest.raises(FileNotFoundError):
            shared_directory.get_item_by_remote_path(remote_path)


class TestSharesManagerQuery:

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # 1 term matching
            ('rap', ['item3']),
            # 1 term matching, case sensitive
            ('mETAl', ['item2']),
            # multiple terms, out of order
            ('simple folk', ['item1']),
            # multiple terms, include part of directory and filename
            ('song folkalbum', ['item1']),
            # term surrounded by special chars
            ('contrib', ['item1', 'item2']),
            # unicode characters
            ('片仮名', ['item2', 'item3'])
        ]
    )
    @pytest.mark.asyncio
    async def test_querySimpleTerms_matching(self, manager_query: SharesManager, query: str, expected_items: list[str]):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.parametrize(
        'query',
        [
            # not fully matching term
            ('simpl'),
            # not matching because there is a seperator in between
            ('simpleband'),
            # only 1 term matches
            ('simple notfound'),
            # wildcard chars no match
            ('*impler'),
            # only exclusion
            ('-mp3'),
            # only special characters
            ('____')
        ]
    )
    @pytest.mark.asyncio
    async def test_querySimpleTerms_notMatching(self, manager_query: SharesManager, query: str):
        actual_items, locked_items = await manager_query.query(query)
        assert actual_items == []
        assert locked_items == []

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # matching simple term, and containing special chars
            ('simple isn\'t', ['item1']),
            # seperator in between
            ('simple_band', ['item3']),
        ]
    )
    @pytest.mark.asyncio
    async def test_querySpecialCharactersInTerm_matching(
        self,
        manager_query: SharesManager,
        query: str,
        expected_items: list[str],
    ):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # wildcard
            ('*tributer', ['item3']),
            # wildcard special chars
            ('*on\'t', ['item2', 'item3']),
            # wildcard start of line
            ('*etal', ['item2']),
            # wildcard end of line
            ('*lac', ['item2']),
            # wildcard unicode characters
            ('*仮名', ['item2', 'item3']),
        ]
    )
    @pytest.mark.asyncio
    async def test_queryWildcard_matching(self, manager_query: SharesManager, query: str, expected_items: list[str]):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # exclude, start of line
            ('simple -mp3', ['item2']),
            # exclude, end of line
            ('simple -rap', ['item1', 'item2']),
            # exclude, multiple end of line
            ('simple -mp3 -contributer', ['item2']),
            # exclude unicode characters
            ('simple -片仮名', ['item1'])
        ]
    )
    @pytest.mark.asyncio
    async def test_queryExcludeTerm(self, manager_query: SharesManager, query: str, expected_items: list[str]):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.asyncio
    async def test_queryExcludedSearchPhrases(self, manager_query: SharesManager):
        expected_items = [SHARED_ITEMS[item_name] for item_name in ['item3']]
        actual_items, locked_items = await manager_query.query(
            'song',
            excluded_search_phrases=['simple band']
        )
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # something with loads of special characters between words
            ('singer)_-_don\'t', ['item2']),
            # wildcard, something with loads of special characters between words
            ('*inger)_-_don\'t', ['item2']),
            # exclude, something with loads of special characters between words
            ('simple -singer)_-_don\'t', ['item1', 'item3']),
        ]
    )
    @pytest.mark.asyncio
    async def test_edgeCases(self, manager_query: SharesManager, query: str, expected_items: list[str]):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.parametrize(
        'query,expected_items',
        [
            # Backslash, inclusion, directory
            ('song folk\\folkalbum', ['item1']),
            # Backslash exclusion, directory
            ('song -folk\\folkalbum', ['item2', 'item3']),
            # Backslash, inclusion, filename
            ('folkalbum, release\\simple band', ['item1']),
            # Backslash, exclusion, filename
            ('song -rapalbum_release\\simple band', ['item1', 'item2']),
            # Slash inclusion
            ('song folk/folkalbum', []),
            # Slash exclusion
            ('song -folk/folkalbum', ['item1', 'item2', 'item3']),
        ]
    )
    @pytest.mark.asyncio
    async def test_queryTermsWithSlashes(self, manager_query: SharesManager, query: str, expected_items: list[str]):
        expected_items = [SHARED_ITEMS[item_name] for item_name in expected_items]
        actual_items, locked_items = await manager_query.query(query)
        assert expected_items == unordered(actual_items)
        assert locked_items == []

    @pytest.mark.asyncio
    async def test_maxResults(self, manager_query: SharesManager):
        # Get the results without any limit, this is simply to ensure that if
        # the test data is changed that this testcase doesn't become bogus
        actual_items, locked_items = await manager_query.query('simple')
        unlimited_results_len = len(actual_items) + len(locked_items)

        max_results = 2
        manager_query._settings.searches.max_results = max_results

        actual_items, locked_items = await manager_query.query('simple')
        limited_results_len = len(actual_items) + len(locked_items)
        assert limited_results_len == max_results
        assert unlimited_results_len > limited_results_len


class TestSharesManagerSharedDirectoryManagement:

    @pytest.mark.asyncio
    async def test_loadFromSettings(self, manager: SharesManager):
        await manager.load_from_settings()
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        assert 1 == len(visible_dirs + locked_dirs)
        assert (visible_dirs + locked_dirs)[0].absolute_path == os.path.abspath(SHARED_DIR_PATH)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("manager", "executor"),
        [
            ("memory", None),
            ("memory", ThreadPoolExecutor),
            ("memory", _spawn_process_pool_executor),
            ("sqlite", None),
            ("sqlite", ThreadPoolExecutor),
        ],
        indirect=["manager"],
    )
    async def test_scan(
        self,
        manager: SharesManager,
        executor: Optional[Callable[[], Executor]],
    ):
        await manager.load_from_settings()
        created_executor = executor() if executor else None
        manager.executor = created_executor
        try:
            await manager.scan()
        finally:
            if created_executor is not None:
                created_executor.shutdown(wait=True)

        # Both implementations should report the same stats
        folder_count, file_count = await manager.index.get_stats()
        assert file_count == TOTAL_FILES
        # folder_count = number of unique subdirs containing files (3)
        # Not the same as TOTAL_DIRECTORIES which includes intermediate dirs

        # Verify scan extracted attributes (backend-agnostic).
        visible, locked = await manager.create_shares_reply("anyone")
        all_files = [f for d in (visible + locked) for f in d.files]
        assert all_files
        assert all(f.attributes is not None for f in all_files)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("manager", "executor_factory"),
        [
            ("memory", _spawn_process_pool_executor),
            ("sqlite", ThreadPoolExecutor),
        ],
        indirect=["manager"],
    )
    async def test_scan_withExecutorFactory(
        self, manager: SharesManager, executor_factory: Callable[[], Executor]
    ):
        await manager.load_from_settings()
        manager.executor_factory = executor_factory

        await manager.start()
        assert manager.executor is not None

        await manager.scan()

        await manager.stop()
        assert manager.executor is None

        # Verify via stats for both implementations
        folder_count, file_count = await manager.index.get_stats()
        assert file_count == TOTAL_FILES

        visible, locked = await manager.create_shares_reply("anyone")
        all_files = [f for d in (visible + locked) for f in d.files]
        assert all_files
        assert all(f.attributes is not None for f in all_files)

    @pytest.mark.asyncio
    async def test_scan_nestedDirectories(self, manager: SharesManager):
        manager._settings.shares.directories = [
            SharedDirectorySettingEntry(
                path=SHARED_DIR_PATH, share_mode=DirectoryShareMode.EVERYONE
            ),
            SharedDirectorySettingEntry(
                path=SUBDIR_PATH, share_mode=DirectoryShareMode.FRIENDS
            )
        ]
        await manager.load_from_settings()
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        assert len(visible_dirs + locked_dirs) == 2

        await manager.scan()

        # Verify total file count
        folder_count, file_count = await manager.index.get_stats()
        assert file_count == TOTAL_FILES  # 4 files total

    @pytest.mark.parametrize(
        'directory_path',
        [
            ('/home/user/music'),
            ('/home/user/music/something/../')
        ]
    )
    @pytest.mark.asyncio
    async def test_getSharedDirectory_returnObject(self, manager: SharesManager, directory_path: str):
        path = '/home/user/music'
        expected_shared_dir = await manager.add_shared_directory(path)

        shared_dir = await manager.get_shared_directory(directory_path)
        assert shared_dir == expected_shared_dir

    @pytest.mark.parametrize(
        'directory_path',
        [
            ('/home/user/'),
            ('/home/user'),
            ('/home/user/music/some'),
            ('/home/user/music/some/thing/..')
        ]
    )
    @pytest.mark.asyncio
    async def test_getSharedDirectory_raisesError(self, manager: SharesManager, directory_path: str):
        path = '/home/user/music'
        await manager.add_shared_directory(path)

        with pytest.raises(SharedDirectoryError):
            await manager.get_shared_directory(directory_path)

    @pytest.mark.asyncio
    async def test_isDirectoryShared_returnTrue(self, manager: SharesManager):
        path = '/home/user/music'
        await manager.add_shared_directory(path)

        value = await manager.index.is_directory_shared('/home/user/music/something/../')
        assert value is True

    @pytest.mark.asyncio
    async def test_isDirectoryShared_returnFalse(self, manager: SharesManager):
        path = '/home/user/music'
        await manager.add_shared_directory(path)

        value = await manager.index.is_directory_shared('/home/user/music/something')
        assert value is False

    @pytest.mark.asyncio
    async def test_addSharedDirectory_alreadyAdded(self, manager: SharesManager):
        await manager.add_shared_directory('test/path')

        with pytest.raises(SharedDirectoryError):
            await manager.add_shared_directory('test/inbetween/../path')

    @pytest.mark.asyncio
    async def test_addSharedDirectory_existingSubDirectory(self, manager: SharesManager):
        await manager.load_from_settings()
        await manager.scan()
        await manager.add_shared_directory(SUBDIR_PATH, DirectoryShareMode.FRIENDS)

        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        all_dirs = visible_dirs + locked_dirs
        assert len(all_dirs) == 2

    @pytest.mark.asyncio
    async def test_updateSharedDirectory(self, manager: SharesManager):
        directory0 = await manager.add_shared_directory(
            'test/path',
            share_mode=DirectoryShareMode.USERS,
            users=['user1']
        )

        directory1 = await manager.update_shared_directory(
            'test/path',
            share_mode=None,
            users=['user1', 'user2']
        )

        assert directory1 == directory0
        assert directory1.share_mode == DirectoryShareMode.USERS
        assert directory1.users == ['user1', 'user2']

        directory2 = await manager.update_shared_directory(
            'test/path/test/../',
            share_mode=DirectoryShareMode.EVERYONE,
            users=None
        )

        assert directory2 == directory0
        assert directory2.share_mode == DirectoryShareMode.EVERYONE
        assert directory2.users == ['user1', 'user2']

    @pytest.mark.asyncio
    async def test_removeSharedDirectory_usingPath(self, manager: SharesManager):
        directory = await manager.add_shared_directory(
            'test/path',
            share_mode=DirectoryShareMode.USERS,
            users=['user1']
        )

        removed_directory = await manager.remove_shared_directory('test/path')

        assert removed_directory == directory
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        assert directory not in (visible_dirs + locked_dirs)

    @pytest.mark.asyncio
    async def test_removeSharedDirectory_nonExisting_shouldRaise(self, manager: SharesManager):
        directory = await manager.add_shared_directory('test/path')

        await manager.remove_shared_directory('test/path')

        with pytest.raises(SharedDirectoryError):
            await manager.remove_shared_directory(directory)

    @pytest.mark.asyncio
    async def test_removeSharedDirectory_withSubdir(self, manager: SharesManager):
        manager._settings.shares.directories = [
            SharedDirectorySettingEntry(
                path=SHARED_DIR_PATH, share_mode=DirectoryShareMode.EVERYONE
            ),
            SharedDirectorySettingEntry(
                path=SUBDIR_PATH, share_mode=DirectoryShareMode.FRIENDS
            )
        ]
        await manager.load_from_settings()
        await manager.scan()

        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        all_dirs = visible_dirs + locked_dirs
        sub_directory = all_dirs[1]

        await manager.remove_shared_directory(sub_directory)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        all_dirs = visible_dirs + locked_dirs
        assert sub_directory not in all_dirs
        assert len(all_dirs) == 1


class TestManagerReplies:

    @pytest.mark.asyncio
    async def test_createSharesReply_everyone(self, manager: SharesManager):
        await _load_and_scan(manager)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        alias = (visible_dirs + locked_dirs)[0].alias

        unlocked, locked = await manager.create_shares_reply('anyone')
        unlocked_directories = {directory.name: directory.files for directory in unlocked}

        assert f'@@{alias}' in unlocked_directories
        assert f'@@{alias}\\Cool_Test_Album' in unlocked_directories
        assert f'@@{alias}\\Hot_Test_Album' in unlocked_directories
        assert f'@@{alias}\\Hot_Test_Album\\CD01' in unlocked_directories

        assert len(unlocked_directories[f'@@{alias}']) == 2
        assert len(unlocked_directories[f'@@{alias}\\Cool_Test_Album']) == 1
        assert len(unlocked_directories[f'@@{alias}\\Hot_Test_Album']) == 0
        assert len(unlocked_directories[f'@@{alias}\\Hot_Test_Album\\CD01']) == 1

        for directory in unlocked:
            for file in directory.files:
                assert "\\" not in file.filename

        assert locked == []

    @pytest.mark.asyncio
    async def test_createSharesReply_friends_isFriend(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.create_shares_reply(FRIEND)

        assert len(unlocked) == TOTAL_DIRECTORIES
        assert locked == []

    @pytest.mark.asyncio
    async def test_createSharesReply_friends_isNotFriend(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.create_shares_reply('notfriend')
        assert len(unlocked) == TOTAL_DIRECTORIES - 1
        assert len(locked) == 1
        locked_files = {file.filename for directory in locked for file in directory.files}
        assert any(filename.endswith(SUBDIR_FILENAME) for filename in locked_files)

    @pytest.mark.asyncio
    async def test_createSharesReply_users_isInList(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_USERS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.create_shares_reply(USER)
        assert len(unlocked) == TOTAL_DIRECTORIES
        assert locked == []

    @pytest.mark.asyncio
    async def test_createSharesReply_users_isNotInList(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_USERS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.create_shares_reply('notuser')
        assert len(unlocked) == TOTAL_DIRECTORIES - 1
        assert len(locked) == 1
        locked_files = {file.filename for directory in locked for file in directory.files}
        assert any(filename.endswith(SUBDIR_FILENAME) for filename in locked_files)

    @pytest.mark.asyncio
    async def test_createSharesReply_lockedDirectoriesContainFiles(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        _, locked = await manager.create_shares_reply("notfriend")

        locked_files = [file for directory in locked for file in directory.files]
        assert locked_files
        assert any(file.filename.endswith(SUBDIR_FILENAME) for file in locked_files)

    @pytest.mark.asyncio
    async def test_createDirectoryReply(self, manager: SharesManager):
        await _load_and_scan(manager)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        root_dir = (visible_dirs + locked_dirs)[0].get_remote_path()
        directories = await manager.create_directory_reply(root_dir)
        assert len(directories) == 1
        assert len(directories[0].files) == 2

    @pytest.mark.asyncio
    async def test_createDirectoryReply_emptyDirectory(self, manager: SharesManager):
        await _load_and_scan(manager)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        empty_dir = '\\'.join([(visible_dirs + locked_dirs)[0].get_remote_path(), 'Hot_Test_Album'])
        directories = await manager.create_directory_reply(empty_dir)

        assert len(directories) == 1
        assert directories[0].name == empty_dir
        assert len(directories[0].files) == 0

    @pytest.mark.asyncio
    async def test_createDirectoryReply_nonExistantDirectory(self, manager: SharesManager):
        await _load_and_scan(manager)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        empty_dir = '\\'.join([(visible_dirs + locked_dirs)[0].get_remote_path(), 'Non_existant'])
        directories = await manager.create_directory_reply(empty_dir)

        assert len(directories) == 0

    @pytest.mark.asyncio
    async def test_createDirectoryReply_subDir(self, manager: SharesManager):
        await _load_and_scan(manager)
        visible_dirs, locked_dirs = await manager.index.get_shared_directories()
        root_dir = (visible_dirs + locked_dirs)[0].get_remote_path()
        subdir = '\\'.join([root_dir, 'Cool_Test_Album'])
        directories = await manager.create_directory_reply(subdir)
        assert len(directories) == 1
        assert len(directories[0].files) == 1

    @pytest.mark.asyncio
    async def test_query_friends_isNotFriend(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.query('Strange', username='notfriend')
        assert len(unlocked) == 0
        assert len(locked) == 1

    @pytest.mark.asyncio
    async def test_query_isFriend(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)
        unlocked, locked = await manager.query('Strange', username=FRIEND)
        assert len(unlocked) == 1
        assert len(locked) == 0


class TestSharesManager:

    @pytest.mark.asyncio
    async def test_getSharedItem_withUsername_notShared_shouldRaise(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        # Get items as a non-friend - the FRIENDS-only subdirectory will be locked
        visible, locked = await manager.index.get_shared_items_for_user('notfriend')

        # locked should contain directory names that are locked for this user
        # Find the first locked directory that has files (from the visible listing as FRIEND)
        visible_friend, _ = await manager.index.get_shared_items_for_user(FRIEND)

        # Find a file that's locked for 'notfriend' but visible to FRIEND
        remote_path = None
        visible_dirs_notfriend = {d.name for d in visible}
        for dir_data in visible_friend:
            if dir_data.name not in visible_dirs_notfriend and dir_data.files:
                remote_path = f"{dir_data.name}\\{dir_data.files[0].filename}"
                break

        if remote_path is None:
            pytest.fail("No locked file found for test")

        with pytest.raises(FileNotSharedError):
            await manager.get_shared_item(remote_path, 'notfriend')

    @pytest.mark.asyncio
    async def test_getSharedItem_withUsername_shared_shouldReturn(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        # Get items as FRIEND - the FRIENDS-only subdirectory will be visible
        visible, locked = await manager.index.get_shared_items_for_user(FRIEND)

        # Get items as non-friend for comparison
        visible_notfriend, _ = await manager.index.get_shared_items_for_user('notfriend')
        visible_dirs_notfriend = {d.name for d in visible_notfriend}

        # Find a file that's visible to FRIEND but not to non-friend
        # (i.e., from the FRIENDS-only subdirectory)
        remote_path = None
        for dir_data in visible:
            if dir_data.name not in visible_dirs_notfriend and dir_data.files:
                remote_path = f"{dir_data.name}\\{dir_data.files[0].filename}"
                break

        if remote_path is None:
            pytest.fail("No file found in FRIENDS-only directory for test")

        item = await manager.get_shared_item(remote_path, FRIEND)
        assert item is not None

    @pytest.mark.asyncio
    async def test_getSharedItem_notExistsOnDisk_shouldRaise(self, manager: SharesManager):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        # Get any file path
        visible, locked = await manager.index.get_shared_items_for_user(FRIEND)
        remote_path = None
        for dir_data in visible:
            if dir_data.files:
                remote_path = f"{dir_data.name}\\{dir_data.files[0].filename}"
                break

        if remote_path is None:
            pytest.fail("No file found for test")

        with patch('aiofiles.os.path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                await manager.get_shared_item(remote_path, FRIEND)

    def test_calculateDownloadPath(self, manager: SharesManager):
        if sys.platform.startswith('win32'):
            download_path = 'C:\\download'
        else:
            download_path = '/home/download'
        manager._settings.shares.download = download_path

        manager.naming_strategies = [
            DefaultNamingStrategy(),
            KeepDirectoryStrategy()
        ]

        remote_path = '@@abcdef\\directory\\file.mp3'
        local_path, local_filename = manager.calculate_download_path(remote_path)
        assert local_path == os.path.join(download_path, 'directory')
        assert local_filename == 'file.mp3'


@pytest.mark.parametrize("manager", ["memory", "sqlite"], indirect=True)
class TestPrebakedListings:
    """Tests for prebaked listing functionality."""

    @staticmethod
    def _to_name_sets(directories: list[DirectoryData]) -> tuple[set[str], set[str]]:
        return (
            {d.name for d in directories},
            {f.filename for d in directories for f in d.files},
        )

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_createsListings(self, manager: SharesManager):
        """After scan, prebaked listings should be created for EVERYONE and FRIENDS modes."""
        await _load_and_scan(manager)

        # Prebaked listings should now exist
        everyone_listing = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        assert everyone_listing is not None
        assert len(everyone_listing) > 0

        friends_listing = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        assert friends_listing is not None
        assert len(friends_listing) > 0

    @pytest.mark.asyncio
    async def test_no_friends_uses_everyone_prebake_for_friends_mode(
        self, manager: SharesManager
    ):
        # FRIENDS mode is meaningless when no friends are configured; both
        # backends should serve the EVERYONE listing for FRIENDS.
        manager._settings.users.friends = []
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        everyone = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        friends = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        assert friends == everyone

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_matchesDynamicGeneration(self, manager: SharesManager):
        """Prebaked listings should produce same result as dynamic generation."""
        await _load_and_scan(manager)

        # Get dynamic result
        visible, locked = await manager.create_shares_reply('random_user')

        # Get prebaked result and deserialize
        from aioslsk.protocol.messages import PeerSharesReply
        prebaked_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        prebaked_msg = PeerSharesReply.Request.deserialize(0, prebaked_bytes)

        # Compare full visible/locked payload sets
        assert self._to_name_sets(prebaked_msg.directories) == self._to_name_sets(visible)
        assert self._to_name_sets(prebaked_msg.locked_directories or []) == self._to_name_sets(locked)

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_friendsVsEveryone(self, manager: SharesManager):
        """FRIENDS prebaked listing should include friend-only directories as visible."""
        # Configure with friend-only subdirectory
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        from aioslsk.protocol.messages import PeerSharesReply

        everyone_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        everyone_msg = PeerSharesReply.Request.deserialize(0, everyone_bytes)

        friends_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        friends_msg = PeerSharesReply.Request.deserialize(0, friends_bytes)

        expected_everyone_visible, expected_everyone_locked = await manager.create_shares_reply("notfriend")
        expected_friends_visible, expected_friends_locked = await manager.create_shares_reply(FRIEND)

        assert self._to_name_sets(everyone_msg.directories) == self._to_name_sets(expected_everyone_visible)
        assert self._to_name_sets(everyone_msg.locked_directories or []) == self._to_name_sets(expected_everyone_locked)
        assert self._to_name_sets(friends_msg.directories) == self._to_name_sets(expected_friends_visible)
        assert self._to_name_sets(friends_msg.locked_directories or []) == self._to_name_sets(expected_friends_locked)

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_matchesRoleVisibility_forFriendsShares(
        self, manager: SharesManager
    ):
        manager._settings = SETTINGS_SUBDIR_FRIENDS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        from aioslsk.protocol.messages import PeerSharesReply

        # Non-friend view
        expected_visible_nf, expected_locked_nf = await manager.create_shares_reply("notfriend")
        everyone_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        everyone_msg = PeerSharesReply.Request.deserialize(0, everyone_bytes)
        assert self._to_name_sets(everyone_msg.directories) == self._to_name_sets(expected_visible_nf)
        assert self._to_name_sets(everyone_msg.locked_directories or []) == self._to_name_sets(expected_locked_nf)

        # Friend view
        expected_visible_friend, expected_locked_friend = await manager.create_shares_reply(FRIEND)
        friends_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        friends_msg = PeerSharesReply.Request.deserialize(0, friends_bytes)
        assert self._to_name_sets(friends_msg.directories) == self._to_name_sets(expected_visible_friend)
        assert self._to_name_sets(friends_msg.locked_directories or []) == self._to_name_sets(expected_locked_friend)

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_matchesRoleVisibility_forUsersShares(
        self, manager: SharesManager
    ):
        manager._settings = SETTINGS_SUBDIR_USERS_WITH_FRIEND
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        from aioslsk.protocol.messages import PeerSharesReply

        # Non-friend view
        expected_visible_nf, expected_locked_nf = await manager.create_shares_reply("notfriend")
        everyone_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        everyone_msg = PeerSharesReply.Request.deserialize(0, everyone_bytes)
        assert self._to_name_sets(everyone_msg.directories) == self._to_name_sets(expected_visible_nf)
        assert self._to_name_sets(everyone_msg.locked_directories or []) == self._to_name_sets(expected_locked_nf)

        # Friend but not in USERS list should still see USERS shares locked
        expected_visible_friend, expected_locked_friend = await manager.create_shares_reply(FRIEND)
        friends_bytes = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        friends_msg = PeerSharesReply.Request.deserialize(0, friends_bytes)
        assert self._to_name_sets(friends_msg.directories) == self._to_name_sets(expected_visible_friend)
        assert self._to_name_sets(friends_msg.locked_directories or []) == self._to_name_sets(expected_locked_friend)

    @pytest.mark.asyncio
    async def test_updatePrebakedListings_allFriendsPrivileged_usesEveryoneViewForFriends(
        self, manager: SharesManager
    ):
        manager._settings = Settings(**{
            'credentials': {'username': 'user0', 'password': 'pass0'},
            'shares': {
                'directories': [
                    {'path': SHARED_DIR_PATH, 'share_mode': 'everyone'},
                    {'path': SUBDIR_PATH, 'share_mode': 'users', 'users': [USER]}
                ]
            },
            'users': {'friends': [USER]}
        })
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        everyone_listing = await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
        friends_listing = await manager.index.get_prebaked_listing(DirectoryShareMode.FRIENDS)
        assert friends_listing == everyone_listing

    @pytest.mark.asyncio
    async def test_sendSharesReply_usesPrebakedForNonPrivileged(self, manager: SharesManager):
        """send_shares_reply should use prebaked listing for non-privileged users."""
        await _load_and_scan(manager)

        # Create mock connection
        mock_connection = AsyncMock()
        mock_connection.username = 'random_user'
        mock_connection.obfuscated = True  # Use obfuscated path which calls send_message
        mock_connection._writer = AsyncMock()

        await manager.send_shares_reply(mock_connection)
        # Should have sent prebaked bytes via send_message
        mock_connection.send_message.assert_called_once()
        # The argument should be bytes (prebaked), not a message object
        call_arg = mock_connection.send_message.call_args[0][0]
        assert isinstance(call_arg, bytes)

    @pytest.mark.asyncio
    async def test_sendSharesReply_streamsPrebakedForNonObfuscated(
        self, manager: SharesManager, monkeypatch
    ):
        """send_shares_reply should stream prebaked listing for non-obfuscated connections."""
        await _load_and_scan(manager)

        mock_connection = AsyncMock()
        mock_connection.username = 'random_user'
        mock_connection.obfuscated = False
        mock_connection._writer = AsyncMock()

        copy_mock = AsyncMock(return_value=123)
        monkeypatch.setattr(manager.index, "copy_prebaked_listing_to", copy_mock)

        await manager.send_shares_reply(mock_connection)

        copy_mock.assert_awaited_once_with(mock_connection._writer, DirectoryShareMode.EVERYONE)
        mock_connection.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_sendSharesReply_generatesForPrivilegedUser(self, manager: SharesManager):
        """send_shares_reply should generate dynamic reply for privileged users."""
        # Configure with USERS-only subdirectory
        manager._settings = SETTINGS_SUBDIR_USERS
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        # Create mock connection for user in USERS list
        mock_connection = AsyncMock()
        mock_connection.username = USER  # This user is in the USERS list
        mock_connection._writer = AsyncMock()

        await manager.send_shares_reply(mock_connection)
        # Should have sent a PeerSharesReply message (not bytes)
        mock_connection.send_message.assert_called_once()
        from aioslsk.protocol.messages import PeerSharesReply
        call_arg = mock_connection.send_message.call_args[0][0]
        assert isinstance(call_arg, PeerSharesReply.Request)

    @pytest.mark.asyncio
    async def test_sendSharesReply_generatesForPrivilegedFriend(self, manager: SharesManager):
        """Privileged users should always get dynamically generated listings."""
        manager._settings = SETTINGS_SUBDIR_USERS_WITH_PRIVILEGED_FRIEND
        manager.index._settings = manager._settings
        await _load_and_scan(manager)

        mock_connection = AsyncMock()
        mock_connection.username = USER  # USER is both friend and privileged
        mock_connection._writer = AsyncMock()

        await manager.send_shares_reply(mock_connection)
        mock_connection.send_message.assert_called_once()

        from aioslsk.protocol.messages import PeerSharesReply
        call_arg = mock_connection.send_message.call_args[0][0]
        assert isinstance(call_arg, PeerSharesReply.Request)

    @pytest.mark.asyncio
    async def test_sendSharesReply_noUsernameSendsNothing(self, manager: SharesManager):
        """send_shares_reply should do nothing if connection has no username."""
        await _load_and_scan(manager)

        mock_connection = AsyncMock()
        mock_connection.username = None

        await manager.send_shares_reply(mock_connection)
        mock_connection.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_copyPrebakedListingTo_streamWriter(self, manager: SharesManager):
        """copy_prebaked_listing_to should write to a StreamWriter."""
        await _load_and_scan(manager)

        import io
        buffer = io.BytesIO()

        bytes_written = await manager.index.copy_prebaked_listing_to(
            buffer, DirectoryShareMode.EVERYONE
        )

        assert bytes_written > 0
        buffer.seek(0)
        assert len(buffer.read()) == bytes_written


@pytest.mark.asyncio
async def test_get_directory_files_empty_shared_root_returns_empty_listing(
    manager: SharesManager, tmp_path
):
    shared_dir = SharedDirectory("shared", str(tmp_path / "shared"), "alias_empty")
    await manager.index.add_shared_directory(shared_dir)

    root_remote_dir = "@@alias_empty"
    directory_data = await manager.index.get_directory_files(root_remote_dir)
    assert directory_data is not None
    assert directory_data.name == root_remote_dir
    assert directory_data.files == []


@pytest.mark.asyncio
async def test_get_shared_directories_username_none_not_locked(manager: SharesManager, tmp_path):
    friends_dir = SharedDirectory(
        "friends", str(tmp_path / "friends"), "alias1",
        share_mode=DirectoryShareMode.FRIENDS
    )
    users_dir = SharedDirectory(
        "users", str(tmp_path / "users"), "alias2",
        share_mode=DirectoryShareMode.USERS
    )
    users_dir.users = ["someuser"]

    await manager.index.add_shared_directory(friends_dir)
    await manager.index.add_shared_directory(users_dir)

    visible, locked = await manager.index.get_shared_directories(username=None)
    assert locked == []
    assert {d.alias for d in visible} >= {"alias1", "alias2"}


@pytest.mark.asyncio
async def test_scan_purges_deleted_files(manager: SharesManager, tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    (shared_dir / "a.txt").write_text("a")
    (shared_dir / "b.txt").write_text("b")

    manager._settings.shares.directories = [
        SharedDirectorySettingEntry(
            path=str(shared_dir),
            share_mode=DirectoryShareMode.EVERYONE,
        )
    ]
    manager.index._settings = manager._settings

    await manager.load_from_settings()
    await manager.scan()

    _, file_count = await manager.index.get_stats()
    assert file_count == 2

    (shared_dir / "b.txt").unlink()
    await manager.scan()

    _, file_count = await manager.index.get_stats()
    assert file_count == 1


@pytest.mark.asyncio
async def test_get_directory_files_glob_escape(manager: SharesManager, tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    hot_dir = shared_dir / "HotAAlbum"
    hot_dir.mkdir()
    (hot_dir / "song.mp3").write_text("x")

    manager._settings.shares.directories = [
        SharedDirectorySettingEntry(
            path=str(shared_dir),
            share_mode=DirectoryShareMode.EVERYONE,
        )
    ]
    manager.index._settings = manager._settings

    await manager.load_from_settings()
    await manager.scan()

    visible, locked = await manager.index.get_shared_directories()
    alias = (visible + locked)[0].alias

    bad_remote_dir = f"@@{alias}\\Hot[Album]"
    assert await manager.index.get_directory_files(bad_remote_dir) is None

    good_remote_dir = f"@@{alias}\\HotAAlbum"
    directory_data = await manager.index.get_directory_files(good_remote_dir)
    assert directory_data is not None
    assert len(directory_data.files) == 1


@pytest.mark.asyncio
async def test_get_directory_files_root_with_only_subdirs(manager: SharesManager, tmp_path):
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    subdir = shared_dir / "Only_Subdir"
    subdir.mkdir()
    (subdir / "song.mp3").write_text("x")

    manager._settings.shares.directories = [
        SharedDirectorySettingEntry(
            path=str(shared_dir),
            share_mode=DirectoryShareMode.EVERYONE,
        )
    ]
    manager.index._settings = manager._settings

    await manager.load_from_settings()
    await manager.scan()

    visible, locked = await manager.index.get_shared_directories()
    alias = (visible + locked)[0].alias

    root_remote_dir = f"@@{alias}"
    directory_data = await manager.index.get_directory_files(root_remote_dir)
    assert directory_data is not None
    assert directory_data.name == root_remote_dir
    assert directory_data.files == []


@pytest.mark.asyncio
async def test_add_shared_directory_parent_detection_prefix_collision(
    manager: SharesManager, tmp_path
):
    parent_path = str(tmp_path / "foo")
    sibling_path = str(tmp_path / "foobar")

    parent = SharedDirectory("foo", parent_path, "alias1")
    parent.items.add(SharedItem(parent, "", "song.mp3", 0.0, 1))
    sibling = SharedDirectory("foobar", sibling_path, "alias2")

    await manager.index.add_shared_directory(parent)
    await manager.index.add_shared_directory(sibling)

    item = await manager.index.get_shared_item(f"@@{parent.alias}\\song.mp3")
    assert item is not None


@pytest.mark.asyncio
async def test_standalone_directory_scan_updates_search_and_removes_deleted_files(
    manager: SharesManager, tmp_path
):
    root = tmp_path / "standalone"
    root.mkdir()
    old = root / "old.txt"
    old.write_text("old")
    shared = await manager.add_shared_directory(str(root))
    await manager.scan_directory_files(shared)
    old_results, _ = await manager.query("old")
    assert len(old_results) == 1

    old.unlink()
    (root / "new.txt").write_text("new")
    await manager.scan_directory_files(shared)
    assert await manager.query("old") == ([], [])
    new_results, _ = await manager.query("new")
    assert len(new_results) == 1


@pytest.mark.asyncio
async def test_failed_directory_scan_preserves_previous_items(
    manager: SharesManager, tmp_path, monkeypatch
):
    root = tmp_path / "failed_scan"
    root.mkdir()
    (root / "old.txt").write_text("old")
    shared = await manager.add_shared_directory(str(root))
    await manager.scan()

    async def fail_scan(*args, **kwargs):
        yield [SharedItem(shared, "", "partial.txt", 0.0, 1)]
        raise OSError("scan failed")

    monkeypatch.setattr("aioslsk.shares.manager.iter_scanned_item_batches", fail_scan)
    with pytest.raises(OSError, match="scan failed"):
        await manager.scan_directory_files(shared)
    assert await manager.index.get_shared_item(f"@@{shared.alias}\\old.txt") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("remove", [False, True])
async def test_scan_batch_does_not_restore_outdated_sharing_configuration(
    manager: SharesManager, tmp_path, monkeypatch, remove
):
    root = tmp_path / "changing_share"
    root.mkdir()
    shared = await manager.add_shared_directory(str(root))

    async def changed_during_scan(scanned, **kwargs):
        if remove:
            await manager.remove_shared_directory(scanned.absolute_path)
        else:
            await manager.update_shared_directory(
                scanned.absolute_path, share_mode=DirectoryShareMode.USERS, users=["friend"]
            )
        yield [SharedItem(scanned, "", "private.txt", 0.0, 1)]

    monkeypatch.setattr(
        "aioslsk.shares.manager.iter_scanned_item_batches", changed_during_scan
    )
    await manager.scan()
    if remove:
        assert not await manager.index.is_directory_shared(shared.absolute_path)
        assert await manager.query("private") == ([], [])
    else:
        current = await manager.get_shared_directory(shared.absolute_path)
        assert current.share_mode == DirectoryShareMode.USERS
        assert current.users == ["friend"]
        visible, locked = await manager.query("private", username="stranger")
        assert not visible
        assert len(locked) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["add", "update", "remove", "rescan"])
async def test_share_changes_invalidate_prebaked_listings(
    manager: SharesManager, tmp_path, change
):
    from aioslsk.protocol.messages import PeerSharesReply

    root = tmp_path / "cached"
    root.mkdir()
    (root / "song.txt").write_text("song")
    shared = await manager.add_shared_directory(str(root))
    await manager.scan()
    assert await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)

    if change == "add":
        await manager.add_shared_directory(str(tmp_path / "additional"))
    elif change == "update":
        await manager.update_shared_directory(shared, share_mode=DirectoryShareMode.FRIENDS)
    elif change == "remove":
        await manager.remove_shared_directory(shared)
    else:
        (root / "song.txt").unlink()
        (root / "new.txt").write_text("new")
        await manager.scan_directory_files(shared)

    with pytest.raises(FileNotFoundError):
        await manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)

    connection = AsyncMock()
    connection.username = "stranger"
    connection.obfuscated = True
    await manager.send_shares_reply(connection)
    sent = connection.send_message.call_args.args[0]
    assert isinstance(sent, PeerSharesReply.Request)
    visible, locked = await manager.create_shares_reply("stranger")
    assert sent.directories == visible
    assert sent.locked_directories == locked


@pytest.mark.asyncio
@pytest.mark.parametrize("remove", [False, True])
async def test_share_notification_aborts_upload_after_committed_change(
    manager: SharesManager, tmp_path, remove
):
    root = tmp_path / "revoked"
    root.mkdir()
    (root / "song.txt").write_text("song")
    shared = await manager.add_shared_directory(
        str(root), share_mode=DirectoryShareMode.USERS, users=["stranger"]
    )
    await manager.scan()
    transfers = TransferManager(
        manager._settings, manager._event_bus, AsyncMock(), manager, AsyncMock()
    )
    upload = await transfers._add_upload("stranger", f"@@{shared.alias}\\song.txt")
    await upload.state.queue()

    cycles = []

    def on_change(event):
        cycles.append(asyncio.create_task(transfers.manage_shares_changed()))

    manager._event_bus.register(SharedDirectoryChangeEvent, on_change)
    if remove:
        await manager.remove_shared_directory(shared)
    else:
        await manager.update_shared_directory(shared, users=[])
    await asyncio.gather(*cycles)
    assert upload.state.VALUE == TransferState.ABORTED


@pytest.mark.asyncio
@pytest.mark.parametrize("replace", [False, True])
async def test_scan_does_not_attach_files_to_replaced_or_restricted_roots(
    manager: SharesManager, tmp_path, monkeypatch, replace
):
    root = tmp_path / "parent"
    root.mkdir()
    shared = await manager.add_shared_directory(str(root))

    async def change_roots(scanned, **kwargs):
        if replace:
            await manager.remove_shared_directory(scanned)
            target = root
            subdir = ""
        else:
            target = root / "restricted"
            subdir = "restricted"
        await manager.add_shared_directory(
            str(target), share_mode=DirectoryShareMode.USERS, users=["friend"]
        )
        yield [SharedItem(scanned, subdir, "private.txt", 0.0, 1)]

    monkeypatch.setattr("aioslsk.shares.manager.iter_scanned_item_batches", change_roots)
    await manager.scan_directory_files(shared)
    assert await manager.query("private", username="stranger") == ([], [])


@pytest.mark.asyncio
@pytest.mark.parametrize("attributes", [[], [(0, 128), (1, 15)]])
@pytest.mark.parametrize("full_scan", [False, True])
async def test_scan_reuses_unchanged_attributes(manager, tmp_path, attributes, full_scan):
    root = tmp_path / "music"
    root.mkdir()
    (root / "song.mp3").write_bytes(b"audio")
    shared = await manager.add_shared_directory(str(root))

    async def scan():
        if full_scan:
            await manager.scan()
        else:
            await manager.scan_directory_files(shared)

    with patch("aioslsk.shares.manager.extract_attributes", return_value=attributes) as extract:
        await scan()
        await scan()

    extract.assert_called_once_with(str(root / "song.mp3"))
    item = await manager.index.get_shared_item(shared.get_remote_path() + "\\song.mp3")
    assert item.attributes == attributes


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["mtime", "size", "rename"])
async def test_scan_reextracts_changed_attributes(manager, tmp_path, change):
    root = tmp_path / "music"
    root.mkdir()
    path = root / "song.mp3"
    path.write_bytes(b"audio")
    shared = await manager.add_shared_directory(str(root))
    with patch("aioslsk.shares.manager.extract_attributes", side_effect=[[(0, 128)], [(0, 256)]]) as extract:
        await manager.scan_directory_files(shared)
        before = path.stat()
        if change == "mtime":
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 2_000_000_000))
        elif change == "size":
            path.write_bytes(b"longer audio")
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        else:
            path = path.rename(root / "renamed.mp3")
        await manager.scan_directory_files(shared)

    assert extract.call_count == 2
    item = await manager.index.get_shared_item(shared.get_remote_path() + "\\" + path.name)
    assert item.attributes == [(0, 256)]
    assert item.size_bytes == path.stat().st_size


@pytest.mark.asyncio
async def test_scan_extracts_missing_cached_attributes(manager, tmp_path):
    root = tmp_path / "music"
    root.mkdir()
    path = root / "song.mp3"
    path.write_bytes(b"audio")
    shared = await manager.add_shared_directory(str(root))
    stat = path.stat()
    pending = SharedItem(shared, "", path.name, stat.st_mtime, stat.st_size)
    await manager.index.begin_directory_scan(shared)
    await manager.index.persist_scanned_items_batch(shared, [pending])
    await manager.index.end_directory_scan(shared)

    with patch("aioslsk.shares.manager.extract_attributes", return_value=[(0, 128)]) as extract:
        await manager.scan_directory_files(shared)
        await manager.scan_directory_files(shared)

    extract.assert_called_once_with(str(path))
    item = await manager.index.get_shared_item(shared.get_remote_path() + "\\song.mp3")
    assert item.attributes == [(0, 128)]


@pytest.mark.asyncio
async def test_scan_retries_failed_attribute_extraction(manager, tmp_path):
    root = tmp_path / "music"
    root.mkdir()
    path = root / "song.mp3"
    path.write_bytes(b"audio")
    shared = await manager.add_shared_directory(str(root))
    with patch("aioslsk.shares.manager.extract_attributes", side_effect=[OSError("read failed"), []]) as extract:
        await manager.scan_directory_files(shared)
        await manager.scan_directory_files(shared)
        await manager.scan_directory_files(shared)

    assert extract.call_count == 2
    item = await manager.index.get_shared_item(shared.get_remote_path() + "\\song.mp3")
    assert item.attributes == []
