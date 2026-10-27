import asyncio
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Executor, TimeoutError as FutureTimeoutError
from functools import partial
import logging
import mutagen
from mutagen.mp3 import BitrateMode
import os
import sys
import threading
import time
from typing import Optional, Union, TYPE_CHECKING
import uuid

from aiofiles import os as asyncos
from aioslsk.shares.index import MemoryShareIndex, ShareIndex, SQLiteShareIndex

from ..base_manager import BaseManager
from .cache import SharesCache
from ..events import (
    EventBus,
    ScanCompleteEvent,
    SessionInitializedEvent,
    SessionDestroyedEvent,
    SharedDirectoryChangeEvent,
)
from ..exceptions import (
    FileNotFoundError,
    FileNotSharedError,
    SharedDirectoryError,
)
from .model import DirectoryShareMode, SharedDirectory, SharedItem
from ..naming import (
    chain_strategies,
    DefaultNamingStrategy,
    NumberDuplicateStrategy,
)
from ..network.network import Network
from ..protocol.messages import SharedFoldersFiles
from ..protocol.primitives import DirectoryData
from ..search.model import SearchQuery
from ..session import Session
from ..settings import Settings

if TYPE_CHECKING:
    from aioslsk.database.sqlite import SQLiteDatabase
    from ..network.connection import PeerConnection

logger = logging.getLogger(__name__)

ItemAttributes = list[tuple[int, int]]
ExecutorFactory = Callable[[], Executor]
SCAN_STAT_CONCURRENCY = 64
SCAN_FILE_BATCH_SIZE = 500
SCAN_DIRECTORIES_IN_FLIGHT = 2

_COMPRESSED_FORMATS = [
    'MP3',
    'MP4',
    'ASF',  # WMA
    'OggVorbis'
]
_LOSSLESS_FORMATS = [
    'FLAC',
    'WAVE'
]


async def iter_scanned_item_batches(
    shared_directory: SharedDirectory,
    children: Optional[list[SharedDirectory]] = None,
    stat_concurrency: int = SCAN_STAT_CONCURRENCY,
    batch_size: int = SCAN_FILE_BATCH_SIZE,
) -> AsyncIterator[list[SharedItem]]:
    """Yield scanned shared items in bounded-size batches.

    :param shared_directory: :class:`.SharedDirectory` instance
    :param children: list of :class:`.SharedDirectory` instances, the items in
        this list will not be returned and should be scanned individually
    :param stat_concurrency: retained for compatibility; stats now run in the
        walker thread rather than scheduling a task for each file
    :param batch_size: max files to process and emit per batch
    :yield: batch of :class:`.SharedItem` objects found during the scan
    """
    children = children or []

    # Walk and stat in the same worker, keeping both blocking filesystem calls
    # and per-file executor dispatch off the event loop. The queue bounds prefetch.
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[list[SharedItem] | BaseException | None] = asyncio.Queue(maxsize=8)
    stop_event = threading.Event()

    def _put_from_thread(msg: list[SharedItem] | BaseException | None) -> bool:
        if stop_event.is_set():
            return False
        try:
            put_future = asyncio.run_coroutine_threadsafe(queue.put(msg), loop)
        except RuntimeError:
            return False

        while True:
            try:
                put_future.result(timeout=0.2)
                return True
            except FutureTimeoutError:
                if stop_event.is_set():
                    put_future.cancel()
                    return False

    def _walk_in_thread() -> None:
        try:
            for directory, _, files in os.walk(shared_directory.absolute_path):
                abs_dir = os.path.normpath(os.path.abspath(directory))
                if any(child_dir.is_parent_of(abs_dir) for child_dir in children):
                    continue

                subdir = os.path.relpath(directory, shared_directory.absolute_path)
                if subdir == '.':
                    subdir = ''

                for idx in range(0, len(files), batch_size):
                    items = []
                    for filename in files[idx:idx + batch_size]:
                        if stop_event.is_set():
                            return
                        filepath = os.path.join(directory, filename)
                        try:
                            stat = os.stat(filepath)
                        except OSError:
                            logger.debug("could not stat file: %r", filepath)
                            continue
                        items.append(SharedItem(
                            shared_directory, subdir, filename, stat.st_mtime, stat.st_size
                        ))
                    if items and not _put_from_thread(items):
                        return
        except BaseException as exc:
            _put_from_thread(exc)
        finally:
            _put_from_thread(None)

    producer_task = asyncio.create_task(asyncio.to_thread(_walk_in_thread))
    try:
        while True:
            msg = await queue.get()
            if msg is None:
                break
            if isinstance(msg, BaseException):
                raise msg

            yield msg
    finally:
        stop_event.set()
        await producer_task


async def scan_directory(
        shared_directory: SharedDirectory,
        children: Optional[list[SharedDirectory]] = None,
        stat_concurrency: int = SCAN_STAT_CONCURRENCY) -> set[SharedItem]:
    """Scans the directory for items to share.

    This compatibility helper materializes all batches into one set.
    """
    shared_items: set[SharedItem] = set()
    async for batch in iter_scanned_item_batches(
        shared_directory,
        children=children,
        stat_concurrency=stat_concurrency,
    ):
        shared_items.update(batch)
    return shared_items


def extract_attributes(filepath: str) -> ItemAttributes:
    """Attempts to extract attributes from the file at ``filepath``. If there was
    an error attempting to extract the attributes this method will log a warning
    and return an empty list of attributes
    """
    attributes = []
    try:
        mutagen_file = mutagen.File(filepath)  # type: ignore
        if not mutagen_file:
            return []

        if mutagen_file.__class__.__name__ in _COMPRESSED_FORMATS:
            attributes = [
                (0, int(mutagen_file.info.bitrate / 1000), ),
                (1, int(mutagen_file.info.length), )
            ]

            # Figure out bitrate mode if applicable
            bitrate_mode = getattr(mutagen_file, 'bitrate_mode', BitrateMode.UNKNOWN)
            if bitrate_mode == BitrateMode.CBR:
                attributes.append((2, 0, ))
            elif bitrate_mode in (BitrateMode.VBR, BitrateMode.ABR, ):
                attributes.append((2, 1, ))

        elif mutagen_file.__class__.__name__ in _LOSSLESS_FORMATS:
            attributes = [
                (1, int(mutagen_file.info.length), ),
                (4, mutagen_file.info.sample_rate, ),
                (5, mutagen_file.info.bits_per_sample, ),
            ]

    except mutagen.MutagenError as exc:  # type: ignore
        logger.warning(
            "failed retrieve audio file metadata. path=%r", filepath, exc_info=exc)

    return attributes


class SharesManager(BaseManager):
    _ALIAS_LENGTH = 5

    def __init__(
            self, settings: Settings, event_bus: EventBus,
            network: Network, cache: Optional[SharesCache] = None,
            executor_factory: Optional[ExecutorFactory] = None,
            database: "SQLiteDatabase | None" = None):

        self._settings: Settings = settings
        self._event_bus: EventBus = event_bus
        self._network: Network = network
        self._session: Optional[Session] = None
        self._scan_lock = asyncio.Lock()
        self._directory_scan_locks: dict[str, asyncio.Lock] = {}

        self.executor: Optional[Executor] = None
        self.executor_factory: Optional[ExecutorFactory] = executor_factory

        self.index: ShareIndex
        if database is not None:
            self.index = SQLiteShareIndex(database, settings)
        else:
            self.index = MemoryShareIndex(settings, cache)

        self.naming_strategies = [
            DefaultNamingStrategy(),
            NumberDuplicateStrategy()
        ]

        self.register_listeners()

    def register_listeners(self):
        self._event_bus.register(
            SessionInitializedEvent, self._on_session_initialized)
        self._event_bus.register(
            SessionDestroyedEvent, self._on_session_destroyed)

    async def get_shared_directories(self) -> list[SharedDirectory]:
        """Gets all shared directories (both visible and locked)

        :return: list of all shared directories
        """
        visible, locked = await self.index.get_shared_directories()
        return visible + locked

    def generate_alias(self, path: str, offset: int = 0) -> str:
        """Generates a directory alias for the given path, this method will be
        called recursively increasing the offset in case the alias is already
        found in the ``shared_directories``.

        The hardware address is mixed in to avoid getting the same alias for the
        same directory on different machines. Admittedly this is a lousy
        security measure but hopefully this will prevent easy leaking of files
        in case such issue would occur. Example: 'abcde' is always generated for
        'C:\\' so the attacker could guess where a file is located.

        :param path: the path for which to generate an alias
        :param offset: offset for the value of the initial 5 bytes (default=0)
        :return: a string of 5 alphabetic characters all lowercased
        """
        path_bytes = path.encode('utf8')
        unique_id = uuid.getnode().to_bytes(6, sys.byteorder)

        alias_bytearr = bytearray(
            [unique_id[-1] | offset for _ in range(self._ALIAS_LENGTH)])

        # Chunk the path into pieces of 5
        for c_idx in range(0, len(path_bytes), self._ALIAS_LENGTH):
            chunk = path_bytes[c_idx:c_idx + self._ALIAS_LENGTH]
            # previous_iter_byte XOR unique_id XOR current_iter_byte
            for b_idx, byte in enumerate(chunk):
                alias_bytearr[b_idx] ^= byte ^ unique_id[b_idx]

        # To lowercase
        alias_bytes = bytes([(byte % 26) + 0x61 for byte in alias_bytearr])
        alias_string = alias_bytes.decode('utf8')

        return alias_string

    def _validate_executor_compatibility(self):
        """Validate that the executor is compatible with the current index backend."""
        self.index.validate_executor_compatibility(self.executor, self.executor_factory)

    async def start(self):
        if self.executor_factory:
            self.executor = self.executor_factory()
        self._validate_executor_compatibility()

    async def stop(self) -> list[asyncio.Task]:
        if self.executor:
            self.executor.shutdown()
            self.executor = None

        return []

    async def load_data(self):
        await self.index.load_data()
        await self.load_from_settings()

    async def store_data(self):
        await self.index.store_data()

    async def load_from_settings(self):
        """Loads the shared directories from the settings. Existing directories
        will be updated, non-existing directories added, directories that no
        longer exist will be removed
        """
        # Get current directories from index
        visible, locked = await self.index.get_shared_directories()
        current_directories = {d.absolute_path: d for d in visible + locked}

        # Track which directories are still in settings
        settings_paths: set[str] = set()

        # Add or update directories
        for directory_entry in self._settings.shares.directories:
            abs_path = os.path.normpath(os.path.abspath(directory_entry.path))
            settings_paths.add(abs_path)

            shared_directory = current_directories.get(abs_path)
            if shared_directory is None:
                await self.add_shared_directory(
                    directory_entry.path,
                    share_mode=directory_entry.share_mode,
                    users=directory_entry.users
                )
            else:
                await self.update_shared_directory(
                    shared_directory,
                    share_mode=directory_entry.share_mode,
                    users=directory_entry.users or []
                )

        # Remove directories that are no longer in settings
        for abs_path, directory in current_directories.items():
            if abs_path not in settings_paths:
                await self.remove_shared_directory(directory)

        await self.index.on_settings_loaded()

    def get_download_directory(self) -> str:
        """Gets the absolute path the to download directory configured from the
        settings

        :return: Absolute path of the value of the ``shares.download`` setting
        """
        download_dir = self._settings.shares.download
        return os.path.abspath(download_dir)

    async def create_directory(self, absolute_path: str):
        """Ensures the passed directory exists"""
        if not await asyncos.path.exists(absolute_path):
            logger.info("creating directory : %s", absolute_path)
            await asyncos.makedirs(absolute_path, exist_ok=True)

    async def get_shared_item(self, remote_path: str, username: Optional[str] = None) -> SharedItem:
        """Same as :meth:`get_shared_item_cache` but also checks if the file
        exists on disk

        :param remote_path: the remote_path
        :param username: optional username to check if the file is shared or not
        :raise FileNotFoundError: filename was not found in shared_items or was
            found but did not exist on disk
        :raise FileNotSharedError: file is found, but locked for the given
            ``username``
        """
        item = await self.index.get_shared_item(remote_path, username=username)
        if item is None or not await asyncos.path.exists(item.get_absolute_path()):
            raise FileNotFoundError(
                f"file with remote_path {remote_path} found in cache but not on disk"
            )
        return item

    async def find_shared_item_cache(
            self, remote_path: str, username: Optional[str] = None) -> Optional[SharedItem]:
        """Equivalent to :meth:`get_shared_item` but does not check if the file exists on disk.

        Returns ``None`` if the item was not found in the index cache or is locked for ``username``.
        """
        try:
            return await self.index.get_shared_item(remote_path, username=username)
        except (FileNotFoundError, FileNotSharedError):
            return None

    async def find_shared_item(
            self, remote_path: str, username: Optional[str] = None) -> Optional[SharedItem]:
        """Equivelant to :meth:`find_shared_item` but also checks if the file exists on disk.
        Returns None if the file was not found or if the file is found but does not exist on disk.
        """
        try:
            return await self.get_shared_item(remote_path, username=username)
        except (FileNotFoundError, FileNotSharedError):
            return None

    async def add_shared_directory(
            self, shared_directory: str,
            share_mode: DirectoryShareMode = DirectoryShareMode.EVERYONE,
            users: Optional[list[str]] = None) -> SharedDirectory:
        """Adds a shared directory. This method will call :meth:`generate_alias`
        and add the directory to the directory map.

        This method will not scan the directory, however if the directory is a
        (sub)child of an already registered shared directory the items from that
        directory will be removed from the parent directory to the new directory

        For scanning see the :meth:`scan`, :meth:`scan_directory_files` and
        :meth:`scan_directory_file_attributes` methods.

        :param shared_directory: path of the shared directory
        :param share_mode: the share mode for the directory
        :param users: in case the share mode is `USERS`, a list of users to
            share it with
        :return: a :class:`.SharedDirectory` object
        :raise SharedDirectoryError: if the directory is already shared
        """
        if await self.index.is_directory_shared(shared_directory):
            raise SharedDirectoryError(
                f"directory {shared_directory} is already shared")

        # Generate an absolute path, alias and create the object
        abs_directory = os.path.normpath(os.path.abspath(shared_directory))
        alias = self.generate_alias(abs_directory)

        directory_object = SharedDirectory(
            shared_directory,
            abs_directory,
            alias,
            share_mode=share_mode,
            users=users or []
        )

        await self.index.add_shared_directory(directory_object)

        self._event_bus.emit_sync(SharedDirectoryChangeEvent(directory_object))

        return directory_object

    async def update_shared_directory(
            self, directory: Union[str, SharedDirectory],
            share_mode: Optional[DirectoryShareMode] = None,
            users: Optional[list[str]] = None) -> SharedDirectory:
        """Updates ``share_mode`` and ``users`` values for the given directory

        :param directory: if a string is given this method will attempt to find
            the shared directory based on the absolute path
        :return: the updated directory
        :raise SharedDirectoryError: if the given directory is a string and not
            in the list of shared directories
        """
        if isinstance(directory, str):
            shared_directory = await self.index.get_shared_directory(directory)
            if shared_directory is None:
                raise SharedDirectoryError(
                    f"directory {directory} is not shared and cannot be updated")
        else:
            shared_directory = directory

        if share_mode is not None:
            shared_directory.share_mode = share_mode

        if users is not None:
            shared_directory.users = users

        await self.index.update_shared_directory(shared_directory)
        self._event_bus.emit_sync(SharedDirectoryChangeEvent(shared_directory))

        return shared_directory

    async def remove_shared_directory(self, directory: Union[str, SharedDirectory]) -> SharedDirectory:
        """Removes the given shared directory. If the directory was a
        subdirectory of another shared directory its items will be moved into
        that directory

        Example file structure:

        * Music\\

            * Artist_One\\

                * song_one.mp3

            * Artist_Two\\

                * song_two.mp3

        If the user shares 2 directories:

        * Music\\ : EVERYONE
        * Music\\Artist_One\\ : FRIENDS

        And removes the `Music\\Artist_One\\ shared directory the files of that
        directory get returned back to the parent directory. In this case file
        ``song_two.mp3`` will be shared with EVERYONE again

        :param shared_directory: :class:`.SharedDirectory` instance to remove
        :return: the removed directory
        :raise SharedDirectoryError: raised when the passed `directory` was not
            added to the manager
        """
        shared_directory = await self.get_shared_directory(
            directory if isinstance(directory, str) else directory.absolute_path)
        if shared_directory is None:
            raise SharedDirectoryError(
                "attempted to remove directory which was not added to the manager"
            )

        await self.index.delete_shared_directory(shared_directory)
        self._event_bus.emit_sync(SharedDirectoryChangeEvent(shared_directory))

        return shared_directory

    async def get_shared_directory(self, directory: str) -> SharedDirectory:
        """Calculates the absolute path of given ``directory`` and looks for the
        matching :class:`.SharedDirectory` instance

        :raise SharedDirectoryError: if there is no corresponding
            :class:`.SharedDirectory` instance registered in this class
        """
        abs_path = os.path.normpath(os.path.abspath(directory))
        shared_directory = await self.index.get_shared_directory(abs_path)

        if shared_directory is None:
            raise SharedDirectoryError(
                f"did not find shared directory with path : {directory}")
        return shared_directory

    async def scan_directory_files(self, shared_directory: SharedDirectory):
        """Scans the files for the given ``shared_directory``

        :param shared_directory: :class:`.SharedDirectory` instance to scan
        :raise SharedDirectoryError: raised when the passed ``shared_directory``
            was not added to the manager
        """
        lock = self._directory_scan_locks.setdefault(
            shared_directory.absolute_path, asyncio.Lock()
        )
        async with lock:
            await self._scan_directory_files(shared_directory)

    async def _scan_directory_files(self, shared_directory: SharedDirectory):
        if not await self.index.is_directory_shared(shared_directory.absolute_path):
            raise SharedDirectoryError(
                "attempted to scan directory which was not added to the index"
            )

        logger.info("scheduling scan for directory : %r", shared_directory)
        children = await self._get_child_directories(shared_directory)

        await self.index.begin_directory_scan(shared_directory)
        scanned_items_count = 0
        try:
            async for item_batch in iter_scanned_item_batches(
                shared_directory,
                children=children,
                stat_concurrency=SCAN_STAT_CONCURRENCY,
                batch_size=SCAN_FILE_BATCH_SIZE,
            ):
                scanned_items_count += len(item_batch)
                await self.index.persist_scanned_items_batch(shared_directory, item_batch)

                updated_items = await self._scan_items_file_attributes(item_batch)
                if updated_items:
                    await self.index.persist_attribute_updates_batch(updated_items)
        except BaseException:
            await self.index.abort_directory_scan(shared_directory)
            raise
        else:
            await self.index.end_directory_scan(shared_directory)

        logger.debug(
            "scan found %d files for directory : %r",
            scanned_items_count,
            shared_directory,
        )

    async def _scan_items_file_attributes(
        self,
        items: list[SharedItem],
    ) -> list[SharedItem]:
        """Scans file attributes for the given item batch."""
        loop = asyncio.get_running_loop()
        items_to_scan = [item for item in items if item.attributes is None]
        if not items_to_scan:
            return []

        futures: list[asyncio.Future] = []
        for item in items_to_scan:
            futures.append(
                loop.run_in_executor(
                    self.executor,
                    partial(extract_attributes, item.get_absolute_path()),
                )
            )

        await asyncio.gather(*futures, return_exceptions=True)

        updated_items: list[SharedItem] = []
        for item, future in zip(items_to_scan, futures):
            try:
                item.attributes = future.result()
            except Exception:
                logger.warning("exception fetching shared item attributes")
            else:
                updated_items.append(item)

        return updated_items

    async def scan_directory_file_attributes(
        self, shared_directory: SharedDirectory
    ) -> list[SharedItem]:
        """Scans the file attributes for files in the given ``shared_directory``
        only files that do not have attributes will be scanned

        The results of the scan are handled internally and are automatically to
        the :class:`.SharedItem` object for which the scan was performed

        :param shared_directory: :class:`.SharedDirectory` instance for which
            the files need to be scanned
        :return: list of items that had their attributes updated
        """
        return await self._scan_items_file_attributes(list(shared_directory.items))

    async def scan(self):
        """Scan the files and their attributes for all directories currently
        defined in the ``shared_directories``

        This method will emit a :class:`.ScanCompleteEvent` on the event bus
        and report the shares to the server
        """
        async with self._scan_lock:
            await self._scan()

    async def _scan(self):
        start_time = time.perf_counter()

        visible, locked = await self.index.get_shared_directories()
        all_shared_dirs = visible + locked

        await self.index.begin_scan()
        try:
            semaphore = asyncio.Semaphore(SCAN_DIRECTORIES_IN_FLIGHT)

            async def _run_directory_scan(shared_directory: SharedDirectory):
                async with semaphore:
                    await self.scan_directory_files(shared_directory)

            tasks = [
                asyncio.create_task(_run_directory_scan(shared_directory))
                for shared_directory in all_shared_dirs
            ]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

            await self.index.finalize_scan([])
        finally:
            await self.index.end_scan()

        # Update prebaked listings for fast share replies
        await self.update_prebaked_listings()

        logger.info("completed scan in %f seconds", time.perf_counter() - start_time)
        folder_count, file_count = await self.get_stats()
        await self._event_bus.emit(
            ScanCompleteEvent(folder_count, file_count)
        )

        await self.report_shares()

    async def query(
            self, query: Union[str, SearchQuery],
            username: Optional[str] = None,
            excluded_search_phrases: Optional[list[str]] = None) -> tuple[list[SharedItem], list[SharedItem]]:
        """Performs a query on the ``shared_directories`` returning the matching
        items. If ``username`` is passed this method will return a list of
        visible results and list of locked results. If ``None`` the second list
        will always be empty.

        This method makes a first pass using the built in term map and filters
        the remaining results using regular expressions.

        :param query: the query to perform on the shared directories
        :param username: optionally the username of the user making the query.
            This is used to determine locked results, if not given the locked
            results list will be empty
        :param excluded_search_phrases: optional list of search phrases that
            should be excluded from the search results
        :return: two lists of visible results and locked results
        """
        return await self.index.query(query, username, excluded_search_phrases)

    async def get_stats(self) -> tuple[int, int]:
        """Gets the total amount of shared directories and files.

        :return: directory and file count as a ``tuple``
        """
        return await self.index.get_stats()

    def calculate_download_path(self, remote_path: str) -> tuple[str, str]:
        """Calculates the local download path for a remote path returned by
        another peer.

        :return: ``tuple`` of the directory and file name
        """
        download_dir = self.get_download_directory()

        return chain_strategies(
            self.naming_strategies,
            remote_path,
            download_dir
        )

    async def get_shared_directories_for_user(
        self, username: str
    ) -> tuple[list[SharedDirectory], list[SharedDirectory]]:
        return await self.index.get_shared_directories(username=username)

    async def create_shares_reply(
        self, username: str
    ) -> tuple[list[DirectoryData], list[DirectoryData]]:
        """Creates a complete list of the currently shared items as a reply to
        a :class:`.PeerSharesRequest` messages

        :param username: username of the user requesting the shares reply, this
            is used to determine the locked results
        :return: tuple of (visible directories, locked directories)
        """
        return await self.index.get_shared_items_for_user(username)

    async def send_shares_reply(self, connection: "PeerConnection") -> None:
        """Send shares reply to a peer connection.

        Uses prebaked listings for non-privileged users for efficiency. Falls
        back to dynamic generation for privileged users (those in a directory's
        user list) who need custom USERS-mode visibility.

        :param connection: the peer connection to send the reply to
        """
        from ..protocol.messages import PeerSharesReply

        username = connection.username
        if not username:
            return

        is_friend = username in self._settings.users.friends
        is_privileged = await self.index.is_privileged_user(username)

        # Privileged users need dynamic generation for USERS-mode directories
        if is_privileged:
            visible, locked = await self.create_shares_reply(username)
            await connection.send_message(
                PeerSharesReply.Request(
                    directories=visible,
                    locked_directories=locked
                )
            )
            return

        mode = DirectoryShareMode.FRIENDS if is_friend else DirectoryShareMode.EVERYONE

        # Try prebaked listing, fall back to dynamic if not available.
        # Streaming is bounded by timeouts to avoid hanging on stalled peers.
        try:
            if connection.obfuscated:
                # For obfuscated connections, load bytes and send through normal path
                data = await self.index.get_prebaked_listing(mode)
                await connection.send_message(data)
            else:
                # Hold the same lock as ordinary sends for the whole frame,
                # not just an individual chunk or drain.
                async with connection._send_lock:
                    writer = connection._writer
                    if writer is None:
                        raise ValueError("connection has no writer")
                    await self.index.copy_prebaked_listing_to(writer, mode)
        except FileNotFoundError:
            logger.debug("prebaked listing not available, generating dynamically")
            visible, locked = await self.create_shares_reply(username)
            await connection.send_message(
                PeerSharesReply.Request(
                    directories=visible,
                    locked_directories=locked
                )
            )
        except asyncio.CancelledError:
            from ..network.connection import CloseReason

            await connection.disconnect(CloseReason.REQUESTED)
            raise
        except asyncio.TimeoutError:
            from ..network.connection import CloseReason

            await connection.disconnect(CloseReason.TIMEOUT)
            return
        except OSError:
            from ..network.connection import CloseReason

            await connection.disconnect(CloseReason.WRITE_ERROR)
            return
        except Exception:
            from ..network.connection import CloseReason

            # A partial frame may already be on the wire. Sending a fallback
            # response here would corrupt the connection's framing.
            logger.exception("failed to stream shares reply")
            await connection.disconnect(CloseReason.WRITE_ERROR)

    async def create_directory_reply(self, remote_directory: str) -> list[DirectoryData]:
        """Lists directory data as a response to a directory request. This will
        not contain any information about subdirectories, only the files within
        that directory

        This method differs from the official clients who don't respond at all
        to the message if the directory does not exist. Instead if the directory
        does not exist and empty list is returned

        :param remote_directory: remote path of the directory
        :return: list of directories. Empty if the directory is not shared, a
            list with one entry if the directory is found
        """
        directory_data = await self.index.get_directory_files(remote_directory)
        if directory_data is None:
            return []
        return [directory_data]

    async def _get_child_directories(self, shared_directory: SharedDirectory):
        """Returns a list of child directories"""
        visible_directories, locked_directories = await self.index.get_shared_directories()
        all_directories = visible_directories + locked_directories
        children = [
            directory for directory in all_directories
            if directory.absolute_path != shared_directory.absolute_path and directory.is_child_of(shared_directory)
        ]
        return children

    def is_directory_locked(self, directory: SharedDirectory, username: str) -> bool:
        """Checks if the shared directory is locked for the given ``username``
        """
        if directory.share_mode == DirectoryShareMode.FRIENDS:
            return username not in self._settings.users.friends
        elif directory.share_mode == DirectoryShareMode.USERS:
            return username not in directory.users
        return False

    def is_item_locked(self, item: SharedItem, username: str) -> bool:
        """Checks if the shared item is locked for the given ``username``"""
        return self.is_directory_locked(item.shared_directory, username)

    async def update_prebaked_listings(self):
        """Updates the prebaked share listings for EVERYONE and FRIENDS modes.

        This should be called after a scan completes to update the cached
        serialized listings used for fast share reply generation.
        """
        # Generate listing for non-friend, non-privileged users (EVERYONE mode).
        # Using a synthetic username avoids the username=None semantics where
        # FRIENDS/USERS directories would appear unlocked.
        base_non_friend = "__aioslsk_nonfriend_prebake__"
        everyone_username = base_non_friend
        suffix = 0
        while (
            everyone_username in self._settings.users.friends
            or await self.index.is_privileged_user(everyone_username)
        ):
            suffix += 1
            everyone_username = f"{base_non_friend}{suffix}"

        everyone_visible_count, everyone_locked_count = await self.index.rebuild_prebaked_listing(
            DirectoryShareMode.EVERYONE,
            username=everyone_username,
        )
        logger.debug(
            "updated prebaked listing for EVERYONE: %d visible, %d locked dirs",
            everyone_visible_count, everyone_locked_count,
        )

        # Generate listing for friends (FRIENDS mode) using a non-privileged
        # friend as reference. Privileged friends need dynamic generation.
        friends = self._settings.users.friends
        if not friends:
            logger.debug(
                "skipping FRIENDS prebake listing update: no friends configured"
            )
            return

        friend_username = None
        for username in friends:
            if not await self.index.is_privileged_user(username):
                friend_username = username
                break

        if friend_username is None:
            # No suitable non-privileged friend configured:
            # FRIENDS listing is unused (no friends) or privileged users
            # will be handled dynamically.
            friends_visible_count, friends_locked_count = await self.index.rebuild_prebaked_listing(
                DirectoryShareMode.FRIENDS,
                username=everyone_username,
            )
        else:
            friends_visible_count, friends_locked_count = await self.index.rebuild_prebaked_listing(
                DirectoryShareMode.FRIENDS,
                username=friend_username,
            )
        logger.debug(
            "updated prebaked listing for FRIENDS: %d visible, %d locked dirs",
            friends_visible_count, friends_locked_count,
        )

    async def report_shares(self):
        """Reports the shares amount to the server"""
        if not self._session:
            return

        folder_count, file_count = await self.index.get_stats()
        logger.debug("reporting shares (folder_count=%d, file_count=%d)", folder_count, file_count)
        await self._network.send_server_messages(
            SharedFoldersFiles.Request(
                shared_folder_count=folder_count,
                shared_file_count=file_count
            )
        )

    async def _on_session_initialized(self, event: SessionInitializedEvent):
        self._session = event.session
        await self.report_shares()

    async def _on_session_destroyed(self, event: SessionDestroyedEvent):
        self._session = None
