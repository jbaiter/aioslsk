import itertools
from pathlib import Path
from aiofiles import os as asyncos
import asyncio
from collections.abc import Callable
from concurrent.futures import Executor
from functools import partial
import logging
import mutagen
from mutagen.mp3 import BitrateMode
import os
import queue
import sys
import time
from typing import Optional, Union, TYPE_CHECKING
import uuid

from aioslsk.shares.index import MemoryShareIndex

from ..base_manager import BaseManager
from .cache import SharesCache, SharesShelveCache
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
    from ..network.connection import PeerConnection

logger = logging.getLogger(__name__)

ItemAttributes = list[tuple[int, int]]
ExecutorFactory = Callable[[], Executor]

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


async def scan_directory(
        shared_directory: SharedDirectory,
        children: Optional[list[SharedDirectory]] = None,
        stat_concurrency: int = 64) -> set[SharedItem]:
    """Scans the directory for items to share with parallel stat calls.

    :param shared_directory: :class:`.SharedDirectory` instance
    :param children: list of :class:`.SharedDirectory` instances, the items in
        this list will not be returned and should be scanned individually
    :param stat_concurrency: max concurrent stat calls per directory (default 64)
    :return: set of :class:`.SharedItem` objects found during the scan
    """
    children = children or []
    shared_items: set[SharedItem] = set()
    semaphore = asyncio.Semaphore(stat_concurrency)

    async def stat_file(filepath: str) -> os.stat_result | None:
        async with semaphore:
            try:
                return await asyncio.to_thread(os.stat, filepath)
            except OSError:
                logger.debug("could not stat file: %r", filepath)
                return None

    for directory, _, files in os.walk(shared_directory.absolute_path):
        abs_dir = os.path.normpath(os.path.abspath(directory))
        if any(child_dir.is_parent_of(abs_dir) for child_dir in children):
            continue

        subdir = os.path.relpath(directory, shared_directory.absolute_path)
        if subdir == '.':
            subdir = ''

        filepaths = [os.path.join(directory, f) for f in files]
        stats = await asyncio.gather(*[stat_file(fp) for fp in filepaths])

        for filename, stat in zip(files, stats):
            if stat is not None:
                shared_items.add(
                    SharedItem(shared_directory, subdir, filename, stat.st_mtime)
                )

    return shared_items


def extract_attributes(filepath: str) -> ItemAttributes:
    """Attempts to extract attributes from the file at ``filepath``. If there was
    an error attempting to extract the attributes this method will log a warning
    and return an empty list of attributes
    """
    attributes = []
    try:
        mutagen_file = mutagen.File(filepath)
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

    except mutagen.MutagenError as exc:
        logger.warning(
            "failed retrieve audio file metadata. path=%r", filepath, exc_info=exc)

    return attributes


class SharesManager(BaseManager):
    _ALIAS_LENGTH = 5

    def __init__(
            self, settings: Settings, event_bus: EventBus,
            network: Network, cache: Optional[SharesCache] = None,
            executor_factory: Optional[ExecutorFactory] = None):

        self._settings: Settings = settings
        self._event_bus: EventBus = event_bus
        self._network: Network = network
        self._session: Optional[Session] = None

        self.executor: Optional[Executor] = None
        self.executor_factory: Optional[ExecutorFactory] = executor_factory

        prebake_dir: Path | None = None
        if isinstance(cache, SharesShelveCache):
            prebake_dir = Path(cache.data_directory)
        self.index = MemoryShareIndex(settings, cache, prebake_dir=prebake_dir)

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

    async def start(self):
        if self.executor_factory:
            self.executor = self.executor_factory()

    async def stop(self) -> list[asyncio.Task]:
        if self.executor:
            self.executor.shutdown()
            self.executor = None

        return []

    async def load_data(self):
        if isinstance(self.index, MemoryShareIndex):
            self.index.read_cache()
        await self.load_from_settings()

    async def store_data(self):
        if isinstance(self.index, MemoryShareIndex):
            self.index.write_cache()

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

        if isinstance(self.index, MemoryShareIndex):
            self.index.rebuild_term_map()

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
        """Equivelant to :meth:`get_shared_item_cache` but returns ``None`` if the
        shared item is not found
        """
        try:
            return await self.index.get_shared_item(remote_path, username=username)
        except (FileNotFoundError, FileNotSharedError):
            return None

    async def find_shared_item(
            self, remote_path: str, username: Optional[str] = None) -> Optional[SharedItem]:
        """Equivelant to :meth:`get_shared_item` but returns ``None`` if the
        shared item is not found
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

        # Emit event BEFORE yielding to index update, so transfers can be
        # aborted immediately before other tasks (like downloads) get a chance
        # to run due to us yielding
        self._event_bus.emit_sync(SharedDirectoryChangeEvent(shared_directory))

        await self.index.update_shared_directory(shared_directory)

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
        if directory is None:
            raise SharedDirectoryError(
                "attempted to remove directory which was not added to the manager"
            )

        self._event_bus.emit_sync(SharedDirectoryChangeEvent(shared_directory))

        await self.index.delete_shared_directory(shared_directory)

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

    async def _scan_directory_files_internal(
        self, shared_directory: SharedDirectory
    ) -> SharedDirectory | None:
        """Internal method that scans files for a directory without persisting.

        :param shared_directory: :class:`.SharedDirectory` instance to scan
        :return: the shared_directory with updated items, or None if scan failed
        """
        if not await self.index.is_directory_shared(shared_directory.absolute_path):
            logger.warning(
                "attempted to scan directory which was not added to the index: %r",
                shared_directory
            )
            return None

        logger.info("scheduling scan for directory : %r", shared_directory)
        children = await self._get_child_directories(shared_directory)
        try:
            shared_items: set[SharedItem] = await scan_directory(
                shared_directory,
                children=children
            )
        except Exception:
            logger.exception("exception scanning directory : %r", shared_directory)
            return None

        logger.debug("scan found %d files for directory : %r", len(shared_items), shared_directory)

        # Adds all new items to the directory items
        shared_directory.items |= shared_items

        # Remove all items from the cache that weren't in the returned items
        # set. Because the `modified` parameter is part of the hash items
        # with a changed `modified` parameter will also be removed meaning
        # their attributes will be reset and these files attributes need
        # to be rescanned
        shared_directory.items -= (shared_directory.items ^ shared_items)

        return shared_directory

    async def scan_directory_files(self, shared_directory: SharedDirectory):
        """Scans the files for the given ``shared_directory``

        :param shared_directory: :class:`.SharedDirectory` instance to scan
        :raise SharedDirectoryError: raised when the passed ``shared_directory``
            was not added to the manager
        """
        if not await self.index.is_directory_shared(shared_directory.absolute_path):
            raise SharedDirectoryError(
                "attempted to scan directory which was not added to the index"
            )

        result = await self._scan_directory_files_internal(shared_directory)
        if result is not None:
            # Persist the updated items to the index
            await self.index.add_shared_directory(shared_directory)

            if isinstance(self.index, MemoryShareIndex):
                self.index.build_term_map(shared_directory)
                self.index.cleanup_term_map()

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
        loop = asyncio.get_running_loop()

        # Track items being processed and their futures
        items_to_scan: list[SharedItem] = []
        futures: list[asyncio.Future] = []

        for item in shared_directory.items:
            if item.attributes is None:
                items_to_scan.append(item)
                future = loop.run_in_executor(
                    self.executor,
                    partial(extract_attributes, item.get_absolute_path())
                )
                future.add_done_callback(
                    partial(self._extract_attributes_callback, item)
                )
                futures.append(future)

        logger.debug(
            "scheduled %d / %d items for attribute extracting for directory %s",
            len(futures), len(shared_directory.items), shared_directory
        )

        start = time.perf_counter()

        await asyncio.gather(*futures, return_exceptions=True)

        logger.debug(
            "scanned attributes for %d items in %f s", len(futures), time.perf_counter() - start)

        # Return items that now have attributes
        return [item for item in items_to_scan if item.attributes is not None]

    def _extract_attributes_callback(self, item: SharedItem, future: asyncio.Future):
        try:
            item.attributes = future.result()
        except Exception:
            logger.warning("exception fetching shared item attributes")

    async def scan(self):
        """Scan the files and their attributes for all directories currently
        defined in the ``shared_directories``

        This method will emit a :class:`.ScanCompleteEvent` on the event bus
        and report the shares to the server
        """
        start_time = time.perf_counter()

        visible, locked = await self.index.get_shared_directories()
        all_shared_dirs = visible + locked

        files_tasks = [
            self.scan_directory_files(shared_directory)
            for shared_directory in all_shared_dirs
        ]
        await asyncio.gather(*files_tasks, return_exceptions=True)

        # Scan attributes for all directories
        attribute_results = await asyncio.gather(*[
            self.scan_directory_file_attributes(shared_directory)
            for shared_directory in all_shared_dirs
        ])

        await self.index.update_item_attributes(
            list(itertools.chain.from_iterable(attribute_results)))

        # Update prebaked listings for fast share replies
        await self.update_prebaked_listings()

        logger.info("completed scan in %f seconds", time.perf_counter() - start_time)
        folder_count, file_count = await self.get_stats()
        await self._event_bus.emit(
            ScanCompleteEvent(folder_count, file_count)
        )

        await self.report_shares()

    async def get_filesize(self, shared_item: SharedItem) -> int:
        return await asyncos.path.getsize(shared_item.get_absolute_path())

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
            self, username: str) -> tuple[list[SharedDirectory], list[SharedDirectory]]:
        return await self.index.get_shared_directories(username=username)

    async def create_shares_reply(self, username: str) -> tuple[list[DirectoryData], list[DirectoryData]]:
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

        # Try prebaked listing, fall back to dynamic if not available
        try:
            writer = connection._writer
            if writer is None:
                raise ValueError("connection has no writer")

            if connection.obfuscated:
                # For obfuscated connections, load bytes and send through normal path
                data = await self.index.get_prebaked_listing(mode)
                await connection.send_message(data)
            else:
                # For non-obfuscated connections, stream directly
                await self.index.copy_prebaked_listing_to(writer, mode)
                await writer.drain()

        except Exception:
            logger.debug("prebaked listing not available, generating dynamically")
            visible, locked = await self.create_shares_reply(username)
            await connection.send_message(
                PeerSharesReply.Request(
                    directories=visible,
                    locked_directories=locked
                )
            )

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
        # Generate listing for non-privileged users (EVERYONE mode)
        # Pass None as username - this means FRIENDS dirs will be locked
        everyone_visible, everyone_locked = await self.index.get_shared_items_for_user(
            username=None
        )
        await self.index.save_prebaked_listing(
            DirectoryShareMode.EVERYONE,
            visible=everyone_visible,
            locked=everyone_locked,
        )
        logger.debug(
            "updated prebaked listing for EVERYONE: %d visible, %d locked dirs",
            len(everyone_visible), len(everyone_locked)
        )

        # Generate listing for friends (FRIENDS mode)
        # Find any friend to use as reference, or use a synthetic marker
        friends = self._settings.users.friends
        if friends:
            # Use an arbitrary friend - they all see the same FRIENDS directories
            friend_username = next(iter(friends))
            friends_visible, friends_locked = await self.index.get_shared_items_for_user(
                username=friend_username
            )
        else:
            # No friends configured - FRIENDS listing is same as EVERYONE
            friends_visible, friends_locked = everyone_visible, everyone_locked

        await self.index.save_prebaked_listing(
            DirectoryShareMode.FRIENDS,
            visible=friends_visible,
            locked=friends_locked,
        )
        logger.debug(
            "updated prebaked listing for FRIENDS: %d visible, %d locked dirs",
            len(friends_visible), len(friends_locked)
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
