from __future__ import annotations

import asyncio
import logging
import os.path
import re
from pathlib import Path
from typing import BinaryIO
from weakref import WeakSet

from async_timeout import timeout as atimeout

from .base import ShareIndex, STREAM_WRITE_TIMEOUT_S
from ..cache import SharesCache
from ..model import SharedItem, SharedDirectory, DirectoryShareMode
from ..utils import convert_items_to_file_data
from ...exceptions import FileNotFoundError, FileNotSharedError, SharedDirectoryError
from ...protocol.messages import PeerSharesReply
from ...protocol.primitives import (
    DirectoryData,
)
from ...search.model import SearchQuery
from ...settings import Settings


_QUERY_CLEAN_PATTERN = re.compile(r"[\W_]")
"""Pattern to remove all non-word/digit characters from a string"""


class MemoryShareIndex(ShareIndex):
    cache: SharesCache | None

    _logger: logging.Logger
    _settings: Settings
    _term_map: dict[str, WeakSet[SharedItem]]
    _shared_directories: list[SharedDirectory]
    _prebake_dir: Path | None
    _prebaked_listings: dict[DirectoryShareMode, bytes] | None
    _scan_seen_items: dict[str, set[SharedItem]]

    def __init__(
        self,
        settings: Settings,
        cache: SharesCache | None = None,
        prebake_dir: Path | None = None,
    ) -> None:
        self._logger = logging.getLogger(self.__class__.__qualname__)
        self._term_map = {}
        self._shared_directories = []
        self._settings = settings
        self._prebake_dir = prebake_dir
        self._prebaked_listings = None
        self._scan_seen_items = {}
        self.cache = cache

    async def add_shared_directory(
        self,
        shared_directory: SharedDirectory,
    ) -> None:
        # Update if already exists, otherwise add
        existing = await self.get_shared_directory(shared_directory.absolute_path)
        if existing is not None:
            # Update the existing directory in place
            existing.directory = shared_directory.directory
            existing.alias = shared_directory.alias
            existing.share_mode = shared_directory.share_mode
            existing.users = shared_directory.users
            existing.items |= shared_directory.items
        else:
            # If new directory is child of existing, move items from parent
            parents = self._get_parent_directories(shared_directory)
            if parents:
                parent = parents[-1]
                children = parent.get_items_for_directory(shared_directory)
                shared_directory.items |= children
                parent.items -= children

            self._shared_directories.append(shared_directory)

    async def is_directory_shared(self, directory: str) -> bool:
        """Checks if a directory is already a shared directory by checking the
        absolute path of that directory
        """
        shared_directory = await self.get_shared_directory(directory)
        return shared_directory is not None

    async def get_shared_directory(self, directory: str) -> SharedDirectory | None:
        """Calculates the absolute path of given ``directory`` and looks for the
        matching :class:`.SharedDirectory` instance

        :return: the corresponding :class:`.SharedDirectory` instance or ``None``
                 if not found
        """
        abs_path = os.path.normpath(os.path.abspath(directory))
        for shared_directory in self._shared_directories:
            if shared_directory.absolute_path == abs_path:
                return shared_directory

        return None

    async def get_shared_directories(
        self, username: str | None = None
    ) -> tuple[list[SharedDirectory], list[SharedDirectory]]:
        visible = []
        locked = []
        for shared_directory in self._shared_directories:
            if username and self._is_directory_locked(shared_directory, username):
                locked.append(shared_directory)
            else:
                visible.append(shared_directory)
        return visible, locked

    async def update_shared_directory(
        self,
        shared_directory: SharedDirectory,
    ) -> None:
        if shared_directory not in self._shared_directories:
            raise SharedDirectoryError("attempted to update unknown shared directory")
        # No-op for memory index

    async def delete_shared_directory(
        self, directory: str | SharedDirectory
    ) -> SharedDirectory:
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
        if isinstance(directory, str):
            shared_directory = await self.get_shared_directory(directory)
        else:
            shared_directory = directory
        if shared_directory is None or shared_directory not in self._shared_directories:
            raise SharedDirectoryError(
                "attempted to remove directory which was not added to the manager"
            )

        self._shared_directories.remove(shared_directory)

        parents = self._get_parent_directories(shared_directory)
        # If the directory has a parent directory, move all items into that
        # directory
        if parents:
            parent = parents[-1]
            parent.items |= shared_directory.items

        self.cleanup_term_map()

        return shared_directory

    async def get_stats(self) -> tuple[int, int]:
        """Gets the total amount of shared directories and files.

        :return: directory and file count as a ``tuple``
        """
        file_count = sum(len(directory.items) for directory in self._shared_directories)
        dir_count = sum(
            len(set(item.subdir for item in directory.items))
            for directory in self._shared_directories
        )
        return dir_count, file_count

    async def get_shared_item(
        self, remote_path: str, username: str | None = None
    ) -> SharedItem | None:
        for shared_directory in self._shared_directories:
            try:
                item = shared_directory.get_item_by_remote_path(remote_path)
            except FileNotFoundError:
                continue  # Try next directory
            else:
                if username and self._is_item_locked(item, username):
                    raise FileNotSharedError(f"file is not shared to user {username}")
                return item
        return None

    async def get_shared_items_for_user(
        self, username: str | None = None
    ) -> tuple[list[DirectoryData], list[DirectoryData]]:
        visible_shared_dirs, locked_shared_dirs = await self.get_shared_directories(username)

        def _build_directory_data(
            shared_dirs: list[SharedDirectory],
        ) -> list[DirectoryData]:
            directory_files: dict[str, list[SharedItem]] = {}
            all_directories: set[str] = set()

            for directory in shared_dirs:
                all_directories.add(directory.get_remote_path())
                for item in directory.items:
                    dir_path = item.get_remote_directory_path()
                    all_directories.add(dir_path)

                    parts = dir_path.split("\\")
                    for i in range(1, len(parts)):
                        all_directories.add("\\".join(parts[: i + 1]))

                    directory_files.setdefault(dir_path, []).append(item)

                if not directory.items:
                    all_directories.add(directory.get_remote_path())

            return [
                DirectoryData(
                    name=dir_path,
                    files=convert_items_to_file_data(
                        directory_files.get(dir_path, []),
                        use_full_path=False,
                    ),
                )
                for dir_path in sorted(all_directories)
            ]

        return _build_directory_data(visible_shared_dirs), _build_directory_data(
            locked_shared_dirs
        )

    async def get_directory_files(self, remote_directory: str) -> DirectoryData | None:
        items: list[SharedItem] = []

        remote_dir_parts = tuple(remote_directory.split("\\"))
        remote_dir_parts_len = len(remote_dir_parts)
        is_shared = False

        for shared_dir in self._shared_directories:
            if not is_shared and shared_dir.get_remote_path() == remote_directory:
                # Shared root should be considered shared even if it has no files.
                is_shared = True
            for item in shared_dir.items:
                # Funny logic to determine if the directory is shared
                if not is_shared:
                    if (
                        item.get_remote_directory_path_parts()[:remote_dir_parts_len]
                        == remote_dir_parts
                    ):
                        is_shared = True

                if item.get_remote_directory_path() == remote_directory:
                    items.append(item)

        if not is_shared:
            return None

        return DirectoryData(
            name=remote_directory,
            files=convert_items_to_file_data(items, use_full_path=False),
        )

    async def query(
        self,
        query: str | SearchQuery,
        username: str | None = None,
        excluded_search_phrases: list[str] | None = None,
    ) -> tuple[list[SharedItem], list[SharedItem]]:
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
        if not isinstance(query, SearchQuery):
            search_query = SearchQuery.parse(query)
        else:
            search_query = query

        # Ignore if no valid include or wildcard terms are given
        if not search_query.has_inclusion_terms():
            return [], []

        # First round using the term map
        include_terms = []
        for term in search_query.include_terms:
            subterms = re.split(_QUERY_CLEAN_PATTERN, term)
            for subterm in subterms:
                if not subterm:
                    continue

                if subterm not in self._term_map:  # Optimization
                    return [], []

                include_terms.append(subterm)

        for term in search_query.wildcard_terms:
            subterms = re.split(_QUERY_CLEAN_PATTERN, term)
            for idx, subterm in enumerate(subterms):
                if not subterm:
                    continue

                if idx == 0:
                    matching_terms = [
                        map_term
                        for map_term in self._term_map.keys()
                        if map_term.endswith(subterm)
                    ]

                    if not matching_terms:  # Optimization
                        return [], []

                    include_terms.extend(matching_terms)
                else:
                    if subterm not in self._term_map:  # Optimization
                        return [], []

                    include_terms.append(subterm)

        found_items = set(self._term_map[include_terms[0]])
        for include_term in include_terms:
            found_items &= set(self._term_map[include_term])

        # Regular expressions on the remaining items

        to_keep = set()
        excl_phrases = excluded_search_phrases or []
        for found_item in found_items:
            if not all(
                matcher(found_item.get_query_path())
                for matcher in search_query.matchers_iter()
            ):
                continue

            # Excluded search phrases
            for excl_phrase in excl_phrases:
                if excl_phrase in found_item.get_query_path().lower():
                    self._logger.debug(
                        "removing search result %r due to excluded phrase %r",
                        found_item.get_absolute_path(),
                        excl_phrase,
                    )
                    break

            else:
                to_keep.add(found_item)

            if len(to_keep) >= self._settings.searches.receive.max_results:
                break

        found_items = to_keep

        # Order by visible and locked results when username is given
        if username:
            visible_results = []
            locked_results = []
            for item in found_items:
                if self._is_item_locked(item, username):
                    locked_results.append(item)
                else:
                    visible_results.append(item)
            return visible_results, locked_results
        else:
            return list(found_items), []

    async def save_prebaked_listing(
        self,
        mode: DirectoryShareMode,
        visible: list[DirectoryData],
        locked: list[DirectoryData],
    ) -> None:
        """Save a prebaked listing to memory or disk."""
        reply_data = PeerSharesReply.Request(
            directories=visible,
            locked_directories=locked,
        ).serialize(compress=True)

        if self._prebake_dir is None:
            if self._prebaked_listings is None:
                self._prebaked_listings = {}
            self._prebaked_listings[mode] = reply_data
            self._logger.debug(
                f"Saved prebaked listing for {mode} to in-memory storage"
            )
        else:
            # Create the directory if it doesn't exist
            self._prebake_dir.mkdir(parents=True, exist_ok=True)

            # Use the share mode name as the filename
            filename = f"{mode.name.lower()}.bin"
            filepath = self._prebake_dir / filename

            # Write the binary data to the file
            filepath.write_bytes(reply_data)
            self._logger.debug(f"Saved prebaked listing for {mode} to {filepath}")

    def _resolve_prebaked_mode(self, mode: DirectoryShareMode) -> DirectoryShareMode:
        if mode == DirectoryShareMode.FRIENDS and not self._settings.users.friends:
            return DirectoryShareMode.EVERYONE
        return mode

    async def get_prebaked_listing(self, mode: DirectoryShareMode) -> bytes:
        """Get a prebaked listing from disk."""
        resolved_mode = self._resolve_prebaked_mode(mode)
        if self._prebake_dir is None:
            listing = (self._prebaked_listings or {}).get(resolved_mode)
            if listing is None:
                raise FileNotFoundError(
                    f"No prebaked listing found for share mode {resolved_mode}"
                )
            return listing

        filename = f"{resolved_mode.name.lower()}.bin"
        filepath = self._prebake_dir / filename

        if not filepath.exists():
            raise FileNotFoundError(
                f"No prebaked listing found for share mode {resolved_mode}"
            )

        return filepath.read_bytes()

    async def copy_prebaked_listing_to(
        self,
        target: BinaryIO | asyncio.StreamWriter,
        mode: DirectoryShareMode,
        chunk_size: int = 131072,
    ) -> int:
        """Copy a prebaked listing from disk to a file-like object."""
        resolved_mode = self._resolve_prebaked_mode(mode)
        if self._prebake_dir is None:
            listing = (self._prebaked_listings or {}).get(resolved_mode)
            if listing is None:
                raise FileNotFoundError(
                    f"No prebaked listing found for share mode {resolved_mode}"
                )

            if isinstance(target, asyncio.StreamWriter):
                # Stream in chunks so we can apply timeouts and avoid a single
                # huge write buffering indefinitely.
                for offset in range(0, len(listing), chunk_size):
                    target.write(listing[offset: offset + chunk_size])
                    async with atimeout(STREAM_WRITE_TIMEOUT_S):
                        await target.drain()
                return len(listing)
            else:
                target.write(listing)
                return len(listing)

        filename = f"{resolved_mode.name.lower()}.bin"
        filepath = self._prebake_dir / filename

        if not filepath.exists():
            raise FileNotFoundError(
                f"No prebaked listing found for share mode {resolved_mode}"
            )

        total_bytes = 0
        with filepath.open("rb") as fp:
            if isinstance(target, asyncio.StreamWriter):
                while True:
                    chunk = fp.read(chunk_size)
                    if not chunk:
                        break
                    target.write(chunk)
                    total_bytes += len(chunk)
                    async with atimeout(STREAM_WRITE_TIMEOUT_S):
                        await target.drain()
            else:
                while True:
                    chunk = fp.read(chunk_size)
                    if not chunk:
                        break
                    target.write(chunk)
                    total_bytes += len(chunk)

        return total_bytes

    async def is_privileged_user(self, username: str) -> bool:
        return any(
            username in shared_dir.users for shared_dir in self._shared_directories
        )

    def set_shared_directories(self, shared_directories: list[SharedDirectory]) -> None:
        self._shared_directories = shared_directories
        self.rebuild_term_map()

    def rebuild_term_map(self):
        self._logger.info("rebuilding term map")
        self._term_map = {}
        for shared_directory in self._shared_directories:
            self.build_term_map(shared_directory)

    def read_cache(self):
        """Reads the share index from cache if available"""
        if self.cache is None:
            self._logger.info("no cache configured, skipping reading share index")
            return

        cached_directories = self.cache.read()
        if cached_directories is None:
            self._logger.info("no cached share index found")
            return

        self._logger.info(
            "loaded %d shared directories from cache",
            len(cached_directories),
        )
        self.set_shared_directories(cached_directories)

    def write_cache(self):
        """Writes the share index to cache if configured"""
        if self.cache is None:
            self._logger.info("no cache configured, skipping writing share index")
            return

        self.cache.write(self._shared_directories)
        self._logger.info(
            "wrote %d shared directories to cache",
            len(self._shared_directories),
        )

    def build_term_map(self, shared_directory: SharedDirectory):
        """Builds a list of valid terms for the given shared directory"""
        for item in shared_directory.items:
            self._add_item_to_term_map(item)
        self._logger.debug("term map contains %d terms", len(self._term_map))

    def _add_item_to_term_map(self, item: SharedItem):
        path = (item.subdir + "\\" + item.filename).lower()
        terms = re.split(_QUERY_CLEAN_PATTERN, path)
        for term in terms:
            if not term:
                continue

            if term not in self._term_map:
                self._term_map[term] = WeakSet()
            self._term_map[term].add(item)

    def cleanup_term_map(self):
        self._term_map = {
            term: values for term, values in self._term_map.items() if len(values) > 0
        }

    def _is_directory_locked(
        self, directory: SharedDirectory, username: str | None = None
    ) -> bool:
        """Checks if the shared directory is locked for the given ``username``"""
        if directory.share_mode == DirectoryShareMode.FRIENDS:
            return username not in self._settings.users.friends
        elif directory.share_mode == DirectoryShareMode.USERS:
            return username not in directory.users
        return False

    def _is_item_locked(self, item: SharedItem, username: str | None = None) -> bool:
        """Checks if the shared item is locked for the given ``username``"""
        return self._is_directory_locked(item.shared_directory, username)

    def _get_parent_directories(
        self, shared_directory: SharedDirectory
    ) -> list[SharedDirectory]:
        """Returns a list of parent shared directories. The parent directories
        will be sorted by length of the absolute path (longest last)
        """
        parent_dirs = [
            directory
            for directory in self._shared_directories
            if directory != shared_directory
            and directory.is_parent_of(shared_directory)
        ]
        return sorted(parent_dirs, key=lambda d: len(d.absolute_path))

    async def update_item_attributes(self, items: list[SharedItem]) -> None:
        # In-memory index: items are mutable, attributes are already set
        pass

    async def begin_scan(self) -> None:
        self._scan_seen_items = {}

    async def begin_directory_scan(self, shared_directory: SharedDirectory) -> None:
        self._scan_seen_items[shared_directory.absolute_path] = set()

    async def persist_scanned_items_batch(
        self,
        shared_directory: SharedDirectory,
        items: list[SharedItem],
    ) -> None:
        if not items:
            return
        seen = self._scan_seen_items.setdefault(shared_directory.absolute_path, set())
        seen.update(items)

    async def persist_attribute_updates_batch(self, items: list[SharedItem]) -> None:
        # In-memory index: SharedItem objects are mutated in place.
        return None

    async def end_directory_scan(self, shared_directory: SharedDirectory) -> None:
        seen_items = self._scan_seen_items.pop(shared_directory.absolute_path, set())

        # Adds all new items to the directory items
        shared_directory.items |= seen_items

        # Remove all items from the cache that weren't in the returned items set.
        shared_directory.items -= (shared_directory.items ^ seen_items)

    async def load_data(self) -> None:
        self.read_cache()

    async def store_data(self) -> None:
        self.write_cache()

    async def on_settings_loaded(self) -> None:
        self.rebuild_term_map()

    async def persist_scanned_directory(self, shared_directory: SharedDirectory) -> None:
        await self.add_shared_directory(shared_directory)
        self.build_term_map(shared_directory)
        self.cleanup_term_map()

    async def finalize_scan(self, updated_items: list[SharedItem]) -> None:
        self.rebuild_term_map()

    async def end_scan(self) -> None:
        self._scan_seen_items = {}


