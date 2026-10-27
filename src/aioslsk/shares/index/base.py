import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import Executor
from typing import BinaryIO, Callable

from ..model import SharedDirectory, SharedItem, DirectoryShareMode
from ...protocol.messages import DirectoryData
from ...search.model import SearchQuery


STREAM_WRITE_TIMEOUT_S = 10.0


class ShareIndex(ABC):
    """
    Interface for managing the index of shared directories and files.

    This protocol defines the contract for a backend database or in-memory
    stores that track which directories are shared, as well as the files they
    contain, with metadata about them. It allows for CRUD operations on those
    entities, as well as querying and retrieving shared items based on various
    criteria.
    """

    @abstractmethod
    async def add_shared_directory(
        self,
        shared_directory: SharedDirectory,
    ) -> None:
        """
        Adds a new shared directory to the index.

        :param shared_directory: the :class:`.SharedDirectory` instance to add
        """
        ...

    @abstractmethod
    async def is_directory_shared(self, directory: str) -> bool:
        """
        Checks if the directory is a known shared directory.

        :param directory: absolute path of the directory to check, with native
                          path separators (/ on *nix, \\ on Windows)
        :return: whether the directory is shared or not
        """
        ...

    @abstractmethod
    async def get_shared_directory(self, directory: str) -> SharedDirectory | None:
        """
        Get the :class:`.SharedDirectory` instance for the given absolute path.

        Implementations are allowed to leave :field:`SharedDirectory.items` empty,
        i.e. the intent of this method is to only retrieve the metadata of the shared
        directory, not its contents.

        :param directory: absolute path of the shared directory, with native
                          path separators (/ on *nix, \\ on Windows)
        :return: the corresponding :class:`.SharedDirectory` instance or ``None``
        """
        ...

    @abstractmethod
    async def get_shared_directories(
        self, username: str | None = None
    ) -> tuple[list[SharedDirectory], list[SharedDirectory]]:
        """
        Retrieves the metadata for all shared directories, optionally filtering by
        visibility for the given ``username``.

        A directory is locked for a user, if its share mode is set to FRIENDS and the
        user is not in the friends list, or if its share mode is set to USERS and the
        user is not in the list of allowed users.

        Implementations are allowed to leave :field:`SharedDirectory.items` empty, i.e.
        the intent of this method is to only retrieve the metadata of the shared
        directories, not their contents.

        :param username: optional username to filter locked directories for
        :return: a tuple of visible and locked :class:`.SharedDirectory` instances
        """
        ...

    @abstractmethod
    async def update_shared_directory(self, shared_directory: SharedDirectory) -> None:
        """
        Updates the metadata of an existing shared directory.

        :param shared_directory: the :class:`.SharedDirectory` instance with updated
                                 metadata
        """
        ...

    @abstractmethod
    async def delete_shared_directory(
        self, directory: str | SharedDirectory
    ) -> SharedDirectory:
        """
        Removes the given shared directory and all of its items from the index.

        Implementations are free to decide whether to fill the :field:`SharedDirectory.items`
        field of the returned removed directory.

        If the removed directory is a subdirectory of another shared directory, its items
        should be moved into that parent directory.

        :param directory: absolute path or :class:`.SharedDirectory` instance to remove
        :return: the removed :class:`.SharedDirectory` instance, excluding its items.
        """
        ...

    @abstractmethod
    async def get_stats(self) -> tuple[int, int]:
        """
        Gets the total amount of shared directories and files.

        :return: tuple of directory and file count
        """
        ...

    @abstractmethod
    async def get_shared_item(
        self, remote_path: str, username: str | None = None
    ) -> SharedItem | None:
        """
        Retrieves a shared item by its remote path.

        :param remote_path: the remote path of the shared item
        :param username: optional username to check if the item is visible to
        :raise FileNotSharedError: if the item is locked for the given username
        :return: the corresponding :class:`.SharedItem` instance or ``None``
                 if no such item was found
        """
        ...

    @abstractmethod
    async def get_shared_items_for_user(
        self, username: str | None = None
    ) -> tuple[list[DirectoryData], list[DirectoryData]]:
        """
        Retrieves all visible and locked shared items across all shared directories
        for the given username.

        Each :class:`.DirectoryData` instance corresponds to a shared directory and
        contains the list of files visible or locked for the given user across the
        whole tree of that shared directory.


        :param username: the username requesting the shared items
        :return: a tuple of visible and locked directories for the requesting user
        """
        ...

    @abstractmethod
    async def get_directory_files(self, remote_directory: str) -> DirectoryData | None:
        """
        Retrieves the files in the given directory.

        Implementations must take care not to return `None` for empty directories,
        in this an empty :class:`.DirectoryData` instance with an empty `files` list
        must be returned. `None` must only be returned if the directory is not shared.

        :param remote_directory: the remote directory path to list files for
        :return: a :class:`.DirectoryData` instance containing the files in the directory,
                    or ``None`` if the directory is not shared
        """
        ...

    @abstractmethod
    async def query(
        self,
        query: str | SearchQuery,
        username: str | None = None,
        excluded_search_phrases: list[str] | None = None,
    ) -> tuple[list[SharedItem], list[SharedItem]]:
        """
        Perform a full-text search on the shared items.

        Searches are term-based, i.e. the query should be split into individual terms
        and each term is matched against the terms in the remote path of shared items.
        Tokenization into terms is performed by splitting on non-word/digit characters.

        Query strings follow the Soulseek search syntax, supporting:
        - Exclusion: dash-character gets used to exclude terms. Example: `-mp3`,
          would exclude all mp3 files
        - Wildcard: asterisk-character for wildcard searches. Example: `*oney`,
          would match `‘honey’` and `‘money’`
        - Sentence matching: double quotes would get used to keep terms together.
          Example: `"my song"` would perform an exact match for those terms.

        The following rules apply:
        - Searches are case-insensitive
        - Placement of terms is irrelevant. This also applies to exclusions:
          `-mp3 song` is the same as `song -mp3`
        - Wildcard/exclusion: placement is also irrelevant
        - Wildcard: can only be used in the beginning of the word. `some*` is
          not valid and neither is `some*thing`
        - Wildcard: doesn’t need to match a character. Query `*song.mp3` will
          match `song.mp3`
        - Wildcard: query `song *` will return something
        - Path seperators can be included (backslash and forward slash) but only
          apply to the directory part of the filename. Query `my\\cool` will
          match file `my\\cool\\band\\song.mp3`, but query `band\\song` will not
          match the same file

        :param query: the query string or :class:`.SearchQuery` instance to
                      perform the search with
        :param username: optional username to determine visibility by
        :excluded_search_phrases: optional list of phrases that should be
                                  excluded from the search results
        :return: a tuple of visible and locked :class:`.SharedItem` instances
                 matching the query
        """
        ...

    @abstractmethod
    async def invalidate_prebaked_listings(self) -> None:
        """Discard cached listings after changes to files or sharing rules."""
        ...

    @abstractmethod
    async def save_prebaked_listing(
        self,
        mode: DirectoryShareMode,
        visible: list[DirectoryData],
        locked: list[DirectoryData],
    ) -> None:
        """
        To optimize share listing replies, prebaked listings can be saved
        to the index, avoiding expensive querying and serialization on each
        directory listing request.

        This is only possible for listings requested by non-privileged users
        or users in the friend list, but not in the user list of a specific
        directory, as the view of visible/locked directories is different for
        users with directory-specific privileges.

        :param mode: the share mode the prebaked listing is for, must be
                     one of `DirectoryShareMode.EVERYONE` or
                    `DirectoryShareMode.FRIENDS`
        :param visible: list of visible :class:`.DirectoryData` instances
        :param locked: list of locked :class:`.DirectoryData` instances
        """
        ...

    async def rebuild_prebaked_listing(
        self, mode: DirectoryShareMode, username: str | None
    ) -> tuple[int, int]:
        """Rebuild and persist one prebaked listing for the given user view.

        The default implementation materializes directory data via
        :meth:`get_shared_items_for_user` and persists it via
        :meth:`save_prebaked_listing`. Backends can override this to provide a
        more efficient implementation.
        """
        visible, locked = await self.get_shared_items_for_user(username=username)
        await self.save_prebaked_listing(mode, visible=visible, locked=locked)
        return len(visible), len(locked)

    @abstractmethod
    async def get_prebaked_listing(self, mode: DirectoryShareMode) -> bytes:
        """
        Retrieve a prebaked listing for the given share mode.

        :param mode: the share mode to retrieve the prebaked listing for, must be
                     one of `DirectoryShareMode.EVERYONE` or
                     `DirectoryShareMode.FRIENDS`
        :return: serialized bytes of the prebaked listing
        """
        ...

    @abstractmethod
    async def copy_prebaked_listing_to(
        self,
        target: BinaryIO | asyncio.StreamWriter,
        mode: DirectoryShareMode,
        chunk_size: int = 131072,
    ) -> int:
        """
        Convenience method to copy a prebaked listing to a file-like object or
        a stream writer.

        Implementers are encouraged to optimize this method as best a they can,
        e.g. by using `os.sendfile` for file-like objects that are backed by
        a real file descriptor and by avoiding loading the whole listing into
        memory at once.

        :param target: the target to copy the prebaked listing to, either a
                          file-like object or an `asyncio.StreamWriter`
        :param mode: the share mode to retrieve the prebaked listing for, must be
                     one of `DirectoryShareMode.EVERYONE` or
                     `DirectoryShareMode.FRIENDS`
        :param chunk_size: size of chunks to read when copying the listing
                           to the target
        :return: total number of bytes copied to the target
        """
        ...

    @abstractmethod
    async def is_privileged_user(self, username: str) -> bool:
        """
        Check if the given username belongs to a privileged user, i.e. a user
        who is listed in the user list of at least one shared directory.

        :param username: the username to check for privileges
        :return: whether the user is privileged or not
        """
        ...

    @abstractmethod
    async def update_item_attributes(self, items: list[SharedItem]) -> None:
        """
        Update only the attributes field for a batch of items.

        This is more efficient than a full save when only attributes need to be
        updated (e.g., after audio metadata extraction).

        :param items: list of SharedItem instances with attributes to update
        """
        ...

    def validate_executor_compatibility(
        self,
        executor: Executor | None,
        executor_factory: Callable[[], Executor] | None,
    ) -> None:
        """Validate that the configured executor is compatible with this backend."""
        return None

    async def load_data(self) -> None:
        """Load backend-specific persisted runtime state."""
        return None

    async def store_data(self) -> None:
        """Store backend-specific runtime state."""
        return None

    async def on_settings_loaded(self) -> None:
        """Finalize backend state after settings have been synchronized."""
        return None

    async def begin_scan(self) -> None:
        """Prepare backend-specific state for a full scan."""
        return None

    async def begin_directory_scan(self, shared_directory: SharedDirectory) -> None:
        """Prepare backend-specific state for scanning one shared directory."""
        return None

    async def persist_scanned_items_batch(
        self,
        shared_directory: SharedDirectory,
        items: list[SharedItem],
    ) -> None:
        """Persist one scanned batch for a shared directory."""
        if not items:
            return
        shared_directory.items |= set(items)

    async def persist_attribute_updates_batch(self, items: list[SharedItem]) -> None:
        """Persist one batch of attribute updates."""
        await self.update_item_attributes(items)

    async def end_directory_scan(self, shared_directory: SharedDirectory) -> None:
        """Finalize backend-specific state for one scanned shared directory."""
        await self.persist_scanned_directory(shared_directory)

    async def abort_directory_scan(self, shared_directory: SharedDirectory) -> None:
        """Discard scan bookkeeping without purging previously indexed files."""
        return None

    async def persist_scanned_directory(self, shared_directory: SharedDirectory) -> None:
        """Persist results for one scanned shared directory."""
        await self.add_shared_directory(shared_directory)

    async def finalize_scan(self, updated_items: list[SharedItem]) -> None:
        """Persist scan-wide updates after directory/file scans completed."""
        await self.update_item_attributes(updated_items)

    async def end_scan(self) -> None:
        """Clean up backend-specific scan state."""
        return None
