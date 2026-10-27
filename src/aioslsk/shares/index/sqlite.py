from __future__ import annotations

import asyncio
import json
import logging
import os.path
import sqlite3
import tempfile
import zlib
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Any, BinaryIO

import aiosqlite
from async_timeout import timeout as atimeout

from .base import STREAM_WRITE_TIMEOUT_S, ShareIndex
from ..model import SharedItem, SharedDirectory, DirectoryShareMode
from ..utils import normalize_remote_path
from ...database.sqlite import SQLiteDatabase
from ...exceptions import FileNotFoundError, FileNotSharedError, SharedDirectoryError
from ...protocol.messages import PeerSharesReply
from ...protocol.primitives import (
    Attribute,
    DirectoryData,
    FileData,
    string,
    uint8,
    uint32,
    uint64,
)
from ...search.model import SearchQuery
from ...settings import Settings


SHARE_MODES = [mode for mode in DirectoryShareMode]
PREBAKE_STREAM_CHUNK_SIZE = 262144

logger = logging.getLogger(__name__)


def reverse_string(s: str | None) -> str:
    """Implements a REVERSE(string) function for SQLite."""
    return s[::-1] if s else ""


class SQLiteShareIndex(ShareIndex):
    """ A share index backed by a SQLite database.

    Implements search via the FTS5 extension.
    """
    db: SQLiteDatabase
    _settings: Settings
    _logger: logging.Logger

    def __init__(self, db: SQLiteDatabase, settings: Settings) -> None:
        self.db = db
        self._settings = settings
        self._logger = logging.getLogger(self.__class__.__qualname__)
        self._scan_directory_ids: dict[str, int] = {}

    def validate_executor_compatibility(
        self,
        executor: Executor | None,
        executor_factory: Callable[[], Executor] | None,
    ) -> None:
        if isinstance(executor, ProcessPoolExecutor) or executor_factory is ProcessPoolExecutor:
            raise SharedDirectoryError(
                "ProcessPoolExecutor cannot be used with SQLite backend. "
                "SQLite connections cannot be pickled and passed to child processes. "
                "Use ThreadPoolExecutor instead."
            )

    async def add_shared_directory(self, shared_directory: SharedDirectory) -> None:
        await self.add_shared_directories_batch([shared_directory])

    async def add_shared_directories_batch(
        self, shared_directories: list[SharedDirectory]
    ) -> None:
        """Add multiple shared directories in a single transaction.

        This is more efficient than calling add_shared_directory multiple times
        when adding many directories at once (e.g., during scanning).
        """
        if not shared_directories:
            return

        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")
            for shared_directory in shared_directories:
                # Check if this directory is a child of an existing directory
                # If so, move items from the parent to this new child
                cursor = await conn.execute(
                    """--sql
                    SELECT id, absolute_path
                    FROM directories
                    WHERE
                        SUBSTR(
                            :child_path,
                            1,
                            LENGTH(absolute_path)
                        ) = absolute_path
                        AND SUBSTR(
                            :child_path,
                            LENGTH(absolute_path) + 1,
                            1
                        ) = '/'
                    ORDER BY LENGTH(absolute_path) DESC
                    LIMIT 1
                    """,
                    {"child_path": shared_directory.absolute_path},
                )
                parent_row = await cursor.fetchone()

                cursor = await conn.execute(
                    """--sql
                    INSERT INTO directories (directory, absolute_path, alias, share_mode)
                    VALUES (:directory, :absolute_path, :alias, :share_mode)
                    ON CONFLICT(absolute_path) DO UPDATE SET
                        directory = excluded.directory,
                        alias = excluded.alias,
                        share_mode = excluded.share_mode
                    RETURNING id
                    """,
                    self._directory_to_values(shared_directory),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise RuntimeError("Failed to upsert shared directory.")
                directory_id = row[0]

                await conn.executemany(
                    """--sql
                    INSERT INTO directory_users (directory_id, username)
                    VALUES (:directory_id, :username)
                    ON CONFLICT(directory_id, username) DO NOTHING
                    """,
                    (
                        {
                            "directory_id": directory_id,
                            "username": user,
                        }
                        for user in shared_directory.users
                    ),
                )

                await conn.executemany(
                    """--sql
                    INSERT INTO files (directory_id, subdir, filename, mtime, size_bytes, attributes)
                    VALUES (:directory_id, :subdir, :filename, :mtime, :size_bytes, :attributes)
                    ON CONFLICT(directory_id, subdir, filename) DO UPDATE SET
                        mtime = excluded.mtime,
                        size_bytes = excluded.size_bytes,
                        attributes = excluded.attributes
                    """,
                    (
                        self._item_to_values(directory_id, item)
                        for item in shared_directory.items
                    ),
                )

                # If this is a child of an existing directory, move items from parent
                if parent_row is not None:
                    parent_id = parent_row["id"]
                    parent_abs_path = parent_row["absolute_path"]
                    # Calculate relative path: e.g., parent=/Music, child=/Music/Rock -> "Rock"
                    relative_prefix = shared_directory.absolute_path[
                        len(parent_abs_path):
                    ].strip(os.sep)
                    relative_prefix = normalize_remote_path(relative_prefix)
                    relative_prefix_glob = self._escape_glob(relative_prefix) + "\\*"

                    # Update directory and subdir for files that belong to the new directory
                    await conn.execute(
                        """--sql
                        UPDATE files
                        SET
                            directory_id = :child_id,
                            subdir = CASE
                                WHEN subdir = :relative_prefix THEN ''
                                WHEN subdir GLOB :relative_prefix_glob THEN
                                    SUBSTR(subdir, LENGTH(:relative_prefix) + 2)
                                ELSE subdir -- should never happen
                            END
                        WHERE directory_id = :parent_id
                          AND (subdir = :relative_prefix
                               OR subdir GLOB :relative_prefix_glob)
                        """,
                        {
                            "parent_id": parent_id,
                            "child_id": directory_id,
                            "relative_prefix": relative_prefix,
                            "relative_prefix_glob": relative_prefix_glob,
                        },
                    )

    async def is_directory_shared(self, directory: str) -> bool:
        cursor = await self.db.execute(
            """--sql
            SELECT 1
            FROM directories
            WHERE absolute_path = :absolute_path
            LIMIT 1
            """,
            {"absolute_path": os.path.normpath(os.path.abspath(directory))},
        )
        row = await cursor.fetchone()
        return row is not None

    async def get_shared_directory(self, directory: str) -> SharedDirectory | None:
        abs_path = os.path.normpath(os.path.abspath(directory))
        cursor = await self.db.execute(
            """--sql
            SELECT directory, absolute_path, alias, share_mode
            FROM directories
            WHERE absolute_path = :absolute_path
            """,
            {"absolute_path": abs_path},
        )
        row = await cursor.fetchone()
        if row is None:
            return None

        shared_directory = self._directory_from_row(row)

        # Fetch the users for this directory
        users_cursor = await self.db.execute(
            """--sql
            SELECT username
            FROM directory_users du
            JOIN directories d ON d.id = du.directory_id
            WHERE d.absolute_path = :absolute_path
            """,
            {"absolute_path": abs_path},
        )
        shared_directory.users = [u["username"] async for u in users_cursor]

        return shared_directory

    async def get_shared_directories(
        self, username: str | None = None
    ) -> tuple[list[SharedDirectory], list[SharedDirectory]]:
        cursor = await self.db.execute(
            """--sql
            SELECT
                d.directory,
                d.absolute_path,
                d.alias,
                d.share_mode,
                CASE
                    WHEN :username IS NULL THEN 0
                    ELSE (
                        (d.share_mode = 2 AND user_check.username IS NULL)
                        OR (d.share_mode = 1 AND :is_friend = 0)
                    )
                END AS is_locked,
                GROUP_CONCAT(all_users.username) AS users
            FROM directories d
            LEFT JOIN directory_users user_check
                ON d.id = user_check.directory_id
                AND user_check.username = :username
            LEFT JOIN directory_users all_users
                ON d.id = all_users.directory_id
            GROUP BY d.id
            """,
            {
                "is_friend": int(username in self._settings.users.friends),
                "username": username,
            },
        )
        locked = []
        visible = []
        async for row in cursor:
            shared_directory = self._directory_from_row(row)
            # Parse comma-separated users from GROUP_CONCAT
            users_str = row["users"]
            shared_directory.users = users_str.split(",") if users_str else []
            if row["is_locked"]:
                locked.append(shared_directory)
            else:
                visible.append(shared_directory)

        return visible, locked

    async def update_shared_directory(self, shared_directory: SharedDirectory) -> None:
        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")
            await conn.execute(
                """--sql
                UPDATE directories
                SET
                    directory = :directory,
                    alias = :alias,
                    share_mode = :share_mode
                WHERE absolute_path = :absolute_path
                """,
                self._directory_to_values(shared_directory),
            )

            await conn.execute(
                """--sql
                DELETE FROM directory_users
                WHERE directory_id = (
                    SELECT id FROM directories WHERE absolute_path = :absolute_path
                )
                """,
                {"absolute_path": shared_directory.absolute_path},
            )

            await conn.executemany(
                """--sql
                INSERT INTO directory_users (directory_id, username)
                VALUES (
                    (SELECT id FROM directories WHERE absolute_path = :absolute_path),
                    :username
                )
                ON CONFLICT(directory_id, username) DO NOTHING
                """,
                (
                    {
                        "absolute_path": shared_directory.absolute_path,
                        "username": user,
                    }
                    for user in shared_directory.users
                ),
            )

    async def delete_shared_directory(
        self, directory: str | SharedDirectory
    ) -> SharedDirectory:
        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")
            # Get the directory to be deleted
            to_delete_abs_path = (
                directory.absolute_path
                if isinstance(directory, SharedDirectory)
                else directory
            )

            cursor = await conn.execute(
                """--sql
                SELECT id, directory, absolute_path, alias, share_mode
                FROM directories
                WHERE absolute_path = :absolute_path
                """,
                {"absolute_path": to_delete_abs_path},
            )
            row = await cursor.fetchone()
            if row is None:
                raise SharedDirectoryError(
                    "attempted to remove not indexed shared directory"
                )

            to_delete_id = row["id"]

            # Find parent directory (closest shared directory that contains this one)
            # A parent must have absolute_path that is a prefix of to_delete_abs_path
            # and must be different from the deleted directory
            cursor = await conn.execute(
                """--sql
                SELECT id, absolute_path
                FROM directories
                WHERE
                    SUBSTR(
                        :deleted_path,
                        1,
                        LENGTH(absolute_path)
                    ) = absolute_path
                    AND SUBSTR(
                        :deleted_path,
                        LENGTH(absolute_path) + 1,
                        1
                    ) = '/'
                ORDER BY LENGTH(absolute_path) DESC
                LIMIT 1
                """,
                {
                    "deleted_path": to_delete_abs_path,
                },
            )
            parent_row = await cursor.fetchone()

            if parent_row is not None:
                parent_id = parent_row["id"]
                parent_abs_path = parent_row["absolute_path"]

                # Calculate the relative path from parent to deleted directory
                # E.g., parent=/home/user/Music, deleted=/home/user/Music/Rock
                # relative_prefix = "Rock"
                relative_prefix = to_delete_abs_path[len(parent_abs_path):].strip(os.sep)
                relative_prefix = normalize_remote_path(relative_prefix)

                # Move files from deleted directory to parent directory
                # Update subdir to include the relative prefix
                await conn.execute(
                    """--sql
                    UPDATE files
                    SET
                        directory_id = :parent_id,
                        subdir = CASE
                            WHEN subdir = '' THEN :relative_prefix
                            ELSE :relative_prefix || '\\' || subdir
                        END
                    WHERE directory_id = :deleted_id
                    """,
                    {
                        "parent_id": parent_id,
                        "deleted_id": to_delete_id,
                        "relative_prefix": relative_prefix,
                    },
                )

            # Now delete the directory (associated files are either moved or will be cascade deleted)
            await conn.execute(
                """--sql
                DELETE FROM directories
                WHERE id = :deleted_id
                """,
                {"deleted_id": to_delete_id},
            )

            return SharedDirectory(
                directory=row["directory"],
                absolute_path=row["absolute_path"],
                alias=row["alias"],
                share_mode=SHARE_MODES[row["share_mode"]],
            )

    async def get_stats(self) -> tuple[int, int]:
        cursor = await self.db.execute(
            """--sql
            SELECT
                (
                    SELECT COUNT(*)
                    FROM (SELECT DISTINCT directory_id, subdir FROM files)
                ) AS num_dirs,
                COUNT(*) AS num_files
            FROM files
            """
        )
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("Failed to fetch stats from database.")
        return row['num_dirs'], row['num_files']

    async def get_shared_item(
        self, remote_path: str, username: str | None = None
    ) -> SharedItem | None:
        # Parse @@alias\subdir\filename format
        if not remote_path.startswith('@@'):
            return None

        path_without_prefix = remote_path[2:]  # Remove @@

        # Split into alias and file_path (subdir\filename)
        first_sep = path_without_prefix.find('\\')
        if first_sep == -1:
            return None

        alias = path_without_prefix[:first_sep]
        file_path = path_without_prefix[first_sep + 1:]  # subdir\filename

        # The database's remote_path column is computed as subdir || '\' || filename
        # For root-level files (empty subdir), this results in '\filename'
        # For files in subdirs, it's 'subdir\filename' (no leading backslash)
        # The incoming file_path is already in the format we need for subdirs
        cursor = await self.db.execute(
            """--sql
            SELECT
                directory,
                absolute_path,
                alias,
                share_mode,
                subdir,
                filename,
                mtime,
                size_bytes,
                attributes,
                (
                    CASE
                        WHEN :username IS NULL THEN 0
                        WHEN (d.share_mode = 2 AND du.username IS NULL) THEN 1
                        WHEN (d.share_mode = 1 AND :is_friend = 0) THEN 1
                        ELSE 0
                    END
                    ) AS is_locked
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN directory_users du
                ON d.id = du.directory_id
                AND du.username = :username
            WHERE d.alias = :alias
              AND (f.remote_path = :file_path OR f.remote_path = ('\\' || :file_path))
            LIMIT 1
            """,
            {
                "alias": alias,
                "file_path": file_path,
                "username": username,
                "is_friend": int(username is not None and username in self._settings.users.friends)
            },
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        if row["is_locked"]:
            raise FileNotSharedError(f"file is not shared to user {username}")
        shared_directory = self._directory_from_row(row)
        return self._item_from_row(row, shared_directory)

    async def get_shared_items_for_user(
        self, username: str | None = None
    ) -> tuple[list[DirectoryData], list[DirectoryData]]:
        cursor = await self.db.execute(
            """--sql
            WITH directory_info AS (
              SELECT
                  d.id,
                  d.directory,
                  d.alias,
                  CASE
                    WHEN :username IS NULL THEN 0
                    WHEN (d.share_mode = 2 AND du.username IS NULL) THEN 1
                    WHEN (d.share_mode = 1 AND :is_friend = 0) THEN 1
                    ELSE 0
                  END AS is_locked
              FROM directories d
              LEFT JOIN directory_users du
                ON d.id = du.directory_id
              AND du.username = :username
            )
            SELECT
              di.id AS directory_id,
              di.directory,
              di.alias,
              di.is_locked AS dir_is_locked,
              f.filename,
              f.subdir,
              f.size_bytes,
              f.attributes
            FROM directory_info di
            LEFT JOIN files f
              ON f.directory_id = di.id
            ORDER BY di.id, f.subdir, f.filename;
            """,
            {
                "username": username,
                "is_friend": int(username in self._settings.users.friends),
            },
        )
        visible_dirs: list[DirectoryData] = []
        locked_dirs: list[DirectoryData] = []

        current_id = None
        dirs: dict[tuple[str, ...], list[FileData]] = {}
        target_dirs: list[DirectoryData] | None = None

        async for row in cursor:
            # Start a new directory group?
            if row["directory_id"] != current_id:
                # flush previous group
                if current_id is not None and target_dirs is not None:
                    for dir_path, files in dirs.items():
                        target_dirs.append(DirectoryData(name="\\".join(dir_path), files=files))

                current_id = row["directory_id"]
                alias = row["alias"]
                target_dirs = locked_dirs if row["dir_is_locked"] else visible_dirs

                dirs = {}
                root = (f"@@{alias}",)
                dirs[root] = []

            # Directory with no files (LEFT JOIN yields NULL filename)
            if row["filename"] is None:
                continue

            alias = row["alias"]
            subdir_parts = (row["subdir"] or "").split("\\") if row["subdir"] else []
            dir_tuple = (f"@@{alias}", *subdir_parts)

            # ensure we output a parent directory entry for each level of the
            # directory tree, even if it doesn't have files directly in it.
            for i in range(len(dir_tuple)):
                dirs.setdefault(dir_tuple[: i + 1], [])

            dirs[dir_tuple].append(
                FileData(
                    unknown=1,
                    filename=row["filename"],
                    filesize=row["size_bytes"],
                    extension=os.path.splitext(row["filename"])[-1][1:],
                    attributes=[
                        Attribute(*map(int, a.split(":")))
                        for a in (row["attributes"] or "").split("|")
                        if a
                    ],
                )
            )

        # flush last group
        if current_id is not None and target_dirs is not None:
            for dir_path, files in dirs.items():
                target_dirs.append(DirectoryData(name="\\".join(dir_path), files=files))

        visible_dirs.sort(key=lambda d: d.name)
        locked_dirs.sort(key=lambda d: d.name)

        return visible_dirs, locked_dirs

    async def get_directory_files(self, remote_directory: str) -> DirectoryData | None:
        normalized_dir = normalize_remote_path(remote_directory)

        if not normalized_dir.startswith('@@'):
            return None

        path_without_prefix = normalized_dir[2:]

        first_sep = path_without_prefix.find('\\')
        if first_sep == -1:
            # Just @@alias with no subdirectory
            alias = path_without_prefix
            dir_path = ''
        else:
            alias = path_without_prefix[:first_sep]
            dir_path = path_without_prefix[first_sep + 1:]
        dir_path_glob = self._escape_glob(dir_path) + '\\*'

        # To differentiate between a non-existent directory and an existing
        # empty directory, we first check if any file exists with this directory
        # as a prefix (or if the alias exists for root-level directories). If
        # not, we return None to indicate the directory doesn't exist. If it
        # does exist, we then query for files directly in this directory to
        # return the DirectoryData (which may have an empty files list if there
        # are no files directly in this directory).
        if dir_path == "":
            # Root directory should be considered shared if the alias exists,
            # even when it currently has no files.
            cursor = await self.db.execute(
                """--sql
                SELECT 1
                FROM directories d
                WHERE d.alias = :alias
                LIMIT 1
                """,
                {"alias": alias},
            )
            row = await cursor.fetchone()
            if row is None:
                return None
        else:
            cursor = await self.db.execute(
                """--sql
                SELECT 1
                FROM directories d
                JOIN files f ON f.directory_id = d.id
                WHERE d.alias = :alias
                  AND (
                      f.subdir = :dir_path
                      OR f.subdir GLOB :dir_path_glob
                  )
                LIMIT 1
                """,
                {"alias": alias, "dir_path": dir_path, "dir_path_glob": dir_path_glob},
            )
            row = await cursor.fetchone()
            if row is None:
                return None

        # Now get only files directly in this directory (not subdirectories)
        # FIXME: Do we really need the json_group array here?
        cursor = await self.db.execute(
            """--sql
            SELECT
                f.filename,
                f.size_bytes,
                f.attributes
            FROM directories d
            JOIN files f ON f.directory_id = d.id
            WHERE d.alias = :alias
              AND f.subdir = :dir_path
            """,
            {"alias": alias, "dir_path": dir_path},
        )
        files: list[FileData] = []
        async for row in cursor:
            files.append(
                FileData(
                    unknown=1,
                    filename=row["filename"],
                    filesize=row["size_bytes"],
                    extension=os.path.splitext(row["filename"])[-1][1:],
                    attributes=[
                        Attribute(*map(int, attr.split(":")))
                        for attr in (row["attributes"] or "").split("|")
                        if attr
                    ],
                )
            )
        return DirectoryData(
            name=normalized_dir,
            files=files,
        )

    async def query(
        self,
        query: str | SearchQuery,
        username: str | None = None,
        excluded_search_phrases: list[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[SharedItem], list[SharedItem]]:
        if not isinstance(query, SearchQuery):
            parsed_query = SearchQuery.parse(query)
        else:
            parsed_query = query
            query = query.query

        if not parsed_query.has_inclusion_terms():
            return [], []

        # Use settings-based limit if not explicitly provided
        if limit is None:
            limit = self._settings.searches.receive.max_results

        # Excluded phrases are added as additional trailing exclusion phrase
        # queries to the search query
        if excluded_search_phrases is not None:
            for phrase in excluded_search_phrases:
                parsed_query.exclude_terms.add(f'NOT "{phrase.lower()}"')

        fts_query = self._build_query(parsed_query)
        if fts_query is None:
            return [], []

        fts_batch = max(limit * 2, 1)
        all_directories: dict[int, SharedDirectory] = {}
        allowed_items: list[SharedItem] = []
        locked_items: list[SharedItem] = []

        # Post-filter results using regex matching (like memory implementation)
        # This ensures exact term matching with word boundaries, FTS5 over-fetches
        # due to differences in tokenization and lack of true regex support.
        excl_phrases = excluded_search_phrases or []

        # Due to the way we're doing post-filtering and the fact that FTS5
        # doesn't guarantee order when using OR queries, we have to page through
        # results until we have enough allowed items to satisfy the limit. We
        # can't just fetch limit*2 and post-filter because we might end up with
        # too few allowed items after filtering.
        while True:
            cursor = await self.db.execute(
                """--sql
                -- Fetch matching rowids with pagination, without relevance scoring.
                WITH matched_files AS (
                    SELECT rowid
                    FROM files_fts
                    WHERE files_fts MATCH :fts_query
                    LIMIT :limit
                    OFFSET :offset
                )
                -- ... which we then join back to the main files and directories
                -- tables to get full info and apply locking logic
                SELECT
                    f.id,
                    f.subdir,
                    f.filename,
                    f.mtime,
                    f.size_bytes,
                    f.attributes,
                    d.id AS dir_id,
                    d.directory,
                    d.absolute_path,
                    d.alias,
                    d.share_mode,
                    CASE
                        WHEN :username IS NULL THEN 0
                        WHEN d.share_mode = 2 AND NOT EXISTS (
                            SELECT 1 FROM directory_users du
                            WHERE du.directory_id = d.id AND du.username = :username
                        ) THEN 1
                        WHEN d.share_mode = 1 AND :is_friend = 0 THEN 1
                        ELSE 0
                    END AS locked
                FROM matched_files mf
                JOIN files f ON f.id = mf.rowid
                JOIN directories d ON d.id = f.directory_id
                """,
                {
                    "fts_query": fts_query,
                    "username": username,
                    "is_friend": int(username in self._settings.users.friends)
                    if username
                    else 0,
                    "limit": fts_batch,
                    "offset": offset,
                },
            )
            rows = list(await cursor.fetchall())
            if not rows:
                break

            for row in rows:
                dir_id = row["dir_id"]
                if (shared_directory := all_directories.get(dir_id)) is None:
                    shared_directory = self._directory_from_row(row)
                    all_directories[dir_id] = shared_directory

                # Build query path for regex matching
                # Strip trailing backslash from subdir to avoid double-backslash
                clean_subdir = row["subdir"].rstrip('\\/')
                filename = row["filename"]
                query_path = f"{clean_subdir}\\{filename}" if clean_subdir else filename

                # Apply regex matching to ensure exact term matches
                # Fresh iterator each time to work around closure capture bug
                if not all(matcher(query_path) for matcher in parsed_query.matchers_iter()):
                    continue

                # Check excluded search phrases
                if any(excl_phrase in query_path.lower() for excl_phrase in excl_phrases):
                    continue

                shared_item = self._item_from_row(row, shared_directory)

                shared_directory.items.add(shared_item)

                if row["locked"]:
                    locked_items.append(shared_item)
                else:
                    allowed_items.append(shared_item)

                # Respect max results limit
                if len(allowed_items) + len(locked_items) >= limit:
                    break

            # Reached user-defined limit, stop fetching more results
            if len(allowed_items) + len(locked_items) >= limit:
                break

            # No more results from FTS query, stop
            if len(rows) < fts_batch:
                break

            offset += fts_batch

        return allowed_items, locked_items

    async def rebuild_prebaked_listing(
        self, mode: DirectoryShareMode, username: str | None
    ) -> tuple[int, int]:
        # Counts, serialized rows and publication must describe one snapshot.
        # A sharing change afterwards invalidates this listing in its transaction.
        async with self.db.transaction() as conn:
            return await self._rebuild_prebaked_listing(conn, mode, username)

    async def _rebuild_prebaked_listing(
        self, conn: aiosqlite.Connection, mode: DirectoryShareMode, username: str | None
    ) -> tuple[int, int]:
        if mode == DirectoryShareMode.USERS:
            raise ValueError("Cannot get prebaked listing for USERS share mode")

        visible_count = await self._count_prebaked_directories(
            username=username, locked=False
        )
        locked_count = await self._count_prebaked_directories(
            username=username, locked=True
        )

        uncompressed_body = bytearray()
        compressed_body = bytearray()
        compressor = zlib.compressobj()

        def flush_uncompressed(force: bool = False) -> None:
            if not uncompressed_body:
                return
            if not force and len(uncompressed_body) < PREBAKE_STREAM_CHUNK_SIZE:
                return
            compressed_chunk = compressor.compress(uncompressed_body)
            if compressed_chunk:
                compressed_body.extend(compressed_chunk)
            uncompressed_body.clear()

        await self._serialize_prebaked_group_into(
            username=username,
            locked=False,
            directory_count=visible_count,
            out=uncompressed_body,
            flush=flush_uncompressed,
        )
        uint32(0).serialize_into(uncompressed_body)
        await self._serialize_prebaked_group_into(
            username=username,
            locked=True,
            directory_count=locked_count,
            out=uncompressed_body,
            flush=flush_uncompressed,
        )
        flush_uncompressed(force=True)
        compressed_tail = compressor.flush()
        if compressed_tail:
            compressed_body.extend(compressed_tail)

        message_id = PeerSharesReply.Request.MESSAGE_ID.serialize()
        serialized = bytearray()
        uint32(len(message_id) + len(compressed_body)).serialize_into(serialized)
        serialized.extend(message_id)
        serialized.extend(compressed_body)
        await self._write_prebaked_listing_bytes(conn, mode, bytes(serialized))
        return visible_count, locked_count

    async def _execute_prebaked_file_rows_query(
        self, username: str | None, locked: bool
    ) -> aiosqlite.Cursor:
        return await self.db.connection.execute(
            """--sql
            WITH directory_info AS (
                SELECT
                    d.id,
                    d.alias,
                    (
                        CASE
                            WHEN :username IS NULL THEN 0
                            WHEN (d.share_mode = 2 AND du.username IS NULL) THEN 1
                            WHEN (d.share_mode = 1 AND :is_friend = 0) THEN 1
                            ELSE 0
                        END
                    ) AS is_locked
                FROM directories d
                LEFT JOIN directory_users du
                    ON d.id = du.directory_id
                    AND du.username = :username
            ),
            file_counts AS (
                SELECT
                    di.alias,
                    di.id AS directory_id,
                    f.subdir,
                    COUNT(*) AS file_count
                FROM directory_info di
                JOIN files f ON f.directory_id = di.id
                WHERE di.is_locked = :locked_flag
                GROUP BY di.alias, di.id, f.subdir
            ),
            file_rows AS (
                SELECT
                    fc.alias,
                    f.subdir,
                    f.filename,
                    f.size_bytes,
                    f.attributes,
                    fc.file_count
                FROM file_counts fc
                JOIN files f
                    ON f.directory_id = fc.directory_id
                    AND f.subdir = fc.subdir
            ),
            empty_roots AS (
                SELECT
                    di.alias,
                    '' AS subdir,
                    NULL AS filename,
                    NULL AS size_bytes,
                    NULL AS attributes,
                    0 AS file_count
                FROM directory_info di
                WHERE di.is_locked = :locked_flag
                  AND NOT EXISTS (
                      SELECT 1 FROM files f WHERE f.directory_id = di.id
                  )
            )
            SELECT
                alias,
                subdir,
                filename,
                size_bytes,
                attributes,
                file_count
            FROM (
                SELECT * FROM file_rows
                UNION ALL
                SELECT * FROM empty_roots
            )
            -- Sort by path components, not the raw path. NUL is impossible in
            -- filenames and sorts before every sibling character.
            ORDER BY alias, REPLACE(subdir, CHAR(92), CHAR(0)), filename
            """,
            {
                "username": username,
                "is_friend": int(username in self._settings.users.friends)
                if username
                else 0,
                "locked_flag": int(locked),
            },
        )

    async def _execute_prebaked_leaf_rows_query(
        self, username: str | None, locked: bool
    ) -> aiosqlite.Cursor:
        return await self.db.connection.execute(
            """--sql
            WITH directory_info AS (
                SELECT
                    d.id,
                    d.alias,
                    (
                        CASE
                            WHEN :username IS NULL THEN 0
                            WHEN (d.share_mode = 2 AND du.username IS NULL) THEN 1
                            WHEN (d.share_mode = 1 AND :is_friend = 0) THEN 1
                            ELSE 0
                        END
                    ) AS is_locked
                FROM directories d
                LEFT JOIN directory_users du
                    ON d.id = du.directory_id
                    AND du.username = :username
            ),
            leaf_dirs AS (
                SELECT
                    di.alias AS alias,
                    f.subdir AS subdir
                FROM directory_info di
                JOIN files f ON f.directory_id = di.id
                WHERE di.is_locked = :locked_flag
                GROUP BY di.alias, f.subdir
            ),
            empty_roots AS (
                SELECT
                    di.alias AS alias,
                    '' AS subdir
                FROM directory_info di
                WHERE di.is_locked = :locked_flag
                  AND NOT EXISTS (
                      SELECT 1 FROM files f WHERE f.directory_id = di.id
                  )
            )
            SELECT alias, subdir
            FROM (
                SELECT * FROM leaf_dirs
                UNION ALL
                SELECT * FROM empty_roots
            )
            ORDER BY alias, REPLACE(subdir, CHAR(92), CHAR(0))
            """,
            {
                "username": username,
                "is_friend": int(username in self._settings.users.friends)
                if username
                else 0,
                "locked_flag": int(locked),
            },
        )

    async def _count_prebaked_directories(
        self, username: str | None, locked: bool
    ) -> int:
        cursor = await self._execute_prebaked_leaf_rows_query(
            username=username, locked=locked
        )
        previous_parts: tuple[str, ...] | None = None
        count = 0

        async for row in cursor:
            leaf = self._prebaked_leaf_directory_name(row["alias"], row["subdir"])

            parts = tuple(leaf.split("\\"))
            if previous_parts is None:
                count += len(parts)
            else:
                common = self._common_prefix_len(previous_parts, parts)
                count += len(parts) - common

            previous_parts = parts

        return count

    async def _serialize_prebaked_group_into(
        self,
        username: str | None,
        locked: bool,
        directory_count: int,
        out: bytearray,
        flush: Callable[[bool], None],
    ) -> None:
        uint32(directory_count).serialize_into(out)
        flush(False)

        cursor = await self._execute_prebaked_file_rows_query(
            username=username, locked=locked
        )
        previous_leaf: str | None = None
        previous_parts: tuple[str, ...] | None = None

        async for row in cursor:
            alias = row["alias"]
            subdir = row["subdir"]
            filename = row["filename"]
            leaf = self._prebaked_leaf_directory_name(alias, subdir)

            if leaf != previous_leaf:
                parts = tuple(leaf.split("\\"))
                common = self._common_prefix_len(previous_parts, parts)
                leaf_file_count = int(row["file_count"]) if filename is not None else 0

                for idx in range(common, len(parts)):
                    dir_name = "\\".join(parts[: idx + 1])
                    file_count = leaf_file_count if idx == len(parts) - 1 else 0
                    self._serialize_directory_header_into(out, dir_name, file_count)
                    flush(False)

                previous_leaf = leaf
                previous_parts = parts

            if filename is None:
                continue

            self._serialize_file_row_into(
                out,
                filename=filename,
                size_bytes=int(row["size_bytes"]),
                attributes=row["attributes"],
            )
            flush(False)

    @staticmethod
    def _prebaked_leaf_directory_name(alias: str, subdir: str) -> str:
        if subdir:
            return normalize_remote_path(f"@@{alias}/{subdir}")
        return normalize_remote_path(f"@@{alias}")

    @staticmethod
    def _common_prefix_len(
        left: tuple[str, ...] | None, right: tuple[str, ...]
    ) -> int:
        if left is None:
            return 0
        idx = 0
        max_idx = min(len(left), len(right))
        while idx < max_idx and left[idx] == right[idx]:
            idx += 1
        return idx

    @staticmethod
    def _serialize_directory_header_into(
        out: bytearray, directory_name: str, file_count: int
    ) -> None:
        string(directory_name).serialize_into(out)
        uint32(file_count).serialize_into(out)

    @staticmethod
    def _serialize_file_row_into(
        out: bytearray,
        filename: str,
        size_bytes: int,
        attributes: str | None,
    ) -> None:
        uint8(1).serialize_into(out)
        string(filename).serialize_into(out)
        uint64(size_bytes).serialize_into(out)
        string(os.path.splitext(filename)[-1][1:]).serialize_into(out)
        SQLiteShareIndex._serialize_attributes_into(out, attributes)

    @staticmethod
    def _serialize_attributes_into(out: bytearray, attributes: str | None) -> None:
        if not attributes:
            uint32(0).serialize_into(out)
            return

        entries = [entry for entry in attributes.split("|") if entry]
        uint32(len(entries)).serialize_into(out)
        for entry in entries:
            key, value = entry.split(":", 1)
            uint32(int(key)).serialize_into(out)
            uint32(int(value)).serialize_into(out)

    async def invalidate_prebaked_listings(self) -> None:
        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")

    async def on_settings_loaded(self) -> None:
        await self.invalidate_prebaked_listings()

    async def save_prebaked_listing(
        self,
        mode: DirectoryShareMode,
        visible: list[DirectoryData],
        locked: list[DirectoryData],
    ) -> None:
        if mode == DirectoryShareMode.USERS:
            raise ValueError("Cannot get prebaked listing for USERS share mode")
        reply_data = PeerSharesReply.Request(
            directories=visible,
            locked_directories=locked,
        ).serialize(compress=True)
        await self._save_prebaked_listing_bytes(mode, reply_data)

    async def _save_prebaked_listing_bytes(
        self, mode: DirectoryShareMode, listing: bytes
    ) -> None:
        async with self.db.transaction() as conn:
            await self._write_prebaked_listing_bytes(conn, mode, listing)

    async def _write_prebaked_listing_bytes(
        self, conn: aiosqlite.Connection, mode: DirectoryShareMode, listing: bytes
    ) -> None:
        cursor = await conn.execute(
            """--sql
            INSERT INTO prebaked_listings (share_mode, listing)
            VALUES (:share_mode, ZEROBLOB(:listing_size))
            ON CONFLICT(share_mode) DO UPDATE SET
                listing = excluded.listing,
                last_updated = CURRENT_TIMESTAMP
            RETURNING rowid
            """,
            {
                "share_mode": SHARE_MODES.index(mode),
                "listing_size": len(listing),
            },
        )
        if (row := await cursor.fetchone()) is None:
            raise RuntimeError("Failed to upsert prebaked listing.")

        rowid = row[0]
        await conn._execute(
            write_blob,
            conn._conn,
            table="prebaked_listings",
            column="listing",
            row=rowid,
            data=listing,
        )

    def _resolve_prebaked_mode(self, mode: DirectoryShareMode) -> DirectoryShareMode:
        if mode == DirectoryShareMode.FRIENDS and not self._settings.users.friends:
            return DirectoryShareMode.EVERYONE
        return mode

    async def _get_prebaked_rowid(
        self, conn: aiosqlite.Connection, mode: DirectoryShareMode
    ) -> int:
        if mode == DirectoryShareMode.USERS:
            raise ValueError("Cannot get prebaked listing for USERS share mode")
        resolved_mode = self._resolve_prebaked_mode(mode)
        cursor = await conn.execute(
            "SELECT rowid FROM prebaked_listings WHERE share_mode = ?",
            (SHARE_MODES.index(resolved_mode),),
        )
        row = await cursor.fetchone()
        if row is None:
            raise FileNotFoundError(
                f"No prebaked listing found for share mode {resolved_mode}"
            )
        return row[0]

    async def get_prebaked_listing(self, mode: DirectoryShareMode) -> bytes:
        async with self.db.transaction() as conn:
            rowid = await self._get_prebaked_rowid(conn, mode)
            return await conn._execute(
                read_blob, conn._conn,
                table="prebaked_listings", column="listing", row=rowid,
            )

    async def copy_prebaked_listing_to(
        self,
        target: BinaryIO | asyncio.StreamWriter,
        mode: DirectoryShareMode,
        chunk_size: int = 131072,
    ) -> int:
        if not isinstance(target, asyncio.StreamWriter):
            return await self._copy_prebaked_listing_to_file(target, mode, chunk_size)

        # Stage a stable, bounded-memory snapshot before touching the network.
        # Never hold the shared DB worker (or transaction) across peer drains.
        with tempfile.TemporaryFile() as spool:
            await self._copy_prebaked_listing_to_file(spool, mode, chunk_size)
            spool.seek(0)
            total_bytes = 0
            while chunk := await asyncio.to_thread(spool.read, chunk_size):
                target.write(chunk)
                async with atimeout(STREAM_WRITE_TIMEOUT_S):
                    await target.drain()
                total_bytes += len(chunk)
            return total_bytes

    async def _copy_prebaked_listing_to_file(
        self, target: BinaryIO, mode: DirectoryShareMode, chunk_size: int
    ) -> int:
        async with self.db.transaction() as conn:
            rowid = await self._get_prebaked_rowid(conn, mode)
            return await conn._execute(
                copy_blob, target, conn._conn,
                table="prebaked_listings", column="listing", row=rowid,
                chunk_size=chunk_size,
            )

    async def is_privileged_user(self, username: str) -> bool:
        cursor = await self.db.execute(
            """--sql
            SELECT 1
            FROM directories d
            JOIN directory_users du ON d.id = du.directory_id
            WHERE du.username = :username
            LIMIT 1
            """,
            {"username": username},
        )
        row = await cursor.fetchone()
        return row is not None

    async def update_item_attributes(self, items: list[SharedItem]) -> None:
        """Update only the attributes field for a batch of items.

        This is more efficient than save_shared_items when only attributes
        need to be updated (e.g., after audio metadata extraction).

        :param items: list of SharedItem instances with attributes to update
        """
        if not items:
            return

        params = []
        skipped_count = 0
        for item in items:
            if item.attributes is None:
                continue
            try:
                # Keep attribute-only writes aligned with save_shared_items UTF-8 filtering.
                item.filename.encode("utf-8", errors="strict")
                item.subdir.encode("utf-8", errors="strict")
                item.shared_directory.absolute_path.encode("utf-8", errors="strict")
            except UnicodeEncodeError as e:
                skipped_count += 1
                logger.warning(
                    "Skipping attribute update for file with invalid UTF-8 encoding: "
                    "%s/%s/%s - %s",
                    item.shared_directory.absolute_path,
                    item.subdir,
                    item.filename,
                    e,
                )
                continue

            params.append(
                {
                    "dir_path": item.shared_directory.absolute_path,
                    "subdir": normalize_remote_path(item.subdir),
                    "filename": item.filename,
                    "attributes": "|".join(
                        f"{attr[0]}:{attr[1]}" for attr in (item.attributes or [])
                    ),
                }
            )

        if skipped_count > 0:
            logger.warning(
                "Skipped %d attribute updates with invalid UTF-8 encoding",
                skipped_count,
            )

        if not params:
            return

        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")
            await conn.executemany(
                """--sql
                UPDATE files
                SET attributes = :attributes
                WHERE directory_id = (
                    SELECT id FROM directories WHERE absolute_path = :dir_path
                )
                AND subdir = :subdir
                AND filename = :filename
                """,
                params,
            )

    async def _reuse_unchanged_attributes(
        self, conn: aiosqlite.Connection, items: list[SharedItem]
    ) -> None:
        candidates = {
            (
                self._scan_directory_ids[item.shared_directory.absolute_path],
                normalize_remote_path(item.subdir),
                item.filename,
            ): item
            for item in items if item.attributes is None
        }
        if not candidates:
            return

        # JSON1 is already required by the database. One bound batch avoids both
        # per-file queries and SQLite's older 999-variable parameter limit.
        cursor = await conn.execute(
            """--sql
            SELECT f.directory_id, f.subdir, f.filename, f.mtime, f.size_bytes, f.attributes
            FROM json_each(?) incoming
            JOIN files f
              ON f.directory_id = json_extract(incoming.value, '$[0]')
             AND f.subdir = json_extract(incoming.value, '$[1]')
             AND f.filename = json_extract(incoming.value, '$[2]')
            WHERE f.attributes IS NOT NULL
            """,
            (json.dumps(list(candidates)),),
        )
        for row in await cursor.fetchall():
            item = candidates[(row["directory_id"], row["subdir"], row["filename"])]
            if row["mtime"] == item.modified and row["size_bytes"] == item.size_bytes:
                item.attributes = self._item_from_row(row, item.shared_directory).attributes

    async def save_shared_items(
        self,
        items: list[SharedItem],
        record_seen: bool = False,
    ) -> None:
        # Filter out files with invalid UTF-8 in filenames or paths
        valid_files = []
        skipped_count = 0
        for item in items:
            try:
                # Test if we can encode all string fields to UTF-8
                item.filename.encode('utf-8', errors='strict')
                item.subdir.encode('utf-8', errors='strict')
                item.shared_directory.absolute_path.encode('utf-8', errors='strict')
                valid_files.append(item)
            except UnicodeEncodeError as e:
                skipped_count += 1
                logger.warning(
                    f"Skipping file with invalid UTF-8 encoding: "
                    f"{item.shared_directory.absolute_path}/{item.subdir}/{item.filename} - {e}"
                )

        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} files with invalid UTF-8 encoding")

        async with self.db.transaction() as conn:
            if record_seen:
                await self._reuse_unchanged_attributes(conn, valid_files)
            files_list = [
                {
                    "subdir": normalize_remote_path(item.subdir),
                    "filename": item.filename,
                    "mtime": item.modified,
                    "size_bytes": item.size_bytes,
                    "attributes": (
                        "|".join(f"{attr[0]}:{attr[1]}" for attr in item.attributes)
                        if item.attributes is not None else None
                    ),
                    "dir_path": item.shared_directory.absolute_path,
                    "local_path": item.get_absolute_path(),
                    "path_separator": os.sep,
                    "expected_directory_id": (
                        self._scan_directory_ids[item.shared_directory.absolute_path]
                        if record_seen else None
                    ),
                }
                for item in valid_files
            ]
            await conn.execute("DELETE FROM prebaked_listings")
            # Disable background maintenance (makes Triggers faster)
            await conn.execute(
                "INSERT INTO files_fts(files_fts, rank) VALUES('automerge', 0)"
            )
            await conn.executemany(
                """--sql
                INSERT INTO files
                    (directory_id, subdir, filename, mtime, size_bytes, attributes)
                SELECT d.id, :subdir, :filename, :mtime, :size_bytes, :attributes
                FROM directories d
                WHERE d.absolute_path = :dir_path
                  AND (:expected_directory_id IS NULL OR d.id = :expected_directory_id)
                  AND NOT EXISTS (
                    SELECT 1 FROM directories child
                    WHERE LENGTH(child.absolute_path) > LENGTH(d.absolute_path)
                      AND SUBSTR(:local_path, 1, LENGTH(child.absolute_path) + 1)
                          = child.absolute_path || :path_separator
                  )
                ON CONFLICT(directory_id, subdir, filename) DO UPDATE SET
                    mtime = excluded.mtime,
                    size_bytes = excluded.size_bytes,
                    attributes = excluded.attributes
                """,
                files_list,
            )
            await conn.execute(
                "INSERT INTO files_fts(files_fts, rank) VALUES('automerge', 4)"
            )

            if record_seen:
                await conn.executemany(
                    """--sql
                    INSERT OR IGNORE INTO scan_seen (directory_id, file_id)
                    SELECT f.directory_id, f.id
                    FROM files f
                    JOIN directories d ON d.absolute_path = :dir_path
                    WHERE f.directory_id = d.id
                      AND d.id = :expected_directory_id
                      AND f.subdir = :subdir
                      AND f.filename = :filename
                    """,
                    files_list,
                )

    async def begin_directory_scan(self, shared_directory: SharedDirectory) -> None:
        async with self.db.transaction() as conn:
            await conn.execute(
                """--sql
                CREATE TEMP TABLE IF NOT EXISTS scan_seen (
                    directory_id INTEGER NOT NULL,
                    file_id INTEGER PRIMARY KEY
                )
                """
            )
            cursor = await conn.execute(
                "SELECT id FROM directories WHERE absolute_path = ?",
                (shared_directory.absolute_path,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise SharedDirectoryError("attempted to scan an unshared directory")
            directory_id = row[0]
            await conn.execute(
                "DELETE FROM scan_seen WHERE directory_id = ?", (directory_id,)
            )
        self._scan_directory_ids[shared_directory.absolute_path] = directory_id

    async def persist_scanned_items_batch(
        self,
        shared_directory: SharedDirectory,
        items: list[SharedItem],
    ) -> None:
        if items:
            await self.save_shared_items(items, record_seen=True)

    async def persist_attribute_updates_batch(self, items: list[SharedItem]) -> None:
        await self.update_item_attributes(items)

    async def end_directory_scan(self, shared_directory: SharedDirectory) -> None:
        directory_id = self._scan_directory_ids[shared_directory.absolute_path]
        async with self.db.transaction() as conn:
            await conn.execute("DELETE FROM prebaked_listings")
            await conn.execute(
                """--sql
                DELETE FROM files
                WHERE directory_id = :directory_id
                  AND NOT EXISTS (
                    SELECT 1 FROM scan_seen s WHERE s.file_id = files.id
                  )
                """,
                {"directory_id": directory_id},
            )
            await conn.execute(
                "DELETE FROM scan_seen WHERE directory_id = ?", (directory_id,)
            )
        self._scan_directory_ids.pop(shared_directory.absolute_path)

    async def abort_directory_scan(self, shared_directory: SharedDirectory) -> None:
        directory_id = self._scan_directory_ids.pop(shared_directory.absolute_path)
        async with self.db.transaction() as conn:
            await conn.execute(
                "DELETE FROM scan_seen WHERE directory_id = ?", (directory_id,)
            )

    async def persist_scanned_directory(self, shared_directory: SharedDirectory) -> None:
        await self.begin_directory_scan(shared_directory)
        try:
            await self.persist_scanned_items_batch(
                shared_directory, list(shared_directory.items)
            )
        except BaseException:
            await self.abort_directory_scan(shared_directory)
            raise
        else:
            await self.end_directory_scan(shared_directory)

    def _quote_fts5_term(self, term: str) -> str:
        """Treat all user input as literal FTS text, never query syntax."""
        return '"' + term.replace('"', '""') + '"'

    def _build_query(
        self,
        parsed_query: SearchQuery
    ) -> str | None:
        """Builds an SQLite FTS5 query string from the given user input.

        Since FTS5 only supports suffix wildcards, but Soulseek needs
        prefix wildcards, we use reversed terms for searching, i.e.
        the terms in the FTS5 index are stored reversed, and we thus
        have to reverse the search terms as well, keeping the control
        characters at the front.

        Note: We don't include exclude terms in the FTS query because
        FTS5's tokenization may cause false exclusions. The regex post-filter
        handles exclusions correctly (same as the memory implementation).
        """
        terms = []

        for term in parsed_query.include_terms:
            reversed_term = reverse_string(term)
            terms.append(self._quote_fts5_term(reversed_term))

        for term in parsed_query.wildcard_terms:
            reversed_term = reverse_string(term)
            terms.append(f"{self._quote_fts5_term(reversed_term)}*")

        # NOTE: Exclude terms are NOT added to the FTS query.
        # The regex post-filter in `query()` handles exclusions correctly.

        if not terms:
            return None

        return " ".join(terms)

    # Helper methods to convert between database rows/value bindings and model classes
    def _directory_to_values(self, shared_directory: SharedDirectory) -> dict[str, Any]:
        return {
            "directory": shared_directory.directory,
            "absolute_path": shared_directory.absolute_path,
            "alias": shared_directory.alias,
            "share_mode": SHARE_MODES.index(shared_directory.share_mode),
        }

    def _directory_from_row(self, row: aiosqlite.Row) -> SharedDirectory:
        return SharedDirectory(
            directory=row["directory"],
            absolute_path=row["absolute_path"],
            alias=row["alias"],
            share_mode=SHARE_MODES[row["share_mode"]],
        )

    def _item_to_values(self, directory_id: int, item: SharedItem) -> dict[str, Any]:
        # Normalize the subdir to use backslashes (protocol format)
        subdir = item.subdir.replace('/', '\\') if item.subdir else ''
        return {
            "directory_id": directory_id,
            "subdir": subdir,
            "filename": item.filename,
            "mtime": item.modified,
            "size_bytes": item.size_bytes,
            "attributes": (
                "|".join(f"{attr[0]}:{attr[1]}" for attr in item.attributes)
                if item.attributes is not None else None
            ),
        }

    @staticmethod
    def _escape_glob(value: str) -> str:
        """ When querying with GLOB in SQLite, we need to escape the special characters: *, ?, [, ].

        :param value: The string to escape for GLOB queries.
        :return: The escaped string safe for GLOB queries.
        """
        escaped = []
        for ch in value:
            if ch == "[":
                escaped.append("[[]")
            elif ch == "]":
                escaped.append("[]]")
            elif ch == "*":
                escaped.append("[*]")
            elif ch == "?":
                escaped.append("[?]")
            else:
                escaped.append(ch)
        return "".join(escaped)

    def _filedata_from_row(self, row: aiosqlite.Row) -> FileData:
        file_data = FileData(
            unknown=1,
            filename=normalize_remote_path(
                f"@@{row['alias']}\\{row['subdir']}\\{row['filename']}"
            ),
            filesize=row["size_bytes"],
            extension=os.path.splitext(row["filename"])[-1][1:],
            attributes=[],
        )
        if row["attributes"]:
            for attr in row["attributes"].split("|"):
                vals = attr.split(":")
                file_data.attributes.append(Attribute(int(vals[0]), int(vals[1])))
        return file_data

    def _item_from_row(
        self, row: aiosqlite.Row, shared_directory: SharedDirectory
    ) -> SharedItem:
        subdir = row["subdir"] or ""
        # SQLite stores remote-style subdir paths (backslashes). SharedItem expects
        # a filesystem-relative path for get_absolute_path(), so normalize here.
        subdir = subdir.replace("\\", os.path.sep)
        itm = SharedItem(
            shared_directory=shared_directory,
            subdir=subdir,
            filename=row["filename"],
            modified=row["mtime"],
            size_bytes=row["size_bytes"],
        )
        if row["attributes"] is not None:
            itm.attributes = []
            for attr in row["attributes"].split("|"):
                if not attr:
                    continue
                vals = attr.split(":")
                itm.attributes.append((int(vals[0]), int(vals[1])))
        return itm


def _has_blobopen(sync_conn: sqlite3.Connection) -> bool:
    return hasattr(sync_conn, "blobopen")


def write_blob(
    sync_conn: sqlite3.Connection,
    table: str,
    column: str,
    row: int,
    data: bytes,
):
    """Helper function to write BLOB data using sqlite3's blobopen in a
    synchronous context.

    Intended to be used with aiosqlite's connection._execute method.
    """
    if _has_blobopen(sync_conn):
        try:
            with sync_conn.blobopen(
                table,
                column,
                row,
                readonly=False,
            ) as blob:
                blob.write(data)
            return
        except (AttributeError, sqlite3.OperationalError):
            # Fall back to standard UPDATE below
            pass

    sync_conn.execute(
        f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
        (data, row),
    )


def read_blob(
    sync_conn: sqlite3.Connection,
    table: str,
    column: str,
    row: int,
) -> bytes:
    """Helper function to read BLOB data using sqlite3's blobopen in a
    synchronous context.

    Intended to be used with aiosqlite's connection._execute method.
    """
    if _has_blobopen(sync_conn):
        try:
            with sync_conn.blobopen(
                table,
                column,
                row,
                readonly=True,
            ) as blob:
                return blob.read()
        except (AttributeError, sqlite3.OperationalError):
            # Fall back to standard SELECT below
            pass

    cursor = sync_conn.execute(
        f"SELECT {column} FROM {table} WHERE rowid = ?",
        (row,),
    )
    row_data = cursor.fetchone()
    if row_data is None:
        raise RuntimeError("BLOB row not found")
    return row_data[0]


def copy_blob(
    target: BinaryIO,
    sync_conn: sqlite3.Connection,
    table: str,
    column: str,
    row: int,
    chunk_size: int = 131072,
) -> int:
    """Copy to a local file on the DB worker, never to a network writer."""
    total_bytes = 0
    if _has_blobopen(sync_conn):
        try:
            with sync_conn.blobopen(table, column, row, readonly=True) as blob:
                while chunk := blob.read(chunk_size):
                    target.write(chunk)
                    total_bytes += len(chunk)
            return total_bytes
        except (AttributeError, sqlite3.OperationalError):
            pass

    data = read_blob(sync_conn, table, column, row)
    for offset in range(0, len(data), chunk_size):
        chunk = data[offset:offset + chunk_size]
        target.write(chunk)
        total_bytes += len(chunk)
    return total_bytes
