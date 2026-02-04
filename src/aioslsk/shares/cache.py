import os
import shelve
import logging
from typing import Protocol
from .model import SharedDirectory


logger = logging.getLogger(__name__)


class SharesCache(Protocol):
    """Abstract base class for storing shares"""

    def read(self) -> list[SharedDirectory]:
        ...

    def write(self, shared_directories: list[SharedDirectory]):
        ...


class SharesNullCache:

    def read(self) -> list[SharedDirectory]:  # pragma: no cover
        return []

    def write(self, shared_directories: list[SharedDirectory]):  # pragma: no cover
        pass


class SharesShelveCache:
    """Shares cache that uses the Python built-in `shelve` module"""
    DEFAULT_FILENAME = 'shares_index'

    def __init__(self, data_directory: str, filename: str = DEFAULT_FILENAME):
        self.data_directory: str = data_directory
        self.filename: str = filename

    def _get_index_path(self) -> str:
        return os.path.join(self.data_directory, self.filename)

    def read(self) -> list[SharedDirectory]:
        with shelve.open(self._get_index_path(), 'c') as db:
            directories: list[SharedDirectory] = db.get('index', list())
            dropped_paths: list[str] = []
            for directory in directories:
                new_items = set()
                for item in directory.items:
                    item.shared_directory = directory
                    # Backwards compatibility: populate size_bytes if it was
                    # not stored (i.e. is -1)
                    if item.size_bytes < 0:
                        try:
                            item.size_bytes = os.path.getsize(
                                item.get_absolute_path())
                        except OSError:
                            dropped_paths.append(item.get_absolute_path())
                            continue
                    new_items.add(item)
                directory.items = new_items

            if dropped_paths:
                logger.warning(
                    "Dropped %d legacy share cache entries with unknown/invalid filesize; "
                    "these files will be missing from shares/search results. "
                    "Fix the underlying issue (missing file/permissions/etc) and run a rescan to restore them. "
                    "First entries: %s%s",
                    len(dropped_paths),
                    ", ".join(repr(p) for p in dropped_paths[:5]),
                    "" if len(dropped_paths) <= 5 else f" (+{len(dropped_paths) - 5} more)"
                )
            return directories

    def write(self, shared_directories: list[SharedDirectory]):
        with shelve.open(self._get_index_path(), 'c') as db:
            db['index'] = shared_directories
