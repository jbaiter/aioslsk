import asyncio
import importlib.util
import logging
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from types import ModuleType
from typing import AsyncIterator

import aiosqlite

MIGRATIONS_PATH = Path(__file__).parent / "migrations"
PRAGMAS = [
    "foreign_keys = ON",     # Enforce foreign key constraints
    "journal_mode = WAL",    # Use Write-Ahead Logging for better concurrency and crash resilience
    "synchronous = NORMAL",  # Balance between performance and durability for WAL mode
    "temp_store = MEMORY",   # Store temporary tables in memory for faster access
    "cache_size = -128000",  # 128MB cache
]

logger = logging.getLogger(__name__)


def reverse_string(s: str | None) -> str:
    """Implements a REVERSE(string) function for SQLite"""
    return s[::-1] if s else ""


class SQLiteDatabase:
    """ Convenience wrapper around an aiosqlite connection.

    Adds support for running migrations on initial connection and provides
    a context manager for transactions.
    """
    db_path: str
    _connection: aiosqlite.Connection | None = None

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def connect(self) -> None:
        """Establishes a connection to the SQLite database and runs migrations if needed."""
        if self._connection is not None:
            return
        connection = await aiosqlite.connect(self.db_path)
        self._connection = connection
        try:
            connection.row_factory = aiosqlite.Row
            await connection.create_function("REVERSE", 1, reverse_string, True)
            for pragma in PRAGMAS:
                await connection.execute(f"PRAGMA {pragma};")
            await self._ensure_features()
            await self._run_migrations()
        except BaseException:
            self._connection = None
            try:
                await connection.close()
            except Exception:
                logger.exception("failed to close SQLite connection after connect error")
            raise

    async def close(self) -> None:
        """Closes the database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """Async context manager for SQLite transactions."""
        try:
            yield self.connection
            await self.connection.commit()
        except BaseException:
            await self.connection.rollback()
            raise

    async def execute(
        self, sql: str, parameters: dict | tuple | None = None
    ) -> aiosqlite.Cursor:
        return await self.connection.execute(sql, parameters)

    async def executemany(
        self, sql: str, param_seq: list[dict] | list[tuple]
    ) -> aiosqlite.Cursor:
        return await self.connection.executemany(sql, param_seq)

    async def _run_migrations(self) -> None:
        async with self.transaction() as conn:
            # Check current schema version
            try:
                cursor = await conn.execute(
                    """--sql
                    SELECT MAX(version) AS version FROM schema_version;
                    """
                )
                row = await cursor.fetchone()
                current_version = (
                    row["version"] if row and row["version"] is not None else 0
                )
            except aiosqlite.OperationalError:
                # schema_version table does not exist yet, i.e. initial setup
                current_version = 0

            # Get both SQL and Python migration files
            migration_files = sorted(
                f
                for f in MIGRATIONS_PATH.iterdir()
                if f.name.endswith((".sql", ".py")) and f.name.split("-")[0].isdigit()
            )

            for migration_file in migration_files:
                version = int(migration_file.stem.split("-")[0])
                if version <= current_version:
                    continue

                if migration_file.suffix == ".sql":
                    await self._run_sql_migration(conn, migration_file)
                elif migration_file.suffix == ".py":
                    await self._run_py_migration(conn, migration_file, version)

                await conn.execute(
                    """--sql
                    INSERT INTO schema_version (version, name)
                    VALUES (:version, :name);
                    """,
                    {"version": version, "name": migration_file.name},
                )
                logger.info(f"Applied migration {migration_file.name}")

    async def _ensure_features(self) -> None:
        """Ensure SQLite has required features (JSON1 + FTS5)."""
        conn = self.connection
        try:
            cursor = await conn.execute("SELECT json('[]') AS ok;")
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("SQLite JSON1 check failed")
        except (aiosqlite.OperationalError, sqlite3.OperationalError) as exc:
            raise RuntimeError(
                "SQLite JSON1 extension is required for the shares index."
            ) from exc

        try:
            await conn.execute(
                """--sql
                CREATE VIRTUAL TABLE IF NOT EXISTS __fts5_check
                USING fts5(content);
                """
            )
            await conn.execute("DROP TABLE IF EXISTS __fts5_check;")
        except (aiosqlite.OperationalError, sqlite3.OperationalError) as exc:
            raise RuntimeError(
                "SQLite FTS5 extension is required for the shares index."
            ) from exc

    async def _run_sql_migration(
        self, conn: aiosqlite.Connection, migration_file: Path
    ) -> None:
        """Execute a SQL migration file."""
        sql_script = migration_file.read_text()
        await conn.executescript(sql_script)

    async def _run_py_migration(
        self, conn: aiosqlite.Connection, migration_file: Path, version: int
    ) -> None:
        """Execute a Python migration file."""
        spec = importlib.util.spec_from_file_location(
            f"migration_{version}", migration_file
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load migration {migration_file.name}")

        migration_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration_module)

        if not self._py_migration_has_correct_api(migration_module):
            raise RuntimeError(
                f"Migration {migration_file.name} does not have a 'migrate' function. "
                "Migrations must define a 'async def migrate(conn: aiosqlite.Connection) -> None' function."
            )

        await migration_module.migrate(conn)

    @staticmethod
    def _py_migration_has_correct_api(module: ModuleType) -> bool:
        """Check if the migration module has the correct migrate function signature."""
        if not hasattr(module, "migrate"):
            return False
        migrate_func = getattr(module, "migrate")
        return callable(migrate_func) and asyncio.iscoroutinefunction(migrate_func)

    @property
    def connection(self) -> aiosqlite.Connection:
        """Get the active database connection."""
        if not self._connection:
            raise RuntimeError("Database connection is not established.")
        return self._connection
