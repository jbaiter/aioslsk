import pytest

from aioslsk.database.sqlite import SQLiteDatabase


@pytest.mark.asyncio
async def test_sqlite_database_runs_migrations(tmp_path):
    db = SQLiteDatabase(str(tmp_path / "test_migrations.db"))
    await db.connect()

    cursor = await db.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    )
    table_names = {row[0] async for row in cursor}
    assert {
        "schema_version",
        "directories",
        "directory_users",
        "files",
        "prebaked_listings",
    }.issubset(table_names)

    cursor = await db.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='files_fts';"
    )
    assert await cursor.fetchone() is not None

    await db.close()


@pytest.mark.asyncio
async def test_sqlite_database_feature_check_failure(tmp_path, monkeypatch):
    db = SQLiteDatabase(str(tmp_path / "test_features.db"))
    original_ensure_features = SQLiteDatabase._ensure_features

    async def _raise(*_args, **_kwargs):
        raise RuntimeError("feature check failed")

    monkeypatch.setattr(SQLiteDatabase, "_ensure_features", _raise)

    with pytest.raises(RuntimeError, match="feature check failed"):
        await db.connect()

    assert db._connection is None

    # Verify a later retry can connect successfully.
    monkeypatch.setattr(SQLiteDatabase, "_ensure_features", original_ensure_features)
    await db.connect()
    assert db._connection is not None
    await db.close()
