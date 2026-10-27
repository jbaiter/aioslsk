import asyncio

import pytest

from aioslsk.database.sqlite import SQLiteDatabase


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_concurrent_transactions_do_not_commit_failed_writes(tmp_path, cancel):
    db = SQLiteDatabase(str(tmp_path / "transactions.db"))
    await db.connect()
    try:
        async with db.transaction() as conn:
            await conn.execute("CREATE TABLE probe (value TEXT)")

        inserted = asyncio.Event()
        release = asyncio.Event()
        attempted = asyncio.Event()

        async def failing_writer():
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO probe VALUES ('rollback')")
                inserted.set()
                await release.wait()
                raise ValueError("rollback")

        async def successful_writer():
            attempted.set()
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO probe VALUES ('keep')")

        first = asyncio.create_task(failing_writer())
        await inserted.wait()
        second = asyncio.create_task(successful_writer())
        await attempted.wait()
        # Give the second writer a chance to enter the connection. It must
        # remain outside the first writer's transaction until it rolls back.
        await asyncio.wait([second], timeout=0.05)
        if cancel:
            first.cancel()
        else:
            release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError if cancel else ValueError)
        assert results[1] is None
        rows = await (await db.execute("SELECT value FROM probe")).fetchall()
        assert [row[0] for row in rows] == ["keep"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_read_waits_for_permission_transaction_to_finish(tmp_path):
    db = SQLiteDatabase(str(tmp_path / "readers.db"))
    await db.connect()
    release = asyncio.Event()
    try:
        async with db.transaction() as conn:
            await conn.execute("CREATE TABLE probe (value TEXT)")
            await conn.execute("INSERT INTO probe VALUES ('old')")
        inserted = asyncio.Event()

        async def writer():
            async with db.transaction() as conn:
                await conn.execute("UPDATE probe SET value = 'new'")
                inserted.set()
                await release.wait()
                raise ValueError("rollback")

        first = asyncio.create_task(writer())
        await inserted.wait()
        read = asyncio.create_task(db.execute("SELECT value FROM probe"))
        ready, _ = await asyncio.wait([read], timeout=0.05)
        release.set()
        await asyncio.gather(first, return_exceptions=True)
        cursor = await read
        assert not ready
        assert (await cursor.fetchone())[0] == "old"
    finally:
        release.set()
        await db.close()
