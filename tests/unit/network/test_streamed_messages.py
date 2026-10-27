import asyncio
import random
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from aioslsk.database.sqlite import SQLiteDatabase
from aioslsk.events import EventBus
from aioslsk.network.connection import CloseReason, PeerConnection
from aioslsk.protocol.primitives import DirectoryData, FileData
from aioslsk.settings import Settings
from aioslsk.shares.manager import SharesManager
from aioslsk.shares.model import DirectoryShareMode


@pytest_asyncio.fixture(params=["memory", "sqlite"])
async def streaming_manager(request, tmp_path):
    settings = Settings(credentials={"username": "local", "password": "password"})
    db = None
    if request.param == "sqlite":
        db = SQLiteDatabase(str(tmp_path / "streaming.db"))
        await db.connect()
    try:
        manager = SharesManager(settings, EventBus(), AsyncMock(), database=db)
        rng = random.Random(42)
        files = [
            FileData(1, rng.randbytes(128).hex(), 10, "", [])
            for _ in range(2000)
        ]
        await manager.index.save_prebaked_listing(
            DirectoryShareMode.EVERYONE, [DirectoryData("@@alias", files)], []
        )
        yield manager
    finally:
        if db:
            await db.close()


def connection_with_writer():
    connection = PeerConnection("127.0.0.1", 1234, AsyncMock(), username="stranger")
    connection._writer = Mock(spec=asyncio.StreamWriter)
    connection._writer.drain = AsyncMock()
    connection.disconnect = AsyncMock()
    return connection


@pytest.mark.asyncio
async def test_streamed_listing_and_other_message_are_not_interleaved(streaming_manager):
    connection = connection_with_writer()
    entered = asyncio.Event()
    release = asyncio.Event()
    writes = []
    connection._writer.write.side_effect = writes.append

    async def drain():
        entered.set()
        await release.wait()

    connection._writer.drain = drain
    listing = await streaming_manager.index.get_prebaked_listing(DirectoryShareMode.EVERYONE)
    assert len(listing) > 131072
    streaming = asyncio.create_task(streaming_manager.send_shares_reply(connection))
    other = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        other = asyncio.create_task(connection.send_message(b"OTHER_MESSAGE"))
        ready, _ = await asyncio.wait([other], timeout=0.05)
        release.set()
        await asyncio.gather(streaming, other)
        assert not ready
        assert b"".join(writes) == listing + b"OTHER_MESSAGE"
    finally:
        release.set()
        await asyncio.gather(streaming, *([other] if other else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_listing_disconnects_instead_of_leaving_partial_frame(streaming_manager):
    connection = connection_with_writer()
    entered = asyncio.Event()

    async def drain():
        entered.set()
        await asyncio.Event().wait()

    connection._writer.drain = drain
    streaming = asyncio.create_task(streaming_manager.send_shares_reply(connection))
    await asyncio.wait_for(entered.wait(), 2)
    streaming.cancel()
    with pytest.raises(asyncio.CancelledError):
        await streaming
    connection.disconnect.assert_awaited_once_with(CloseReason.REQUESTED)


@pytest.mark.asyncio
async def test_listing_timeout_disconnects(streaming_manager):
    connection = connection_with_writer()
    connection._writer.drain.side_effect = asyncio.TimeoutError
    await streaming_manager.send_shares_reply(connection)
    connection.disconnect.assert_awaited_once_with(CloseReason.TIMEOUT)


@pytest.mark.asyncio
async def test_partial_listing_failure_does_not_send_a_second_frame(streaming_manager):
    connection = connection_with_writer()
    connection.send_message = AsyncMock()
    connection._writer.write.side_effect = [None, ValueError("failed after first chunk")]
    await streaming_manager.send_shares_reply(connection)
    connection.send_message.assert_not_awaited()
    connection.disconnect.assert_awaited_once_with(CloseReason.WRITE_ERROR)
