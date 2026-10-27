import pytest
from unittest.mock import AsyncMock

from aioslsk.client import SoulSeekClient
from aioslsk.database.sqlite import SQLiteDatabase
from aioslsk.settings import Settings


@pytest.mark.asyncio
async def test_client_stop_closes_database(tmp_path):
    db = SQLiteDatabase(str(tmp_path / "client.db"))
    await db.connect()

    settings = Settings(credentials={"username": "user0", "password": "pass0"})
    client = SoulSeekClient(settings, database=db)

    client.network.disconnect = AsyncMock()
    for svc in client.services:
        svc.stop = AsyncMock(return_value=[])
        svc.store_data = AsyncMock()

    await client.stop()

    client.network.disconnect.assert_awaited_once()
    for svc in client.services:
        svc.stop.assert_awaited_once()
        svc.store_data.assert_awaited_once()
    assert db._connection is None
