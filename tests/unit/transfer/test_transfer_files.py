from unittest.mock import AsyncMock

import pytest

from aioslsk.events import EventBus
from aioslsk.settings import Settings
from aioslsk.shares.manager import SharesManager
from aioslsk.shares.model import SharedDirectory, SharedItem
from aioslsk.transfer.manager import TransferManager


def make_manager(tmp_path):
    settings = Settings(
        credentials={"username": "local", "password": "password"},
        shares={"download": str(tmp_path)},
    )
    events = EventBus()
    shares = SharesManager(settings, events, AsyncMock())
    return TransferManager(settings, events, AsyncMock(), shares, AsyncMock())


@pytest.mark.asyncio
async def test_queued_downloads_choose_paths_when_they_start(tmp_path):
    manager = make_manager(tmp_path)
    first = await manager.download("alice", "@@alice\\song.mp3", paused=True)
    second = await manager.download("bob", "@@bob\\song.mp3", paused=True)

    await manager._prepare_download_path(first)
    with open(first.local_path, "wb") as file:
        file.write(b"alice's completed download")
    await manager._prepare_download_path(second)

    assert second.local_path != first.local_path
    assert second.local_path.endswith("song (1).mp3")
    with open(first.local_path, "rb") as file:
        assert file.read() == b"alice's completed download"


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"1", b"1234567890"])
async def test_upload_size_is_read_from_current_file(tmp_path, content):
    manager = make_manager(tmp_path)
    path = tmp_path / "song.mp3"
    path.write_bytes(b"123")
    shared = SharedDirectory(str(tmp_path), str(tmp_path), "alias")
    item = SharedItem(shared, "", path.name, 0.0, 3)
    manager._shares_manager.get_shared_item = AsyncMock(return_value=item)
    path.write_bytes(content)

    upload = await manager._add_upload("stranger", item.get_remote_path())
    assert upload.filesize == len(content)
    assert upload.local_path == str(path)
