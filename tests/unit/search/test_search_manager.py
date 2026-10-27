from aioslsk.events import SearchResultEvent
from aioslsk.protocol.primitives import FileData
from aioslsk.protocol.messages import (
    UserSearch,
    FileSearch,
    RoomSearch,
    PeerSearchReply,
)
from aioslsk.search.manager import SearchManager
from aioslsk.search.model import SearchType, SearchRequest
from aioslsk.shares.model import SharedDirectory, SharedItem
from aioslsk.settings import Settings

import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call


DEFAULT_USER = 'testuser0'
DEFAULT_SETTINGS = {
    'credentials': {
        'username': DEFAULT_USER,
        'password': 'password0'
    }
}


@pytest.fixture
def manager() -> SearchManager:
    network = Mock()
    network.send_server_messages = AsyncMock()
    event_bus = Mock()
    event_bus.emit = AsyncMock()
    event_bus.register = Mock()
    shares_manager = Mock()
    transfer_manager = Mock()

    return SearchManager(
        Settings(**DEFAULT_SETTINGS),
        event_bus,
        shares_manager,
        transfer_manager,
        network
    )


class TestSearchManager:

    @pytest.mark.asyncio
    async def test_search(self, manager: SearchManager):
        request = await manager.search('query')

        assert request.search_type == SearchType.NETWORK
        assert request.ticket is not None
        assert request.query == 'query'
        assert request.username is None
        assert request.room is None

        assert request.ticket in manager.requests

        manager._network.send_server_messages.assert_awaited_once_with(
            FileSearch.Request(request.ticket, 'query')
        )

    @pytest.mark.asyncio
    async def test_searchUser(self, manager: SearchManager):
        request = await manager.search_user('user0', 'query')

        assert request.search_type == SearchType.USER
        assert request.ticket is not None
        assert request.query == 'query'
        assert request.username == 'user0'
        assert request.room is None

        assert request.ticket in manager.requests

        manager._network.send_server_messages.assert_awaited_once_with(
            UserSearch.Request('user0', request.ticket, 'query')
        )

    @pytest.mark.asyncio
    async def test_searchRoom(self, manager: SearchManager):
        request = await manager.search_room('room0', 'query')

        assert request.search_type == SearchType.ROOM
        assert request.ticket is not None
        assert request.query == 'query'
        assert request.username is None
        assert request.room == 'room0'

        assert request.ticket in manager.requests

        manager._network.send_server_messages.assert_awaited_once_with(
            RoomSearch.Request('room0', request.ticket, 'query')
        )

    @pytest.mark.asyncio
    async def test_onPeerSearchReply_shouldStoreResultsAndEmit(self, manager: SearchManager):
        TICKET = 1234
        connection = AsyncMock()

        manager.requests[TICKET] = SearchRequest(
            TICKET, 'search', SearchType.NETWORK)

        reply_message = PeerSearchReply.Request(
            'user0',
            TICKET,
            results=[
                FileData(1, 'z.mp3', 10000, 'mp3', attributes=[]),
                FileData(1, 'a.mp3', 10000, 'mp3', attributes=[]),
            ],
            has_slots_free=True,
            avg_speed=100,
            queue_size=2,
            locked_results=[FileData(1, 'locked.mp3', 10000, 'mp3', attributes=[])]
        )
        await manager._on_peer_search_reply(reply_message, connection)

        assert 1 == len(manager.requests[TICKET].results)
        stored = manager.requests[TICKET].results[0]
        assert [item.filename for item in stored.shared_items] == ['z.mp3', 'a.mp3']
        assert [item.filename for item in reply_message.results] == ['z.mp3', 'a.mp3']

        manager._event_bus.emit.assert_has_awaits(
            [
                call(
                    SearchResultEvent(
                        manager.requests[TICKET],
                        manager.requests[TICKET].results[0]
                    )
                )
            ]
        )

    @pytest.mark.asyncio
    async def test_querySharesAndReply_shouldUseFullRemotePaths(self, manager: SearchManager):
        # This is a search reply (flat list) so filenames must be full remote paths.
        manager._session = SimpleNamespace(user=SimpleNamespace(name=DEFAULT_USER))
        manager._network.send_peer_messages = AsyncMock()

        shared_directory = SharedDirectory(
            directory="music",
            absolute_path="/tmp/music",
            alias="abcde",
        )
        visible_item = SharedItem(shared_directory, "Album", "song.mp3", 0.0, 123)
        locked_item = SharedItem(shared_directory, "Album2", "locked.flac", 0.0, 456)
        manager._shares_manager.query = AsyncMock(return_value=([visible_item], [locked_item]))

        manager._upload_info_provider.has_slots_free = Mock(return_value=True)
        manager._upload_info_provider.get_average_upload_speed = Mock(return_value=100)
        manager._upload_info_provider.get_queue_size = Mock(return_value=2)

        await manager._query_shares_and_reply(ticket=1, username="peer0", query="song")
        tasks = list(manager._search_reply_tasks)
        assert tasks
        await asyncio.gather(*tasks)

        called_username, message = manager._network.send_peer_messages.call_args.args
        assert called_username == "peer0"
        assert isinstance(message, PeerSearchReply.Request)

        assert message.username == DEFAULT_USER
        assert message.ticket == 1
        assert [f.filename for f in message.results] == [visible_item.get_remote_path()]
        assert [f.filename for f in message.locked_results or []] == [locked_item.get_remote_path()]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("visible_count,locked_count", [(4, 4), (4, 0), (0, 4)])
    async def test_outgoing_reply_sorts_selected_full_paths_without_changing_selection(
        self, manager: SearchManager, visible_count, locked_count
    ):
        manager._session = SimpleNamespace(user=SimpleNamespace(name=DEFAULT_USER))
        manager._network.send_peer_messages = AsyncMock()
        first_root = SharedDirectory("first", "/tmp/first", "aaa")
        last_root = SharedDirectory("last", "/tmp/last", "zzz")
        specs = [(last_root, "A", "a"), (first_root, "a", "a"),
                 (first_root, "Z", "a"), (first_root, "A", "z")]
        visible = [SharedItem(root, subdir, name + ".mp3", 0.0, 123) for root, subdir, name in specs][:visible_count]
        locked = [SharedItem(root, subdir, name + ".flac", 0.0, 456) for root, subdir, name in specs][:locked_count]
        original_visible = list(visible)
        original_locked = list(locked)
        manager._shares_manager.query = AsyncMock(return_value=(visible, locked))
        manager._upload_info_provider.has_slots_free = Mock(return_value=True)
        manager._upload_info_provider.get_average_upload_speed = Mock(return_value=100)
        manager._upload_info_provider.get_queue_size = Mock(return_value=2)

        await manager._query_shares_and_reply(ticket=1, username="peer0", query="song")
        await asyncio.gather(*list(manager._search_reply_tasks))

        username, message = manager._network.send_peer_messages.call_args.args
        assert username == "peer0"
        assert message.ticket == 1
        # Nicotine+ sorts by full path with ordinary case-sensitive string order.
        expected = ["@@aaa\\A\\z", "@@aaa\\Z\\a", "@@aaa\\a\\a", "@@zzz\\A\\a"]
        assert [item.filename for item in message.results] == (
            [path + ".mp3" for path in expected] if visible_count else []
        )
        assert [item.filename for item in message.locked_results or []] == (
            [path + ".flac" for path in expected] if locked_count else []
        )
        assert all(item.filesize == 123 for item in message.results)
        assert all(item.filesize == 456 for item in message.locked_results or [])
        assert visible == original_visible
        assert locked == original_locked
