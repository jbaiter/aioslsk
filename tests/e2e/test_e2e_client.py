from aioslsk.network.connection import ConnectionState
from .mock.server import MockServer
from .fixtures import create_client
from pathlib import Path
import pytest


class TestE2EClient:

    @pytest.mark.asyncio
    async def test_start_stop_explicit(self, mock_server: MockServer, tmp_path: Path, shares_backend: str):
        """Start stop without login"""
        use_sqlite = shares_backend == "sqlite"
        client = await create_client(tmp_path, 'client_test_0', 45000, use_sqlite=use_sqlite)

        try:
            await client.start()

            assert client.network.server_connection.state == ConnectionState.CONNECTED

        finally:
            await client.stop()

        assert client.network.server_connection.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_start_login_stop_explicit(self, mock_server: MockServer, tmp_path: Path, shares_backend: str):
        """Start stop without login"""
        use_sqlite = shares_backend == "sqlite"
        client = await create_client(tmp_path, 'client_test_1', 46000, use_sqlite=use_sqlite)

        try:
            await client.start()
            await client.login()

            assert client.network.server_connection.state == ConnectionState.CONNECTED
            assert client.session is not None

        finally:
            await client.stop()

        assert client.network.server_connection.state == ConnectionState.CLOSED
        assert client.session is None

    @pytest.mark.asyncio
    async def test_start_login_stop_contextManager(self, mock_server: MockServer, tmp_path: Path, shares_backend: str):
        use_sqlite = shares_backend == "sqlite"
        client = await create_client(tmp_path, 'client_test_2', 47000, use_sqlite=use_sqlite)

        async with client:
            await client.login()

            assert client.network.server_connection.state == ConnectionState.CONNECTED
            assert client.session is not None

        assert client.network.server_connection.state == ConnectionState.CLOSED
        assert client.session is None
