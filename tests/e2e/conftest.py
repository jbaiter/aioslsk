# Re-export fixtures from fixtures.py so pytest can discover them
from .fixtures import (
    mock_server,
    client_1,
    client_2,
    clients,
    shares_backend,
)

# Make shares_backend available to all tests
__all__ = ['mock_server', 'client_1', 'client_2', 'clients', 'shares_backend']
