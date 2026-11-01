import pytest
from helpers import start_server


@pytest.fixture(scope="session")
def target():
    server, address = start_server()
    yield address
    server.stop(None)
