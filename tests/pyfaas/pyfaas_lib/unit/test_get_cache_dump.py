import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_get_cache_dump, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSCacheDumpingError
)


@pytest.fixture(autouse=True)
def reset_client_manager():
    """
    Ensures _CLIENT_MANAGER is reset before each test.
    """
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False
    yield
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False


def test_cache_dump_raises_if_not_configured():
    with pytest.raises(RuntimeError):
        pyfaas_get_cache_dump("worker-1")


def test_cache_dump_raises_if_worker_id_missing():
    _CLIENT_MANAGER.configured = True
    with pytest.raises(PyFaaSCacheDumpingError):
        pyfaas_get_cache_dump("")


def test_cache_dump_timeout():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_get_cache_dump.side_effect = zmq.Again
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_get_cache_dump("worker-1")


def test_cache_dump_success():
    _CLIENT_MANAGER.configured = True

    mock_response = {
        "status": "ok",
        "result": {"cache": {"a": 1, "b": 2}}
    }

    mock_client = MagicMock()
    mock_client.pyfaas_get_cache_dump.return_value = mock_response
    _CLIENT_MANAGER.client = mock_client

    result = pyfaas_get_cache_dump("worker-1")

    assert result == {"cache": {"a": 1, "b": 2}}
    mock_client.pyfaas_get_cache_dump.assert_called_once_with("worker-1")


def test_cache_dump_error_from_director():
    _CLIENT_MANAGER.configured = True

    mock_response = {
        "status": "error",
        "message": "Worker not found"
    }

    mock_client = MagicMock()
    mock_client.pyfaas_get_cache_dump.return_value = mock_response
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSCacheDumpingError) as exc:
        pyfaas_get_cache_dump("worker-1")

    assert "Worker not found" in str(exc.value)
    mock_client.pyfaas_get_cache_dump.assert_called_once_with("worker-1")
