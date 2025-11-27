import pytest
from unittest.mock import MagicMock
import zmq

from pyfaas.pyfaas import pyfaas_get_stats, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSStatisticsRetrievalError
)


@pytest.fixture(autouse=True)
def reset_manager():
    """Reset global client manager before each test."""
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False
    yield
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False


def test_get_stats_not_configured():
    _CLIENT_MANAGER.configured = False
    with pytest.raises(RuntimeError):
        pyfaas_get_stats()


def test_get_stats_timeout():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_stats.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_get_stats()


def test_get_stats_success_general():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_stats.return_value = {
        "status": "ok",
        "result": {"total_calls": 10, "unique_functions": 3},
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    stats = pyfaas_get_stats()

    assert stats == {"total_calls": 10, "unique_functions": 3}
    mock_client.pyfaas_get_stats.assert_called_once_with(None)


def test_get_stats_success_specific():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_stats.return_value = {
        "status": "ok",
        "result": {"calls": 5, "avg_time_ms": 12.3},
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    stats = pyfaas_get_stats("my_func")

    assert stats == {"calls": 5, "avg_time_ms": 12.3}
    mock_client.pyfaas_get_stats.assert_called_once_with("my_func")


def test_get_stats_error_general():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_stats.return_value = {
        "status": "err",
        "result": None,
        "message": "General stats error"
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSStatisticsRetrievalError):
        pyfaas_get_stats()


def test_get_stats_error_specific():
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_stats.return_value = {
        "status": "err",
        "result": None,
        "message": "Function not found"
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSStatisticsRetrievalError):
        pyfaas_get_stats("missing_func")
