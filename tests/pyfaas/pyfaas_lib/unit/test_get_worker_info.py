import pytest
from unittest.mock import MagicMock
import zmq

from pyfaas.pyfaas import pyfaas_get_worker_info, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSWorkerInfoError,
)


@pytest.fixture(autouse=True)
def reset_manager():
    """Ensure global state is reset before each test."""
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False
    yield
    _CLIENT_MANAGER.client = None
    _CLIENT_MANAGER.configured = False


def test_worker_info_not_configured():
    """Should raise if PyFaaS is not configured."""
    with pytest.raises(RuntimeError):
        pyfaas_get_worker_info("worker-123")


def test_worker_info_missing_id():
    """Missing worker_id should raise PyFaaSWorkerInfoError."""
    _CLIENT_MANAGER.configured = True
    with pytest.raises(PyFaaSWorkerInfoError):
        pyfaas_get_worker_info("")


def test_worker_info_timeout():
    """Timeout (zmq.Again) should raise PyFaaSTimeoutError."""
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_info.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_get_worker_info("worker-123")


def test_worker_info_success():
    """Successful response should return the result dict."""
    _CLIENT_MANAGER.configured = True

    fake_result = {
        "id": "worker-123",
        "status": "running",
        "functions": 12,
    }

    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_info.return_value = {
        "status": "ok",
        "result": fake_result,
        "message": "",
    }
    _CLIENT_MANAGER.client = mock_client

    res = pyfaas_get_worker_info("worker-123")

    assert res == fake_result
    mock_client.pyfaas_get_worker_info.assert_called_once_with("worker-123")


def test_worker_info_director_error():
    """Director returns error → raise PyFaaSWorkerInfoError."""
    _CLIENT_MANAGER.configured = True

    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_info.return_value = {
        "status": "err",
        "result": None,
        "message": "Worker not found",
    }
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSWorkerInfoError):
        pyfaas_get_worker_info("worker-123")
