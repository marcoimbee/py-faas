import pytest
from unittest.mock import MagicMock, patch
import zmq

from pyfaas.pyfaas import pyfaas_get_worker_ids, _CLIENT_MANAGER
from pyfaas.exceptions import (
    PyFaaSTimeoutError,
    PyFaaSWorkerIDsRetrievalError
)


def test_get_worker_ids_not_configured():
    _CLIENT_MANAGER.configured = False

    with pytest.raises(RuntimeError):
        pyfaas_get_worker_ids()

def test_get_worker_ids_success():
    _CLIENT_MANAGER.configured = True
    expected_ids = ["worker_A", "worker_B", "worker_C"]
    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_ids.return_value = {
        "status": "ok",
        "result": expected_ids,
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        worker_ids = pyfaas_get_worker_ids()

    assert worker_ids == expected_ids
    mock_client.pyfaas_get_worker_ids.assert_called_once()
    mock_logger.debug.assert_called_once_with(f'Currently connected workers: {expected_ids}')

def test_get_worker_ids_empty_list_success():
    _CLIENT_MANAGER.configured = True
    expected_ids = []
    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_ids.return_value = {
        "status": "ok",
        "result": expected_ids,
        "message": ""
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        worker_ids = pyfaas_get_worker_ids()

    assert worker_ids == expected_ids
    mock_client.pyfaas_get_worker_ids.assert_called_once()
    mock_logger.debug.assert_called_once_with(f'Currently connected workers: {expected_ids}')

def test_get_worker_ids_director_error():
    _CLIENT_MANAGER.configured = True
    error_message = "Director database access failed"
    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_ids.return_value = {
        "status": "error",
        "result": None,
        "message": error_message
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger") as mock_logger:
        with pytest.raises(PyFaaSWorkerIDsRetrievalError):
            pyfaas_get_worker_ids()

    mock_logger.warning.assert_called_once_with(f'Error while retrieving currently connected workers IDs: {error_message}')

def test_get_worker_ids_timeout():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    mock_client.pyfaas_get_worker_ids.side_effect = zmq.Again()
    _CLIENT_MANAGER.client = mock_client

    with pytest.raises(PyFaaSTimeoutError):
        pyfaas_get_worker_ids()

def test_get_worker_ids_no_action():
    _CLIENT_MANAGER.configured = True
    mock_client = MagicMock()
    unexpected_message = "Unexpected status received"
    mock_client.pyfaas_get_worker_ids.return_value = {
        "status": "pending",  # Not 'ok'
        "result": None,
        "message": unexpected_message
    }
    _CLIENT_MANAGER.client = mock_client

    with patch("pyfaas.pyfaas.logger"):
        with pytest.raises(PyFaaSWorkerIDsRetrievalError):
            pyfaas_get_worker_ids()
